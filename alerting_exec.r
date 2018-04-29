#####################################################
####                  SCOPE Exec                 ####
####            Author: Stefanie Molin           ####
####                                             ####
## Sends alerts when region trends have anomalies. ##
#####################################################

# Libraries
msg.out <- capture.output(suppressMessages(require(scopeR)))
msg.out <- capture.output(suppressMessages(require(futile.logger)))
msg.out <- capture.output(suppressMessages(require(yaml)))
msg.out <- capture.output(suppressMessages(require(dplyr)))

# Logging configuration
script_name <- "alerting_exec.r"
layout <- layout.format(paste('[~l] [', script_name, '] ~m'))
l <- flog.layout(layout)

scopeExec <- function(){
  
  # Read command line arguments
  args <- commandArgs(trailingOnly = TRUE)
  
  # Set constants
  root_config <- yaml.load_file("config.yml")
  regional_config <- yaml.load_file(args[1])
  
  if(length(args) > 0 && args[2] == "last_try=True"){
    flog.info("Last try: Will notify if late data")
    last_try <- TRUE
  } else{
    last_try <- FALSE
  }
  
  # Set credentials
  alert_sender_email  <- decrypt(filename = root_config[["cred"]][["mailbox"]])[[1]]
  mailbox <- decrypt(filename = root_config[["cred"]][["mailbox"]])[[2]]
  username  <- decrypt(filename = root_config[["cred"]][["main"]])[[1]]
  password <- decrypt(filename = root_config[["cred"]][["main"]])[[2]]
  
  # Check if test mode
  flog.warn(paste("DEPLOYMENT_ENVIRONMENT is set to: ", Sys.getenv('DEPLOYMENT_ENVIRONMENT')))
  testMode <- !identical(Sys.getenv('DEPLOYMENT_ENVIRONMENT'), "production")
  if(testMode){
    flog.warn("Running in test mode")
  }
  
  # Check if script has run already
  history_file <- root_config[["run_history"]][["history_file"]]
  already_ran <- check_history(history_file, script_name, regional_config[["exec"]][["geo"]])
  if(already_ran){
    flog.info("Exiting: Script already ran successfully today")
    return()
  }
  
  #get dates
  today <- as.Date(format_datetime(timezone = regional_config[["exec"]][["timezone"]]))
  yesterday <- today - 1
  dayBeforeYesterday <- yesterday - 1
  
  # pull max dates
  check_dates <- get_max_dates(username = username, password = password, scope_product = "EXEC", last_try = last_try, 
                               test_mode = testMode, root_config_list = root_config, regional_config_list = regional_config,
                               sender = alert_sender_email, mailbox_password = mailbox)
  
  #status update
  flog.info("Checking if we have yesterday's data...")
  
  #check the max dates of the tables used and stop if not updated
  if(data_late(data = check_dates, table_name = gsub("'", "", unlist(strsplit(regional_config[["exec"]][["query"]][["tableList"]], "', '"))))){
    late_data_notifications(last_try = last_try, scope_product = "EXEC", root_config_list = root_config, 
                            regional_config_list = regional_config, test_mode = testMode, 
                            sender = alert_sender_email, mailbox_password = mailbox)
    return()
  }
  
  
  ################################################################################################
  ####   START Anomaly Detection     #############################################################
  ################################################################################################
  
  #status update
  flog.info("Querying Vertica for dataframe americasRexTdf ...")
  
  # pull account data
  americasRexTdf <- pull_Exec_data(territory_level = TRUE, username = username, password = password, last_try = last_try, 
                                   test_mode = testMode, root_config_list = root_config, regional_config_list = regional_config,
                                   sender = alert_sender_email, mailbox_password = mailbox)
  
  # run through pipeline
  territory_data <- anomaly_pipeline_territory_rext(americasRexTdf, yesterday = yesterday)
  
  anomaliesAlerts <- territory_data$results %>% 
    dplyr::filter(is_alert) %>% 
    dplyr::select(series) %>% 
    unlist() %>% 
    paste(collapse = ", ")
  
  if(anomaliesAlerts == ""){
    anomaliesAlerts <- "NONE"
  }
  
  #status update
  flog.info("Formatting for email...")
  
  expected_values <- territory_data$cleaned_data %>% 
    dplyr::filter(day < yesterday & day >= yesterday - 7) %>% 
    dplyr::group_by(series) %>% 
    dplyr::summarize(avg_last_7D = mean(RexT))
  actuals <- territory_data$cleaned_data %>% 
    dplyr::filter(day == yesterday) %>% 
    dplyr::select(series, yesterday_RexT = RexT)
  email_data <- expected_values %>% 
    dplyr::inner_join(actuals, by = "series") %>% 
    dplyr::mutate(differences = percent_difference(avg_last_7D, yesterday_RexT, showPlusSign = TRUE)) %>% 
    dplyr::mutate(yesterday_RexT = format_currency(yesterday_RexT), avg_last_7D = format_currency(avg_last_7D))
  
  # allow more digits to be printed
  options(digits = 12)

  ################################################################################################
  ####   MAKE PLOTS   ############################################################################
  ################################################################################################
  options(scipen = 10000)
  
  #status update
  flog.info("Generating plots...")
  
  # get data for graphs
  graph_data <- territory_data$cleaned_data %>% 
    dplyr::filter(day > yesterday - 15) %>% 
    dplyr::mutate(RexT = round(RexT, digits = 2), plot_labels = format_currency(RexT))
  americasPlotDF <- graph_data %>% 
    dplyr::filter(series %in% c("AMERICAS", "NORTH AMERICA", "LATAM"))
  naPlotDF <- graph_data %>% 
    dplyr::filter(series %in% c("US", "CANADA"))
  latamPlotDF <- graph_data %>% 
    dplyr::filter(series %in% c("ARGENTINA", "BRAZIL", "CHILE", "COLOMBIA", "MEXICO", "SAM OTHER"))
  
  # plots
  americasPlot <- exec_graph(df = americasPlotDF, 
                             nudge_list = list(series = "LATAM", isSeriesNudge = 20000, isNotSeriesNudge = -5000),
                             label_date_range = c(yesterday, dayBeforeYesterday))
  
  naPlot <- exec_graph(df = naPlotDF, 
                       nudge_list = list(series = "CANADA", isSeriesNudge = 20000, isNotSeriesNudge = -5000),
                       label_date_range = c(yesterday, dayBeforeYesterday))
  
  latamPlot <- exec_graph(df = latamPlotDF, 
                          nudge_list = list(series = "BRAZIL", isSeriesNudge = -5000, isNotSeriesNudge = 5000),
                          label_date_range = c(yesterday, dayBeforeYesterday))
  
  ################################################################################################
  #### GET ACCOUNT DATA ##########################################################################
  ################################################################################################
  
  #status update
  flog.info("Querying Vertica for dataframe accountsRexTdf ...")
  
  # pull account data
  accountsRexTdf <- pull_Exec_data(territory_level = FALSE, username = username, password = password, last_try = last_try, 
                                   test_mode = testMode, root_config_list = root_config, regional_config_list = regional_config,
                                   sender = alert_sender_email, mailbox_password = mailbox)
  
  #status update
  flog.info("Checking for account-level RexT alerts...")
  
  if(nrow(accountsRexTdf) == 0){
    # data is late so we can't get the clients that were live in the last 25 days
    naAccounts <- exec_late_data_account_list()
    latamAccounts <- exec_late_data_account_list()
  } else{
    # clean data
    accountsRexTdf <- clean_data(accountsRexTdf)
    accountsRexTdf$day <- as.Date(accountsRexTdf$day)
    
    #initialize alerts df
    clientRegion <- unique(accountsRexTdf[,c("client_name", "region")])
    accountAlertsDF <- data.frame(client = clientRegion$client_name, region =  clientRegion$region, actual = 0, 
                                  expectedValue = 0, percentDiff = 0, alert = FALSE, notValid = FALSE, stringsAsFactors = FALSE)
    
    for(i in 1:nrow(accountAlertsDF)){
      #subset data to just that advertiser
      data <- accountsRexTdf[accountsRexTdf$client_name == accountAlertsDF[i, c("client")],]
      
      #check that the client has a value for yesterday
      if(is.na(data[data$day == max(data$day), c("RexT_USD_Constant")])){
        accountAlertsDF[i, c("notValid")] <- TRUE
      } else{
        # replace NA that didn't happen yesterday with zero
        data[is.na(data)] <- 0
        # check for anomalies
        accountAlertsDF[i, c("alert")] <- detect_anomaly(data[,c("day", "RexT_USD_Constant")], "exec", yesterday)
          
        # collect expected values as average last 7 days not including yesterday
        accountAlertsDF[i, c("expectedValue")] <- mean(data[(data$day >= Sys.Date() - 8 & data$day < yesterday), c("RexT_USD_Constant")])
        
        # collect actual values for yesterday
        accountAlertsDF[i, c("actual")] <- data[data$day == max(data$day), c("RexT_USD_Constant")]
      }
    }
    
    # remove accounts that can't trigger alerts bc they have NA actual on yesterday
    accountAlertsDF <- accountAlertsDF[accountAlertsDF$notValid == FALSE,]
    
    # remove accounts that didn't trigger alerts
    accountAlertsDF <- accountAlertsDF[accountAlertsDF$alert == TRUE,]
    
    # remove any alerts where the expectedValue is negative
    accountAlertsDF <- accountAlertsDF[accountAlertsDF$expectedValue > 0, ]
    
    # calculate absolute value difference between actual and expected to show most relevant alerts
    accountAlertsDF$magnitude <- abs(as.numeric(accountAlertsDF$actual) - as.numeric(accountAlertsDF$expectedValue))
    
    # calculate percent differences but don't format
    accountAlertsDF$percentDiff <- percent_difference(accountAlertsDF$expectedValue, accountAlertsDF$actual, format = FALSE)
    
    # format expected and actual values rounded to 2 decimals and with $
    accountAlertsDF$actual <- format_currency(accountAlertsDF$actual)
    accountAlertsDF$expectedValue <- format_currency(accountAlertsDF$expectedValue)
    
    # identify gainers and losers
    latamAccounts <- exec_find_gainers_losers(accountAlertsDF[accountAlertsDF$region == "LATAM",])
    naAccounts <- exec_find_gainers_losers(accountAlertsDF[accountAlertsDF$region == "NORTH AMERICA",])
  }
  ################################################################################################
  #### SEND ALERT EMAILS #########################################################################
  ################################################################################################
  
  build_and_send_exec(alerts = anomaliesAlerts, email_data = email_data, accounts_list = list(na = naAccounts, latam = latamAccounts),
                      plot_list = list(americas = list(file_name = "AmericasRexT.png", plot = americasPlot),
                                       na = list(file_name = "NorthAmericaRexT.png", plot = naPlot),
                                       latam = list(file_name = "LATAMRexT.png", plot = latamPlot)),
                      highlight_color = regional_config[["email"]][["highlight_color"]], 
                      regular_color = regional_config[["email"]][["regular_color"]],
                      regional_config_list = regional_config, root_config_list = root_config, template = "templates/scope_exec_v2.html",
                      test_mode = testMode, sender = alert_sender_email, mailbox_password = mailbox)

  ################################################################################################
  #####END SCRIPT -- ALERTS HAVE BEEN SENT########################################################
  ################################################################################################
  log_to_history(history_file, script_name, regional_config[["exec"]][["geo"]])
}

s <- scopeExec()

rm(list = ls())
