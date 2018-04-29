#####################################################
####               Send SCOPE for TS             ####
####            Author: Stefanie Molin           ####
####                                             ####
##  Sends alerts to TS when there is a             ##
##  statistically significant drop in site events. ##
#####################################################

# increase the java heap size for large query result sets
options(java.parameters = "-Xmx1024m")

# Libraries
msg.out <- capture.output(suppressMessages(require(scopeR)))
msg.out <- capture.output(suppressMessages(require(futile.logger)))
msg.out <- capture.output(suppressMessages(require(yaml)))
msg.out <- capture.output(suppressMessages(require(dplyr))) #all (might be just AS and TS)

# Logging configuration
script_name <- "ts_detect.r"
layout <- layout.format(paste('[~l] [', script_name, '] ~m'))
l <- flog.layout(layout)
logThreshold <- flog.threshold("INFO")

scopeTS <- function(){
  
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
  
  flog.warn(paste("DEPLOYMENT_ENVIRONMENT is set to: ", Sys.getenv('DEPLOYMENT_ENVIRONMENT')))
  testMode <- !identical(Sys.getenv('DEPLOYMENT_ENVIRONMENT'), "production")
  if(testMode){
    scope.maintainers <- root_config[["test_mode"]][["email"]]
  } else{
    scope.maintainers <- root_config[["maintainers"]]
  }
  
  # Toggle slack
  slackOn <- "channel" %in% names(regional_config[["ts"]])
  if(slackOn){
    prep_slack(root_config)
  }
  
  # set credentials
  alert_sender_email  <- decrypt(filename = root_config[["cred"]][["mailbox"]])[[1]]
  mailbox <- decrypt(filename = root_config[["cred"]][["mailbox"]])[[2]]
  username  <- decrypt(filename = root_config[["cred"]][["main"]])[[1]]
  password <- decrypt(filename = root_config[["cred"]][["main"]])[[2]]
  
  # Check if script has run already
  history_file <- root_config[['run_history']][["history_file"]]
  already_ran <- check_history(history_file, script_name, regional_config[["ts"]][["geo"]])
  if(already_ran){
    flog.info("Exiting: Script already ran successfully today")
    return()
  }
  
  # get dates
  today <- as.Date(format_datetime(timezone = regional_config[["ts"]][["timezone"]]))
  yesterday <- today - 1
  
  ################################################################################################
  ### START Settings & Error Checks ##############################################################
  ################################################################################################
  
  # pull max dates
  check_dates <- get_max_dates(username = username, password = password, scope_product = "TS", last_try = last_try, 
                               test_mode = testMode, root_config_list = root_config, regional_config_list = regional_config,
                               sender = alert_sender_email, mailbox_password = mailbox)
  
  #status update
  flog.info("Checking if we have yesterday's data...")
  
  #check the max dates of the tables used and stop if not updated
  if(data_late(data = check_dates, table_name = gsub("'", "", unlist(strsplit(regional_config[["ts"]][["query"]][["tableList"]], "', '"))))){
    late_data_notifications(last_try = last_try, scope_product = "TS", root_config_list = root_config, regional_config_list = regional_config, 
                            test_mode = testMode, sender = alert_sender_email, mailbox_password = mailbox, slack_on = slackOn)
    quit(save = "no", status = 10, runLast = FALSE)
  }
  
  # pull TS data
  test_for_anomalies <- pull_TS_data(username = username, password = password, last_try = last_try, test_mode = testMode,
                                     root_config_list = root_config, regional_config_list = regional_config,
                                     sender = alert_sender_email, mailbox_password = mailbox)
  
  # detect anomalies
  data <- anomaly_pipeline_TS(data = test_for_anomalies, yesterday = yesterday)
  
  # separate the cleaned data and the alerts for later
  cleaned_data <- data$cleaned_data
  alerts <- data$results %>% dplyr::filter(is_alert) %>% 
    dplyr::select(-global_account_name) %>% 
    dplyr::distinct()
  
  # isolate the client alerts to send out as emails
  site_events_alerts <- alerts %>% dplyr::filter(site_type == "SITE LEVEL" & event_name == "SITE LEVEL")
  site_events_stats <- cleaned_data %>% dplyr::filter(site_type == "SITE LEVEL" & event_name == "SITE LEVEL")
  tag_events_alerts <- alerts %>% dplyr::filter(site_type != "SITE LEVEL" & event_name != "SITE LEVEL")
  tag_events_stats <- cleaned_data %>% dplyr::filter(site_type != "SITE LEVEL" & event_name != "SITE LEVEL")
  
  # handle in case of no alerts
  if(nrow(tag_events_alerts) == 0){
    send_no_alerts_found_email(scope_product = "TS", test_mode = testMode, 
                               root_config_list = root_config, regional_config_list = regional_config,
                               sender = alert_sender_email, mailbox_password = mailbox)
    
    if(slackOn){
      slack_no_alerts_found(test_mode = testMode, root_config_list = root_config, regional_config_list = regional_config, scope_product = "TS")
    }
    
    flog.info("[EXPECTED ERROR] No SCOPE TS alerts.")
    # Write empty YAML so that Python will not send out alerts for the last run incorrectly
    write(x = NULL, file = regional_config[["results_file"]])
    return()
  } else{
    # create graphs saved as png, store names in lists
    flog.info("Generating graphs...")
    results_yaml <- ts_yaml_builder(tag_events_alerts, tag_events_stats)
    
    # send summary stats
    send_ts_summary_stats(eventSiteAlertsDF = tag_events_alerts, 
                          test_mode = testMode, root_config_list = root_config, regional_config_list = regional_config, 
                          sender = alert_sender_email, mailbox_password = mailbox, slack_on = slackOn)
    
    # write YAML
    flog.info("Writing to YAML...")
    write(x = as.yaml(results_yaml, unicode = FALSE), file = regional_config[["results_file"]])
    return(TRUE)
  }
  
}

s <- scopeTS()

rm(list = ls())
