################################################################
####                       Send SCOPE AS                    ####
####                  Author: Stefanie Molin                ####
####                                                        ####
## Sends alerts to AS when performance has changed for spend, ##
## COS, CTR, CR, or RexT and the change is considered         ##
## statistically significant.                                 ##
################################################################

# Libraries
msg.out <- capture.output(suppressMessages(require(scopeR)))
msg.out <- capture.output(suppressMessages(require(futile.logger)))
msg.out <- capture.output(suppressMessages(require(yaml)))
msg.out <- capture.output(suppressMessages(require(dplyr))) #all (might be just AS and TS)

# Logging configuration
script_name <- "alerting_web_as.r"
layout <- layout.format(paste('[~l] [', script_name, '] ~m'))
l <- flog.layout(layout)
logThreshold <- flog.threshold("INFO")

scope <- function(){
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
    
    # scriptRunDateTime <- format_datetime(format_string = "%a %b %d %Y %H:%M", timezone = "America/New_York", showTimezone = TRUE)
    # timezone = "Etc/UTC" for consistency

    alert_sender_email  <- decrypt(filename = root_config[["cred"]][["mailbox"]])[[1]]
    mailbox <- decrypt(filename = root_config[["cred"]][["mailbox"]])[[2]]
    username  <- decrypt(filename = root_config[["cred"]][["main"]])[[1]]
    password <- decrypt(filename = root_config[["cred"]][["main"]])[[2]]
    
    flog.warn(paste("DEPLOYMENT_ENVIRONMENT is set to: ", Sys.getenv('DEPLOYMENT_ENVIRONMENT')))
    testMode <- !identical(Sys.getenv('DEPLOYMENT_ENVIRONMENT'), "production")
    if(testMode){
        flog.warn("Running in test mode")
    }
    
    # Toggle slack
    slackOn <- "channel" %in% names(regional_config[["web_as"]])
    if(slackOn){
      prep_slack(root_config)
    }
    
    # Check if script has run already
    history_file <- root_config[["run_history"]][["history_file"]]
    already_ran <- check_history(history_file, script_name, regional_config[["web_as"]][["geo"]])
    if(already_ran){
      flog.info("Exiting: Script already ran successfully today")
      return()
    }
    
    # Get dates using proper timezone
    today <- as.Date(format_datetime(timezone = regional_config[["web_as"]][["timezone"]]))
    yesterday <- today - 1
    
    # pull max dates
    check_dates <- get_max_dates(username = username, password = password, scope_product = "AS", last_try = last_try, 
                                 test_mode = testMode, root_config_list = root_config, regional_config_list = regional_config,
                                 sender = alert_sender_email, mailbox_password = mailbox, slack_on = slackOn)
    
    flog.info("Checking if we have yesterday's data...")
    # check the max dates of the tables used and stop if not updated
    if(data_late(data = check_dates, table_name = gsub("'", "", unlist(strsplit(regional_config[["web_as"]][["query"]][["tableList"]], "', '"))))){
      late_data_notifications(last_try = last_try, scope_product = "AS", root_config_list = root_config, regional_config_list = regional_config, 
                              test_mode = testMode, sender = alert_sender_email, mailbox_password = mailbox, slack_on = slackOn)
      return()
    }
      
    # collect currency symbols
    # symbols <- read.csv("data/world_currency_symbols.csv", stringsAsFactors = FALSE)[, c("currency_code", "symbol")]
    # 
    # for(i in 1:length(symbols$symbol)){
    #   if(length(grep("\\?", symbols[i, 2])) != 0){
    #     symbols[i, 2] <- ""
    #   }
    # }

    # pull AS data
    all_stats <- pull_AS_data(username = username, password = password, last_try = last_try, test_mode = testMode, 
                              root_config_list = root_config, regional_config_list = regional_config, 
                              sender = alert_sender_email, mailbox_password = mailbox, slack_on = slackOn)
    
    # detect anomalies across selected KPI (if query is modified need to modify the function also)
    data <- anomaly_pipeline_AS(data = all_stats, yesterday = yesterday, 
                                alert_kpis =  c("rext", "spend", "cos", "cr", "ctr"))
    
    # separate the cleaned data and the alerts for later
    cleaned_data <- data$cleaned_data
    alerts <- data$results %>% dplyr::filter(is_alert)
    
    # isolate the client alerts to send out as emails
    client_alerts <- alerts %>% dplyr::filter(campaign_name == "NOT A CAMPAIGN")
    client_stats <- cleaned_data %>% dplyr::filter(campaign_name == "NOT A CAMPAIGN")
    
    # add currency symbols (left outer join) -- use currency codes from Vertica instead then we don't need this at all
    # alerts <- merge(alerts, symbols, by = "currency_code", all.x = TRUE)
  
    # TO DO
    # if(nrow(alerts) > 0){
    #   # write alerts to Pinpoint
    #   # this should be done with the original data, not cleaned_data
    # }
    
    if(nrow(client_alerts) == 0){
      # if no client alerts are found end script
      send_no_alerts_found_email(scope_product = "AS", test_mode = testMode, 
                                 root_config_list = root_config, regional_config_list = regional_config,
                                 sender = alert_sender_email, mailbox_password = mailbox)
      if(slackOn){
        slack_no_alerts_found(test_mode = testMode, root_config_list = root_config, regional_config_list = regional_config, scope_product = "AS")
      }
      
      log_to_history(history_file, script_name, regional_config[["web_as"]][["geo"]])
      
      flog.info("[EXPECTED ERROR] No SCOPE alerts.")
      return()
    } else{
      # find alerts for pod leaders, if specified
      if("pod_leaders" %in% names(regional_config[["web_as"]])){
        # filter alerts to the pod leaders as managers
        pod_alerts <- client_alerts %>% dplyr::filter(manager_name %in% toupper(regional_config[["web_as"]][["pod_leaders"]]) |
                                                        AS_name %in% toupper(regional_config[["web_as"]][["pod_leaders"]]))
        pod_stats <- client_stats %>% dplyr::filter(manager_name %in% toupper(regional_config[["web_as"]][["pod_leaders"]]) |
                                                      AS_name %in% toupper(regional_config[["web_as"]][["pod_leaders"]]))
        
        # use the manager as the AS and loop through again
        pod_alerts$AS_email <- ifelse(pod_alerts$manager_name %in% toupper(regional_config[["web_as"]][["pod_leaders"]]),
                                      pod_alerts$manager_email, pod_alerts$AS_email)
        pod_alerts$AS_name <- ifelse(pod_alerts$manager_name %in% toupper(regional_config[["web_as"]][["pod_leaders"]]), 
                                     pod_alerts$manager_name, pod_alerts$AS_name)
        pod_stats$AS_email <- ifelse(pod_stats$manager_name %in% toupper(regional_config[["web_as"]][["pod_leaders"]]), 
                                     pod_stats$manager_email, pod_stats$AS_email)
        pod_stats$AS_name <- ifelse(pod_stats$manager_name %in% toupper(regional_config[["web_as"]][["pod_leaders"]]), 
                                    pod_stats$manager_name, pod_stats$AS_name)
        
        # combine the results removing duplicates (accounts where the pod leader is the true AS)
        if(nrow(pod_alerts) > 0){
          # if we have no pod alerts, this is not necessary
          client_stats <- dplyr::union(client_stats, pod_stats)
          client_alerts <- dplyr::union(client_alerts, pod_alerts)
        }
      }
      
      # find client RexT in euros for ranking alert importance
      rext_data <- client_stats %>% 
        dplyr::filter(combo %in% unique(client_alerts$combo)) %>% 
        dplyr::group_by(combo) %>% 
        dplyr::summarize(avg_rext_euro = mean(rext_euro))
      
      # sort client and metric alerts for the emails and deduplicate for global_account_name
      client_alerts <- client_alerts %>% 
        dplyr::left_join(rext_data, by = "combo") %>% 
        dplyr::arrange(desc(avg_rext_euro), factor(kpi, levels = c("rext", "spend", "cos", "cr", "ctr")))
      
      # build and send alerts
      build_and_send_AS_alerts(client_stats = client_stats, client_alerts = client_alerts, root_config_list = root_config, 
                               regional_config_list = regional_config, run_date = today, test_mode = testMode, 
                               sender = alert_sender_email, mailbox_password = mailbox)
      
      # send out summary email
      send_as_summary_stats(data = client_alerts %>% dplyr::filter(AS_name != manager_name), test_mode = testMode, 
                            root_config_list = root_config, regional_config_list = regional_config, 
                            sender = alert_sender_email, mailbox_password = mailbox, slack_on = slackOn)
      
      # END SCRIPT -- ALERTS HAVE BEEN SENT
      flog.info("SCOPE has finished. Cleaning up...")
      log_to_history(history_file, script_name, regional_config[["web_as"]][["geo"]])
    }
}

s <- scope()

rm(list = ls())
