###################################################
######            Slack Library              ######
######          by: Stefanie Molin           ######
####                                          #####
## Utility functions for using slack with SCOPE. ##
###################################################

#' @title Prep for Slack Notifications
#' @description Sets up the slack bot
#' @author Stefanie Molin
#'
#' @importFrom slackr slackrSetup
#'
#' @param root_config_list List from the root_config file.
#'
#' @export
prep_slack <- function(root_config_list){
  suppressWarnings(slackr::slackrSetup(channel = "@scope", username = root_config_list[["slackbot"]][["username"]],
                                       incoming_webhook_url = root_config_list[["slackbot"]][["incoming_webhook_url"]],
                                       api_token = root_config_list[["slackbot"]][["api_token"]]))
}

#' @title Send Alert to Slack for Late Data
#' @description Posts to slack channel(s) saying data is late
#' @author Stefanie Molin
#'
#' @importFrom slackr slackrMsg
#'
#' @param root_config_list List from the root_config file.
#' @param regional_config_list List from the regional_config file.
#' @param test_mode Boolean whether or not SCOPE is running in test mode or not
#' @param scope_product Which SCOPE product this is for i.e. AS, TS, etc.
#'
slack_data_is_late_alert <- function(test_mode, root_config_list, regional_config_list, scope_product){
  # determine which alerts
  scope_product <- toupper(scope_product)
  
  if(scope_product == "AS"){
    config_variable <- "web_as"
    alert_category <- "business"
  } else if(scope_product == "TS"){
    config_variable <- "ts"
    alert_category <- "technical"
  } else{
    config_variable <- "exec"
    alert_category <- "executive"
  }
  
  if(test_mode){
    channel <- root_config_list[["test_mode"]][["channel"]]
  } else {
    channel <- as.vector(unlist(regional_config_list[[config_variable]][["channel"]]))
  }
  for(slackChannel in channel){
    suppressWarnings(slackr::slackrMsg(txt = paste0("SCOPE _", alert_category, "_ alerts can't be sent today because data is late :cry:"), 
                                       channel = slackChannel))
  }
}

#' @title Slack Notifications for No Alerts
#' @description Posts to slack channel(s) saying there are no alerts
#' @author Stefanie Molin
#'
#' @importFrom slackr slackrMsg
#'
#' @param root_config_list List from the root_config file.
#' @param regional_config_list List from the regional_config file.
#' @param test_mode Boolean whether or not SCOPE is running in test mode or not
#' @param scope_product Which SCOPE product this is for i.e. AS, TS, etc.
#'
#' @export
slack_no_alerts_found <- function(test_mode, root_config_list, regional_config_list, scope_product){
  # determine which alerts
  scope_product <- toupper(scope_product)
  
  if(scope_product == "AS"){
    config_variable <- "web_as"
    alert_category <- "business"
  } else if(scope_product == "TS"){
    config_variable <- "ts"
    alert_category <- "technical"
  } else{
    config_variable <- "exec"
    alert_category <- "executive"
  }
  
  if(test_mode){
    channel <- root_config_list[["test_mode"]][["channel"]]
  } else {
    channel <- as.vector(unlist(regional_config_list[[config_variable]][["channel"]]))
  }
  for(slackChannel in channel){
    suppressWarnings(slackr::slackrMsg(txt = paste0("No _", alert_category, "_ alerts to send for today :grinning:"), channel = slackChannel))
  }
}

#' @title Send AS Alerts Summary to Slack
#' @description Send summary of AS alerts to slack.
#' @author Stefanie Molin
#'
#' @importFrom slackr slackrMsg
#' @importFrom futile.logger flog.info
#' @import dplyr
#'
#' @param accounts_triggered Dataframe of accounts triggered and for which metric
#' @param regional_config_list List from the regional_config file.
#' @param root_config_list List from the root_config file.
#' @param ranking Ranking of the account as found in Vertica (all caps)
#' @param test_mode Boolean whether or not SCOPE is running in test mode or not
#'
send_as_summary_to_slack <- function(accounts_triggered, regional_config_list,
                                     root_config_list, ranking, test_mode){
  tier <- gsub(" ", "", paste(tolower(ranking)))
  if(tier %in% names(regional_config_list[["web_as"]][["channel"]])){
    futile.logger::flog.info(paste("Posting", regional_config_list[["web_as"]][["geo"]], ranking, "alerts to Slack..."))

    if(test_mode){
      channel <- root_config_list[["test_mode"]][["channel"]]
    } else {
      channel <- regional_config_list[["web_as"]][["channel"]][[tier]]
    }

    # format for slack
    slack_triggered_accounts <- accounts_triggered[, c("combo", "kpi")]
    slack_triggered_accounts <- as.data.frame(slack_triggered_accounts %>% dplyr::group_by(combo) %>% 
      dplyr::summarize(alerts = paste(paste0("`", toupper(kpi), "`"), collapse = "  ")))
    slack_triggered_accounts <- slack_triggered_accounts[order(slack_triggered_accounts$combo),]
    scope_slack_updates <- paste(slack_triggered_accounts$combo, 
                                 root_config_list[["slackbot"]][["client_metric_separator"]], 
                                 slack_triggered_accounts$alerts)

    # send messages
    suppressWarnings(slackr::slackrMsg(txt = paste("*Business alert(s)* sent to AS at",
                                                   format_datetime(format_string = "%a %e %b %Y %I:%M%p",
                                                                   timezone = regional_config_list[["web_as"]][["timezone"]], showTimezone = TRUE)),
                                       channel = channel))
    # print alerts not exceeding max at once
    max_alerts_per_message <- root_config_list[["slackbot"]][["max_alerts_per_message"]]
    clients_with_alerts <- nrow(slack_triggered_accounts)
    last_index_used <- 0
    for(i in unique(c(seq(from = 1, to = clients_with_alerts, by = max_alerts_per_message), clients_with_alerts))){
      if(last_index_used < clients_with_alerts){
        end_index <- min(i + max_alerts_per_message - 1, clients_with_alerts)
        suppressWarnings(slackr::slackrMsg(txt = paste0(">>>", 
                                                        paste0(scope_slack_updates[i:end_index], 
                                                               collapse = "\n")), 
                                           channel = channel))
        last_index_used <- end_index
      } else{
        break
      }
    }
    
    suppressWarnings(slackr::slackrMsg(txt = paste0("*Total accounts with business alert(s): ", clients_with_alerts, "*",
                                                    "\n-------------------------------------------------------------------------------"), 
                                       channel = channel))
  }
}

#' @title Send TS Alerts Summary to Slack
#' @description Send summary of TS alerts to slack.
#' @author Stefanie Molin
#'
#' @importFrom slackr slackrMsg
#' @importFrom futile.logger flog.info
#' @importFrom R.utils capitalize
#' @import dplyr
#'
#' @param accounts_triggered Dataframe of accounts triggered and for which metric
#' @param regional_config_list List from the regional_config file.
#' @param root_config_list List from the root_config file.
#' @param ranking Ranking of the account as found in Vertica (all caps)
#' @param test_mode Boolean whether or not SCOPE is running in test mode or not
#'
send_ts_summary_to_slack <- function(accounts_triggered, regional_config_list,
                                     root_config_list, ranking, test_mode){
  futile.logger::flog.info(paste("Posting", regional_config_list[["ts"]][["geo"]], "TS alerts to Slack..."))
    
  if(test_mode){
    channel <- root_config_list[["test_mode"]][["channel"]]
  } else {
    channel <- regional_config_list[["ts"]][["channel"]]
  }
  
  # format for slack
  slack_triggered_accounts <- accounts_triggered %>% 
    dplyr::distinct(partner_name, event_name, site_type) %>% 
    dplyr::mutate(alerts = paste0(R.utils::capitalize(event_name), 
                                  ifelse(event_name != "" & site_type != "", " on ", ""), 
                                  ifelse(is.null(site_type) | site_type == "", "", toupper(site_type)))) %>% 
    dplyr::mutate(alerts = ifelse(alerts == "", "site-level", alerts)) %>% 
    dplyr::group_by(partner_name) %>% 
    dplyr::summarize(alerts = paste(paste0("`", alerts, "`"), collapse = "  ")) %>% 
    dplyr::arrange(partner_name) %>% 
    as.data.frame()
  scope_slack_updates <- paste(slack_triggered_accounts$partner_name, 
                               root_config_list[["slackbot"]][["client_metric_separator"]], 
                               slack_triggered_accounts$alerts)
  
  # send messages
  suppressWarnings(slackr::slackrMsg(txt = paste("*Technical alert(s)* sent to TS at",
                                                 format_datetime(format_string = "%a %e %b %Y %I:%M%p",
                                                                 timezone = regional_config_list[["ts"]][["timezone"]], showTimezone = TRUE)),
                                     channel = channel))
  
  # print alerts not exceeding max at once
  max_alerts_per_message <- root_config_list[["slackbot"]][["max_alerts_per_message"]]
  clients_with_alerts <- nrow(slack_triggered_accounts)
  last_index_used <- 0
  for(i in unique(c(seq(from = 1, to = clients_with_alerts, by = max_alerts_per_message), clients_with_alerts))){
    if(last_index_used < clients_with_alerts){
      end_index <- min(i + max_alerts_per_message - 1, clients_with_alerts)
      suppressWarnings(slackr::slackrMsg(txt = paste0(">>>", 
                                                      paste0(scope_slack_updates[i:end_index], 
                                                             collapse = "\n")), 
                                         channel = channel))
      last_index_used <- end_index
    } else{
      break
    }
  }
  
  suppressWarnings(slackr::slackrMsg(txt = paste0("*Total accounts with technical alert(s): ", clients_with_alerts, "*",
                                                  "\n-------------------------------------------------------------------------------"), 
                                     channel = channel))
}
