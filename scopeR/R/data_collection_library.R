######################################################
####           Data Collection Library            ####
####            Author: Stefanie Molin            ####
####                                              ####
## Pull all data to prep for use in pipeline        ##
## functions (as a dataframe).                      ##
######################################################

#' @title Pull AS Data
#' @description Pull data in the proper format for AS anomaly detection
#' @author Stefanie Molin
#'
#' @import dplyr
#' @importFrom futile.logger flog.info
#'
#' @param username Vertica username
#' @param password Vertica password
#' @param last_try Boolean indicating whether or not this is the last try for the script as a whole
#' @param test_mode Boolean indicating whether or not SCOPE is in test mode
#' @param root_config_list List read from the root config YAML file
#' @param regional_config_list List read from the regional config YAML file
#' @param sender Address of the sender of the error emails.
#' @param mailbox_password Password of the sender
#' @param slack_on Boolean indicating whether or not current SCOPE configuration can post to slack. Defaults to FALSE
#' 
#' @return Dataframe of results
#'
#' @export
pull_AS_data <- function(username, password, last_try, test_mode, root_config_list, regional_config_list, sender, mailbox_password, slack_on = FALSE){
  # status update
  flog.info("Querying Vertica for dataframe all_stats ...")
  
  # read query for stats to run anomaly detection on and modify query for geo + ranking
  anomaly_detection_query <- modify_query(read_query_from_file(system.file("queries", "anomaly_query_campaign_ww.sql", package = "scopeR")), 
                                          sub_list = list(countryConditions = regional_config_list[["web_as"]][["query"]][["countryConditions"]],
                                                          tiering = regional_config_list[["web_as"]][["query"]][["tiering"]]))
  
  # query for campaign daily stats
  campaign_stats <- query_try_block(query = anomaly_detection_query, df_name = "campaign_stats",
                                    username = username, password = password, timeout = 120, 
                                    scope_product = "AS", last_try = last_try, test_mode = test_mode, 
                                    root_config_list = root_config_list, regional_config_list = regional_config_list,
                                    sender = sender, mailbox_password = mailbox_password, slack_on = slack_on)
  
  # pivot to get client anomalies df
  client_stats <- suppressWarnings(as.data.frame(campaign_stats %>% 
                                                   group_by(client_name, client_id, global_account_name,
                                                            RTC_vertical, vertical_name, AS_name, AS_email, 
                                                            manager_name, manager_email, cost_center, currency_code, 
                                                            ranking, country, subregion, region, day) %>% 
                                                   summarise(clicks = sum(clicks), displays = sum(displays), 
                                                             conversions = sum(conversions), spend = sum(spend),
                                                             tac = sum(tac), rext = sum(rext), rext_euro = sum(rext_euro),
                                                             order_value = sum(order_value))))
  
  # align columns
  client_stats$campaign_name <- "NOT A CAMPAIGN"
  client_stats$campaign_id <- -1
  client_stats$campaign_scenario <- "NOT A CAMPAIGN"
  client_stats$campaign_type_name <- "NOT A CAMPAIGN"
  client_stats$campaign_revenue_type <- -11 # -1 is already used
  client_stats$campaign_funnel_id <- -11
  client_stats$combo <- client_stats$client_name
  campaign_stats$combo <- paste0(campaign_stats$client_name, ": ", campaign_stats$campaign_name)
  
  # bind campaign data to client data
  all_stats <- rbind(client_stats, campaign_stats) %>% mutate(margin = rext/spend)
  
  # Reduce accounts to beta testers
  if(regional_config_list[["beta"]][["active"]]){
    flog.info("Reducing to beta-testers.")
    all_stats <- subset(all_stats, AS_email %in% regional_config_list[["beta"]][["testers"]])
  }
  
  return(all_stats)
}

#' @title Pull TS Data
#' @description Pull data in the proper format for TS anomaly detection
#' @author Stefanie Molin
#'
#' @importFrom futile.logger flog.info
#'
#' @param username Vertica username
#' @param password Vertica password
#' @param last_try Boolean indicating whether or not this is the last try for the script as a whole
#' @param test_mode Boolean indicating whether or not SCOPE is in test mode
#' @param root_config_list List read from the root config YAML file
#' @param regional_config_list List read from the regional config YAML file
#' @param sender Address of the sender of the error emails.
#' @param mailbox_password Password of the sender
#' 
#' @return Dataframe of results
#'
#' @export
pull_TS_data <- function(username, password, last_try, test_mode, root_config_list, regional_config_list, sender, mailbox_password){
  # status update
  flog.info("Querying Vertica for dataframe test_for_anomalies ...")
  
  # query for stats to run anomaly detection on
  anomaly_detection_query <- modify_query(read_query_from_file(system.file("queries", "scope_ts_site_events_stats.sql", package = "scopeR")),
                                          sub_list = list(countryConditions = regional_config_list[["ts"]][["query"]][["countryConditions"]],
                                                          tiering = regional_config_list[["ts"]][["query"]][["tiering"]]))
  
  # query for anomaly detection data
  test_for_anomalies <- query_try_block(query = anomaly_detection_query, df_name = "test_for_anomalies",
                                        username = username, password = password, timeout = 120, 
                                        scope_product = "TS", last_try = last_try, test_mode = test_mode,
                                        root_config_list = root_config_list, regional_config_list = regional_config_list,
                                        sender = sender, mailbox_password = mailbox_password)
  
  # Reduce accounts to beta testers
  if(regional_config_list[["beta"]][["active"]]){
    flog.info("Reducing to beta-testers.")
    test_for_anomalies <- subset(test_for_anomalies, ts_engineer_email %in% regional_config_list[["beta"]][["testers"]])
  }
  
  return(test_for_anomalies)
}

#' @title Pull Exec Data
#' @description Pull data in the proper format for Exec anomaly detection
#' @author Stefanie Molin
#'
#' @param territory_level Boolean TRUE will pull territory-level stats, FALSE pulls client-level
#' @param username Vertica username
#' @param password Vertica password
#' @param last_try Boolean indicating whether or not this is the last try for the script as a whole
#' @param test_mode Boolean indicating whether or not SCOPE is in test mode
#' @param root_config_list List read from the root config YAML file
#' @param regional_config_list List read from the regional config YAML file
#' @param sender Address of the sender of the error emails.
#' @param mailbox_password Password of the sender
#' 
#' @return Dataframe of results
#'
#' @export
pull_Exec_data <- function(territory_level, username, password, last_try, test_mode, 
                           root_config_list, regional_config_list, sender, mailbox_password){
  # determine which query to use
  if(territory_level){
    query_file <- "scope_exec_query.sql"
    df_name <- "territory_level_RexT_df"
  } else{
    query_file <- "scope_exec_account_level.sql"
    df_name <- "client_level_RexT_df"
  }
  
  data <- query_try_block(query = read_query_from_file(system.file("queries", query_file, package = "scopeR")),
                          df_name = df_name, username = username, password = password, timeout = 120, 
                          scope_product = "EXEC", last_try = last_try, test_mode = test_mode, 
                          root_config_list = root_config_list, regional_config_list = regional_config_list,
                          sender = sender, mailbox_password = mailbox_password)
  
  return(data)
}