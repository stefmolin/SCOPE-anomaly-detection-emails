##################################################
######    Metis Data Collection Library     ######
######         by: Stefanie Molin           ######
####                                          ####
## Pull data to use with the Metis web app.     ##
##################################################

#' @title Pull AS KPI Evolution Data for Metis
#' @description Queries Vertica for same data SCOPE uses to check for anomalies and 
#' outputs it ready for Metis's database.
#' @author Stefanie Molin
#'
#' @importFrom reshape2 melt
#' @import dplyr
#'
#' @param username Vertica username
#' @param password Vertica password
#' @param dest_path Path to save the CSV to
#' @param country_conditions The country conditions to use in the query (see the .yml files)
#' @param tiering The client ranking to use in the query (see the .yml files)
#' @param job_name The job name conditions to use in the query (see the .yml files)
#'
#' @note Defaults will query US data
#'
#' @return A CSV saved in the dest_path
#'
#' @export
pull_AS_metrics_for_metis <- function(username, password, dest_path,
                                      country_conditions = "em.country IN ('US', 'CA') AND em.cost_center_country NOT IN ('LM') AND em.cost_center_activity IN ('TIE', 'IAP')", 
                                      tiering = "ranking IN ('TIER 1')", 
                                      job_name = "em.job_name IN ('T1 ACCOUNT STRATEGIST - FARMING')"){
  
  # read query for stats to run anomaly detection on and modify query for geo + ranking
  anomaly_detection_query <- scopeR::modify_query(read_query_from_file(system.file("queries", "anomaly_query_campaign_ww.sql", package = "scopeR")), 
                                          sub_list = list(countryConditions = country_conditions,
                                                          tiering = tiering,
                                                          jobName = job_name))
  
  # query for campaign daily stats
  campaign_data <- scopeR::query_vertica(query = anomaly_detection_query, username = username, 
                                         password = password)
  
  # remove columns
  campaign_data <- campaign_data %>% select(-AS_name, -AS_email, -manager_email, -manager_name, -currency_code)
  
  # pivot to get client anomalies df
  client_data <- suppressWarnings(as.data.frame(campaign_data %>% 
                                                   group_by(client_name, client_id, global_account_name,
                                                            RTC_vertical, vertical_name, cost_center,
                                                            ranking, country, subregion, region, day) %>% 
                                                   summarise(clicks = sum(clicks), displays = sum(displays), 
                                                             conversions = sum(conversions), spend = sum(spend),
                                                             tac = sum(tac), rext = sum(rext), rext_euro = sum(rext_euro),
                                                             order_value = sum(order_value))))
  
  # align columns
  client_data$campaign_name <- "NOT A CAMPAIGN"
  client_data$campaign_id <- -1
  client_data$campaign_scenario <- "NOT A CAMPAIGN"
  client_data$campaign_type_name <- "NOT A CAMPAIGN"
  client_data$campaign_revenue_type <- -11 # -1 is already used
  client_data$campaign_funnel_id <- -11
  client_data$series <- client_data$client_name
  campaign_data$series <- paste0(campaign_data$client_name, ": ", campaign_data$campaign_name)
  
  # bind campaign data to client data
  all_data <- rbind(client_data, campaign_data) %>% 
    dplyr::mutate(cos = spend/order_value, ctr = clicks/displays, cr = conversions/clicks, margin = rext/spend) %>% 
    dplyr::rename(client_rext = rext)
  
  # clean data
  all_data <- scopeR::clean_data(all_data)
  all_data$day <- as.Date(all_data$day)
  
  # fill in missing dates
  all_data <- scopeR::fill_in_missing_dates(all_data, series_key_column = "series", 
                                            metric_columns = c("spend", "cos", "ctr", "cr", "client_rext", "margin", "clicks", 
                                                               "displays", "conversions", "tac", "client_rext", "order_value"))
  
  # melt into long dataframe
  all_data <- reshape2::melt(all_data, id = c("series", "client_name", "client_id", "campaign_id", "campaign_name", "global_account_name", 
                                              "RTC_vertical", "vertical_name", "campaign_scenario", "campaign_type_name", "campaign_revenue_type", 
                                              "campaign_funnel_id", "cost_center", "ranking", "country", "subregion", "region", "day"), 
                             variable.name = "kpi")
  
  # add columns and reorder to match the database table
  all_data$run_date <- Sys.Date()
  all_data$event_name <- "N/A"
  all_data$site_type <- "N/A"
  all_data$partner_id <- -1
  all_data$partner_name <- "N/A"
  all_data <- all_data[, c("series", "client_name", "client_id", "global_account_name", "RTC_vertical", "vertical_name",
                           "partner_id", "partner_name", "campaign_id", "campaign_name", "campaign_scenario", 
                           "campaign_type_name", "campaign_revenue_type", "campaign_funnel_id", "cost_center", "ranking", 
                           "country", "subregion", "region", "site_type", "event_name", "day", "run_date", "kpi", "value")]
  
  # output the CSV into desired location
  con <- file(dest_path, encoding = "UTF-8")
  write.csv(x = all_data, file = con, row.names = FALSE)
}

#' @title Pull TS KPI Evolution Data for Metis
#' @description Queries Vertica for same data SCOPE uses to check for anomalies and 
#' outputs it ready for Metis's database.
#' @author Stefanie Molin
#'
#' @import dplyr
#'
#' @param username Vertica username
#' @param password Vertica password
#' @param dest_path Path to save the CSV to
#' @param country_conditions The country conditions to use in the query (see the .yml files)
#' @param tiering The client ranking to use in the query (see the .yml files)
#'
#' @note Defaults will query US data
#'
#' @return A CSV saved in the dest_path
#'
#' @export

pull_TS_metrics_for_metis <- function(username, password, dest_path,
                                      country_conditions = "em.country IN ('US','BR') AND em.cost_center_country IN ('US', 'BR', 'LM') AND em.cost_center_activity IN ('TIE', 'IAP')", 
                                      tiering = "ranking IN ('TIER 1')"){
  # modify query
  anomaly_detection_query <- scopeR::modify_query(read_query_from_file(system.file("queries", "scope_ts_site_events_stats.sql", package = "scopeR")),
                                          sub_list = list(countryConditions = country_conditions,
                                                          tiering = tiering))
  
  # query for anomaly detection data
  site_events_data <- scopeR::query_vertica(query = anomaly_detection_query, 
                                            username = username, password = password)
  
  # make the day column a date
  site_events_data$day <- as.Date(site_events_data$day)
  
  # clean data
  site_events_data <- scopeR::clean_data(site_events_data)
  
  # drop TS columns
  tag_level <- site_events_data %>% dplyr::select(partner_name, partner_id, day, ranking, cost_center = ts_cost_center, 
                                                  country = ts_country, subregion = ts_subregion, region = ts_region, 
                                                  RTC_vertical, vertical_name, global_account_name, site_type, event_name, value = events)
  
  # remove duplicates
  tag_level <- tag_level[!duplicated(tag_level),]
  
  # pivot things out into separate dataframes for each of the necessary pieces
  site_level <- suppressWarnings(as.data.frame(tag_level %>% 
                                                 dplyr::group_by(partner_name, partner_id, day, ranking, cost_center, 
                                                                 country, subregion, region, RTC_vertical, 
                                                                 vertical_name, global_account_name) %>% 
                                                 dplyr::summarise(value = sum(value)) %>% 
                                                 dplyr::mutate(site_type = "SITE LEVEL", event_name = "SITE LEVEL",
                                                               series = partner_name)))
  
  # make series column a combination of the primary keys
  tag_level$series <- paste(tag_level$partner_name, tag_level$event_name, "on", tag_level$site_type)
  
  # remove unknown site_type
  if(length(grep("unknown", tag_level$site_type)) > 0){
    tag_level <- tag_level[-grep("unknown", tag_level$site_type),]
  }
  
  # add KPI column to each
  tag_level$kpi <- 'tag_events'
  site_level$kpi <- 'site_events'
  
  # combine and fill in missing dates
  all_data <- scopeR::fill_in_missing_dates(data = rbind(tag_level, site_level), 
                                            series_key_column = "series", 
                                            metric_columns = "value")
  
  # add run_date column and put in order for Metis table
  all_data <- all_data %>% mutate(run_date = Sys.Date(), client_name = "N/A", client_id = -11, campaign_id = -1, 
                                  campaign_name = "N/A", campaign_scenario = "N/A", campaign_type_name = "N/A", 
                                  campaign_revenue_type = -11, campaign_funnel_id = -11)
  
  all_data <- all_data[, c("series", "client_name", "client_id", "global_account_name", "RTC_vertical", "vertical_name",
                           "partner_id", "partner_name", "campaign_id", "campaign_name", "campaign_scenario", 
                           "campaign_type_name", "campaign_revenue_type", "campaign_funnel_id", "cost_center", "ranking", 
                           "country", "subregion", "region", "site_type", "event_name", "day", "run_date", "kpi", "value")]
  
  # output to CSV
  con <- file(dest_path, encoding = "UTF-8")
  write.csv(x = all_data, file = con, row.names = FALSE)
}

#' @title Pull Exec RexT Evolution Data for Metis
#' @description Queries Vertica for same data SCOPE uses to check for anomalies and 
#' outputs it ready for Metis's database.
#' @author Stefanie Molin
#'
#' @import dplyr
#'
#' @param username Vertica username
#' @param password Vertica password
#' @param dest_path Path to save the CSV to
#'
#' @note Defaults will query Americas data
#'
#' @return A CSV saved in the dest_path
#'
#' @export
pull_exec_metrics_for_metis <- function(username, password, dest_path){
  # run query
  query <- scopeR::read_query_from_file(system.file("queries", "scope_exec_query.sql", package = "scopeR"))
  data <- scopeR::query_vertica(query = query, username = username, password = password)
  
  # clean data
  data <- scopeR::clean_data(data)
  data$day <- as.Date(data$day)
  
  # handle renaming and extra columns
  data <- data %>% dplyr::rename(region = area, subregion = region)
  
  # region pivot
  region_data <- as.data.frame(data %>% 
                               dplyr::group_by(day, region) %>% 
                               dplyr::summarize(value = sum(RexT_USD_Constant)) %>% 
                               dplyr::mutate(series = region, subregion = "N/A", country = "N/A"))
  
  # subregion pivot
  subregion_data <- as.data.frame(data %>% 
                                    dplyr::group_by(day, region, subregion) %>% 
                                    dplyr::summarize(value = sum(RexT_USD_Constant)) %>% 
                                    dplyr::mutate(series = subregion, country = "N/A"))
  
  # country level
  country_data <- as.data.frame(data %>% 
                                  dplyr::group_by(day, region, subregion, country) %>% 
                                  dplyr::summarize(value = sum(RexT_USD_Constant)) %>% 
                                  dplyr::mutate(series = country))
  
  # bind everything together
  all_data <- rbind(region_data, subregion_data, country_data)
  
  # fill in missing dates
  all_data <- scopeR::fill_in_missing_dates(all_data, series_key_column = "series", 
                                            metric_columns = "value")
  
  # add columns for database
  all_data <- all_data %>% mutate(run_date = Sys.Date(), kpi = "territory_rext", client_name = "N/A", client_id = -11, 
                                  global_account_name = "N/A", RTC_vertical = "N/A", vertical_name = "N/A", 
                                  partner_id = -1, partner_name = "N/A", campaign_id = -1, campaign_name = "N/A", 
                                  campaign_scenario = "N/A", campaign_type_name = "N/A", campaign_revenue_type = -11, 
                                  campaign_funnel_id = -11, cost_center = "N/A", ranking = "TIER 1", event_name = "N/A", site_type = "N/A") 
  
  all_data <- all_data[, c("series", "client_name", "client_id", "global_account_name", "RTC_vertical", "vertical_name",
                           "partner_id", "partner_name", "campaign_id", "campaign_name", "campaign_scenario", 
                           "campaign_type_name", "campaign_revenue_type", "campaign_funnel_id", "cost_center", "ranking", 
                           "country", "subregion", "region", "site_type", "event_name", "day", "run_date", "kpi", "value")]
  
  # output to CSV
  con <- file(dest_path, encoding = "UTF-8")
  write.csv(x = all_data, file = con, row.names = FALSE)
}