######################################################
####               Pipeline Library               ####
####            Author: Stefanie Molin            ####
####                                              ####
## Pipelines to clean/prep data, check all series   ##
## for anomalies.                                   ##
######################################################

#' @title Run through AS Detect Anomaly Pipeline
#' @description Clean data, impute the median for missing dates, and detect anomalies for all client/campaign/kpi combinations.
#' @author Stefanie Molin
#'
#' @importFrom data.table data.table setkeyv :=
#' @importFrom dplyr %>% filter distinct mutate group_by summarize
#' @importFrom reshape2 melt
#' @importFrom futile.logger flog.info flog.debug
#'
#' @param data Dataframe of daily data to check for anomalies.
#' @param yesterday Day to check for anomalies.
#' @param alert_kpis Vector of the metrics to detect anomalies on. (They must already be in the query results, see note.)
#' Default checks for RexT (local currency), spend, COS, CR, and CTR.
#'
#' @note This function relies on the structure of the query that provides "data". 
#' If changing column names, adding or subtracting columns, be sure to update here as well.
#' 
#' @note Client/campaign must have TAC in each of the last 2 days and 25 rows in the database in the last 30 days
#'
#' @return A list with 2 items: "results"--a dataframe with everything that was tested and whether or not it was an alert and 
#' "cleaned_data"--a dataframe containing the cleaned and prepped data that was used to check for anomalies.
#'
#' @export
anomaly_pipeline_AS <- function(data, yesterday, alert_kpis =  c("rext", "spend", "cos", "cr", "ctr")){
  # find non-eligible accounts
  disqualified <- disqualify_accounts(data = data, series_column = "combo", yesterday = yesterday, required_entries = 25)
  
  # note that by removing these accounts from the data here they won't be there for the dashboard (revisit later)
  data <- dplyr::anti_join(data, disqualified, by = "combo")
  
  # add metrics to test for anomalies
  data$cos <- data$spend/data$order_value
  data$ctr <- data$clicks/data$displays
  data$cr <- data$conversions/data$clicks
  
  # clean data
  futile.logger::flog.info("Cleaning data...")
  data <- clean_data(data)
  data$day <- as.Date(data$day)
  
  # fill in missing dates
  futile.logger::flog.info("Filling in missing dates...")
  data <- fill_in_missing_dates(data, series_key_column = "combo", 
                                metric_columns = c("spend", "cos", "ctr", "cr", "rext"))
  
  # status update
  futile.logger::flog.info("Checking for anomalies...")
  
  # rearrange the data into a long dataframe
  tracked_kpis <- c("rext", "spend", "cos", "ctr", "cr", "clicks", 
                    "displays", "tac", "conversions", "order_value", "rext_euro")
  melted <- reshape2::melt(data, measure.vars = tracked_kpis, variable.name = "kpi")
  
  # find combinations to test for anomalies
  results <- melted %>% 
    dplyr::distinct(combo, client_name, client_id, campaign_name, 
                    campaign_id, AS_name, AS_email, manager_name,
                    manager_email, cost_center, currency_code,
                    ranking, country, subregion, region, kpi) %>% 
    dplyr::mutate(run_date = Sys.Date(), is_alert = FALSE)
  
  # remove non-alert metrics
  results <- results %>% dplyr::filter(kpi %in% c(alert_kpis, "rext_euro"))
  
  # make a data.table
  DT_results <- data.table::data.table(results)
  DT <- data.table::data.table(data)
  
  # set keys
  data.table::setkeyv(DT, "combo")
  data.table::setkeyv(DT_results, c("combo", "kpi"))
  to_test <- unique(DT_results$combo)
  
  # find all anomalies (client/campaign/kpi combinations)
  for(series in to_test){
    futile.logger::flog.debug(paste("Checking", series, "for anomalies"))
    for(metric in alert_kpis){
      DT_results[combo == series & kpi == metric, 
                 is_alert := (detect_anomaly(as.data.frame(DT[combo == series,
                                                             c("day", metric), with = FALSE]),
                                            metric = metric, yesterday = yesterday) & 
                                # make sure value deviates from median by more than 0.0001 for percentages and 1 for values
                                ifelse(length(DT[combo == series & day == yesterday, metric, with = FALSE][[1]]) == 1,
                                       abs(
                                         abs(median(DT[combo == series, metric, with = FALSE][[1]])) - 
                                           abs(DT[combo == series & day == yesterday, metric, with = FALSE])
                                       ) >= ifelse(metric %in% c("cos", "cr", "ctr"), 0.0001, 1), FALSE)) | 
                   # always flag RexT lower than -1000 euros (to avoid issues with highly inflated currencies like JPY, Colombian Pesos, etc.)
                   ifelse(metric == "rext" & length(DT[combo == series & day == yesterday, rext_euro]) == 1,
                          DT[combo == series & day == yesterday, rext_euro] <= -1000, FALSE)]
    }
  }
  
  return(list(results = as.data.frame(DT_results), cleaned_data = data))
}

#' @title TS Detect Anomaly Pipeline
#' @description  Clean data, impute the median for missing dates, and detect anomalies for all client/campaign/kpi combinations.
#' @author Stefanie Molin
#' 
#' @importFrom futile.logger flog.info flog.debug
#' @importFrom data.table data.table setkeyv :=
#' @importFrom dplyr %>% filter distinct mutate group_by summarize
#' 
#' @param data Dataframe of the data to check for anomalies
#' @param yesterday Day to check for anomalies
#' 
#' @return A list with 2 items: "results"--a dataframe with everything that was tested and whether or not it was an alert and 
#' "cleaned_data"--a dataframe containing the cleaned and prepped data that was used to check for anomalies.
#'
#' @export
anomaly_pipeline_TS <- function(data, yesterday){
  # prep the data
  data$day <- as.Date(data$day)
  
  # clean data
  flog.info("Cleaning data...")
  data <- clean_data(data)
  
  # calculate site level metrics from the tag level data
  site_level <- suppressWarnings(as.data.frame(data %>% 
                                                 dplyr::group_by(partner_name, partner_id, day, 
                                                                 TS_name, ts_engineer_email, manager_name, manager_email,
                                                                 ranking, ts_cost_center, ts_country, ts_subregion, ts_region,
                                                                 RTC_vertical, vertical_name, global_account_name) %>% 
                                                 dplyr::summarize(events = sum(events)) %>% 
                                                 dplyr::mutate(site_type = "SITE LEVEL", event_name = "SITE LEVEL") %>% 
                                                 dplyr::select(partner_name, partner_id, day, TS_name, ts_engineer_email, manager_name, manager_email,
                                                               ranking, ts_cost_center, ts_country, ts_subregion, ts_region,
                                                               RTC_vertical, vertical_name, global_account_name,
                                                               site_type, event_name, events))) 
  
  # combine into 1 dataframe
  data <- rbind(data, site_level)
  
  # remove unknown site_type
  # if(length(grep("unknown", data$site_type)) > 0){
  #   data <- data[-grep("unknown", data$site_type),]
  # }
  
  # make a concatenated name column for analysis
  data$combo <- paste(data$partner_name, data$event_name, "on", data$site_type, "managed by", data$TS_name)
  
  # fill in missing dates
  flog.info("Filling in values for missing dates...")
  data <- fill_in_missing_dates(data = data, series_key_column = "combo", metric_columns = "events")
  
  # status update
  futile.logger::flog.info("Checking for anomalies...")
  
  # find combinations to test for anomalies
  results <- data %>% 
    dplyr::distinct(partner_name, partner_id, TS_name, ts_engineer_email, manager_name, manager_email,
                    ranking, ts_cost_center, ts_country, ts_subregion, ts_region,
                    RTC_vertical, vertical_name, global_account_name, site_type, event_name, combo) %>% 
    dplyr::mutate(run_date = Sys.Date(), is_alert = FALSE)
  
  # make a data.table
  DT_results <- data.table::data.table(results)
  DT <- data.table::data.table(data)
  
  # set keys
  data.table::setkeyv(DT, "combo")
  data.table::setkeyv(DT_results, "combo")
  to_test <- unique(DT_results$combo)
  
  # find all anomalies (client/campaign/kpi combinations)
  metric <- "events"
  for(series in to_test){
    futile.logger::flog.debug(paste("Checking", series, "for anomalies"))
    DT_results[combo == series, 
               is_alert := (detect_anomaly(as.data.frame(DT[combo == series,
                                                            c("day", metric), with = FALSE]),
                                           metric = metric, yesterday = yesterday) & 
                              # make sure value deviates from median by more than 1 for values
                              ifelse(length(DT[combo == series & day == yesterday, metric, with = FALSE][[1]]) == 1,
                                     abs(
                                       abs(median(DT[combo == series, metric, with = FALSE][[1]])) - 
                                         abs(DT[combo == series & day == yesterday, metric, with = FALSE])
                                     ) >= 1, FALSE))]
  }
  
  return(list(results = as.data.frame(DT_results), cleaned_data = data))
}


#' @title Run through Territory RexT Detect Anomaly Pipeline
#' @description Clean data, impute the median for missing dates, and detect anomalies for all territory RexT series
#' @author Stefanie Molin
#'
#' @importFrom data.table data.table setkeyv :=
#' @importFrom dplyr %>% filter distinct mutate group_by summarize
#' @importFrom reshape2 melt
#' @importFrom futile.logger flog.info flog.debug
#'
#' @param data Dataframe of daily data to check for anomalies.
#' @param yesterday Day to check for anomalies.
#'
#' @note This function relies on the structure of the query that provides "data". 
#' If changing column names, adding or subtracting columns, be sure to update here as well.
#' 
#' @note Client/campaign must have TAC in each of the last 2 days and 25 rows in the database in the last 30 days
#'
#' @return A list with 2 items: "results"--a dataframe with everything that was tested and whether or not it was an alert and 
#' "cleaned_data"--a dataframe containing the cleaned and prepped data that was used to check for anomalies.
#'
#' @export
anomaly_pipeline_territory_rext <- function(data, yesterday){
  # clean data
  data <- scopeR::clean_data(data)
  data$day <- as.Date(data$day)
  
  # handle renaming and extra columns
  data <- data %>% dplyr::rename(region = area, subregion = region)
  
  # region pivot
  region_data <- as.data.frame(data %>% 
                                 dplyr::group_by(day, region) %>% 
                                 dplyr::summarize(RexT = sum(RexT_USD_Constant)) %>% 
                                 dplyr::select(series = region, day, RexT))
  
  # subregion pivot
  subregion_data <- as.data.frame(data %>% 
                                    dplyr::group_by(day, subregion) %>% 
                                    dplyr::summarize(RexT = sum(RexT_USD_Constant)) %>% 
                                    dplyr::select(series = subregion, day, RexT))
  
  # country level
  country_data <- as.data.frame(data %>% 
                                  dplyr::group_by(day, country) %>% 
                                  dplyr::summarize(RexT = sum(RexT_USD_Constant)) %>% 
                                  dplyr::select(series = country, day, RexT))
  
  # bind everything together
  data <- rbind(region_data, subregion_data, country_data)
  
  # fill in missing dates
  data <- scopeR::fill_in_missing_dates(data, series_key_column = "series", metric_columns = "RexT")
  
  # status update
  futile.logger::flog.info("Checking for anomalies...")
  
  # find combinations to test for anomalies
  results <- data %>% 
    dplyr::distinct(series) %>% 
    dplyr::mutate(is_alert = FALSE)
  
  # make a data.table
  DT_results <- data.table::data.table(results)
  DT <- data.table::data.table(data)
  
  # set keys
  data.table::setkeyv(DT, "series")
  data.table::setkeyv(DT_results, "series")
  to_test <- unique(DT_results$series)
  
  # find all anomalies (territory RexT)
  metric <- "RexT"
  stat_sig_level <- "exec"
  for(combo in to_test){
    futile.logger::flog.debug(paste("Checking", combo, "for anomalies"))
    DT_results[series == combo, 
               is_alert := (detect_anomaly(as.data.frame(DT[series == combo,
                                                            c("day", metric), with = FALSE]),
                                           metric = stat_sig_level, yesterday = yesterday) & 
                              # make sure value deviates from median by more than 1 for values
                              ifelse(length(DT[series == combo & day == yesterday, metric, with = FALSE][[1]]) == 1,
                                     abs(
                                       abs(median(DT[series == combo, metric, with = FALSE][[1]])) - 
                                         abs(DT[series == combo & day == yesterday, metric, with = FALSE])
                                     ) >= 1, FALSE))]
  }
  
  return(list(results = as.data.frame(DT_results), cleaned_data = data))
}





















