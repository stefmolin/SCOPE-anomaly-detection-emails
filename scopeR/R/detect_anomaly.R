######################################################
####            Detect Anomaly Library            ####
####            Author: Stefanie Molin            ####
####                                              ####
## Clean dataframes of a particular structure for   ##
## anomaly detection. Functions to detect anomalies ##
## and a wrapper function fo SCOPE use.             ##
######################################################

#' @title Disqualify Accounts
#' @description Return the series that aren't eligible for alerts (TAC of 0 for last 2 days and/or not live for 25 days in the last 30 days)
#' @author Stefanie Molin
#'
#' @import dplyr
#'
#' @param data Dataframe to search. Must have columns: tac and day.
#' @param series_column Column name of the series to check for disqualification
#' @param yesterday Yesterday's date
#' @param required_entries Number of days the series must have data for in the last 30 days to be considered live
#'
#' @return A dataframe
#'
#' @export
disqualify_accounts <- function(data, series_column, yesterday, required_entries = 25){
  # remove accounts that don't have data for yesterday
  has_data_for_yesterday <- data %>% 
    dplyr::filter(day == yesterday) %>% 
    dplyr::distinct_(series_column)
  no_data_for_yesterday <- data %>% 
    dplyr::anti_join(has_data_for_yesterday, by = series_column) %>% 
    dplyr::distinct_(series_column)
  
  # handle disqualifications (only keep clients/campaigns that have positive tac on at least 1 of the last 2 days)
  data$tac <- round(data$tac, 2)
  
  disqualified_tac <- data %>% 
    dplyr::filter((day == yesterday | day == yesterday - 1)) %>% 
    dplyr::group_by_(series_column) %>% 
    dplyr::summarize(tac = sum(tac)) %>% 
    dplyr::filter(tac <= 0) %>% 
    dplyr::distinct_(series_column)
  
  # require data for x out of the last 30 days
  disqualified_days_live <- data %>% 
    dplyr::filter(day >= yesterday - (required_entries + 4)) %>% 
    dplyr::group_by_(series_column) %>% 
    dplyr::summarize(entries = n()) %>% 
    dplyr::filter(entries < required_entries) %>% 
    dplyr::distinct_(series_column)
  
  # all disqualifications
  disqualified <- rbind(disqualified_days_live, disqualified_tac, no_data_for_yesterday)
  
  return(disqualified)
}

#' @title Clean Data
#' @description Replace Inf, NaN and NA values with 0
#' @author Stefanie Molin
#'
#' @param df Dataframe to clean
#'
#' @return A dataframe
#'
#' @export
clean_data <- function(df){
  for(column in names(df)){
    if(identical(column, "day")){
      next
    }
    # remove NA, NaN and Inf values
    df[is.infinite(df[,column]), column] <- 0
    df[is.nan(df[,column]), column] <- 0
    df[is.na(df[,column]), column] <- 0
  }

  return(df)
}

#' @title Fill In Missing Dates
#' @description For any missing dates between the minimum and maximum dates in the time series, impute the median for the metrics.
#' @author Stefanie Molin
#'
#' @importFrom data.table data.table setkeyv :=
#' @importFrom dplyr %>% arrange_
#' @importFrom futile.logger flog.debug
#'
#' @param data Dataframe to check and fill for missing dates (see note for structure)
#' @param series_key_column The name of the column that defines a unique time series (to separate from the rest in the dataframe)
#' @param metric_columns The names of the columns with metrics to impute
#'
#' @note There must be a column "day" in data of type "Date"
#'
#' @return A dataframe
#'
#' @export
fill_in_missing_dates <- function(data, series_key_column, metric_columns){
  # find global min and max dates; get sequence of maximum date range possible
  global_min_date <- min(data$day)
  global_max_date <- max(data$day)
  max_date_range <- seq(from = global_min_date, to = global_max_date, by = 1)
  
  # find globally missing dates (these should be imputed all else set to 0)
  global_dates <- unique(data$day)
  globally_missing_dates <- max_date_range[!(max_date_range %in% global_dates)]
  
  flog.debug("Globally missing dates:")
  flog.debug(paste(globally_missing_dates, collapse = ","))
  
  # turn into data.table for indexing
  data <- data.table::data.table(data)
  data.table::setkeyv(data, series_key_column)
  combos <- unlist(unique(data[, series_key_column, with = FALSE]))

  # empty dataframe for the replacements
  all_replacements <- data[1,][0,]

  for(combo in combos){
    # get data for the time series
    df <- data[combo,]

    # get date range
    dates <- unique(df$day)
    max_local_date <- max(dates)
    date_sequence <- seq(from = min(dates), to = max_local_date, by = 1)

    # missing dates in advertiser's data
    missing_dates <- date_sequence[!(date_sequence %in% dates)]
    
    if(max_local_date < global_max_date){
      missing_dates <- c(missing_dates, 
                         seq(from = max_local_date + 1, to = global_max_date, by = 1))
    }
    # including the global max in case the advertiser is missing data for yesterday but it isn't missing at a global level
    dates_to_fill <- unique(c(missing_dates, globally_missing_dates))

    if(length(dates_to_fill) > 0){
      # isolate replacement row
      replacement <- df[1,]

      for(i in 1:length(dates_to_fill)){
        # replace the date with the missing date
        replacement$day <- dates_to_fill[i]

        if(dates_to_fill[i] %in% globally_missing_dates){
          flog.debug(sprintf("Date %s is globally missing filling in %s with the median", dates_to_fill[i], combo))
          replacement[, (metric_columns) := lapply(df[, metric_columns, with = FALSE], function(x) { median(x)})]
        } else{
          flog.debug(sprintf("Date %s isn't globally missing, filling in %s with 0", dates_to_fill[i], combo))
          replacement[, (metric_columns) := 0]
        }
        # add row to replacements
        all_replacements <- rbind(all_replacements, replacement)
      }
    }
  }
  # add replacements to data
  data <- rbind(data, all_replacements)

  # re-sort data
  data <- data %>% dplyr::arrange_(series_key_column, "day")
  return(as.data.frame(data))
}

#' @title Run Anomaly Detection
#' @description Determine whether or not yesterday was an anomaly
#' @author Stefanie Molin
#'
#' @importFrom outliers scores
#'
#' @param data Dataframe where first column is "day" and second is the metric you want to check for anomalies
#' @param direction Can be "both", "neg", or "pos". Determines whether you only want to check for positive anomalies, negative, or both.
#' @param stat_sig The statistical significance for the anomaly test.
#' @param yesterday The date to check for alerts.
#'
#' @return A boolean indicating whether or not yesterday was an alert
#'
run_anomaly_detection <- function(data, direction, stat_sig, yesterday){
  minValidData <- 25
  tukey_multiplier <- 3
  direction <- tolower(direction)

  # second column contains data to check for anomaly
  data_vector <- data[,2]

  if(nrow(data) >= minValidData){
    # handle one-sided
    if(direction %in% c("neg", "pos")){

      if(identical(direction, "neg")){

        # check if value is more extreme than day before
        isMoreExtreme <- data[as.Date(data$day) == yesterday, 2] <= data[as.Date(data$day) == yesterday - 1, 2] * 0.9

        # if the value isn't there it generates numeric(0) and the equation will evaluate to logical(0), so we need to check length below
        isMoreExtreme <- ifelse(length(isMoreExtreme) == 0, FALSE, isMoreExtreme)

        # determine if yesterday meets criteria
        results <- outliers::scores(data_vector, "chisq", stat_sig) & data_vector < quantile(data_vector, 0.25) - tukey_multiplier*IQR(data_vector)
      } else{

        # check if value is more extreme than day before
        isMoreExtreme <- data[as.Date(data$day) == yesterday, 2] >= data[as.Date(data$day) == yesterday - 1, 2] * 1.1

        # if the value isn't there it generates numeric(0) and the equation will evaluate to logical(0), so we need to check length below
        isMoreExtreme <- ifelse(length(isMoreExtreme) == 0, FALSE, isMoreExtreme)

        # determine if yesterday meets criteria
        results <- outliers::scores(data_vector, "chisq", stat_sig) & data_vector > quantile(data_vector, 0.75) + tukey_multiplier*IQR(data_vector)
      }

      # check if yesterday was an anomaly
      if(!is.na(results[length(results)]) && as.Date(data[length(results), "day"]) == yesterday && isMoreExtreme){
        anomaly <- results[length(results)]
      } else{
        anomaly <- FALSE
      }

    } else if(identical(direction, "both")){
      # handle two-sided
      anomaly <- run_anomaly_detection(data, "neg", stat_sig, yesterday) | run_anomaly_detection(data, "pos", stat_sig, yesterday)
    } else{
      stop("The direction argument can be either \"both\", \"pos\", or \"neg\".", call. = FALSE)
    }

  } else{
    anomaly <- FALSE
  }


  return(anomaly)
}

#' @title Detect Anomaly
#' @description Determine whether or not yesterday was an anomaly after cleaning data.
#' @author Stefanie Molin
#'
#' @param data Dataframe where first column is "day" and second is the metric you want to check for anomalies
#' @param metric Can be "events", "COS", "CR", "CTR", "spend", "rext", "exec". Which metric to check for anomaly.
#' @param yesterday The date to check for alerts.
#'
#' @return A boolean indicating whether or not yesterday was an alert
#'
#' @export
detect_anomaly <- function(data, metric, yesterday){
  # stat sig per metric
  statSigCOS <- 0.9995
  statSigCTR <- 0.999
  statSigCR <- 0.999
  statSigSiteEvents <- 0.975
  statSigSpend <- 0.9975
  statSigRexT <- 0.9975
  statSigExec <- 0.975

  metric <- tolower(metric)
  direction <- "both"

  if(identical(metric, "events")){
    stat_sig <- statSigSiteEvents
    direction <- "neg"
  } else if(identical(metric, "cos")){
    stat_sig <- statSigCOS
  } else if(identical(metric, "cr")){
    stat_sig <- statSigCR
  } else if(identical(metric, "ctr")){
    stat_sig <- statSigCTR
  } else if(identical(metric, "spend")){
    stat_sig <- statSigSpend
  } else if(identical(metric, "rext")){
    stat_sig <- statSigRexT
  } else if(identical(metric, "exec")){
    stat_sig <- statSigExec
  } else{
    stop("Acceptable values for metric are: events, COS, CR, CTR, spend, rext, exec", call. = FALSE)
  }

  anomaly <- run_anomaly_detection(data, direction = direction, stat_sig = stat_sig, yesterday = yesterday)
  return(anomaly)
}

#' @title Record Anomaly
#' 
#' @description Record the anomaly adding to a string of anomalies
#' @author Stefanie Molin
#' 
#' @param current_alerts String containing the alerts already flagged
#' @param alert_to_add  String containing the newest flagged alert.
#' 
#' @return String
#' 
#' @export
record_anomaly <- function(current_alerts, alert_to_add){
  if(identical(current_alerts, "")){
    return(alert_to_add)
  }
  else{
    return(paste(current_alerts, alert_to_add, sep = ", "))
  }
}
