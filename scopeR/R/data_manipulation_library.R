####################################################
######         Data Manipulation Library      ######
######            by: Stefanie Molin          ######
####                                            ####
## Utility functions for manipulating SCOPE data. ##
####################################################

#' @title Create Exec Territory Dataframe
#' @description Turn vectors of stats by region into a dataframe
#' @author Stefanie Molin
#'
#' @param dates_vector Vector of the dates to use for all series
#' @param rext_list A list of RexT series where the name is the series name how it should appear in the email and the value is the corresponding RexT vector.
#'
#' @return Dataframe with the results of binding together information
#'
#' @export
#'
exec_df_binder <- function(dates_vector, rext_list){
  df <- data.frame()
  for(series_name in names(rext_list)){
    df <- rbind(df, data.frame(Day = dates_vector, Series = rep(series_name, length(rext_list[[series_name]])), RexT = rext_list[[series_name]]))
  }
  return(df)
}

#' @title Reduce Vector Size
#' @description Reduce a vector to the last x entries
#' @author Stefanie Molin
#'
#' @param vector Data as a vector
#' @param last_x_entries Number of entries from the end of the vector desired
#'
#' @return Vector reduced to the last x entries
#'
#' @export
#'
vector_shrinker <- function(vector, last_x_entries){
  if(last_x_entries < length(vector)){
    offset <- last_x_entries - 1
    result <- vector[(length(vector) - offset):length(vector)]
  } else{
    result <- vector
  }
  return(result)
}

#' @title Round and Shrink Exec Data Lists
#' @description For each vector in the rext_list reduce it to the last x days and round all entries to specified decimal places
#' @author Stefanie Molin
#'
#' @param rext_list List of RexT vectors
#' @param last_x_days Last x days to isolate in the vectors
#' @param decimal_places The number of decimal places to round the RexT numbers.
#'
#' @return List where all entries have been rounded and number of entries reduced to last x
#'
#' @export
exec_series_shrink_round <- function(rext_list, last_x_days, decimal_places){
    # shrink each series
  results <- lapply(rext_list, vector_shrinker, last_x_days)

  # round to appropriate number of decimal places
  results <- lapply(results, round, 2)

  # return resulting list
  return(results)
}

#' @title Percent Difference
#' @description Calculate percent difference and return it formatted if desired
#' @author Stefanie Molin
#'
#' @param before_values Vector of values in "before" condition
#' @param after_values Vector of values in "after" condition
#' @param format Whether or not to format as a percent
#' @param ... Additional arguments to be passed to format_percent()
#'
#' @return Vector of characters
#'
#' @export
percent_difference <- function(before_values, after_values, format = TRUE, ...){
  # calculate the change
  percent_diff <- (after_values - before_values)/before_values

  # return formatted percent difference if chosen otherwise return value
  if(format){
    percent_diff <- format_percent(percent_diff, ...)
  }

  return(percent_diff)
}

#' @title Basis Points Change
#' @description Calculate basis point change and return formatted if desired
#' @author Stefanie Molin
#'
#' @param before_values Vector of values in "before" condition
#' @param after_values Vector of values in "after" condition
#' @param multiplier Value to multiple the difference by (need to use if both the input values were modified elsewhere)
#' @param format Whether or not to format as bps
#' @param ... Additional arguments to be passed to format_bps()
#'
#' @return Vector of characters
#'
#' @export
change_in_bps <- function(before_values, after_values, multiplier = 1, format = TRUE, ...){
    # calculate the change
  change <- (after_values - before_values) * multiplier

  # return formatted bps if chosen otherwise return value
  if(format){
    change <- format_bps(change, ...)
  }

  return(change)
}

#' @title Site Events Last 7 Days Dataframe
#' @description Return in 7 day dataframe, filling dates with "NA" if not available
#' @author Stefanie Molin
#'
#' @param data The dataframe containing 2 columns: date and site events (naming doesn't matter but order does)
#' @param timezone The timezone to create the additional dates in (if necessary)
#'
#' @return Dataframe of site events with first column "day" and second column "total_events"
#'
#' @export
site_events_7D <- function(data, timezone){
  # fill in site events table if not enough data is there (delayed)
  if (nrow(data) < 7){
    j <- nrow(data)
    if(j == 0){
      y1 <- format(Sys.time() - 7*60*60*24, format = "%Y-%m-%d", tz = timezone, usetz = FALSE)
      y2 <- "NA"
      newRow <- c(y1, y2)
      data <- rbind(data, newRow, stringsAsFactors = FALSE)
      colnames(data) <- c("day", "total_events")
      j <- nrow(data)
    }
    while (j < 7){
      y1 <- strftime(as.Date(data[j, 1]) + 1, "%Y-%m-%d")
      y2 <- "NA"
      newRow <- c(y1, y2)
      data <- rbind(data, newRow, stringsAsFactors = FALSE)
      j <- nrow(data)
    }
  }

  return(data)
}

#' @title Initialize Exec Account Dataframe
#' @description Initializes a dataframe for holding account RexT alerts for Exec
#' @author Stefanie Molin
#'
#' @param dataIsLate Boolean indicating whether or not account data is late. Defaults to FALSE
#'
#' @return Dataframe for account RexT alerts section of Exec
#'
#' @export
exec_initialize_account_df <- function(dataIsLate = FALSE){
  if(!dataIsLate){
    df <- data.frame(client = rep("", 5), actual = rep("", 5), expectedValue = rep("", 5),
                     percentDiff = rep("", 5), deviation = rep("", 5), stringsAsFactors = FALSE)
  } else{
    df <- data.frame(client = rep("DATA IS LATE", 5), actual = rep("", 5), expectedValue = rep("", 5),
                     percentDiff = rep("", 5), deviation = rep("", 5), stringsAsFactors = FALSE)
  }
  return(df)
}

#' @title Create Exec Late Data Account List
#' @description Creates account lists for when data is late
#' @author Stefanie Molin
#'
#' @return List containing 2 dataframes: gainers and losers
#'
#' @export
exec_late_data_account_list <- function(){
  account_list <- list(gainers = exec_initialize_account_df(dataIsLate = TRUE),
                       losers = exec_initialize_account_df(dataIsLate = TRUE))
  return(account_list)
}

#' @title Generate Exec Client Empty Row
#' @description Initializes a 1-row dataframe for holding account RexT alerts for Exec
#' @author Stefanie Molin
#'
#' @return 1-row dataframe for account RexT alerts section of Exec
#'
#' @export
exec_account_df_empty_row <- function(){
  row <- data.frame(client =  "", actual = "", expectedValue = "", percentDiff = "",
                    magnitude = "", deviation = "", stringsAsFactors = FALSE)
  return(row)
}

#' @title Exec Account Dataframe Builder
#' @description Creates the gainers or losers dataframe for Exec
#' @author Stefanie Molin
#'
#' @param df Dataframe with gainer or loser data
#' @param isGainer Boolean indicating whether data is for gainers or not.
#'
#' @return Dataframe
#'
#' @export
exec_account_df_builder <- function(df, isGainer){
  if(isGainer){
    sign <- "+"
  } else{
    sign <- "-"
  }

  # if dataframe doesn't have 5 rows fill in rows of "" under them until there are 5; if already 0 fill with ""
  if(nrow(df) == 0){
    df <- exec_initialize_account_df()
  } else{
    # get a vector with the indices of the rows sorted by magnitude
    index <- head(rev(order(as.numeric(df$magnitude))), n = 5)

    # ordered dataframe to use in printing
    df <- df[index,]

    # format the percent diff column
    df$percentDiff <- format_percent(df$percentDiff, showPlusSign = TRUE)

    # make deviation column
    df$deviation <- format_currency(df$magnitude, prefix = sign)
  }

  return(df)
}

#' @title Exec Account Dataframe Filler
#' @description Fills in empty rows for any Exec account dataframes with less than 5 rows
#' @author Stefanie Molin
#'
#' @param df Dataframe with Exec account data
#'
#' @return Dataframe with 5 rows
#'
#' @export
exec_account_df_filler <- function(df){
  while(nrow(df) < 5){
    df <- rbind(df, exec_account_df_empty_row())
  }
  return(df)
}


#' @title Find Exec Client Gainers and Losers
#' @description Identifies gainers and losers
#' @author Stefanie Molin
#'
#' @param df Dataframe with client RexT data
#'
#' @return List containing 2 dataframes: gainers and losers
#'
#' @export
exec_find_gainers_losers <- function(df){

  # mark whether the changes are positive or negative
  df$positive <- df$percentDiff > 0

  # if there were alerts, add up to 5 (can be 0) each to email for top gainers and top losers
  if(nrow(df) > 0){
    # separate gainers and losers and isolate valid alerts
    gainers <- df[df$positive == TRUE, c("client", "actual", "expectedValue", "percentDiff", "magnitude")]
    losers <- df[df$positive == FALSE, c("client", "actual", "expectedValue", "percentDiff", "magnitude")]

    # handle missing rows
    gainers <- exec_account_df_builder(gainers, isGainer = TRUE)
    losers <- exec_account_df_builder(losers, isGainer = FALSE)

    gainers <- exec_account_df_filler(gainers)
    losers <- exec_account_df_filler(losers)
  } else{
    gainers <- exec_initialize_account_df()
    losers <- exec_initialize_account_df()
  }
  return(list(gainers = gainers, losers = losers))
}
