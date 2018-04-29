##################################################
######         Formatting Library           ######
######         by: Stefanie Molin           ######
####                                          ####
## Utility functions for formatting SCOPE data. ##
##################################################

#' @title Format Numbers
#' @description Format values with x digits after the decimal and comma separators at thousands
#' @author Stefanie Molin
#'
#' @param values Vector of values to be formatted
#' @param digits How many digits to round to and display
#'
#' @return Vector of characters
#'
#' @export

format_number <- function(values, digits = 2){
  return(format(round(values, digits = digits), big.mark = ",", scientific = FALSE, trim = TRUE, nsmall = digits))
}

#' @title Format as Currency
#' @description Format values with the currency symbol in front and 2 digits after the decimal and comma separators at thousands
#' @author Stefanie Molin
#'
#' @param values Vector of values to be formatted
#' @param prefix Character to add to the front of the values (no space will be put between value here and the currency value)
#' @param symbol Symbol for the currency, defaults to "$" (added before the number)
#' @param currency_code Code for the currency to be appended at end for disambiguation of currency being shown
#'
#' @return Vector of characters
#'
#' @export
#'
format_currency <- function(values, prefix = "", symbol = "$", currency_code = ""){

  # see which are negative
  isNegative <- values < 0

  # determine prefix
  prefix <- ifelse(isNegative, "-", prefix)

  # store only absolute values
  values <- abs(values)

  # modify currency_code
  if(!identical(currency_code, "")){
    currency_code <- paste0(" ", currency_code)
  }

  # return formatted as dollar
  return(paste0(prefix, symbol, format_number(values), currency_code))
}


#' @title Format as Percent
#' @description Format values with "\%" at end, x digits after the decimal and comma separators at thousands, optional "+" in front
#' @author Stefanie Molin
#'
#' @param values Vector of values to be formatted (as decimals)
#' @param showPlusSign Whether or not to show "+" in front of positive percentages
#' @param digits How many digits to round to and display
#'
#' @return Vector of characters
#'
#' @export
format_percent <- function(values, showPlusSign = FALSE, digits = 2){

  # mulitply by 100 and round
  values <- round(values * 100, digits = digits)

  # format
  if(showPlusSign){
    percent <- paste0(ifelse(values > 0, "+", ""), format_number(values, digits = digits), "%")
  } else{
    percent <- paste0(format_number(values, digits = digits), "%")
  }
  return(percent)
}

#' @title Format in Basis Points (bps)
#' @description Format values with "bps" at end, x digits after the decimal and comma separators at thousands, optional "+" in front
#' @author Stefanie Molin
#'
#' @param values Vector of values to be formatted (as basis points)
#' @param showPlusSign Whether or not to show "+" in front of positive percentages
#' @param digits How many digits to round to and display
#'
#' @return Vector of characters
#'
#' @export
format_bps <- function(values, showPlusSign = FALSE, digits = 0){
  # round
  values <- round(values, digits = digits)

  # format
  if(showPlusSign){
    change <- paste0(ifelse(values > 0, "+", ""), format_number(values, digits = digits), " bps")
  } else{
    change <- paste0(format_number(values, digits = digits), " bps")
  }
  return(change)
}

#' @title Capitalize Name
#' @description Capitalize the first letter of each word in a string
#' @author Stefanie Molin
#'
#' @importFrom R.utils capitalize
#'
#' @param name String to modify
#'
#' @return String
#'
#' @export
capitalize_name <- function(name){
  return(paste(R.utils::capitalize(strsplit(tolower(name), " ")[[1]]), collapse = " "))
}

#' @title Format Current Date/Time
#' @description Return a string representing the datetime in the specified format for the designated timezone. Optionally show the timezone name.
#' @author Stefanie Molin
#'
#' @param format_string The string representing how to format the date. Defaults to YYYY-MM-DD
#' @param timezone String for the name of the timezone, i.e. "America/New_York"
#' @param showTimezone Boolean indicating whether or not to show the timezone name. Defaults to FALSE (not shown).
#'
#' @return String
#'
#' @export
format_datetime <- function(format_string = "%Y-%m-%d", timezone, showTimezone = FALSE){
  return(format(Sys.time(), format = format_string, tz = timezone, usetz = showTimezone))
}

#' @title Thousands Formatter
#'
#' @description Format numbers with thousands place to say "K" at end and "M" for millions
#'
#' @author Stefanie Molin
#' 
#' @return Formatted number(s)
#' 
#' @export
#'
thousands_formatter <- function(x){
  return(ifelse(x < 1000, scales::comma_format()(c(x)), 
                ifelse(x < 1000000, paste0(round(x/1000, 0), "K"), 
                       ifelse(x < 1000000000, paste0(round(x/1000000, 0), "M"), 
                              paste0(round(x/1000000000, 0), "B")))))
}