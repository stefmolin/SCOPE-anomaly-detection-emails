##################################################
######            Logging Functions         ######
####                                          ####
## Functions for logging and checking history.  ##
##################################################

#' @title Log to the History File
#' @description Log when and which scripts have run
#' @author Francois Pillot
#' @author Stefanie Molin
#'
#' @importFrom yaml yaml.load_file as.yaml
#'
#' @param history_file Path of the YAML file containing the run history
#' @param script_name Name of the script currently running
#' @param geo Location currently running
#'
#' @return None, updates the history file if necessary
#'
#' @export
log_to_history <- function(history_file, script_name, geo = ""){
    if(!file.exists(history_file)){
      file.create(history_file)
      }
    history <- yaml::yaml.load_file(history_file)
    if(typeof(history) != "list"){
      history = list()
      }
    history[[toString(format_datetime(timezone = "America/New_York"))]] = c(history[[toString(format_datetime(timezone = "America/New_York"))]], paste(script_name, geo))
    fileConn <- file(history_file)
    writeLines(yaml::as.yaml(history), fileConn)
    close(fileConn)
}

#' @title Check If Script Ran
#' @description Check which scripts have run in which geos
#' @author Francois Pillot
#' @author Stefanie Molin
#'
#' @importFrom yaml yaml.load_file
#'
#' @param history_file Path of the YAML file containing the run history
#' @param script_name Name of the script to check for
#' @param geo Location to check for
#'
#' @return Boolean indicating whether or not the script_name / geo combination has already run
#'
#' @export
check_history <- function(history_file, script_name, geo = ""){
    if(!file.exists(history_file)){
      file.create(history_file)
      }
    history <- yaml::yaml.load_file(history_file)
    if(typeof(history) != "list"){
      history = list()
      }
    if(paste(script_name, geo) %in% history[[toString(format_datetime(timezone = "America/New_York"))]]){
        return(TRUE)
    } else{
        return(FALSE)
    }
}
