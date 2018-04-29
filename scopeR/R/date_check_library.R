##################################################
######          Date Check Library          ######
######          by: Stefanie Molin          ######
####                                          ####
## Functions to check if tables are up to date. ##
##################################################

#' @title Pull Max Dates Data
#' @description Pull data to check if detection can run.
#' @author Stefanie Molin
#'
#' @importFrom futile.logger flog.info
#'
#' @param username Vertica username
#' @param password Vertica password
#' @param scope_product Which SCOPE product the code is running in (determines error handling). Choices: "AS", "EXEC", "TS"
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
get_max_dates <- function(username, password, scope_product, last_try, test_mode, root_config_list, 
                          regional_config_list, sender, mailbox_password, slack_on){
  flog.info("Querying Vertica to check if data is up to date...")
  
  scope_product <- toupper(scope_product)
  
  if(scope_product == "AS"){
    yaml_section <- "web_as"
  } else if(scope_product == "TS"){
    yaml_section <- 'ts'
  } else{
    yaml_section <- 'exec'
  }
  
  # get query
  query <- modify_query(read_query_from_file(system.file("queries", 
                                                         "vertica_date_check.sql", 
                                                         package = "scopeR")),
                        sub_list = list(schemaList = regional_config_list[[yaml_section]][["query"]][["schemaList"]],
                                        tableList = regional_config_list[[yaml_section]][["query"]][["tableList"]],
                                        todaysDate = paste0("'", 
                                                            format_datetime(timezone = regional_config_list[[yaml_section]][["timezone"]]),
                                                            "'")))
  
  
  # query max dates for the tables used in the query
  check_dates <- query_try_block(query = query, 
                                 df_name = "check_dates", username = username, password = password, timeout = 60, 
                                 scope_product = scope_product, last_try = last_try, test_mode = test_mode, 
                                 root_config_list = root_config_list, regional_config_list = regional_config_list,
                                 sender = sender, mailbox_password = mailbox_password, slack_on = slack_on)
  
  return(check_dates)
}

#' @title Check if Data is Late
#' @description Checks the results of the check dates query for a specific table and determines if that table is late
#' @author Stefanie Molin
#'
#' @param data Dataframe containing 2 columns: the name of the table (table_name) and the update_time. This table is the result of the check dates query.
#' @param table_name Name(s) of the table to check (must be in data)
#'
#' @return Boolean indicating whether or not that table is late.
#'
#' @export
data_late <- function(data, table_name){
  is_late <- FALSE
  for(table in table_name){
    if(!(table %in% data$table_name)){
      is_late <- TRUE
      break
    }
  }
  return(is_late)
}

#' @title Notify of Late Data
#' @description Send the data is late email to the appropriate recipients based on which SCOPE is running (and slack notification if desired)
#' @author Stefanie Molin
#'
#' @param scope_product Which SCOPE product the code is running in (determines recipients). Choices: "AS", "EXEC", "TS"
#' @param root_config_list List read from the root config YAML file
#' @param regional_config_list List read from the regional config YAML file
#' @param test_mode Boolean indicating whether or not SCOPE is in test mode
#' @param sender Address of the sender of the error emails.
#' @param mailbox_password Password of the sender
#' @param slack_on Boolean indicating whether or not to send notifications to slack
#'
notify_late_data <- function(scope_product, root_config_list, regional_config_list,
                             test_mode, sender, mailbox_password, slack_on = FALSE){
  send_late_data_email(scope_product = scope_product, root_config = root_config_list,
                       regional_config = regional_config_list,
                       test_mode = test_mode, sender = sender, mailbox_password = mailbox_password)

  if(slack_on){
    slack_data_is_late_alert(test_mode = test_mode, root_config_list = root_config_list,
                             regional_config_list = regional_config_list, scope_product = scope_product)
  }
}

#' @title Handle Late Data Notifications
#' @description Handles late data notifications when it is the last try and when it will try again later.
#' @author Stefanie Molin
#'
#' @importFrom futile.logger flog.info
#'
#' @param last_try Boolean whether or not this is the last attempt to run SCOPE
#' @param scope_product Which SCOPE product the code is running in (determines recipients). Choices: "AS", "EXEC", "TS"
#' @param root_config_list List read from the root config YAML file
#' @param regional_config_list List read from the regional config YAML file
#' @param test_mode Boolean indicating whether or not SCOPE is in test mode
#' @param sender Address of the sender of the error emails.
#' @param mailbox_password Password of the sender
#' @param slack_on Boolean indicating whether or not to send notifications to slack
#'
#' @export
late_data_notifications <- function(last_try, scope_product, root_config_list, regional_config_list, test_mode, sender, mailbox_password, slack_on = FALSE){
  if(last_try){
    notify_late_data(scope_product = scope_product, root_config_list = root_config_list, regional_config_list = regional_config_list,
                     test_mode = test_mode, sender = sender, mailbox_password = mailbox_password, slack_on = slack_on)

    # status update
    futile.logger::flog.info(sprintf("[EXPECTED ERROR] SCOPE %s has failed to run due to data delays in each of the allowed restarts.", scope_product), call. = FALSE)
  } else{
    futile.logger::flog.info("[EXPECTED ERROR] Data is late", call. = FALSE)
  }
}
