##################################################
######         Query Vertica Library        ######
######           by: Stefanie Molin         ######
####                                          ####
## Utility functions for querying Vertica in R. ##
##################################################

#' @title Query Vertica
#' @description Query Vertica and return a dataframe
#' @author Stefanie Molin
#'
#' @import RJDBC
#'
#' @param username Vertica username
#' @param query Vertica SQL query as a string
#' @param password Vertica password
#'
#' @return Dataframe with the results
#'
#' @export
query_vertica <- function(username, query, password){
  drv <- JDBC("com.vertica.jdbc.Driver", system.file("extdata", "vertica-jdbc-7.1.0.jar", package = "scopeR"))
  conn <- dbConnect(drv, "jdbc:vertica://<cluster>:<port>/<DB>?ConnectionLoadBalance=true&label=SCOPE",
                    username, password)
  data <- dbGetQuery(conn, query)
  dbDisconnect(conn)
  return(data)
}

#' @title Read Query from a File
#' @description Read query from a textfile and collapse lines into 1 string
#' @author Stefanie Molin
#'
#' @param filepath Path to the query (as a string)
#'
#' @return String containing the query.
#'
#' @export
read_query_from_file <- function(filepath){
  return(paste(readLines(filepath, warn = FALSE), collapse = " "))
}

#' @title Modify Query
#' @description Modify a query by replacing keywords with new values
#' @author Stefanie Molin
#'
#' @param query The query string.
#' @param sub_list A list where the names are the words to replace in the query and the values is what to change them to.
#'
#' @return String containing the modified query.
#'
#' @export
modify_query <- function(query, sub_list){
  for(item in names(sub_list)){
    query <- gsub(item, sub_list[[item]], query)
  }
  return(query)
}

#' @title Query Try Block
#' @description Try to query Vertica for a specific query and handle notifications if not working
#' @author Stefanie Molin
#'
#' @importFrom slackr slackrMsg
#' @importFrom futile.logger flog.warn flog.info
#'
#' @param query Vertica SQL query as a string
#' @param df_name Name of the dataframe you are querying for (this will only be used for logging purposes)
#' @param username Vertica username
#' @param password Vertica password
#' @param timeout In seconds, how long should R keep trying to query Vertica for?
#' @param scope_product Which SCOPE product the code is running in (determines error handling). Choices: "AS", "EXEC", "TS"
#' @param last_try Boolean indicating whether or not this is the last try for the script as a whole
#' @param test_mode Boolean indicating whether or not SCOPE is in test mode
#' @param root_config_list List read from the root config YAML file
#' @param regional_config_list List read from the regional config YAML file
#' @param sender Address of the sender of the error emails.
#' @param mailbox_password Password of the sender
#' @param slack_on Boolean indicating whether or not current SCOPE configuration can post to slack. Defaults to FALSE
#' @param send_error_email Boolean indicating whether or not an error email should be sent out if the query fails.
#' Defaults to TRUE, set to FALSE if the results of the query aren't required to finish running successfully.
#'
#' @return FALSE if the query fails and no error email will be sent, otherwise dataframe of results
#'
#' @export
query_try_block <- function(query, df_name, username, password, timeout, scope_product, last_try,
                      test_mode, root_config_list, regional_config_list,
                      sender, mailbox_password, slack_on = FALSE, send_error_email = TRUE){


  # error handling for incorrect input on products
  scope_product <- toupper(scope_product)

  if(!(scope_product %in% c("AS", "EXEC", "TS"))){
    # scope_product can only be "AS", "EXEC", or "TS"
    stop("Not a valid value for scope_product. Don't know how to send error email", call. = FALSE)
  }

  # determine how to modify YAML lookups based on product
  if(identical(scope_product, "AS")){
    yaml_name <- "web_as"
  } else if(identical(scope_product, "TS")){
    yaml_name <- "ts"
  } else{
    yaml_name <- "exec"
  }

  # try the query
  df <- try(query_vertica(username, query, password))

  # retry query if failed
  firstAttempt <- Sys.time()

  # keep retrying to query after a failed attempt or empty results
  while("try-error" %in% class(df) || is.null(nrow(df)) || nrow(df) == 0){
    lastAttempt <- Sys.time()

    # don't try again if time limit was exceeded or results came back empty since that is a data issue.
    if(as.double(lastAttempt - firstAttempt, units = "secs") >= timeout || (!is.null(nrow(df)) && nrow(df) == 0)){
      # handle cases when we should not stop the script or send the error email
      if(!send_error_email){
        # Display error and continue
        suppressWarnings(futile.logger::flog.info(head(df)))
        futile.logger::flog.warn(sprintf("SCOPE %s has failed to query Vertica for dataframe %s.", scope_product, df_name))
        df <- FALSE
        break
      }

      # send out the technical error email on last try if Infrastructure error occurs
      if(last_try){
        # send email that there was a technical issue
        suppressWarnings(send_technical_error_email(scope_product, root_config_list, regional_config_list, test_mode,
                                                    sender, mailbox_password))
        if(slack_on){
          if(test_mode){
            channel <- root_config_list[["test_mode"]][["channel"]]
          } else {
            channel <- as.vector(unlist(regional_config_list[[yaml_name]][["channel"]]))
          }
          for(slackChannel in channel){
            suppressWarnings(slackr::slackrMsg(txt = sprintf("SCOPE %s can't run today due to issues querying Vertica :disappointed:", scope_product),
                                       channel = slackChannel))
          }
        }

      }

      # Display error and stop
      suppressWarnings(futile.logger::flog.info(head(df)))
      stop(sprintf("[INFRASTRUCTURE ERROR] SCOPE %s has failed to query Vertica for dataframe %s.", scope_product, df_name), call. = FALSE)

      } else{
        # time to wait before requerying
        delay <- 30
        futile.logger::flog.info(sprintf("Waiting %d seconds before retrying to query Vertica...", delay))
        Sys.sleep(delay)

        # retry to query
        futile.logger::flog.info(sprintf("Retrying to query Vertica for dataframe %s ...", df_name))
        df <- try(query_vertica(username, query, password), silent = TRUE)
    }
  }
  return(df)
}
