#########################################################
######              Email Utilities                ######
######    by: Francois Pillot & Stefanie Molin     ######
####                                                 ####
##  Utility functions for sending emails.              ##
#########################################################

#' @title Send Email
#' @description Wrapper for the send.mail function, adding a loop for multiple trials
#' @author Francois Pillot
#'
#' @importFrom mailR send.mail
#' @importFrom futile.logger flog.info
#'
#' @param from Sender address
#' @param to Email recipient address
#' @param cc CC recipient address
#' @param bcc BCC recipient address
#' @param max_retry_time In seconds, how long to retry sending email
#' @param password Password for the from address
#' @param timeout In seconds, how long to wait in case of email server timeout
#' @param attachments Path(s) to files to attach.
#' @param subject The subject of the email
#' @param body The body of the email
#' @param encoding The encoding to use in the email
#' @param html Boolean indicating whether or not the email contents will be in HTML
#' @param inline Boolean indicating whether or not the email contains inline images.
#' @param send Boolean indicating whether or not to send the email.
#'
#' @return Boolean indicating whether or not sending the email was successful
#'
send_email <- function(from, to, cc = NULL, bcc = NULL, max_retry_time = 300, password,
                      timeout = 300000, attachments = NULL, subject = "", body = "", encoding = "utf-8",
                      html = FALSE, inline = FALSE, send = TRUE){

  first_time <- Sys.time()
  while(as.double(Sys.time() - first_time, units = "secs") <= max_retry_time){
    t <- try(mailR::send.mail(
      from = from,
      to = to,
      cc = cc,
      bcc = bcc,
      subject = subject,
      body = body,
      html = html,
      inline = inline,
      attach.files = attachments,
      smtp = list(host.name = "smtp.office365.com", port = 587, user.name = from, passwd = password, tls = TRUE),
      authenticate = TRUE,
      encoding = encoding,
      send = send,
      timeout = timeout
    ), silent = TRUE)

    if(!("try-error" %in% class(t))){
      futile.logger::flog.info(paste("Email successfully sent to", to, "with subject:", subject))
      return(TRUE)
    }
  }
  return(FALSE)
}

#' @title Send Email Once
#' @description Ensure that each recpient gets one email max
#' @author Francois Pillot
#'
#' @importFrom yaml yaml.load_file as.yaml
#' @importFrom futile.logger flog.info
#'
#' @param from Sender address
#' @param to Email recipient address
#' @param cc CC recipient address
#' @param bcc BCC recipient address
#' @param max_retry_time In seconds, how long to retry sending email
#' @param password Password for the from address
#' @param timeout In seconds, how long to wait in case of email server timeout
#' @param attachments Path(s) to files to attach.
#' @param subject The subject of the email
#' @param body The body of the email
#' @param encoding The encoding to use in the email
#' @param html Boolean indicating whether or not the email contents will be in HTML
#' @param inline Boolean indicating whether or not the email contains inline images.
#' @param max_retry_message What to print to logs if function has exhausted its retries
#' @param history_file Path to the YAML history file containing the history of sent emails
#'
#' @return Sends email if not sent before and logs it, otherwise doesn't
#'
send_email_once <- function(from, to, cc = NULL, bcc = NULL, max_retry_time = 300, password,
                           timeout = 300000, attachments = NULL, subject = "", body = "", encoding = "utf-8",
                           html = FALSE, inline = FALSE, max_retry_message = "", history_file){

  # Use and check a history file to make sure the email is sent only once a day
  if(!file.exists(history_file)){
    file.create(history_file)
  }
  history <- yaml::yaml.load_file(history_file)
  if(typeof(history) != "list"){
    history <- list()
  }

  if(paste(to, subject) %in% history[[toString(format_datetime(timezone = "America/New_York"))]]){
    futile.logger::flog.info(paste("Not sending: Email already sent today to", to, "with subject:", subject))
    email_sent_already <- TRUE
  }else{
    email_sent_already <- FALSE
  }
  if(!email_sent_already){
    email_sent <- send_email(
      from = from,
      to = to,
      cc = cc,
      bcc = bcc,
      subject = subject,
      body = body,
      html = html,
      password = password,
      timeout = timeout,
      attachments = attachments,
      encoding = encoding,
      max_retry_time = max_retry_time,
      inline = inline
    )
    if(email_sent){
      history[[toString(format_datetime(timezone = "America/New_York"))]] = c(history[[toString(format_datetime(timezone = "America/New_York"))]], paste(to, subject))
      fileConn <- file(history_file)
      writeLines(yaml::as.yaml(history), fileConn)
      close(fileConn)
    } else{
      stop(max_retry_message, call. = FALSE)
    }
  }
}


#' @title Send Technical Error Email
#' @description Send the technical error email to the appropriate recipients based on which SCOPE is running
#' @author Stefanie Molin
#'
#' @param scope_product Which SCOPE product the code is running in (determines recipients). Choices: "AS", "EXEC", "TS"
#' @param root_config List read from the root config YAML file
#' @param regional_config List read from the regional config YAML file
#' @param test_mode Boolean indicating whether or not SCOPE is in test mode
#' @param sender Address of the sender of the error emails.
#' @param mailbox_password Password of the sender
#'
#' @return Sends email if not sent before and logs it, otherwise doesn't
#'
send_technical_error_email <- function(scope_product, root_config, regional_config, test_mode, sender, mailbox_password){
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

  if(identical(scope_product, "AS")){
    history_prefix <- ""
  } else{
    history_prefix <- paste0(tolower(scope_product), "_")
  }

  # modify email info
  if(test_mode){
    to <- root_config[["test_mode"]][["email"]]
    cc <- NULL
    bcc <- NULL
    subject <- paste(sprintf("[TEST] SCOPE %s %s Alerts", scope_product, regional_config[[yaml_name]][["geo"]]), "Unable to Run Today")
  } else{
    if("beta" %in% names(regional_config) && regional_config[["beta"]][["active"]]){
      to <- regional_config[["beta"]][["testers"]]
    } else if("alias" %in% names(regional_config[[yaml_name]])){
      to <- regional_config[[yaml_name]][["alias"]]
    } else{
      to <- regional_config[["exec"]][["recipients"]]
    }
    if("cc" %in% names(regional_config[[yaml_name]])){
      cc <- regional_config[[yaml_name]][["cc"]]
    } else{
      cc <- NULL
    }
    bcc <- root_config[["maintainers"]]
    subject <- paste(sprintf("SCOPE %s %s Alerts", scope_product, regional_config[[yaml_name]][["geo"]]), "Unable to Run Today")
  }

  # send emails
  suppressWarnings(
    send_email_once(
      from = sender,
      to = to,
      subject = subject,
      body = paste(readLines("templates/scope_error.html", warn = FALSE), collapse = ""),
      html = TRUE,
      cc = cc,
      bcc = bcc,
      password = mailbox_password,
      max_retry_message = sprintf("[INFRASTRUCTURE ERROR] SCOPE %s failed to send technical error email.", scope_product),
      history_file = root_config[["email_history"]][[paste0(history_prefix, "email_sent_history")]]
    )
  )
}

#' @title Send Late Data Email
#' @description Send the data is late email to the appropriate recipients based on which SCOPE is running
#' @author Stefanie Molin
#'
#' @param scope_product Which SCOPE product the code is running in (determines recipients). Choices: "AS", "EXEC", "TS"
#' @param root_config List read from the root config YAML file
#' @param regional_config List read from the regional config YAML file
#' @param test_mode Boolean indicating whether or not SCOPE is in test mode
#' @param sender Address of the sender of the error emails.
#' @param mailbox_password Password of the sender
#'
#' @return Sends late data email if not sent before and logs it, otherwise doesn't
#'
send_late_data_email <- function(scope_product, root_config, regional_config, test_mode, sender, mailbox_password){
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

  if(identical(scope_product, "AS")){
    history_prefix <- ""
  } else{
    history_prefix <- paste0(tolower(scope_product), "_")
  }

  # determine email fields
  if(test_mode){
    to <- root_config[["test_mode"]][["email"]]
    cc <- NULL
    bcc <- NULL
    subject <- paste("[TEST] Data Delayed -- No", sprintf("SCOPE %s %s Alerts", scope_product, regional_config[[yaml_name]][["geo"]]))
  } else{
    if("beta" %in% names(regional_config) && regional_config[["beta"]][["active"]]){
      to <- regional_config[["beta"]][["testers"]]
    } else if("alias" %in% names(regional_config[[yaml_name]])){
      to <- regional_config[[yaml_name]][["alias"]]
    } else{
      to <- regional_config[["exec"]][["recipients"]]
    }
    if("cc" %in% names(regional_config[[yaml_name]])){
      cc <- regional_config[[yaml_name]][["cc"]]
    } else{
      cc <- NULL
    }
    bcc <- root_config[["maintainers"]]
    subject <- paste("Data Delayed -- No", sprintf("SCOPE %s %s Alerts", scope_product, regional_config[[yaml_name]][["geo"]]))
  }

  # send message
  suppressWarnings(
    send_email_once(
      from = sender,
      to = to,
      subject = subject,
      body = paste(readLines("templates/scope_delay.html", warn = FALSE), collapse = ""),
      html = TRUE,
      cc = cc,
      bcc = bcc,
      password = mailbox_password,
      max_retry_message = sprintf("[INFRASTRUCTURE ERROR] SCOPE %s failed to send data is late email.", scope_product),
      history_file = root_config[["email_history"]][[paste0(history_prefix, "email_sent_history")]]
    )
  )
}

#' @title Send No Alerts Found Email
#' @description Send email to maintainers that there were no alerts triggered
#' @author Stefanie Molin
#'
#' @param scope_product Which SCOPE product the code is running in (determines recipients). Choices: "AS", "EXEC", "TS"
#' @param test_mode Boolean indicating whether or not SCOPE is in test mode
#' @param root_config_list List read from the root config YAML file
#' @param regional_config_list List read from the regional config YAML file
#' @param sender Address of the sender of the error emails.
#' @param mailbox_password Password of the sender
#'
#' @return Sends late data email if not sent before and logs it, otherwise doesn't
#'
#' @export
send_no_alerts_found_email <- function(scope_product, test_mode, root_config_list, regional_config_list,
                                       sender, mailbox_password){
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

  if(identical(scope_product, "AS")){
    history_prefix <- ""
  } else{
    history_prefix <- paste0(tolower(scope_product), "_")
  }

  if(test_mode){
    to <- root_config_list[["test_mode"]][["email"]]
    subject <- sprintf("[TEST] No Alerts to Send for SCOPE %s %s", scope_product, regional_config_list[[yaml_name]][["geo"]])
  } else{
    to <- root_config_list[["maintainers"]]
    subject <- sprintf("No Alerts to Send for SCOPE %s %s", scope_product, regional_config_list[[yaml_name]][["geo"]])
  }
  # status update
  flog.info(sprintf("No alerts today for SCOPE %s %s!", scope_product, regional_config_list[[yaml_name]][["geo"]]))
  suppressWarnings(
    send_email_once(
      from = sender,
      to = to,
      subject = subject,
      body = "There are no alerts for today.",
      password = mailbox_password,
      max_retry_message = sprintf("[INFRASTRUCTURE ERROR] SCOPE %s failed to send email that there are no alerts today.",
                                 scope_product),
      history_file = root_config_list[["email_history"]][[paste0(history_prefix, "email_sent_history")]]
    )
  )
}

#' @title Build and Send AS Alerts
#' @description Build and send all SCOPE AS alerts
#' @author Stefanie Molin
#'
#' @importFrom futile.logger flog.info
#' @importFrom ggplot2 ggsave
#' @import dplyr
#'
#' @param client_stats Dataframe containing all the data collected during the run
#' @param client_alerts Dataframe containing all the alerts triggered for the day
#' @param root_config_list List read from the root config YAML file
#' @param regional_config_list List read from the regional config YAML file
#' @param run_date Date for which the alerts were calculated (today's date in most cases)
#' @param test_mode Boolean indicating whether or not SCOPE is in test mode
#' @param sender Address of the sender of the error emails.
#' @param mailbox_password Password of the sender
#'
#' @return Builds and sends SCOPE alerts for all AS with alerts on their accounts. No return.
#'
#' @export
build_and_send_AS_alerts <- function(client_stats, client_alerts, root_config_list, regional_config_list, run_date, test_mode, sender, mailbox_password){
  # filter the stats to just those with alerts
  client_stats <- client_stats %>% dplyr::filter(combo %in% client_alerts$combo)

  # load templates
  main_alert_HTML <- paste(readLines("templates/main_as_alert_v3.html", warn = FALSE), collapse = "")
  client_section_HTML <- paste(readLines("templates/client_section.html", warn = FALSE), collapse = "")
  first_client_alert_HTML <- paste(readLines("templates/first_alert_for_client.html", warn = FALSE), collapse = "")
  additional_client_alert_HTML <- paste(readLines("templates/multiple_alerts_per_client.html", warn = FALSE), collapse = "")

  # send out alerts to all AS on the accounts (loop through the AS_email field)
  for(AS in unique(client_alerts$AS_email)){
    # pull out alerts to for AS
    alerts_for_AS <- client_alerts %>% dplyr::filter(AS_email == AS) %>% dplyr::select(combo, kpi, AS_name)

    # pull out matching data
    stats_for_alerts <- client_stats %>% dplyr::semi_join(alerts_for_AS, by = c("combo", "AS_name"))

    # grab AS name
    AS_name <- stats_for_alerts %>% dplyr::distinct(AS_name)

    # status update
    futile.logger::flog.info(paste("Preparing SCOPE AS Alert for", AS_name))

    # initialize alerts
    main_alert <- ""
    clients_section <- ""
    graph_filenames <- character()

    # loop through clients
    for(client in unique(alerts_for_AS$combo)){
      # determine the client number
      mod <- which(client == unique(alerts_for_AS$combo)) %% 3

      # define client color and border color
      if(mod == 1){
        # first color set
        client_color <- "#30395e"
        border_color <- "#272e4a"
      } else if(mod == 2){
        # second color set
        client_color <- "#4a6491"
        border_color <- "#40567d"
      } else{# mod == 0
        # third color set
        client_color <- "#85a5cc"
        border_color <- "#6f8aac"
      }

      # isolate alerts on that particular client
      alerts_for_client <- alerts_for_AS %>% dplyr::filter(combo == client)
      data <- stats_for_alerts %>% dplyr::filter(combo == client)
      client_alerts_for_email <- ""

      for(i in 1:nrow(alerts_for_client)){
        metric <- alerts_for_client[i, "kpi"]
        client_id <- data[i, "client_id"]

        # generate graph
        plot <- graph_for_email(data = data[data$combo == client,], metric = metric)
        plot_filename <- paste0(client_id,  "_", metric, ".png")
        suppressMessages(ggplot2::ggsave(plot,
                                         filename = plot_filename,
                                         bg = "transparent",
                                         dpi = 72,
                                         height = 8,
                                         width = 14,
                                         units = "cm"))
        graph_filenames <- c(graph_filenames, plot_filename)

        # generate Opera link
        opera_link <- paste0(regional_config_list[["email"]][["opera_url1"]], client_id,
                             regional_config_list[["email"]][["opera_url2"]])

        # generate Metis feedback links
        feedback_link <- paste0(root_config_list[["feedback_link"]][["base"]], root_config_list[["feedback_link"]][["AS_query_string"]])
        feedback_link <- gsub("<username>", unlist(strsplit(AS, "@"))[1], feedback_link)
        feedback_link <- gsub("<client_id>", client_id, feedback_link)
        feedback_link <- gsub("<date>", run_date, feedback_link)
        feedback_link <- gsub("<kpi>", ifelse(as.character(metric) == 'rext', 'client_rext', as.character(metric)), feedback_link)
        feedback_link <- gsub("<client_name>", client, feedback_link)
        feedback_link <- gsub("<series>", client, feedback_link) # this is only different from client for campaign alerts
        feedback_link <- gsub("<cost_center>", stats_for_alerts[i, "cost_center"], feedback_link)
        feedback_link <- gsub("<ranking>", stats_for_alerts[i, "ranking"], feedback_link)
        feedback_link <- gsub("<country>", stats_for_alerts[i, "country"], feedback_link)
        feedback_link <- gsub("<subregion>", stats_for_alerts[i, "subregion"], feedback_link)
        feedback_link <- gsub("<region>", stats_for_alerts[i, "region"], feedback_link)
        metis_true_alert_link <- URLencode(gsub("<is_alert>", "true", feedback_link))
        metis_false_alert_link <- URLencode(gsub("<is_alert>", "false", feedback_link))

        # generate Sherlock link
        sherlock_link <- paste0(root_config_list[["sherlock_link"]][["base"]], root_config_list[["sherlock_link"]][["AS_query_string"]])
        sherlock_link <- gsub("<kpi>", as.character(metric), sherlock_link)
        sherlock_link <- gsub("<client_id>", client_id, sherlock_link)
        sherlock_link <- gsub("<start_date>", Sys.Date() - 16, sherlock_link)
        sherlock_link <- gsub("<end_date>", Sys.Date() - 1, sherlock_link)

        # add first alert for the client (first_client_alert)
        if(i == 1){
          # fill in HTML
          first_client_alert <- gsub("clientNameHere", client, first_client_alert_HTML)
          first_client_alert <- gsub("anomalyMetricHere", toupper(metric), first_client_alert)
          first_client_alert <- gsub("graphLocationHere", plot_filename, first_client_alert)
          first_client_alert <- gsub("dashboardLinkHere", opera_link, first_client_alert)
          first_client_alert <- gsub("MetisTrueAlertLinkHere", metis_true_alert_link, first_client_alert)
          first_client_alert <- gsub("MetisFalseAlertLinkHere", metis_false_alert_link, first_client_alert)
          first_client_alert <- gsub("SherlockLinkHere", sherlock_link, first_client_alert)
        } else{
          # add remaining alerts to client if applicable (additional_client_alerts)
          additional_client_alert <- gsub("anomalyMetricHere", toupper(metric), additional_client_alert_HTML)
          additional_client_alert <- gsub("graphLocationHere", plot_filename, additional_client_alert)
          additional_client_alert <- gsub("dashboardLinkHere", opera_link, additional_client_alert)
          additional_client_alert <- gsub("MetisTrueAlertLinkHere", metis_true_alert_link, additional_client_alert)
          additional_client_alert <- gsub("MetisFalseAlertLinkHere", metis_false_alert_link, additional_client_alert)
          additional_client_alert <- gsub("SherlockLinkHere", sherlock_link, additional_client_alert)

          # change border color when adding additional alerts, border color depends on client color
          additional_client_alert <- gsub("borderColorHere", border_color, additional_client_alert)
        }
        # assemble all alerts for that client
        client_alerts_for_email <- paste(client_alerts_for_email, ifelse(i == 1, first_client_alert, additional_client_alert))
      }
      # add client's alerts as client_section
      clients_section <- paste(clients_section, gsub("clientAlertsHere", client_alerts_for_email, client_section_HTML))

      # color client section the appropriate shade
      clients_section <- gsub("clientColorHere", client_color, clients_section)
    }
    # add the all the client section pieces to the main alert
    main_alert <- gsub("alertsHere", clients_section, main_alert_HTML)
    alertHTMLFile <- "scope_alert.html"
    write(main_alert, alertHTMLFile)

    # modify if test mode
    if(test_mode){
      subject <- paste("[TEST] SCOPE AS Alert for", capitalize_name(AS_name))
      to <- root_config_list[["test_mode"]][["email"]]
      cc <- NULL
      bcc <- NULL
    } else{
      to <- AS
      subject <- paste("SCOPE AS Alert for", capitalize_name(AS_name))
      if("cc" %in% names(regional_config_list[["web_as"]])){
        cc <- regional_config_list[["web_as"]][["cc"]]
      } else{
        cc <- NULL
      }
      bcc <- root_config_list[["maintainers"]]
    }

    # send alert
    suppressWarnings(
      send_email_once(
        from = sender,
        to = to,
        cc = cc,
        bcc = bcc,
        subject = subject,
        body = alertHTMLFile,
        html = TRUE,
        inline = TRUE,
        password = mailbox_password,
        max_retry_message = "[INFRASTRUCTURE ERROR] SCOPE has failed to send an alert.",
        history_file = root_config_list[["email_history"]][["email_sent_history"]]
      )
    )

    # clean up
    file.remove(alertHTMLFile, graph_filenames)
  }
}

#' @title Send AS Summary Stats
#' @description Send summary of AS alerts via email (and slack if desired).
#' @author Stefanie Molin
#'
#' @importFrom futile.logger flog.info
#'
#' @param data Dataframe of accounts triggered and for which metric
#' @param test_mode Boolean whether or not SCOPE is running in test mode or not
#' @param root_config_list List from the root_config file.
#' @param regional_config_list List from the regional_config file.
#' @param sender Email address to send from
#' @param mailbox_password Password for the sender
#' @param slack_on Boolean indicating whether or not information should be sent to slack
#'
#' @export
send_as_summary_stats <- function(data, test_mode, root_config_list, regional_config_list,
                                  sender, mailbox_password, slack_on = FALSE){
  # status update
  futile.logger::flog.info("Gathering summary stats...")

  for(ranking in unique(data$ranking)){
    clientRanking <- gsub(" ", "", paste(tolower(ranking)))
    triggeredAccounts <- data[data$ranking == ranking, c("AS_name", "combo", "kpi")]
    triggeredAccounts <- triggeredAccounts[order(triggeredAccounts$AS_name, triggeredAccounts$combo),]
    emailsSent <- length(unique(triggeredAccounts$AS_name))

    #prepare to store as csv
    rownames(triggeredAccounts) <- NULL
    triggeredAccountsFile <- "triggered_accounts.csv"
    write.table(triggeredAccounts, triggeredAccountsFile, sep = ",", row.names = FALSE)

    if(test_mode){
      to <- root_config_list[["test_mode"]][["email"]]
      cc <- NULL
      subject <- paste("[TEST] SCOPE Alerts", regional_config_list[["web_as"]][["geo"]], ranking, "Daily Stats",
                       format_datetime(format_string = "%a %e %b %Y %I:%M%p", timezone = regional_config_list[["web_as"]][["timezone"]], showTimezone = TRUE))
    } else{
      to <- root_config_list[["maintainers"]]
      if("summary_email" %in% names(regional_config_list[["web_as"]])){
        if(clientRanking %in% names(regional_config_list[["web_as"]][["summary_email"]])){
          cc <- regional_config_list[["web_as"]][["summary_email"]][[clientRanking]]
        } else{
          cc <- NULL
        }
      } else{
        cc <- NULL
      }
      subject <- paste("SCOPE Alerts", regional_config_list[["web_as"]][["geo"]], ranking, "Daily Stats",
                       format_datetime(format_string = "%a %e %b %Y", timezone = regional_config_list[["web_as"]][["timezone"]], showTimezone = FALSE))
    }
    body <- paste0("Emails Sent: ", emailsSent, "\n",
                   "Total Alerts Triggered: ", nrow(triggeredAccounts), "\n\n", "Recap of triggered accounts/alerts attached.")

    suppressWarnings(
      send_email_once(
        from = sender,
        to = to,
        cc = cc,
        subject = subject,
        body = body,
        password = mailbox_password,
        attachments = triggeredAccountsFile,
        max_retry_message = "[INFRASTRUCTURE ERROR] SCOPE has failed to send summary email.",
        history_file = root_config_list[["email_history"]][["email_sent_history"]]
      )
    )

    # remove file
    file.remove(triggeredAccountsFile)

    # ping slack channel
    if(slack_on){
      send_as_summary_to_slack(accounts_triggered = triggeredAccounts,
                               regional_config_list = regional_config_list,
                               root_config_list = root_config_list,
                               ranking = ranking, test_mode = test_mode)
    }
  }
}

#' @title Send TS Summary Stats
#' @description Send summary of TS alerts via email.
#' @author Stefanie Molin
#'
#' @importFrom futile.logger flog.info
#'
#' @param eventSiteAlertsDF Dataframe of tag-level triggered alerts
#' @param test_mode Boolean whether or not SCOPE is running in test mode or not
#' @param root_config_list List from the root_config file.
#' @param regional_config_list List from the regional_config file.
#' @param sender Email address to send from
#' @param mailbox_password Password for the sender
#' @param slack_on Boolean whether or not to send alerts to slack channel
#'
#' @export
send_ts_summary_stats <- function(eventSiteAlertsDF, test_mode,
                                  root_config_list, regional_config_list, sender, mailbox_password, slack_on = FALSE){
  # status update
  futile.logger::flog.info("Sending summary email...")

  triggeredAccounts <- eventSiteAlertsDF[order(eventSiteAlertsDF$TS_name, eventSiteAlertsDF$partner_name,
                                               eventSiteAlertsDF$event_name, eventSiteAlertsDF$site_type),]
  emailsSent <- length(unique(eventSiteAlertsDF$TS_name))

  # prepare to store as csv
  rownames(triggeredAccounts) <- NULL
  triggeredAccountsFile <- "triggered_accounts.csv"
  write.table(triggeredAccounts, triggeredAccountsFile, sep = ",", row.names = FALSE, fileEncoding = "UTF-8")

  if(test_mode){
    to <- root_config_list[["test_mode"]][["email"]]
    cc <- NULL
    subject <- paste("[TEST] SCOPE TS Alerts", regional_config_list[["ts"]][["geo"]], "Daily Stats",
                     format_datetime(format_string = "%a %e %b %Y",
                            timezone = regional_config_list[["ts"]][["timezone"]], showTimezone = FALSE))
  } else{
    to <- root_config_list[["maintainers"]]
    if("summary_email" %in% names(regional_config_list[["ts"]])){
      cc <- regional_config_list[["ts"]][["summary_email"]]
    } else{
      cc <- NULL
    }
    subject <- paste("SCOPE TS Alerts", regional_config_list[["ts"]][["geo"]], "Daily Stats",
                     format_datetime(format_string = "%a %e %b %Y",
                            timezone = regional_config_list[["ts"]][["timezone"]], showTimezone = FALSE))
  }
  body <- paste0("Emails Sent: ", emailsSent, "\n", "Total Alerts Triggered: ", nrow(triggeredAccounts),
                 "\n\n", "Recap of triggered accounts/alerts attached.")

  suppressWarnings(
    send_email_once(
      from = sender,
      to = to,
      cc = cc,
      subject = subject,
      body = body,
      password = mailbox_password,
      attachments = triggeredAccountsFile,
      max_retry_message = "[INFRASTRUCTURE ERROR] SCOPE TS has failed to send summary email.",
      history_file = root_config_list[["email_history"]][["ts_email_sent_history"]]
    )
  )

  #remove file
  file.remove(triggeredAccountsFile)

  # ping slack channel
  if(slack_on){
    send_ts_summary_to_slack(accounts_triggered = triggeredAccounts,
                             regional_config_list = regional_config_list,
                             root_config_list = root_config_list,
                             ranking = ranking, test_mode = test_mode)
  }
}

#' @title Build and Send Exec
#' @description Build and send the SCOPE Exec email.
#' @author Stefanie Molin
#'
#' @importFrom futile.logger flog.info
#'
#' @param alerts String of alerts
#' @param email_data Dataframe of email data
#' @param accounts_list List containing accounts to use to determine gainers/losers section.
#' @param plot_list Nested lists containing file_name and plot for each region
#' @param highlight_color String containing the hexcode of the highlighted text color
#' @param regular_color String containing the hexcode of the regular text color
#' @param regional_config_list List of regional_config settings.
#' @param root_config_list List from the root_config file.
#' @param template Path to the HTML template.
#' @param test_mode Boolean indicating whether or not SCOPE Exec is running in test mode.
#' @param sender Email address to send from
#' @param mailbox_password Password for the sender
#'
#' @export
build_and_send_exec <- function(alerts, email_data, accounts_list,
                                plot_list, highlight_color, regular_color, regional_config_list, root_config_list, template,
                                test_mode, sender, mailbox_password){
  # status update
  futile.logger::flog.info("Preparing SCOPE Exec...")

  # define file name
  scopeAlertHTMLFile <- "templates/SCOPE Exec.html"

  scopeHTML <- build_scope_exec(alerts = alerts, email_data = email_data, accounts_list = accounts_list, plot_list = plot_list,
                                highlight_color = highlight_color, regular_color = regular_color,
                                regional_config_list = regional_config_list, template = template)

  write(scopeHTML, scopeAlertHTMLFile)

  # send email
  if(test_mode){
    subjectLine <- "[TEST] Americas SCOPE Alert"
    to <- root_config_list[["test_mode"]][["email"]]
    cc <- NULL
  } else{
    subjectLine <- "Americas SCOPE Alert"
    to <- regional_config_list[["exec"]][["recipients"]]
    cc <- regional_config_list[["exec"]][["cc"]]
  }
  # status update
  futile.logger::flog.info("Sending SCOPE Exec...")

  suppressWarnings(
    send_email_once(
      from = sender,
      to = to,
      cc = cc,
      subject = subjectLine,
      body = scopeAlertHTMLFile,
      html = TRUE,
      inline = TRUE,
      password = mailbox_password,
      max_retry_message = "[INFRASTRUCTURE ERROR] SCOPE Exec has failed to send alert email.",
      history_file = root_config_list[["email_history"]][["exec_email_sent_history"]]
    )
  )

  # status update
  futile.logger::flog.info("SCOPE Exec has finished! Cleaning up and updating run history...")

  # clean up
  file.remove(scopeAlertHTMLFile)
  for(region in names(plot_list)){
    file.remove(plot_list[[region]][["file_name"]])
  }
}
