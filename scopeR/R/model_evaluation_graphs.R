##################################################
######        Model Evaluation Library      ######
######           by: Stefanie Molin         ######
####                                          ####
## Generate graphs for all alerts to evaluate   ##
## model accuracy.                              ##
##################################################

#' @title AS Alert Graphs
#' @description Generate graphs for all alerts using a given model for visual inspection.
#' @author Stefanie Molin
#'
#' @import ggplot2
#' @importFrom dplyr %>% select filter
#' @importFrom stringr str_split
#' @importFrom scales percent dollar comma
#' @importFrom stringr str_replace_all
#'
#' @param alertsDF Dataframe containing 1 row per alert with the client name (or client campaign combo) and the alerts column.
#' @param data The dataframe containing the daily data used to run the model and check for alerts.
#' @param folder_name Folder name of where to save the plots
#'
#' @return Graphs for all non-"Negative RexT" alerts using the given model.
#'
#' @export
as_alert_graphs <- function(alertsDF, data, folder_name){
  # clean up alerts dataframe
  alertsDF <- alertsDF %>%
    dplyr::select(combo, alerts) %>%
    dplyr::filter(!(alerts %in% "Negative RexT"))

  # start graphing
  for(i in 1:nrow(alertsDF)){
    alertVector <- stringr::str_split(alertsDF[i, "alerts"], ", ")[[1]]
    for(alert in alertVector){
      if(identical(alert, "Negative RexT")){
        next
        # go to next alert if the alert is Negative RexT since that isn't the model
      }
      if(identical(alert, "Drop in Site Events")){
        alert <- "site_events"
      }
      # format data for graph
      df <- data[data$combo == alertsDF[i, "combo"], c("day", tolower(alert))]
      names(df) <- c("day", "alert")

      # make plot
      p <- df %>%
        ggplot2::ggplot(aes(x = as.Date(day), y = alert)) +
        ggplot2::geom_line() +
        ggplot2::labs(x = "Date", y = alert) +
        ggplot2::scale_y_continuous(labels = switch(tolower(alert),
                                                    cos = scales::percent,
                                                    ctr = scales::percent,
                                                    cr = scales::percent,
                                                    spend = scales::dollar,
                                                    site_events = scales::comma))

      # save plot
      ggplot2::ggsave(paste0(folder_name, "/", stringr::str_replace_all(alertsDF[i, "combo"], ":", ""),
                             " ", alert, ".png"), p)
    }
  }
}

#' @title TS Alert Graphs
#' @description Generate graphs for all alerts using a given model for visual inspection.
#' @author Stefanie Molin
#'
#' @import ggplot2
#' @importFrom dplyr %>%
#' @importFrom scales comma
#'
#' @param tagAlerts Dataframe containing 1 row per alert with the client name (or client tag combo) and the alerts column.
#' @param data The dataframe containing the daily data used to run the model and check for alerts.
#' @param folder_name Folder name of where to save the plots
#'
#' @return Graphs for all alerts using the given model.
#'
#' @export
ts_alert_graphs <- function(tagAlerts, data, folder_name){
  for(i in 1:nrow(tagAlerts)){
    df <- data[data$combo == tagAlerts[i, "combo"],]
    # make plot
    p <- df %>%
      ggplot2::ggplot(aes(x = as.Date(day), y = events)) +
      ggplot2::geom_line() +
      ggplot2::labs(x = "Date", y = "Site Events") +
      ggplot2::scale_y_continuous(labels = scales::comma) +
      ggplot2::coord_cartesian(ylim = c(0, 1.1 * max(df$events, na.rm = TRUE)))

    # save plot
    ggplot2::ggsave(paste0(folder_name, "/", tagAlerts[i, "combo"], ".png"), p)
  }
}

#' @title AS Metric Graphs
#' @description Generate graphs for all alerts using a given model for visual inspection.
#' @author Stefanie Molin
#'
#' @import ggplot2
#' @importFrom dplyr %>%
#' @importFrom scales comma dollar percent
#' @importFrom stringr str_replace_all
#'
#' @param combosDF Dataframe containing per client name (or client campaign combo).
#' @param data The dataframe containing the daily data used to run the model and check for alerts.
#' @param check_for_alert_date The date you went to check for anomalies.
#' @param folder_name Folder name of where to save the plots
#'
#' @return Graphs for all possible combinations of client (or campaign) and metric saved in specified folder
#' and returns all the client + metric combos and whether or not eligible they are for detection that day.
#'
#' @export
#'
as_metric_graphs <- function(combosDF, data, check_for_alert_date, folder_name){
  # track eligibility
  eligibility <- data.frame(combo = character(0), metric = character(0), isEligible = logical(0), stringsAsFactors = FALSE)

  # start graphing
  for(combo in unique(combosDF$combo)){
    comboData <- data[data$combo == combo,]
    for(metric in c("spend", "cos", "ctr", "cr", "site_events")){
      # format data for graph
      df <- comboData[, c("day", metric)]
      names(df) <- c("day", "metric")

      if(nrow(df) >= 25 && as.Date(df[nrow(df), "day"]) == check_for_alert_date){
        isEligible <- TRUE

        # make plot
        p <- df %>%
          ggplot2::ggplot(aes(x = as.Date(day), y = metric)) +
          ggplot2::geom_line() +
          ggplot2::labs(x = "Date", y = metric) +
          ggplot2::scale_y_continuous(labels = switch(metric,
                                                      cos = scales::percent,
                                                      ctr = scales::percent,
                                                      cr = scales::percent,
                                                      spend = scales::dollar,
                                                      site_events = scales::comma)) +
          ggplot2::coord_cartesian(ylim = c(0, 1.1 * max(df$metric, na.rm = TRUE)))

        # save plot
        ggplot2::ggsave(paste0(folder_name, "/", stringr::str_replace_all(combo, ":", ""), " ", metric, ".png"), p)
      } else{
        # not eligible
        isEligible <- FALSE
      }
      eligibility <- rbind(eligibility, data.frame(combo = combo, metric = metric, isEligible = isEligible, stringsAsFactors = FALSE))
    }
  }
  # return what isn't eligible
  return(eligibility)
}
