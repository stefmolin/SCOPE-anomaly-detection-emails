#################################################
######          Graphing Utilities         ######
######          by: Stefanie Molin          #####
####                                         ####
##  Functions for graphing and base64 images.  ##
#################################################

#' @title Base64 Encode a Graph
#'
#' @description Use base64 to encode an image, then remove the <img> tags for easy use in HTML.
#'
#' @importFrom base64 img
#' @import ggplot2
#'
#' @author Stefanie Molin
#'
#' @param plot_object Plot to encode.
#'
#' @return base64 encoding of the plot
#'
base64_encode_graph <- function(plot_object){
  # save plot locally
  plotFileName <- "plot.png"
  suppressMessages(ggplot2::ggsave(filename = plotFileName, plot = plot_object))

  # create base64 encoding
  encodedImg <- base64::img(plotFileName)
  encodedImg <- strsplit(encodedImg, "<img src=\"")[[1]][2]
  encodedImg <- strsplit(encodedImg, "\" alt=\"image\" />")[[1]]

  # remove from disk
  file.remove(plotFileName)

  # return encoding
  return(encodedImg)
}

#' @title Exec Territory Graphs
#'
#' @description Make a ggplot2 graph for RexT series using ggrepel to label
#'
#' @import ggplot2
#' @importFrom ggrepel geom_label_repel
#' @importFrom scales dollar
#'
#' @author Stefanie Molin
#'
#' @param df dataframe for the exec graph
#' @param nudge_list Series, isSeriesNudge, and isNotSeriesNudge to pass to ggrepel for labeling
#' @param label_date_range vector of dates to show labels for
#'
#' @examples
#' \dontrun{
#' americasPlot <- exec_graph(df = americasPlotDF,
#'                            nudge_list = list(Series = "LATAM", isSeriesNudge = 20000, isNotSeriesNudge = -5000),
#'                            label_date_range = c(yesterday, dayBeforeYesterday))
#' }
#'
#' @return A ggplot
#' 
#' @export
#'
exec_graph <- function(df, nudge_list = list(series = "LATAM", isSeriesNudge = 20000, isNotSeriesNudge = -5000),
                       label_date_range){
  plot <- ggplot2::ggplot(data = df, aes(x = day, y = RexT, col = series)) +
    ggplot2::geom_line() +
    ggplot2::ylab("RexT in $USD (Local + Imports)") +
    ggplot2::xlab("Last 15 Days") +
    ggplot2::theme(legend.title = element_blank()) +
    ggplot2::theme(legend.position = "top") +
    ggrepel::geom_label_repel(data = df[df$day %in% label_date_range,],
                              aes(x = day, y = RexT, label = plot_labels),
                              size = 3, force = 1, show.legend = FALSE, nudge_x = -.25,
                              nudge_y = ifelse(df[df$day %in% label_date_range, "series"] == nudge_list$series,
                                               nudge_list$isSeriesNudge, nudge_list$isNotSeriesNudge)) +
    ggplot2::scale_y_continuous(labels = scales::dollar)

  return(plot)
}

#' @title TS Graph and Save
#'
#' @description Make a ggplot2 graph and save, type of graph depends on FUN argument
#'
#' @import ggplot2
#'
#' @author Stefanie Molin
#'
#' @param df Dataframe containing the site events data for the advertiser.
#' @param FUN TS graphing function to use; either ts_tag_level_graph or ts_site_level_graph
#' @param plotname String for the name you want to save the plot as; this will be returned upon successful completion
#' @param ... Parameters passed to FUN
#'
#' @return The name of the plot after it has been successfully saved.
#'
ts_graph_and_save <- function(df, plotname, FUN, ...){
  p <- FUN(df, ...)
  suppressMessages(ggplot2::ggsave(filename = plotname, plot = p, bg = "transparent",
                                   dpi = 720, width = 160, height = 42, units = "mm"))
  return(plotname)
}

#' @title TS Alerts Graph List
#'
#' @description Make graphs for all TS alerts and return a list with their names indexed by TS
#'
#' @import ggplot2
#' @import dplyr
#' @importFrom futile.logger flog.debug
#'
#' @author Stefanie Molin
#'
#' @param graph_stats Dataframe of data for a particular TS
#'
#' @return A list containing the names of the saved ggplots indexed by TS name
#'
#' @export
#'
ts_alerts_graph_list <- function(graph_stats){
  graphList <- list()
  for(partner in unique(graph_stats$partner_name)){
    futile.logger::flog.debug(paste("Partner:", partner))
    
    data <- graph_stats %>% 
      dplyr::filter(partner_name == partner)
    
    partner_id <- data$partner_id %>% 
      unique() %>% 
      as.integer()
    
    site_type_list <- list()
    for(site in unique(data$site_type)) {
      futile.logger::flog.debug(paste("Site type:", site))
      
      site_data <- data %>% 
        dplyr::filter(site_type == site)
      
      event_list <- list()
      for(event in unique(site_data$event_name)) {
        futile.logger::flog.debug(paste("Event name:", event))
        
        event_data <- site_data %>% 
          dplyr::filter(event_name == event)
        
        series <- unique(event_data$combo)
        
        plot_name <- ts_graph_and_save(df = event_data,
                                       FUN = graph_for_email,
                                       plotname = paste0(paste(partner_id, site, event, sep = "_"), ".png"),
                                       metric = "events")
        
        event_list <- append(event_list, list(list(event_name = event, graph = plot_name)))
      }
      
      site_type_list <- append(site_type_list, list(list(site = site, event = event_list)))
    }

    graphList <- append(graphList, list(list(partner_name = partner, partner_id = partner_id, alerts = site_type_list)))
  }
  return(graphList)
}


#' @title AS Transparent Graph
#'
#' @description Make a ggplot2 graph for given metric in AS alerts
#'
#' @import ggplot2
#' @importFrom scales date_format percent dollar_format
#' @importFrom plyr round_any
#'
#' @author Stefanie Molin
#'
#' @param data Dataframe containing daily data.
#' @param metric The name of the metric to graph, (must match column names, but not case-sensitive).
#' @param client_name Name of the client for which the graph is being created.
#' @param encode Whether or not to base64 encode the graph, defaults to FALSE. 
#' (Needed if sending as part of HTML attachment rather than email body inline images.)
#' @param max_possible_days Controls the shifting of the label by creating a ratio of max days allowed in the graph (defaults to 60) and the total days being graphed. 
#' This makes sure that graphs with less than the max days will shift the label less to keep it on the line.
#'
#' @return A base64 encoded site events plot, or just the plot object (depends on the value of encode).
#'
AS_transparent_graph <- function(data, metric, client_name, encode = FALSE, max_possible_days = 60){
  
  options(scipen = 10000)
  
  # filter data to client
  data <- data[data$combo == client_name,]

  # handle y-axis formatting rules
  metric <- tolower(metric)
  is_spend_metric <- metric %in% c("spend", "rext")
  if(is_spend_metric){
    label_content <- format_currency(data[which.max(data$day), metric], symbol = "", currency_code = data[1, "currency_code"])
  } else{
    label_content <- format_percent(data[which.max(data$day), metric])
  }
  
  max_value <- max(data[, metric])
  round_to <- ifelse(is_spend_metric, ifelse(max_value < 1000, ifelse(max_value < 100, 5, 10), 100), ifelse(max_value < 0.30, 0.01, 0.1))
  y_max <- plyr::round_any(max(0, max(data[, metric]) * ifelse(max(data[, metric]) > 0, 1.05, 0.95)), round_to, f = ceiling)
  y_min <- plyr::round_any(min(0, min(data[, metric]) * ifelse(min(data[, metric]) < 0, 1.05, 0.95)), round_to, f = floor)
  
  labelled_point_value <- data[which.max(data$day), ]
  distance_to_top <- y_max - labelled_point_value[, metric]
  
  # transparent graph with label
  plot <- ggplot2::ggplot(data, aes_string(x = "day", y = metric)) + 
    ggplot2::geom_line(size = 0.7, col = "white") + 
    ggplot2::labs(x = "", y = "") +
    ggplot2::coord_cartesian(ylim = c(y_min, y_max)) +
    ggplot2::scale_y_continuous(labels = ifelse(is_spend_metric, 
                                                function(x) { paste(thousands_formatter(x), data[1, "currency_code"]) },
                                                function(x) { paste(thousands_formatter(x*100), "%") }),
                                breaks = c(y_min, y_max),
                                expand = c(0, 0)) +
    ggplot2::scale_x_date(labels = scales::date_format("%b %d"), 
                          breaks = rev(seq(max(data$day), min(data$day), by = -14)), 
                          limits = c(min(data$day), max(data$day) + 5),
                          expand = c(0.025, 0)) + 
    ggplot2::geom_label(data = labelled_point_value, aes_string(x = "day", y = metric, label = "label_content"), 
                        colour = "#191e2b", fontface = "bold", show.legend = FALSE, label.size = NA,
                        nudge_x = -(max(nchar(label_content) - 5, 0)) / 1.7 * nrow(data)/max_possible_days,
                        nudge_y = distance_to_top/25) + 
    transparent_graph_theme()
  
  # encode if required
  if(encode){
    plot <- base64_encode_graph(plot)
  }
  
  return(plot)
}

#' @title TS Transparent Graph
#'
#' @description Make a ggplot2 graph for given metric in TS alerts
#'
#' @import ggplot2
#' @importFrom scales date_format percent dollar_format
#' @importFrom plyr round_any
#'
#' @author Stefanie Molin
#'
#' @param data Dataframe containing daily data.
#' @param encode Whether or not to base64 encode the graph, defaults to FALSE. 
#' (Needed if sending as part of HTML attachment rather than email body inline images.)
#' @param max_possible_days Controls the shifting of the label by creating a ratio of max days allowed in the graph (defaults to 60) and the total days being graphed. 
#' This makes sure that graphs with less than the max days will shift the label less to keep it on the line.
#'
#' @return A base64 encoded site events plot, or just the plot object (depends on the value of encode).
#'
TS_transparent_graph <- function(data, combo, encode = FALSE, max_possible_days = 60){
  
  options(scipen = 10000)
  
  # handle y-axis formatting rules
  label_content <- format_number(data[which.max(data$day), "events"], digits = 0)
  
  max_value <- max(data[, "events"])
  round_to <- ifelse(max_value < 1000, 10, ifelse(max_value < 10000, 100, 1000))
  y_max <- plyr::round_any(max(0, max(data[, "events"]) * ifelse(max(data[, "events"]) > 0, 1.05, 0.95)), round_to, f = ceiling)
  y_min <- 0
  
  event_name <- data$event_name %>% unique()
  
  labelled_point_value <- data[which.max(data$day), ]
  distance_to_top <- y_max - labelled_point_value$events
  
  # transparent graph with label
  plot <- ggplot2::ggplot(data, aes(x = day, y = events)) + 
    ggplot2::geom_line(size = 0.7, col = "white") + 
    ggplot2::labs(x = "", y = toupper(event_name)) +
    ggplot2::coord_cartesian(ylim = c(y_min, y_max)) +
    ggplot2::scale_y_continuous(labels = thousands_formatter,
                                breaks = c(y_min, y_max),
                                expand = c(0, 0)) +
    ggplot2::scale_x_date(labels = scales::date_format("%b %d"), 
                          breaks = rev(seq(max(data$day), min(data$day), by = -14)), 
                          limits = c(min(data$day), max(data$day) + 2),
                          expand = c(0.025, 0)) + 
    ggplot2::geom_label(data = labelled_point_value, aes(x = day, y = events, label = label_content), 
                        colour = "#191e2b", fontface = "bold", show.legend = FALSE, label.size = NA, size = 3,
                        nudge_x = -(max(nchar(label_content) - 5, 0)) / 1.75 * nrow(data)/max_possible_days,
                        nudge_y = distance_to_top/12.5) + 
    transparent_graph_theme()
  
  # encode if required
  if(encode){
    plot <- base64_encode_graph(plot)
  }
  
  return(plot)
}

#' @title Transparent Graph for Email
#'
#' @description Make a ggplot2 graph for given metric in TS alerts
#'
#' @import ggplot2
#' @importFrom scales date_format percent dollar_format
#' @importFrom plyr round_any
#'
#' @author Stefanie Molin
#'
#' @param data Dataframe containing daily data.
#' @param metric The name of the metric to graph, (must match column names, but not case-sensitive).
#' @param encode Whether or not to base64 encode the graph, defaults to FALSE. 
#' (Needed if sending as part of HTML attachment rather than email body inline images.)
#' @param max_possible_days Controls the shifting of the label by creating a ratio of max days allowed in the graph (defaults to 60) and the total days being graphed. 
#' This makes sure that graphs with less than the max days will shift the label less to keep it on the line.
#'
#' @return A base64 encoded site events plot, or just the plot object (depends on the value of encode).
#'
graph_for_email <- function(data, metric, encode = FALSE, max_possible_days = 60) {
  options(scipen = 10000)
  
  # handle y-axis formatting rules
  metric <- tolower(metric)
  is_spend_metric <- metric %in% c("spend", "rext")
  is_TS_metric <- metric %in% c("events", "dedup_ratio")
  is_number_metric <- metric %in% c("events")
  if(is_number_metric){
    label_content <- format_number(data[which.max(data$day), metric], digits = 0)
  } else if(is_spend_metric){
    label_content <- format_currency(data[which.max(data$day), metric], symbol = "", currency_code = data[1, "currency_code"])
  } else{
    label_content <- format_percent(data[which.max(data$day), metric])
  }
  
  max_value <- max(data[, metric])
  min_value <- min(data[, metric])
  round_to <- ifelse(is_spend_metric | is_number_metric, 
                     ifelse(max_value < 1000, ifelse(max_value < 100, 5, 10), ifelse(max_value < 10000, 100, 1000)), 
                     ifelse(max_value < 0.30, 0.01, 0.1))
  y_max <- plyr::round_any(max(0, max_value * ifelse(max_value > 0, 1.05, 0.95)), round_to, f = ceiling)
  y_min <- ifelse(is_number_metric, 0, plyr::round_any(min(0, min_value * ifelse(min_value < 0, 1.05, 0.95)), round_to, f = floor))
  
  labelled_point_value <- data[which.max(data$day), ]
  distance_to_top <- y_max - labelled_point_value[, metric]
  
  # TS vs. AS formatters (this is only necessary bc the graphs are different sizes)
  if (is_TS_metric) {
    y_axis_label <- toupper(data$event_name %>% unique())
    nudge_x_divisor <- 1.75
    nudge_y_divisor <- 12.5
    date_extension <- 2
    label_size <- 3
  } else{
    y_axis_label <- ""
    nudge_x_divisor <- 1.7
    nudge_y_divisor <- 25
    date_extension <- 5
    label_size <- 3.5
  }
  
  
  # transparent graph with label
  plot <- ggplot2::ggplot(data, aes_string(x = "day", y = metric)) + 
    ggplot2::geom_line(size = 0.7, col = "white") + 
    ggplot2::labs(x = "", y = y_axis_label) +
    ggplot2::coord_cartesian(ylim = c(y_min, y_max)) +
    ggplot2::scale_y_continuous(labels = ifelse(is_spend_metric, 
                                                function(x) { paste(thousands_formatter(x), data[1, "currency_code"]) },
                                                ifelse(is_number_metric,
                                                       thousands_formatter,
                                                       function(x) { paste(thousands_formatter(x*100), "%") })),
                                breaks = c(y_min, y_max),
                                expand = c(0, 0)) +
    ggplot2::scale_x_date(labels = scales::date_format("%b %d"), 
                          breaks = rev(seq(max(data$day), min(data$day), by = -14)), 
                          limits = c(min(data$day), max(data$day) + date_extension),
                          expand = c(0.025, 0)) + 
    ggplot2::geom_label(data = labelled_point_value, aes_string(x = "day", y = metric, label = "label_content"), 
                        colour = "#191e2b", fontface = "bold", show.legend = FALSE, label.size = NA, size = label_size,
                        nudge_x = -(max(nchar(label_content) - 5, 0)) / nudge_x_divisor * nrow(data)/max_possible_days,
                        nudge_y = distance_to_top/nudge_y_divisor) + 
    transparent_graph_theme()
  
  
  # encode if required
  if(encode){
    plot <- base64_encode_graph(plot)
  }
  
  return(plot)
}


#' @title Transparent Graph Theme
#'
#' @description Make a ggplot2 graph theme for SCOPE emails
#'
#' @importFrom ggplot2 theme
#'
#' @author Stefanie Molin
#' 
#' @return A ggplot2 theme.
#' 
#' @export
#'
transparent_graph_theme <- function(){
  ggplot2::theme(
    panel.background = element_rect(fill = "transparent", colour = NA)
    , plot.background = element_rect(fill = "transparent", colour = NA)
    , panel.grid.minor.x = element_blank()
    # , panel.grid.minor.y = element_line(size = 0.1, color = "white")
    , panel.grid.minor.y = element_blank()
    , legend.background = element_rect(fill = "transparent", colour = NA)
    , legend.box.background = element_rect(fill = "transparent", colour = NA)
    , panel.grid.major.x = element_blank()
    # , panel.grid.major.y = element_line(size = 0.1, color = "white")
    , panel.grid.major.y = element_blank()
    , axis.text = element_text(color = "white", size = 10)
    , axis.ticks = element_line(color = "white")
    , axis.ticks.length = unit(.25, "cm")
    , axis.line.x = element_line(color = "white", size = 0.1)
    # , axis.line.x = element_blank()
    , panel.border = element_blank()
    , plot.margin = unit(c(5.5, 10, -5.5, 5.5), "points")
    , axis.title.y = element_text(color = "#cfe4f2", margin = margin(r = 20))
  )
}