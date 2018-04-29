####################################################
######            Templating Library          ######
######            by: Stefanie Molin          ######
####                                            ####
## Library for creating SCOPE alerts emails.      ##
####################################################

#' @title Fill in Exec Account Section
#' @description Fill in the accounts section of the Exec template for a given region.
#' @author Stefanie Molin
#'
#' @param accounts_list List containing the dataframes for gainers and losers
#' @param region String of the region being supplied
#' @param templateHTML HTML template to replace values in
#'
#' @return HTML
#'
fill_in_exec_account_section <- function(accounts_list, region, templateHTML){
  for(i in 1:5){
    for(type in c("gainer", "loser")){
      templateHTML <- gsub(sprintf("replace%s%sAccount%dNameHere", toupper(region), capitalize_name(type), i),
                           accounts_list[[paste0(type, "s")]][i, "client"], templateHTML)
      templateHTML <- gsub(sprintf("replace%s%sAccount%dPercentDiffHere", toupper(region), capitalize_name(type), i),
                           accounts_list[[paste0(type, "s")]][i, "percentDiff"], templateHTML)
      templateHTML <- gsub(sprintf("replace%s%sAccount%dValueDiffHere", toupper(region), capitalize_name(type), i),
                           accounts_list[[paste0(type, "s")]][i, "deviation"], templateHTML)
      templateHTML <- gsub(sprintf("replace%s%sAccount%dActualHere", toupper(region), capitalize_name(type), i),
                           accounts_list[[paste0(type, "s")]][i, "actual"], templateHTML)
      templateHTML <- gsub(sprintf("replace%s%sAccount%dExpectedHere", toupper(region), capitalize_name(type), i),
                           accounts_list[[paste0(type, "s")]][i, "expectedValue"], templateHTML)
    }
  }
  return(templateHTML)
}

#' @title Fill in Exec Regions
#' @description Fill in the region section of the Exec template.
#' @author Stefanie Molin
#' 
#' @param data Dataframe of results
#' @param templateHTML HTML template to replace values in
#'
#' @return HTML
#'
fill_in_exec_regions <- function(data, templateHTML){
  for(region in data$series){
    if(region %in% c("US", "LATAM", "SAM OTHER", "NORTH AMERICA")){
      if(identical(region, "SAM OTHER")){
        region_name <- "SAMOther"
      } else if(identical(region, "NORTH AMERICA")){
        region_name <- "NA"
      } else{
        region_name <- toupper(region)
      }
    } else{
      region_name <- capitalize_name(region)
    }
    templateHTML <- gsub(sprintf("replace%sPercentDiffHere", region_name), data[data$series == region, "differences"], templateHTML)
    templateHTML <- gsub(sprintf("replace%sActualHere", region_name), data[data$series == region, "yesterday_RexT"], templateHTML)
    templateHTML <- gsub(sprintf("replace%sExpectedHere", region_name), data[data$series == region, "avg_last_7D"], templateHTML)
  }
  return(templateHTML)
}

#' @title Exec Text Coloring
#' @description Determine which color to show each regions numbers in.
#' @author Stefanie Molin
#'
#' @param alerted_regions String containing the alerts triggered
#' @param highlight_color String containing the hexcode of the highlighted text color
#' @param regular_color String containing the hexcode of the regular text color
#'
#' @return HTML
#'
exec_text_color <- function(alerted_regions, highlight_color, regular_color){
  # intialize colors list
  colors <- list(americas = regular_color, na = regular_color, latam = regular_color,
                 us = regular_color, canada = regular_color, brazil = regular_color,
                 sam = regular_color, argentina = regular_color, chile = regular_color,
                 colombia = regular_color, mexico = regular_color)

  # highlight if there are alerts in that region
  for(region in names(colors)){
    if(region == "na"){
      locale <- "NORTH AMERICA"
    } else{
      locale <- toupper(region)
    }
    if(length(grep(locale, alerted_regions)) > 0){
      colors[[region]] <- highlight_color
    }
  }
  return(colors)
}

#' @title Exec Highlight Text
#' @description Fill in the text color for the RexT data by region.
#' @author Stefanie Molin
#'
#' @param colors_list List containing the hexcode for each region's RexT
#' @param templateHTML HTML template to replace values in
#'
#' @return HTML
#'
exec_highlight_text <- function(colors_list, templateHTML){
  for(region in names(colors_list)){
    if(region %in% c("us", "latam", "sam", "na") & !(region == "argentina")){
      if(identical(region, "sam")){
        region_name <- "SAMOther"
      } else{
        region_name <- toupper(region)
      }
    } else{
      region_name <- capitalize_name(region)
    }
    templateHTML <- gsub(sprintf("replace%sHighlightFontColorHere", region_name), colors_list[[region]], templateHTML)
  }
  return(templateHTML)
}

#' @title Exec Fill in Plots
#' @description Fill the plots into the template.
#' @author Stefanie Molin
#'
#' @importFrom ggplot2 ggsave
#'
#' @param plot_list Nested lists containing file_name and plot for each region
#' @param templateHTML HTML template to replace values in
#'
#' @return HTML
#'
exec_fill_in_plots <- function(plot_list, templateHTML){
  for(region in names(plot_list)){
    # save plots
    suppressMessages(ggplot2::ggsave(plot_list[[region]][["plot"]], filename = plot_list[[region]][["file_name"]]))
    if(region %in% c("latam", "na") & !(region == "argentina")){
      region_name <- toupper(region)
    } else{
      region_name <- capitalize_name(region)
    }
    templateHTML <- gsub(sprintf("replace%sGraphCIDHere", region_name), plot_list[[region]][["file_name"]], templateHTML)
  }
  return(templateHTML)
}

#' @title Build SCOPE Exec
#' @description Fill in the Exec template
#' @author Stefanie Molin
#'
#' @param alerts String of alerts
#' @param email_data Results data for email
#' @param accounts_list List containing accounts to use to determine gainers/losers section.
#' @param plot_list Nested lists containing file_name and plot for each region
#' @param highlight_color String containing the hexcode of the highlighted text color
#' @param regular_color String containing the hexcode of the regular text color
#' @param regional_config_list List of regional_config settings.
#' @param template HTML template to replace values in
#'
#' @return HTML
#'
build_scope_exec <- function(alerts, email_data, accounts_list,
                             plot_list, highlight_color, regular_color, regional_config_list, template){

  # prepare email
  templateHTML <- readLines(template, warn = FALSE)

  colors_list <- exec_text_color(alerts, highlight_color = highlight_color, regular_color = regular_color)

  templateHTML <- exec_highlight_text(colors_list = colors_list, templateHTML = templateHTML)

  templateHTML <- exec_fill_in_plots(plot_list = plot_list, templateHTML = templateHTML)

  templateHTML <- gsub("replaceDateHere", format_datetime(format_string = "%a %d-%b-%Y | %I:%M%p",
                                              timezone = regional_config_list[["exec"]][["timezone"]], showTimezone = TRUE), templateHTML)
  templateHTML <- gsub("replaceAlertsTriggeredHere", alerts, templateHTML)

  templateHTML <- fill_in_exec_regions(data = email_data, templateHTML = templateHTML)

  for(region in names(accounts_list)){
    templateHTML <- fill_in_exec_account_section(accounts_list[[region]], region = toupper(region), templateHTML = templateHTML)
  }

  return(templateHTML)
}
