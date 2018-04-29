#' @title TS YAML Builder
#'
#' @description Make the graphs and the YAML for the TS alerts
#'
#' @author Stefanie Molin
#'
#' @import dplyr
#' @importFrom futile.logger flog.debug
#'
#' @param tag_level_alerts Dataframe containing the alerts for tag-level
#' @param tag_level_stats Dataframe of tag-level events
#'
#' @return A list that will transform into YAML when yaml::as.yaml() is used. 
#'
#' @export
#'
ts_yaml_builder <- function(tag_level_alerts, tag_level_stats) {
  stats_for_graphs <- tag_level_stats %>% 
    dplyr::semi_join(tag_level_alerts, by = "combo")
  
  ts_DF <- tag_level_alerts %>% 
    dplyr::select(-RTC_vertical, -vertical_name, -site_type, -event_name, -combo, -is_alert) %>% 
    dplyr::distinct() %>% 
    dplyr::arrange(TS_name)
  
  yaml_as_list <- list()
  for (TS in unique(ts_DF$TS_name)) {
    futile.logger::flog.debug(paste("Building YAML for", TS))
    
    filtered_df <- ts_DF %>% 
      dplyr::filter(TS_name == TS) %>% 
      dplyr::select(-partner_name, -partner_id) %>% 
      dplyr::distinct()
    
    graph_stats <- stats_for_graphs %>% 
      dplyr::filter(TS_name == TS)
    
    yaml_as_list <- append(yaml_as_list, list(list(name = TS, 
                                                   email = filtered_df$ts_engineer_email,
                                                   manager_name = filtered_df$manager_name,
                                                   manager_email = filtered_df$manager_email,
                                                   cost_center = filtered_df$ts_cost_center,
                                                   country = filtered_df$ts_country,
                                                   subregion = filtered_df$ts_subregion,
                                                   region = filtered_df$ts_region,
                                                   ranking = filtered_df$ranking,
                                                   run_date = as.character(filtered_df$run_date),
                                                   partners = ts_alerts_graph_list(graph_stats)
    )))
  }
  
  return(list(TS = yaml_as_list))
}
