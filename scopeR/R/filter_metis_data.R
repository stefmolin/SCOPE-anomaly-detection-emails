#' @title Filter data for Metis graphs
#' @description Filter a file of Metis data
#' @author Stefanie Molin
#'
#' @import dplyr
#'
#' @param filename Path to the file to use
#' @param rows_threshold Number of days series must have to stay in data
#'
#' @export 
filter_metis_data <- function(filename, rows_threshold = 3){
  data <- read.csv(filename, header = TRUE, stringsAsFactors = FALSE)
  
  # filter out combos without data for "yesterday"
  filtered_data <- dplyr::semi_join(data, 
                                    data %>% 
                                      dplyr::filter(day == as.Date(run_date) - 1) %>% 
                                      dplyr::select(series, kpi), by = c("series", "kpi"))
  
  # remove combos with less than threshold of data points
  filtered_data <- filtered_data %>% 
    semi_join(filtered_data %>% 
                dplyr::group_by(series, run_date, kpi) %>% 
                dplyr::summarize(count = n()) %>% 
                dplyr::filter(count >= rows_threshold), by = c("series", "kpi"))
  
  # write output back to file
  write.csv(x = filtered_data, file = filename, row.names = FALSE)
}