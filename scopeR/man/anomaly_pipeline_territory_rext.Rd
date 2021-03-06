% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pipelines.R
\name{anomaly_pipeline_territory_rext}
\alias{anomaly_pipeline_territory_rext}
\title{Run through Territory RexT Detect Anomaly Pipeline}
\usage{
anomaly_pipeline_territory_rext(data, yesterday)
}
\arguments{
\item{data}{Dataframe of daily data to check for anomalies.}

\item{yesterday}{Day to check for anomalies.}
}
\value{
A list with 2 items: "results"--a dataframe with everything that was tested and whether or not it was an alert and 
"cleaned_data"--a dataframe containing the cleaned and prepped data that was used to check for anomalies.
}
\description{
Clean data, impute the median for missing dates, and detect anomalies for all territory RexT series
}
\note{
This function relies on the structure of the query that provides "data". 
If changing column names, adding or subtracting columns, be sure to update here as well.

Client/campaign must have TAC in each of the last 2 days and 25 rows in the database in the last 30 days
}
\author{
Stefanie Molin
}
