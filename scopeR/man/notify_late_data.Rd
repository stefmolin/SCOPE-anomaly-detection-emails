% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/date_check_library.R
\name{notify_late_data}
\alias{notify_late_data}
\title{Notify of Late Data}
\usage{
notify_late_data(scope_product, root_config_list, regional_config_list,
  test_mode, sender, mailbox_password, slack_on = FALSE)
}
\arguments{
\item{scope_product}{Which SCOPE product the code is running in (determines recipients). Choices: "AS", "EXEC", "TS"}

\item{root_config_list}{List read from the root config YAML file}

\item{regional_config_list}{List read from the regional config YAML file}

\item{test_mode}{Boolean indicating whether or not SCOPE is in test mode}

\item{sender}{Address of the sender of the error emails.}

\item{mailbox_password}{Password of the sender}

\item{slack_on}{Boolean indicating whether or not to send notifications to slack}
}
\description{
Send the data is late email to the appropriate recipients based on which SCOPE is running (and slack notification if desired)
}
\author{
Stefanie Molin
}
