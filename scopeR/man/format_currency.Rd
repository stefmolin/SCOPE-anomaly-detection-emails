% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/formatting_library.R
\name{format_currency}
\alias{format_currency}
\title{Format as Currency}
\usage{
format_currency(values, prefix = "", symbol = "$", currency_code = "")
}
\arguments{
\item{values}{Vector of values to be formatted}

\item{prefix}{Character to add to the front of the values (no space will be put between value here and the currency value)}

\item{symbol}{Symbol for the currency, defaults to "$" (added before the number)}

\item{currency_code}{Code for the currency to be appended at end for disambiguation of currency being shown}
}
\value{
Vector of characters
}
\description{
Format values with the currency symbol in front and 2 digits after the decimal and comma separators at thousands
}
\author{
Stefanie Molin
}
