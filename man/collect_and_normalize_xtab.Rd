% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/xtabs.R
\name{collect_and_normalize_xtab}
\alias{collect_and_normalize_xtab}
\title{Collect data and normalize result}
\usage{
collect_and_normalize_xtab(connection, disconnect = FALSE)
}
\arguments{
\item{connection}{SQlite connection}

\item{disconnect}{Optional parameter to determine if the SQlite connection should be closed, `FALSE` by default.}
}
\value{
A tibble with the data in long form
}
\description{
Collect data and normalize result
}
