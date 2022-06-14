library(withr)
library(DBI)
library(fr24gu)
library(lubridate)
library(dplyr)
library(janitor)


daio__invalid_argument <- function(arg, ...) {
  msg <- paste0(encodeString(arg, quote = "`"), ...)
  structure(
    list(message = msg),
    class = c("invalid_argument", "error", "condition"))
}

assert_date <- function(x) {
  if (is.Date(x)) return()
  stop(daio__invalid_argument(match.call()$x,
                              " must be a date"))
}


#' Departure, arrival, internal, overflight counts per country.
#'
#' @param con a connection to PRU_DEV
#' @param wef the With-Effect-From (UTC) date (inclusive), must be >= 2019
#' @param til the unTIL (UTC) date (exclusive)
#'
#' @return a data frame with overall counts (`flt_daio`), total overflights
#'         (`flt_o`), internal (`flt_i`),
#' @export
#'
#' @examples
#' \dontrun{
#' con <- fr24gu::db_connection(schema = "PRU_DEV")
#' wef <- "01-01-2022" %>% as_date(format = "%d-%m-%Y")
#' til <- "2022-01-08" %>% as_date()
#' extract_daio(con, wef, til)
#' }
extract_daio <- function(con, wef, til) {
  assert_date(wef)
  assert_date(til)
  query <- "
    SELECT
      *
    FROM
      V_FAC_COUNTRY_FIR_DAIO
    WHERE
      TO_DATE(?WEF, 'YYYY-MM-DD') <= ENTRY_DATE AND ENTRY_DATE < TO_DATE(?TIL, 'YYYY-MM-DD')
  "
  
  # withr::local_envvar(.new = c("TZ" = "UDT", "ORA_SDTZ" = "UTC"))
  withr::local_envvar(.new = c("TZ" = "UTC",
                               "ORA_SDTZ" = "UTC",
                               "NLS_LANG"="AMERICAN_AMERICA.UTF8"))
  query <- DBI::sqlInterpolate(
    con, query,
    WEF = format(wef, "%Y-%m-%d"),
    TIL = format(til, "%Y-%m-%d"))
  
  r <- DBI::dbSendQuery(conn = con, statement = query)
  d <- DBI::dbFetch(r) %>%
    as_tibble() %>%
    janitor::clean_names() %>%
    mutate(entry_date = lubridate::as_date(entry_date)) %>%
    arrange(country_name, entry_date)
  d
}
