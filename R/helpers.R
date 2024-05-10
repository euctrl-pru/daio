library(withr)
library(eurocontrol)
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
#' @param wef the With-Effect-From (UTC) date (inclusive), must be >= 2019
#' @param til the unTIL (UTC) date (exclusive)
#'
#' @return a data frame with overall counts (`flt_daio`), total overflights
#'         (`flt_o`), internal (`flt_i`),
#' @export
#'
#' @examples
#' \dontrun{
#' wef <- "01-01-2022" %>% as_date(format = "%d-%m-%Y")
#' til <- "2022-01-08" %>% as_date()
#' extract_daio(wef, til)
#' }
extract_daio <- function(wef, til) {
  assert_date(wef)
  assert_date(til)

  withr::local_envvar(
    "TZ" = "UTC",
    "ORA_SDTZ" = "UTC",
    "NLS_LANG" =".AL32UTF8")
  
  withr::local_locale(.new = c("LC_COLLATE" = "en_US.utf8"))
  
  con <- withr::local_db_connection(eurocontrol::db_connection(schema = "PRU_DEV"))
  
  query <- "
    SELECT
      *
    FROM
      V_FAC_COUNTRY_FIR_DAIO
    WHERE
      TO_DATE(?WEF, 'YYYY-MM-DD') <= ENTRY_DATE AND ENTRY_DATE < TO_DATE(?TIL, 'YYYY-MM-DD')
  "

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


extract_daio_statfor <- function() {
  query <- "
  WITH STATFOR_DD AS (
    SELECT
      EXTRACT (YEAR FROM F.ENTRY_TIME)  AS YEAR, 
      EXTRACT (MONTH FROM F.ENTRY_TIME) AS MONTH,
      F.ENTRY_TIME                      AS ENTRY_DATE, 
      TO_CHAR(F.ENTRY_TIME, 'MMDD')     AS MMDD,
      MAX(ENTRY_TIME) OVER ()           AS MAX_DATE,
      
      --  EXTRACT (MONTH FROM F.ENTRY_TIME) MONTH_NUM,
      --  TO_CHAR (F.ENTRY_TIME, 'MON')     AS MONTH_MON,
      --  TO_CHAR (F.ENTRY_TIME, 'YYYYMM')  AS YEAR_MONTH,
      --  ENTRY_TIME,
    
      C.TZ_NAME,
      CASE
        WHEN TR_NAME LIKE 'ECAC%' THEN 'ECAC' 
        WHEN TR_NAME = 'TR-SPAIN' THEN 'ECAC'
        WHEN TR_NAME = 'TR-ECAC'  THEN 'ECAC'
      ELSE TR_NAME END                    AS TZ_REGION,
      CASE
        WHEN TZ_CLASS_CODE = 'SPAIN' THEN 'TZ'
        ELSE TZ_CLASS_CODE END            AS TZ_CLASS_TYPE,    
      DAIO,
      TF_TZ 
    FROM
      SWH_DM.DM_TZ_FIR_D2 F
    INNER JOIN
      SWH_FCT.DIMCL_TZ C ON (C.SK_T2TR_ID = F.SK_DIMCL_TZ_ID)
    WHERE 
    (TO_DATE('01-01-2019','DD-MM-YYYY') <= F.ENTRY_TIME )
    AND (C.TZ_CLASS_CODE IN ('TZ') OR TZ_NAME IN ('SPAIN','ECAC'))
  )       
  SELECT 
    YEAR,
    MONTH,
    ENTRY_DATE,
    TZ_NAME,
    TZ_REGION,
    DAIO,
    SUM(TF_TZ) AS FLT,MMDD,
    CASE
      WHEN TO_NUMBER(TO_CHAR(ENTRY_DATE, 'MMDD')) <= TO_NUMBER(TO_CHAR(MAX_DATE - 1, 'MMDD'))
      THEN 'Y'
      ELSE '-' END AS FILTER_YTD 
  FROM
    STATFOR_DD
  WHERE
    ENTRY_DATE < MAX_DATE 
  GROUP BY
    YEAR,
    MONTH,
    TZ_NAME,
    DAIO,
    ENTRY_DATE,
    TZ_REGION,
    MMDD,
    CASE 
      WHEN TO_NUMBER(TO_CHAR(ENTRY_DATE, 'MMDD')) <= TO_NUMBER(TO_CHAR(MAX_DATE - 1, 'MMDD'))
      THEN 'Y'
      ELSE '-'
      END --,
      -- MAX_DATE
  ORDER BY
    ENTRY_DATE,
    TZ_NAME,
    DAIO"
 
  withr::local_envvar(
    "TZ" = "UTC",
    "ORA_SDTZ" = "UTC",
    "NLS_LANG" =".AL32UTF8")
  
  withr::with_locale(LC_ALL = "en_US.utf8")
  
  con <- withr::local_db_connection(eurocontrol::db_connection(schema = "PRU_DEV"))
  
  daio_statfor <- tbl(con, sql(query)) |>
    collect() |> 
    janitor::clean_names()  |> 
    mutate(entry_date = lubridate::as_date(entry_date))
  daio_statfor
}