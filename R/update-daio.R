# this script is executed daily by an automated task in order
# to extract the latest DAIO and commit the relevant files to
# the origin GitHub repository
library(withr)
library(DBI)
library(fr24gu)
library(lubridate)
library(dplyr)
library(janitor)
library(purrr)
library(arrow)
library(readr)
library(gert)


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
#' @param wef the With-Effect-From (UTC) date (inclusive)
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
    WITH
      COUNTRY_ICAO2LETTER AS (
        SELECT COUNTRY_ID ICAO2LETTER,
          CASE
            -- Canary Islands in Spain
            WHEN COUNTRY_ID = 'GC' THEN 'LE'
            WHEN COUNTRY_ID = 'GE' THEN 'LE'
            -- Military Germany in civil Germany
            WHEN COUNTRY_ID = 'ET' THEN 'ED'
            -- Belgium and Luxemburg together
            WHEN COUNTRY_ID = 'EL' THEN 'EB'
            ELSE COUNTRY_ID
          END COUNTRY_CODE,
          CASE
            -- same for names
            WHEN COUNTRY_ID = 'GC' THEN 'Spain'
            WHEN COUNTRY_ID = 'GE' THEN 'Spain'
            WHEN COUNTRY_ID = 'ET' THEN 'Germany'
            WHEN COUNTRY_ID = 'EB' THEN 'Belgium and Luxembourg'
            WHEN COUNTRY_ID = 'EL' THEN 'Belgium and Luxembourg'
            ELSE COUNTRY_NAME
          END COUNTRY_NAME
        FROM ARU_SYN.STAT_CODA_COUNTRY@BO_USER_DWH_OP3
        WHERE (
        -- all countries in E and L ICAO region plus our Asian member States
        (SUBSTR(COUNTRY_ID,1,1)   IN ('E','L')
                OR SUBSTR(COUNTRY_ID,1,2) IN ('GC','GM','GE','UD','UG','UK')) )
                -- minus Palestine (LV), Gilbratar (LX), ??? (EU) and Monaco (LN)
              AND COUNTRY_ID NOT IN ('LV', 'LX', 'EU','LN')
      ),
      LIST_FIR AS (
        SELECT COUNTRY_ID,
          FIR_ID,
          FIR_CODE,
          FIR_NAME,
          COUNTRY_CODE AS ICAO_COUNTRY_CODE
        FROM PRUDEV.V_PRU_REL_COUNTRY_FIR
      ),
      LIST_COUNTRY AS (
        SELECT COUNTRY_NAME
         FROM COUNTRY_ICAO2LETTER
         GROUP BY COUNTRY_NAME
      ),
      LIST_FIR_COUNTRY AS (
        SELECT B.FIR_CODE,
          A.ICAO2LETTER,
          A.COUNTRY_CODE,
          A.COUNTRY_NAME
        FROM COUNTRY_ICAO2LETTER A
        INNER JOIN LIST_FIR B
        ON (A.ICAO2LETTER = B.ICAO_COUNTRY_CODE)
      ),
      CTRY_DAY AS (
        SELECT A.COUNTRY_NAME,
          T.DAY_DATE,
          T.MONTH,
          T.WEEK,
          T.WEEK_NB_YEAR,
          T.DAY_TYPE,
          T.DAY_OF_WEEK_NB AS DAY_OF_WEEK,
          T.YEAR
        FROM LIST_COUNTRY A,
          PRU_TIME_REFERENCES T
        WHERE T.DAY_DATE >= TO_DATE(?WEF, 'dd-mm-yyyy')
        AND   T.DAY_DATE  < TO_DATE(?TIL, 'dd-mm-yyyy')
      ),
      DATA_FLT AS (
        SELECT A.FLT_DEP_AD,
          A.FLT_CTFM_ADES,
          B.COUNTRY_NAME AS DEP_COUNTRY_NAME,
          C.COUNTRY_NAME AS ARR_COUNTRY_NAME,
          TRUNC(A.FLT_A_ASP_PROF_TIME_ENTRY) ENTRY_DATE,
          A.FLT_UID
        FROM ARU_FLT.FLT@BO_USER_DWH_OP3 A
        LEFT OUTER JOIN COUNTRY_ICAO2LETTER b ON (SUBSTR(A.FLT_DEP_AD, 1, 2) = B.ICAO2LETTER)
        LEFT OUTER JOIN COUNTRY_ICAO2LETTER C ON (SUBSTR(A.FLT_CTFM_ADES, 1, 2)  = C.ICAO2LETTER)
        WHERE (
              A.FLT_LOBT       >= TO_DATE(?WEF, 'dd-mm-yyyy') - 1
          AND A.FLT_LOBT        < TO_DATE(?TIL, 'dd-mm-yyyy') + 1
          AND A.FLT_A_ASP_PROF_TIME_ENTRY >= TO_DATE(?WEF, 'dd-mm-yyyy')
          AND A.FLT_A_ASP_PROF_TIME_ENTRY  < TO_DATE(?TIL, 'dd-mm-yyyy')
          )
          AND A.FLT_STATE IN ('TE','TA','AA')
      ),
      DATA_DEP AS (
        SELECT DEP_COUNTRY_NAME AS COUNTRY_NAME,
          A.ENTRY_DATE,
          COUNT(A.FLT_UID) FLT_DAI
        FROM DATA_FLT A
        -- added as per DATA_ARR (CHECK: with Muriel)
        WHERE DEP_COUNTRY_NAME IS NOT NULL
        GROUP BY DEP_COUNTRY_NAME, ENTRY_DATE
      ),
      DATA_ARR AS (
        SELECT ARR_COUNTRY_NAME AS COUNTRY_NAME,
          A.ENTRY_DATE,
          COUNT(A.FLT_UID) FLT_DAI
        FROM DATA_FLT A
        WHERE ARR_COUNTRY_NAME IS NOT NULL
        GROUP BY ARR_COUNTRY_NAME, ENTRY_DATE
      ),
      DATA_DOMESTIC AS (
        SELECT DEP_COUNTRY_NAME AS COUNTRY_NAME,
          A.ENTRY_DATE,
          COUNT(A.FLT_UID) FLT_DAI
        FROM DATA_FLT A
        WHERE DEP_COUNTRY_NAME = ARR_COUNTRY_NAME
        AND ARR_COUNTRY_NAME  IS NOT NULL
        GROUP BY DEP_COUNTRY_NAME, ENTRY_DATE
      ),
      DATA_CIRCULAR AS (
        SELECT DEP_COUNTRY_NAME AS COUNTRY_NAME,
          A.ENTRY_DATE,
          COUNT(A.FLT_UID) FLT_DAI
        FROM DATA_FLT A
        WHERE FLT_DEP_AD = FLT_CTFM_ADES
        AND ARR_COUNTRY_NAME IS NOT NULL
        GROUP BY DEP_COUNTRY_NAME, ENTRY_DATE
      ),
      SPECIAL_ASP_DATA AS (
        SELECT A.FLIGHT_SK,
          c.COUNTRY_NAME,
          ASP_TIME_ENTRY
          --,
          --  row_number() OVER ( PARTITION BY  c.country_name,flight_sk ORDER BY  asp_time_entry) row_num
        FROM ARU_SUM.FACT_FLT_ASP_CALC@BO_USER_DWH_OP3 A
        INNER JOIN ARU_SUM.DIM_ASP@BO_USER_DWH_OP3 B
          ON (A.ASP_SK = B.ASP_SK)
          AND (A.LOBD >= B.WEF AND A.LOBD   < B.TIL)
        INNER JOIN ARU_PER.DIM_FLIGHT_ATTRIBUTE@BO_USER_DWH_OP3 D
          ON (A.FLIGHT_ATTRIBUTE_UID = D.FLIGHT_ATTRIBUTE_UID)
        INNER JOIN LIST_FIR_COUNTRY C
          ON (B.ASP_ID = C.FIR_CODE)
        WHERE
          -- what is all this +/- stuff?
          LOBD           >= TO_DATE(?TIL, 'dd-mm-yyyy') - 7 + 1 - 1
          AND LOBD            < TO_DATE(?TIL, 'dd-mm-yyyy') + 1
          AND A.ASP_TIME_ENTRY >= TO_DATE(?TIL, 'dd-mm-yyyy') - 7 + 1 -1
          AND A.ASP_TIME_ENTRY  < TO_DATE(?TIL, 'dd-mm-yyyy')
          AND ASP_TY  = 'FIR'
          -- get Model 3, i.e. FLOWN
          AND A.FLT_MODEL_TY = 3
          AND FLT_STATE IN ('TE', 'TA', 'AA')
            -- apply skip in to Germany to be consistent with Statfor as it is t zero in FACT_FLT_ASP_CALC
          AND (
            (B.ASP_ID IN ('EDMMFIR','EDGGFIR','EDUUUIR', 'EDVVUIR')
              AND 60 * 60 * 24 * (ASP_TIME_EXIT - ASP_TIME_ENTRY) >= 120)
            OR (B.ASP_ID  ='EDWWFIR'
                AND 60 * 60 * 24 * (ASP_TIME_EXIT - ASP_TIME_ENTRY) >= 60)
            OR (C.COUNTRY_CODE <> 'ED')
          )
      ),
      ASP_DATA AS (
        SELECT FLIGHT_SK,
          COUNTRY_NAME,
          ASP_TIME_ENTRY AS FIRST_ENTRY_TIME,
          ROW_NUMBER() OVER (PARTITION BY COUNTRY_NAME, FLIGHT_SK ORDER BY ASP_TIME_ENTRY) ROW_NUM
        FROM SPECIAL_ASP_DATA
      ),
      DATA_DAIO AS (
        SELECT COUNTRY_NAME,
          TRUNC(FIRST_ENTRY_TIME) AS ENTRY_DATE,
          COUNT(*)                AS FLT_DAIO
        FROM ASP_DATA
        WHERE ROW_NUM = 1
        AND FIRST_ENTRY_TIME >= TO_DATE(?TIL, 'dd-mm-yyyy') - 7
        AND FIRST_ENTRY_TIME  < TO_DATE(?TIL, 'dd-mm-yyyy')
        GROUP BY TRUNC(FIRST_ENTRY_TIME),
          COUNTRY_NAME
        UNION
        SELECT
          CASE
            WHEN unit_name = 'Belgium' THEN 'Belgium and Luxembourg'
            WHEN unit_name = 'Serbia and Montenegro' THEN 'Serbia && Montenegro'
            WHEN unit_name = 'Bosnia and Herzegovina' THEN 'Bosnia-Herzegovina'
            ELSE unit_name
          END         AS COUNTRY_NAME,
          FLIGHT_DATE AS ENTRY_DATE,
          TTF_FLT     AS FLT_DAIO
        FROM PRUDEV.V_PRU_FAC_TD_DD
        WHERE UNIT_KIND  = 'COUNTRY_FIR'
        AND FLIGHT_DATE >= TO_DATE(?WEF, 'dd-mm-yyyy')
        AND FLIGHT_DATE  < TO_DATE(?TIL, 'dd-mm-yyyy') - 7
      ) ,
      DATA_COUNTRY AS (
        SELECT A.DAY_DATE AS ENTRY_DATE,
          A.COUNTRY_NAME,
          COALESCE(F.FLT_DAIO, 0)                                                                              AS FLT_DAIO,
          COALESCE(F.FLT_DAIO, 0) - (COALESCE(B.FLT_DAI, 0) + COALESCE(C.FLT_DAI, 0) - COALESCE(D.FLT_DAI, 0)) AS FLT_O,
          COALESCE( D.FLT_DAI, 0)                                                                              AS FLT_I ,
          COALESCE( E.FLT_DAI, 0)                                                                              AS FLT_C,
          COALESCE( B.FLT_DAI, 0) +  COALESCE(C.FLT_DAI, 0)  - COALESCE(D.FLT_DAI, 0)                          AS FLT_DAI,
          COALESCE( B.FLT_DAI, 0) -  COALESCE(D.FLT_DAI, 0)                                                    AS FLT_D,
          COALESCE( C.FLT_DAI, 0) -  COALESCE(D.FLT_DAI, 0)                                                    AS FLT_A,
          COALESCE( C.FLT_DAI, 0) -  COALESCE(D.FLT_DAI, 0)  + COALESCE(B.FLT_DAI, 0) - COALESCE(D.FLT_DAI, 0) AS FLT_DA,
          COALESCE( D.FLT_DAI, 0) -  COALESCE(E.FLT_DAI, 0)                                                    AS FLT_I_EXCL_C
        FROM CTRY_DAY A
        LEFT JOIN DATA_DEP B
          ON A.COUNTRY_NAME = B.COUNTRY_NAME
          AND A.DAY_DATE    = B.ENTRY_DATE
        LEFT JOIN DATA_ARR C
        ON A.COUNTRY_NAME = C.COUNTRY_NAME
          AND A.DAY_DATE  = C.ENTRY_DATE
        LEFT JOIN DATA_DOMESTIC D
          ON A.COUNTRY_NAME = D.COUNTRY_NAME
          AND A.DAY_DATE    = D.ENTRY_DATE
        LEFT JOIN DATA_CIRCULAR E
          ON A.COUNTRY_NAME = E.COUNTRY_NAME
        AND A.DAY_DATE    = E.ENTRY_DATE
        LEFT JOIN DATA_DAIO F
          ON A.COUNTRY_NAME = F.COUNTRY_NAME
        AND A.DAY_DATE    = F.ENTRY_DATE
      )
    SELECT * FROM DATA_COUNTRY
  "

  # withr::local_envvar(.new = c("TZ" = "UDT", "ORA_SDTZ" = "UTC"))
  withr::local_envvar(.new = c("TZ" = "UTC", "ORA_SDTZ" = "UTC"))
  query <- DBI::sqlInterpolate(
    con, query,
    WEF = format(wef, "%d-%m-%Y"),
    TIL = format(til, "%d-%m-%Y"))

  r <- DBI::dbSendQuery(conn = con, statement = query)
  d <- DBI::dbFetch(r) %>%
    as_tibble() %>%
    janitor::clean_names() %>%
    mutate(entry_date = lubridate::as_date(entry_date)) %>%
    arrange(country_name, entry_date)
  d
}


wef <- "2022-01-01" %>% as_date()
til <- today()

con <- fr24gu::db_connection(schema = "PRU_DEV")

daio <- extract_daio(con, wef, til) %>% 
  mutate(year = year(entry_date))
  

daio %>% 
  group_by(year) %>% 
  group_walk(~ write_csv(.x,
                         stringr::str_c("daio_", .y$year, ".csv"),
                         na = ""))

daio %>% 
  group_by(year) %>% 
  group_walk(~ write_parquet(.x, stringr::str_c("daio_", .y$year, ".parquet")))


git_commit_all(paste0("Data updated till ", til, " (excluded)."))
git_push()
