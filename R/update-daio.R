# this script is executed daily by an automated task in order
# to extract the latest DAIO and commit the relevant files to
# the origin GitHub repository
library(dplyr)
library(lubridate)
library(purrr)
library(arrow)
library(readr)
# library(gert)

source(here::here("R", "helpers.R"))

wef <- "2019-01-01" %>% as_date()
til <- today()

Sys.setenv(
  TZ       = "UTC",
  ORA_SDTZ = "UTC",
  NLS_LANG =".AL32UTF8"
)
Sys.setlocale(locale = "en_US.utf8")

con <- fr24gu::db_connection(schema = "PRU_DEV")

daio <- extract_daio(con, wef, til)

daio_plus <- daio %>% 
  # # fix Türkiye
  # mutate(country_name = if_else(country_icao_code == "LT", "Türkiye", country_name)) %>%
  mutate(year = year(entry_date))
  

daio_plus %>% 
  group_by(year) %>% 
  group_walk(~ write_csv(.x,
                         here::here(stringr::str_c("daio_", .y$year, ".csv")),
                         na = ""))

daio_plus %>% 
  group_by(year) %>% 
  group_walk(~ write_parquet(.x,
                             here::here(stringr::str_c("daio_", .y$year, ".parquet"))))


# git_commit_all(paste0("Data updated till ", til, " (excluded)."))
# git_push()
