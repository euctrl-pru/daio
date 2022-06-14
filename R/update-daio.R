# this script is executed daily by an automated task in order
# to extract the latest DAIO and commit the relevant files to
# the origin GitHub repository
library(dplyr)
library(purrr)
library(arrow)
library(readr)
library(gert)

source(here::here("R", "extract_daio.R"))


wef <- "2022-01-01" %>% as_date()
til <- today()

con <- fr24gu::db_connection(schema = "PRU_DEV")

daio <- extract_daio(con, wef, til)

daio_plus <- daio %>% 
  mutate(year = year(entry_date))
  

daio_plus %>% 
  group_by(year) %>% 
  group_walk(~ write_csv(.x,
                         stringr::str_c("daio_", .y$year, ".csv"),
                         na = ""))

daio_plus %>% 
  group_by(year) %>% 
  group_walk(~ write_parquet(.x, stringr::str_c("daio_", .y$year, ".parquet")))


git_commit_all(paste0("Data updated till ", til, " (excluded)."))
git_push()
