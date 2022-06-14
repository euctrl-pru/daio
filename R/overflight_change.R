# prepare the file for the Overflight % variation map on Observable

library(tidyverse)
library(lubridate)
library(pruatlas)
library(magrittr)
library(tidyr)
library(countrycode)
library(glue)

source(here::here("R", "extract_daio.R"))


ms_codes <- member_states %>%
  # filter out Germany (military, no specific FIR),
  #   Luxembourg (managed by Belgium) and Monaco (managed by France)
  filter(!icao %in% c("ET", "EL", "LN")) %>%
  pull(icao) %>%
  sort() %>%
  unique()


con <- fr24gu::db_connection(schema = "PRU_DEV")
wef <- "2019-01-01" %>% as_date()
til <- today()
daio <- extract_daio(con, wef, til)


# average last week and calculate % difference compared to the same week interval in 2019
til_latest <- daio %>% pull(entry_date) %>% unique() %>% max() %>% add(ddays(1))
wef_latest <- til_latest - days(7)

til_reference <- til_latest
year(til_reference) <- 2019
wef_reference <- til_reference - days(7)

overflight_pct_variation_last_week <- daio %>% 
  filter((wef_latest <= entry_date & entry_date < til_latest) |
           (wef_reference  <= entry_date & entry_date < til_reference)) %>%
  mutate(year = year(entry_date)) %>% 
  group_by(country_name, year) %>%
  summarize(average7d = sum(flt_o) / 7) %>% 
  arrange(country_name, desc(year)) %>% 
  ungroup() %>% 
  pivot_wider(names_from = year, values_from = average7d) %>% 
  mutate(pct = 100* (`2022` / `2019` - 1),
         pct_rounded = round(pct),
         pct_rounded = if_else(pct_rounded <= -99, -100, pct_rounded))


dd <- overflight_pct_variation_last_week %>%
  mutate(
    state = country_name,
    country_name = if_else(country_name == "Serbia & Montenegro", "Serbia", country_name),
    country_name = if_else(country_name == "Belgium and Luxembourg", "Belgium", country_name),
    country_name = if_else(country_name == "Türkiye", "Turkey", country_name),
    iso2a = countrycode(country_name, origin = "country.name", destination = "icao")) %>%
  left_join(member_states) %>%
  select(id = icao, state, variation = pct_rounded) %>%
  write_csv("overflight_fir.csv")
