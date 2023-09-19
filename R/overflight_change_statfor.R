# prepare the file for the Overflight % variation map on Observable
# using STATFOR data (based on Traffic Zones)

library(tidyverse)
library(lubridate)
library(pruatlas)
library(magrittr)
library(tidyr)
library(countrycode)
library(glue)
library(eurocontrol)
library(readxl)

source(here::here("R", "helpers.R"))

ms_codes <- eurocontrol::member_state %>%
  # filter out Germany (military, no specific FIR),
  #   Luxembourg (managed by Belgium) and Monaco (managed by France)
  filter(!icao %in% c("BI", "BK", "ET", "EL", "LN")) %>%
  pull(icao) %>%
  append("GC") |> 
  sort() %>%
  unique()

til_latest <- ymd("2023-09-01") # excluded
wef_latest <- til_latest - months(2)

wef_reference <- wef_latest |> `year<-`(2019)
til_reference <- til_latest |> `year<-`(2019)

days_interval <- (til_reference - wef_reference) |> as.numeric()


daio_tz <- extract_daio_statfor()
daio <- daio_tz |> 
  filter((wef_reference <= entry_date & entry_date < til_reference) | 
           (wef_latest <= entry_date & entry_date < til_latest)) |> 
  filter(daio == "O") |> 
  mutate(
    tz_name = if_else(tz_name == "Belgium/Luxembourg", "Belgium",        tz_name),
    tz_name = if_else(str_detect(tz_name, "Norway"),   "Norway",         tz_name),
    tz_name = if_else(str_detect(tz_name, "Portugal"), "Portugal",       tz_name),
    tz_name = if_else(tz_name == "Serbia/Montenegro",  "Serbia",         tz_name),
    tz_name = if_else(str_detect(tz_name, "UK"),       "United Kingdom", tz_name),
    NULL) |> 
  left_join(eurocontrol::member_state, by = c("tz_name" = "name")) |> 
  mutate(icao = if_else(tz_name == "Spain-Canaries", "GC", icao)) |> 
  filter(icao %in% ms_codes) |> 
  select(year, entry_date, country_icao_code = icao, tz_name, flt_o = flt)



# specific for Observable map
overflight_pct_variation_last_week <- daio %>% 
  group_by(country_icao_code, tz_name, year) %>%
  summarize(average = sum(flt_o) / days_interval) %>% 
  arrange(tz_name, desc(year)) %>% 
  ungroup() %>% 
  pivot_wider(names_from = year, values_from = average7d) %>% 
  mutate(pct = 100* (`2023` / `2019` - 1),
         pct_rounded = round(pct),
         pct_rounded = if_else(pct_rounded <= -99, -100, pct_rounded))


dd <- overflight_pct_variation_last_week %>%
  mutate(
    state = tz_name,
    tz_name = if_else(tz_name == "Serbia & Montenegro", "Serbia", tz_name),
    tz_name = if_else(tz_name == "Belgium and Luxembourg", "Belgium", tz_name)) %>%
  select(id = country_icao_code, state, variation = pct_rounded) %>%
  # # add a row for Canary Islands...
  # add_row(id = "GC",
  #         state = "Spain",
  #         variation = 0) %>%
  # # ...and fill it with variation value for Spain
  # mutate(across(c(variation), ~ if_else(id == "GC", .[id == "LE"], .))) %>%
  write_csv(
    str_glue("overflight_fir_{wef}-{til}.csv",
             wef = format(wef_latest, "%Y%m%d"),
             til = format(til_latest - days(1), "%Y%m%d")))
