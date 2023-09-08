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


daio_tz <- read_xlsx("data/Actuals_Jul_Aug19-23.xlsx", sheet = "data") |> 
  filter(DAIO == "TO") |> 
  mutate(Day = if_else(Yr == 2019, ymd("2019-07-01"), ymd("2023-08-31"))) |> 
  rename(Mvts = mvts)


# daio_tz <- read_xlsx("data/overflight_tz_20230621-20230627_vs_20190626-20190702.xlsx", sheet = "Sheet1")
daio <- daio_tz |> 
  rename(entry_date = Day,
         country_name = TZ,
         flt_o = Mvts) |> 
  mutate(
    entry_date = as_date(entry_date),
    country_name = if_else(country_name == "UK", "United Kingdom", country_name),
    country_name = if_else(country_name == "Lisbon FIR", "Portugal", country_name),
    country_name = if_else(country_name == "Belgium/Luxembourg", "Belgium", country_name),
    country_name = if_else(country_name == "Serbia/Montenegro", "Serbia", country_name),
    NULL) |> 
  left_join(eurocontrol::member_state, by = c("country_name" = "name")) |> 
  mutate(icao = if_else(country_name == "Spain-Canaries", "GC", icao)) |> 
  filter(icao %in% ms_codes) |> 
  select(entry_date, country_icao_code = icao, country_name, flt_o)

til_latest <- daio %>% pull(entry_date) %>% unique() %>% max() %>% add(ddays(1))
# wef_latest <- til_latest - days(7)
wef_latest <- til_latest - months(2)

wef_reference <- daio %>% pull(entry_date) %>% unique() %>% min()
# til_reference <- wef_reference + days(7)
til_reference <- wef_reference + months(2)

overflight_pct_variation_last_week <- daio %>% 
  filter((wef_latest <= entry_date & entry_date < til_latest) |
           (wef_reference  <= entry_date & entry_date < til_reference)) %>%
  mutate(year = year(entry_date)) %>% 
  group_by(country_icao_code, country_name, year) %>%
  summarize(average7d = sum(flt_o) / 7) %>% 
  arrange(country_name, desc(year)) %>% 
  ungroup() %>% 
  pivot_wider(names_from = year, values_from = average7d) %>% 
  mutate(pct = 100* (`2023` / `2019` - 1),
         pct_rounded = round(pct),
         pct_rounded = if_else(pct_rounded <= -99, -100, pct_rounded))


dd <- overflight_pct_variation_last_week %>%
  mutate(
    state = country_name,
    country_name = if_else(country_name == "Serbia & Montenegro", "Serbia", country_name),
    country_name = if_else(country_name == "Belgium and Luxembourg", "Belgium", country_name)) %>%
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
