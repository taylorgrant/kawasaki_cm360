merge_disney <- function(data) {
  # pull in daily Meta data and merge into the performance data
  pacman::p_load(tidyverse, janitor, here, glue, googlesheets4, googledrive)
  options(gargle_oauth_cache = ".secrets",
          gargle_oauth_client_type = "web",
          gargle_oauth_email = TRUE
          # gargle_verbosity = "debug"
  )
  drive_auth()
  
  # Disney sustain
  disney_sustain <- read_sheet(ss = "1HuTM9XxvMoSTIZJaSHl08AWStMRz6q3YyHrcOLbQfMk") |>
    clean_names()
  
  # bind together 
  disney_clean <- disney_sustain |> 
    mutate(date = as.Date(date),
           across(impressions:video_plays, ~ ifelse(is.na(.), 0, .)))
  
  disney_clean <- disney_clean |> 
    group_by(date) |> 
    summarise(media_cost = sum(media_cost),
              impressions = sum(impressions, na.rm = TRUE),
              clicks = sum(clicks, na.rm = TRUE),
              video_plays = sum(video_plays, na.rm = TRUE),
              video_completes = sum(video_completions, na.rm = TRUE)) |> 
    mutate(site_cm360 = "disney",
           type = "CTV",
           placement_type = NA,
           date = as.Date(date),
           advertiser = "KAWASAKI MOTORS CORP",
           campaign = case_when(
             date < "2025-04-01" ~ "FY25Q1_Kawasaki_NAV_Awareness_Brand",
             TRUE ~ "FY25_Kawasaki_NAV_Sustain_Campaign"
           )) |> 
    rename(partner = site_cm360) 
  
  
  # MERGE CLEAN META WITH CM360 ---------------------------------------------
  # meta_dates_in_tmpdat <- tmpdat |> 
  #   filter(partner == "meta") |> 
  #   pull(date)
  
  disney_dates_in_clean <- disney_clean |> 
    pull(date)
  
  tmpdat <- data |> 
    ungroup() |> 
    # filter(!(partner == "disney" & date %in% disney_dates_in_clean)) |> 
    # bind_rows(disney_clean |> filter(date %in% disney_dates_in_clean)) |>
    bind_rows(disney_clean) |> 
    mutate(partner = trimws(partner)) |> 
    group_by(date, advertiser, campaign, partner, type, placement_type) |> 
    summarise(across(impressions:video_completes, sum)) |> 
    mutate(media_cost = ifelse(partner == "disney", (impressions/1000)*17.78, media_cost)) |> 
    arrange(partner, type, date)
  
  return(tmpdat)
}