# Merge daily Nextdoor # 

merge_nextdoor <- function(data) {
  # pull in daily Meta data and merge into the performance data
  pacman::p_load(tidyverse, janitor, here, glue, googlesheets4, googledrive)
  options(gargle_oauth_cache = ".secrets",
          gargle_oauth_client_type = "web",
          gargle_oauth_email = TRUE
          # gargle_verbosity = "debug"
  )
  drive_auth()
  
  # Nextdoor sustain
  nd_sustain <- read_sheet(ss = "1phQYtOnQNwkDs4hwxG5e8CcmGBCUBhdCSPjhcgHWHpo",
                           sheet = "Daily Reporting") |>
    clean_names() |> 
    mutate(date = as.Date(date_pst),
           advertiser = "KAWASAKI MOTORS CORP",
           campaign = "FY25_Kawasaki_NAV_Sustain_Campaign",
           partner = "nextdoor",
           type = "Digital",
           placement_type = NA) |> 
    group_by(date, advertiser, campaign, partner, type, placement_type) |> 
    summarise(impressions = sum(impressions, na.rm = TRUE), 
              clicks = sum(clicks, na.rm = TRUE),
              media_cost = sum(spend, na.rm = TRUE), 
              video_plays = sum(video_starts, na.rm = TRUE),
              video_completes = sum(video_100_percent_completion, na.rm = TRUE))
  
  # MERGE CLEAN NEXTDOOR WITH CM360 ---------------------------------------------
  nd_dates_in_dat <- nd_sustain |> 
    # filter(partner == "nextdoor") |> 
    pull(date)
  
  tmpdat <- data |> 
    ungroup() |> 
    filter(!(partner == "nextdoor" & date %in% nd_dates_in_dat)) |> 
    bind_rows(nd_sustain |> filter(date %in% nd_dates_in_dat)) |> 
    arrange(partner, type, date) 
  return(tmpdat)
}
