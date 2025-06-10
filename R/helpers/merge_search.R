# Merge search data # 

merge_search <- function(data){
  # pull in daily Meta data and merge into the performance data
  pacman::p_load(tidyverse, janitor, here, glue, googlesheets4, googledrive)
  options(gargle_oauth_cache = ".secrets",
          gargle_oauth_client_type = "web",
          gargle_oauth_email = TRUE
          # gargle_verbosity = "debug"
  )
  drive_auth()
  
  # Nextdoor sustain
  search_sustain <- read_sheet(ss = "17ioPxO_FgV8DjT8hHT5O8DMa8JEtRokKsTmoJoW0uKM",
                           sheet = "search sustain") |>
    clean_names() |> 
    mutate(date = as.Date(date),
           advertiser = "KAWASAKI MOTORS CORP",
           campaign = "FY25_Kawasaki_NAV_Sustain_Campaign",
           partner = "google",
           type = "Search",
           placement_type = NA) |> 
    group_by(date, advertiser, campaign, partner, type, placement_type) |> 
    summarise(impressions = sum(impressions, na.rm = TRUE), 
              clicks = sum(clicks, na.rm = TRUE),
              media_cost = sum(spend, na.rm = TRUE), 
              video_plays = NA,
              video_completes = NA)
  
  # MERGE CLEAN NEXTDOOR WITH CM360 ---------------------------------------------
  search_dates_in_dat <- search_sustain |> 
    # filter(partner == "nextdoor") |> 
    pull(date)
  
  tmpdat <- data |> 
    ungroup() |> 
    filter(!(partner == "google" & type == "Search" & date %in% search_dates_in_dat)) |> 
    bind_rows(search_sustain |> filter(date %in% search_dates_in_dat)) |> 
    arrange(partner, type, date) 
  return(tmpdat)
}