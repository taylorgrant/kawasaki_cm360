# CM360 Google Cloud Project Workflow # 

cm360_workflow <- function() {

  # read in Amazon launch data 
  amzn <- readr::read_csv("/home/rstudio/R/kawasaki_cm360/data/amazon_launch.csv")
  amzn_dates <- unique(amzn$date) # dates 
  cat("---------------------  Data pulled on:", as.character(Sys.Date()), "------------------------ \n")
  
  # SET UP ------------------------------------------------------------------
  # packages 
  pacman::p_load(tidyverse, janitor, here, glue, reticulate, fuzzyjoin, googlesheets4, googledrive)
  # googlesheets auth
  options(gargle_oauth_cache = ".secrets",
          gargle_oauth_client_type = "web",
          gargle_oauth_email = TRUE
          # gargle_verbosity = "debug"
  )
  drive_auth()
  # set the python environment used by reticulate
  use_python("/home/rstudio/.local/share/r-miniconda/envs/r-reticulate/bin/python", required = TRUE)
  # test it worked 
  # py_config()
  
  # going to call single Python script and reauth 
  source_python("/home/rstudio/R/kawasaki_cm360/py_scripts/cm360_entrypoint.py")
  source_python("/home/rstudio/R/kawasaki_cm360/py_scripts/gcp_reauth.py")
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/get_utms.R")
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/merge_meta.R")
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/merge_nextdoor.R")
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/merge_search.R")
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/merge_disney.R")
  
  # reauthorize the token here
  credentials <- gcp_reauth()
  
  # GET CAMPAIGN METADATA ---------------------------------------------------
  launch_utms <- get_utms("FY24 Launch KPIs")
  sustain_q1_utms <- get_utms("FY25 Q1 KPIs")
  sustain_q2_utms <- get_utms("FY25 Q2 KPIs")
  
  # DAILY PERFORMANCE -------------------------------------------------------
  media_report <- get_latest_cm360_report(
    profile_id = "10081289",
    report_id = "1424003003"
  )
  media_df <- purrr::map_df(media_report, ~as_tibble(.x)) |> 
    mutate(across("Media Cost":"Video Completions", as.numeric)) |> 
    janitor::clean_names() |> 
    filter(str_detect(campaign, "Sustain")) |> # make sure all data is from sustain campaign now
    filter(!impressions == 0 
           #& !media_cost == 0
           )
  
  # GET FULL PERFORMANCE ----------------------------------------------------
  performance_df <- read_sheet(ss = "1dc-SL4KNa9v89CE4lGxR1ZAdoyW1SbepHzKFf7I9__k",
                               sheet = "performance")
  
  # Bind rows together for full view of campaign 
  performance <- rbind(performance_df, media_df)
  

  # MERGE SITE DIRECT PARTNERS AND CLEAN SPEND ------------------------------
  clean_media <- merge_meta(performance) |> 
    mutate(partner = trimws(partner))
  
  clean_media <- merge_nextdoor(clean_media)
  
  clean_media <- merge_search(clean_media)
  
  # clean_media <- merge_disney(clean_media)
  
  # get spend thresholds and flight dates out of the UTM data
  thresholds <- select(launch_utms, c(partner = source, type = medium, threshold = planned_budget,
                        flight_start, flight_end)) |> 
    bind_rows(select(sustain_q1_utms, c(partner = source, type = medium, threshold = planned_budget,
                             flight_start, flight_end))) |> 
    bind_rows(select(sustain_q2_utms, c(partner = source, type = medium, threshold = planned_budget,
                                        flight_start, flight_end))) |> 
    mutate(partner = tolower(partner))
  
  clean_media_thresholds <- fuzzy_left_join(
    clean_media,
    thresholds,
    by = c(
      "partner" = "partner",
      "type" = "type",
      "date" = "flight_start",
      "date" = "flight_end"
    ),
    match_fun = list(`==`, `==`, `>=`, `<=`)
  ) 
  
  clean_media_thresholds <- clean_media_thresholds |> 
    select(-partner.y, -type.y) |> 
    rename(
      partner = partner.x,
      type = type.x
    ) |> 
    mutate(partner = ifelse(partner == "youtube", "YouTube",
                            stringr::str_to_title(partner)))
  
  # Clean and cap the media spend based on thresholds and flights
  capped_media <- clean_media_thresholds |> 
    arrange(partner, type, flight_start, date) |> 
    group_by(partner, type, flight_start, flight_end) |> 
    mutate(
      # 1. Flag whether date is within flight window
      in_flight = date >= flight_start & date <= flight_end,
      
      # 2. Only include media cost if in flight
      spend_in_flight = if_else(in_flight, media_cost, 0),
      
      # 3. Cumulative raw spend (before capping)
      cumulative_spend_raw = cumsum(spend_in_flight),
      
      # 4. Cumulative spend capped at the threshold
      cumulative_spend_capped = pmin(cumulative_spend_raw, threshold),
      
      # 5. Adjust media cost:
      media_cost_adjusted = case_when(
        !in_flight ~ 0,  # not in flight? zero it
        lag(cumulative_spend_raw, default = 0) < threshold & cumulative_spend_raw >= threshold ~ 
          threshold - lag(cumulative_spend_raw, default = 0),  # this row crosses the cap
        cumulative_spend_raw > threshold ~ 0,  # after threshold exceeded
        TRUE ~ media_cost  # normal
      ),
      threshold_crossed = lag(cumulative_spend_raw, default = 0) < threshold & cumulative_spend_raw >= threshold
    ) |> 
    ungroup() |> 
    filter(!(impressions == 0 & media_cost == 0)) |> 
    filter(!is.na(date), !is.na(media_cost_adjusted)) |> 
    # adding in the Amazon spend from the Launch phase 
    ungroup() |> 
    filter(!(partner == "Amazon" & date %in% amzn_dates)) |> 
    bind_rows(amzn |> filter(date %in% amzn_dates)) |> 
    arrange(date, partner)
  
  if (any(is.na(capped_media$threshold))) {
    warning("Some rows have missing thresholds — check for unmatched partner/type combos.")
  }

  # WRITE TO SHEETS ---------------------------------------------------------
  # put the day's data into the overall performance tab
  sheet_append(ss = "1dc-SL4KNa9v89CE4lGxR1ZAdoyW1SbepHzKFf7I9__k",
               media_df,
               sheet = "performance")
  
  # write to Looker sheet
  sheet_write(ss = "1cNopOsZBrl0jT5yP1ghkLlDYoY_uGoS5cOD1hE4Zrcg",
              select(capped_media, c(date, advertiser, campaign, partner, 
                                     type, placement_type, impressions, clicks, 
                                     media_cost = media_cost_adjusted)),
              sheet = "daily")
  
  # write to NAV Media
  sheet_write(ss = "1dc-SL4KNa9v89CE4lGxR1ZAdoyW1SbepHzKFf7I9__k",
              arrange(capped_media, partner, date),
              sheet = "daily_cleaned")
}
