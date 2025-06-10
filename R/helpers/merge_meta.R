# Merge daily Meta data # 

merge_meta <- function(data) {
  # pull in daily Meta data and merge into the performance data
  pacman::p_load(tidyverse, janitor, here, glue, googlesheets4, googledrive)
  options(gargle_oauth_cache = ".secrets",
          gargle_oauth_client_type = "web",
          gargle_oauth_email = TRUE
          # gargle_verbosity = "debug"
  )
  drive_auth()
  
  # 1. Clean the performance data and summarize to partner and channel
  tmpdat <- data |> 
    # filter(str_detect(campaign, "Sustain")) |> 
    mutate(across(impressions:media_cost, as.numeric)) |> 
    mutate(partner = tolower(sapply(stringr::str_split(placement, "_"), "[[", 1)),
           placement_type = NA,
           type = sapply(str_split(placement, "_"), "[[", 10),
           type = case_when(type == "SOC" ~ "Social",
                            type == "DIS" ~ "Digital",
                            is.na(type) ~ "Unknown",
                            TRUE ~  type),
           campaign = str_replace_all(campaign, "FY26", "FY25"),
           date = as.Date(date)) |> 
    group_by(date, advertiser, campaign, partner, type, placement_type) |> 
    summarise(impressions = sum(impressions),
              clicks = sum(clicks),
              media_cost = round(sum(media_cost)),
              video_plays = sum(video_plays),
              video_completes = sum(video_completions))
  
  # 2. Get Meta Performance Data (for Sustain campaign)
  numeric_cols <- c("amount_spent_usd", "impressions", "link_clicks", "video_plays",
                    "video_plays_at_100_percent", "post_engagements", "leads")
  
  # Meta launch
  df_launch <- read_sheet(ss = "17ioPxO_FgV8DjT8hHT5O8DMa8JEtRokKsTmoJoW0uKM",
                   sheet = "meta launch") |>
    clean_names() |> 
    rename(ad_name = creative)
  
  # Meta sustain
  df_sustain <- read_sheet(ss = "17ioPxO_FgV8DjT8hHT5O8DMa8JEtRokKsTmoJoW0uKM",
                   sheet = "meta sustain") |>
    clean_names()
  
  # function to clean sheet 
  fix_numeric_list_columns <- function(df) {
    for (col in names(df)) {
      col_data <- df[[col]]
      
      if (is.list(col_data)) {
        df[[col]] <- vapply(col_data, function(x) {
          if (is.null(x) || length(x) == 0) return(NA_real_)
          val <- x[[1]]
          if (identical(val, "-")) return(NA_real_)
          out <- suppressWarnings(as.numeric(val))
          if (is.na(out)) NA_real_ else out
        }, numeric(1))
      }
    }
    df
  }
  
  meta_launch_clean <- fix_numeric_list_columns(df_launch) |> 
    rename(creative = ad_name)
  meta_sustain_clean <- fix_numeric_list_columns(df_sustain)
  
  # bind together 
  meta_clean <- rbind(meta_launch_clean, meta_sustain_clean) |> 
    mutate(reporting_starts = as.Date(reporting_starts),
           across(amount_spent_usd:video_plays_at_100_percent, ~ ifelse(is.na(.), 0, .)))
  
  meta_clean <- meta_clean |> 
    group_by(reporting_starts) |> 
    summarise(media_cost = sum(amount_spent_usd),
              impressions = sum(impressions, na.rm = TRUE),
              clicks = sum(link_clicks, na.rm = TRUE),
              video_plays = sum(video_plays, na.rm = TRUE),
              video_completes = sum(video_plays_at_100_percent, na.rm = TRUE)) |> 
    mutate(site_cm360 = "meta",
           type = "Social",
           placement_type = NA,
           reporting_starts = as.Date(reporting_starts),
           advertiser = "KAWASAKI MOTORS CORP",
           campaign = case_when(
             reporting_starts < "2025-04-01" ~ "FY25Q1_Kawasaki_NAV_Awareness_Brand",
             TRUE ~ "FY25_Kawasaki_NAV_Sustain_Campaign"
           )) |> 
    rename(date = reporting_starts,
           partner = site_cm360) 
  

  # MERGE CLEAN META WITH CM360 ---------------------------------------------
  # meta_dates_in_tmpdat <- tmpdat |> 
  #   filter(partner == "meta") |> 
  #   pull(date)
  
  meta_dates_in_clean <- meta_clean |> 
    pull(date)
  
  tmpdat <- tmpdat |> 
    ungroup() |> 
    filter(!(partner == "meta" & date %in% meta_dates_in_clean)) |> 
    bind_rows(meta_clean |> filter(date %in% meta_dates_in_clean)) |>
    mutate(partner = trimws(partner)) |> 
    arrange(partner, type, date)
  
  return(tmpdat)
}