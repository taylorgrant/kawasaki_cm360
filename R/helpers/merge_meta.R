# Merge daily Meta data # 

merge_meta <- function(vehicle, data) {
  
  # read in vehicle config file 
  source <- "/home/rstudio/R/kawasaki_cm360/data/kawasaki_vehicle_config.xlsx"
  sheets <- readxl::excel_sheets(source) # get sheet names
  vehicle_configs <- purrr::set_names(sheets) |>
    purrr::map(~ readxl::read_excel(source, sheet = .x))
  
  # define the META ID for the google sheet
  meta_id <- vehicle_configs[[vehicle]] |>
    dplyr::filter(Source == "meta_daily") |>
    dplyr::pull(ID)
  
  # identify the sheets to use 
  sheets <- if (vehicle == "NAV") c("meta launch", "meta sustain") else c("5525")
  
  # GET THE META DATA ---------------------------------------------------
  
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
  
  # Your existing column-fixing function (unchanged)
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
  
  # Function to read and clean each sheet
  read_and_clean_meta <- function(sheet_name, meta_id) {
    read_sheet(ss = meta_id, sheet = sheet_name) |>
      clean_names() |>
      (\(df) if (sheet_name == "meta launch") rename(df, ad_name = creative) else df)() |>
      fix_numeric_list_columns()
  }
  
  # Read, clean, combine both sheets
  meta_clean <- sheets |>
    map_dfr(~ read_and_clean_meta(.x, meta_id)) |>
    mutate(
      reporting_starts = as.Date(reporting_starts),
      across(amount_spent_usd:video_plays_at_100_percent, ~ ifelse(is.na(.), 0, .))
    ) |>
    group_by(reporting_starts) |>
    summarise(
      media_cost = sum(amount_spent_usd),
      impressions = sum(impressions, na.rm = TRUE),
      clicks = sum(link_clicks, na.rm = TRUE),
      video_plays = sum(video_plays, na.rm = TRUE),
      video_completes = sum(video_plays_at_100_percent, na.rm = TRUE),
      .groups = "drop"
    ) |>
    mutate(
      site_cm360 = "meta",
      type = "Social",
      advertiser = "KAWASAKI MOTORS CORP",
      placement_type = NA,
      campaign = case_when(
        vehicle == "NAV" & reporting_starts < as.Date("2025-04-01") ~ "FY25Q1_Kawasaki_NAV_Awareness_Brand",
        vehicle == "NAV" ~ "FY25_Kawasaki_NAV_Sustain_Campaign",
        vehicle == "5525" & reporting_starts >= as.Date("2025-08-11") ~ "FY25_Kawasaki_5525_Awareness_Brand",
        TRUE ~ NA_character_
      )
    ) |>
    rename(
      date = reporting_starts,
      partner = site_cm360
    )
  

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