# CM360 Google Cloud Project Workflow #

cm360_workflow <- function(vehicle) {
  cat(
    "---------------------  Data pulled on:", as.character(Sys.Date()), "------------------------ \n"
  )

  # read in config file
  source <- "/home/rstudio/R/kawasaki_cm360/data/kawasaki_vehicle_config.xlsx"
  sheets <- readxl::excel_sheets(source) # get sheet names
  vehicle_configs <- purrr::set_names(sheets) |>
    purrr::map(~ readxl::read_excel(source, sheet = .x))

  # get the proper ID based on vehicle
  cm360_id <- vehicle_configs[[vehicle]] |>
    dplyr::filter(Source == "cm360_report") |>
    dplyr::pull(ID)

  # get google sheet id
  gs4_id <- vehicle_configs[[vehicle]] |>
    dplyr::filter(Source == "google_sheet") |>
    dplyr::pull(ID)

  # get looker sheet name to write to for Fuse
  looker_id <- vehicle_configs[[vehicle]] |>
    dplyr::filter(Source == "looker_sheet") |>
    dplyr::pull(ID)

  # SET UP ------------------------------------------------------------------
  # packages
  pacman::p_load(
    tidyverse,
    janitor,
    here,
    glue,
    reticulate,
    fuzzyjoin,
    googlesheets4,
    googledrive
  )
  # googlesheets auth
  options(
    gargle_oauth_cache = "/home/rstudio/R/kawasaki_cm360/.secrets",
    gargle_oauth_client_type = "web",
    gargle_oauth_email = TRUE
    # gargle_verbosity = "debug"
  )
  googledrive::drive_auth()
  # set the python environment used by reticulate
  reticulate::use_python(
    "/home/rstudio/.local/share/r-miniconda/envs/r-reticulate/bin/python",
    required = TRUE
  )
  # test it worked
  # py_config()

  # going to call single Python script and reauth
  reticulate::source_python(
    "/home/rstudio/R/kawasaki_cm360/py_scripts/cm360_entrypoint.py"
  )
  reticulate::source_python(
    "/home/rstudio/R/kawasaki_cm360/py_scripts/gcp_reauth.py"
  )
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/get_utms.R")
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/merge_meta.R")
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/merge_search.R")
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/ecm_helpers.R")

  # reauthorize the token here
  credentials <- gcp_reauth()

  # GET CAMPAIGN METADATA ---------------------------------------------------
  if (vehicle == "NAV") {
    # utms for multiple vehicles are in the same google sheet; need to parse individually here
    launch_utms <- get_utms("FY24 Launch KPIs")
    sustain_q1_utms <- get_utms("FY25 Q1 KPIs")
    sustain_q2_utms <- get_utms("FY25 Q2 KPIs")
    # get spend thresholds and flight dates out of the UTM data
    thresholds <- dplyr::select(
      launch_utms,
      c(
        partner = source,
        type = medium,
        threshold = planned_budget,
        flight_start,
        flight_end
      )
    ) |>
      dplyr::bind_rows(dplyr::select(
        sustain_q1_utms,
        c(
          partner = source,
          type = medium,
          threshold = planned_budget,
          flight_start,
          flight_end
        )
      )) |>
      dplyr::bind_rows(dplyr::select(
        sustain_q2_utms,
        c(
          partner = source,
          type = medium,
          threshold = planned_budget,
          flight_start,
          flight_end
        )
      )) |>
      dplyr::mutate(partner = tolower(partner))
  } else {
    q2_utms <- get_utms("FY25 5525 Q2 UTM & KPIs")
    # get spend thresholds and flights dates out of the UTM data
    thresholds <- dplyr::select(
      q2_utms,
      c(
        partner = source,
        type = medium,
        threshold = planned_budget,
        flight_start,
        flight_end
      )
    ) |>
      dplyr::mutate(partner = tolower(partner)) |>
      dplyr::filter(!is.na(threshold))
  }

  # DAILY PERFORMANCE -------------------------------------------------------

  media_report <- get_latest_cm360_report(
    profile_id = "10081289", # this is constant
    report_id = as.character(cm360_id)
  )

  media_df <- purrr::map_df(media_report, ~ tibble::as_tibble(.x)) |>
    dplyr::mutate(dplyr::across(
      "Media Cost":"Video Completions",
      as.numeric
    )) |>
    janitor::clean_names() |>
    dplyr::filter(
      !impressions == 0
    )

  # GET FULL PERFORMANCE ----------------------------------------------------

  performance_df <- googlesheets4::read_sheet(
    ss = gs4_id,
    sheet = "performance"
  )

  # Bind rows together for full view of campaign
  performance <- dplyr::bind_rows(performance_df, media_df)
  
  # get OPV for modeling 
  opv <- googlesheets4::read_sheet(ss = gs4_id, sheet = "organic_pv")

  # MERGE SITE DIRECT PARTNERS AND CLEAN SPEND ------------------------------

  clean_media <- merge_meta(vehicle, performance) |>
    dplyr::mutate(partner = trimws(partner))

  # this might need to change once TeryxH2 is live (assuming there is a search component)
  if (vehicle == "NAV") {
    clean_media <- merge_search(clean_media)
  }

  clean_media_thresholds <- fuzzyjoin::fuzzy_left_join(
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
    dplyr::select(-partner.y, -type.y) |>
    dplyr::rename(
      partner = partner.x,
      type = type.x
    ) |>
    dplyr::mutate(
      partner = ifelse(
        partner == "youtube",
        "YouTube",
        stringr::str_to_title(partner)
      )
    )

  # Clean and cap the media spend based on thresholds and flights
  capped_media <- clean_media_thresholds |>
    dplyr::arrange(partner, type, flight_start, date) |>
    dplyr::group_by(partner, type, flight_start, flight_end) |>
    dplyr::mutate(
      # 1. Flag whether date is within flight window
      in_flight = date >= flight_start & date <= flight_end,

      # 2. Only include media cost if in flight
      spend_in_flight = dplyr::if_else(in_flight, media_cost, 0),

      # 3. Cumulative raw spend (before capping)
      cumulative_spend_raw = cumsum(spend_in_flight),

      # 4. Cumulative spend capped at the threshold
      cumulative_spend_capped = pmin(cumulative_spend_raw, threshold),

      # 5. Adjust media cost:
      media_cost_adjusted = dplyr::case_when(
        !in_flight ~ 0, # not in flight? zero it
        lag(cumulative_spend_raw, default = 0) < threshold &
          cumulative_spend_raw >= threshold ~
          threshold - lag(cumulative_spend_raw, default = 0), # this row crosses the cap
        cumulative_spend_raw > threshold ~ 0, # after threshold exceeded
        TRUE ~ media_cost # normal
      ),
      threshold_crossed = lag(cumulative_spend_raw, default = 0) < threshold &
        cumulative_spend_raw >= threshold
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(!(impressions == 0 & media_cost == 0)) |>
    dplyr::filter(!is.na(date), !is.na(media_cost_adjusted)) |>
    dplyr::ungroup() |>
    dplyr::arrange(date, partner)

  if (any(is.na(capped_media$threshold))) {
    warning(
      "Some rows have missing thresholds — check for unmatched partner/type combos."
    )
  }

  # EQUILIBRIUM MODEL (ECM) -------------------------------------------------
  # this is not set up for TeryxH2 yet - not enough data # 
  if (vehicle == "NAV") {
    events <- readr::read_csv(
      "/home/rstudio/R/kawasaki_cm360/data/NAV_events_list_daily.csv"
    ) |>
      janitor::clean_names() |>
      dplyr::select(date = dates, event) |>
      dplyr::mutate(date = lubridate::mdy(date))
    
    channel_dat <- capped_media |>
      dplyr::group_by(date, type) |>
      dplyr::summarise(spend = sum(media_cost)) |>
      tidyr::pivot_wider(names_from = type, values_from = spend) |>
      dplyr::mutate(
        dplyr::across(CTV:Search, ~ ifelse(is.na(.), 0, .)),
        date = as.Date(date)
      ) |>
      dplyr::left_join(opv) |>
      dplyr::mutate(date = as.Date(date)) |>
      dplyr::left_join(events) |>
      dplyr::mutate(event = ifelse(is.na(event), 0, event))
    
    # channel model
    res <- dynamac::dynardl(
      pv ~ CTV + OLV + Social + Digital,
      data = channel_dat,
      lags = list("pv" = 1, "CTV" = 1, "OLV" = 1, "Social" = 1, Digital = 1),
      diffs = c("CTV", "OLV", "Social", "Digital"),
      # lagdiffs = list(pv = 1),
      ec = TRUE,
      simulate = FALSE
    )
    
    print(broom::tidy(summary(res)))
  
    # usage - helpers are pre-loaded
    attribution_df <- create_attribution_df(
      model = res,
      data = channel_dat,
      start_row = 30
    )
    
    # write attribution to NAV Media
    googlesheets4::sheet_write(
      ss = gs4_id,
      attribution_df,
      sheet = "attribution_df"
    )
  }
  
  # WRITE TO SHEETS ---------------------------------------------------------
  # put the day's data into the overall performance tab
  googlesheets4::sheet_append(ss = gs4_id, media_df, sheet = "performance")

  # write to Looker sheet
  googlesheets4::sheet_write(
    ss = "1cNopOsZBrl0jT5yP1ghkLlDYoY_uGoS5cOD1hE4Zrcg",
    dplyr::select(
      capped_media,
      c(
        date,
        advertiser,
        campaign,
        partner,
        type,
        placement_type,
        impressions,
        clicks,
        media_cost = media_cost_adjusted
      )
    ),
    sheet = looker_id
  )

  # write to vehicle Media (GSP sheets)
  googlesheets4::sheet_write(
    ss = gs4_id,
    dplyr::arrange(capped_media, partner, date),
    sheet = "daily_cleaned"
  )

  # WRITE RDS TO GOOGLE DRIVE -----------------------------------------------

  # get all of the data
  sheet_names <- googlesheets4::sheet_names(gs4_id)
  all_data <- purrr::set_names(sheet_names) |> # Read some sheets into a named list
    purrr::map(~ googlesheets4::read_sheet(gs4_id, sheet = .x))

  # temporary to save to google drive folder
  tmp <- tempfile(fileext = ".rds")
  saveRDS(all_data, tmp)

  # Upload and overwrite the existing file
  googledrive::drive_upload(
    media = tmp,
    name = paste0(vehicle, "_data.rds"),
    path = googledrive::as_dribble("kawasaki_campaign"),
    overwrite = TRUE
  )
}
