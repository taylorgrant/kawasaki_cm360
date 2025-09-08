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
  sheets <- if (vehicle == "NAV") {
    c("meta launch", "meta sustain")
  } else {
    c("meta")
  }

  # GET THE META DATA ---------------------------------------------------

  # 1. Clean the performance data and summarize to partner and channel
  tmpdat <- data |>
    # filter(str_detect(campaign, "Sustain")) |>
    dplyr::mutate(across(impressions:media_cost, as.numeric)) |>
    dplyr::mutate(
      partner = tolower(sapply(stringr::str_split(placement, "_"), "[[", 1)),
      placement_type = NA,
      type = sapply(stringr::str_split(placement, "_"), "[[", 10),
      type = dplyr::case_when(
        type == "SOC" ~ "Social",
        type == "DIS" ~ "Digital",
        is.na(type) ~ "Unknown",
        TRUE ~ type
      ),
      campaign = stringr::str_replace_all(campaign, "FY26", "FY25"),
      date = as.Date(date)
    ) |>
    dplyr::group_by(
      date,
      advertiser,
      campaign,
      partner,
      type,
      placement_type
    ) |>
    dplyr::summarise(
      impressions = sum(impressions),
      clicks = sum(clicks),
      media_cost = round(sum(media_cost)),
      video_plays = sum(video_plays),
      video_completes = sum(video_completions)
    )

  # 2. Get Meta Performance Data
  numeric_cols <- c(
    "amount_spent_usd",
    "impressions",
    "link_clicks",
    "video_plays",
    "video_plays_at_100_percent",
    "post_engagements",
    "leads"
  )

  # Column-fixing function
  fix_numeric_list_columns <- function(df) {
    for (col in names(df)) {
      col_data <- df[[col]]

      if (is.list(col_data)) {
        df[[col]] <- vapply(
          col_data,
          function(x) {
            if (is.null(x) || length(x) == 0) {
              return(NA_real_)
            }
            val <- x[[1]]
            if (identical(val, "-")) {
              return(NA_real_)
            }
            out <- suppressWarnings(as.numeric(val))
            if (is.na(out)) NA_real_ else out
          },
          numeric(1)
        )
      }
    }
    df
  }

  # Function to read and clean each sheet
  read_and_clean_meta <- function(sheet_name, meta_id) {
    googlesheets4::read_sheet(ss = meta_id, sheet = sheet_name) |>
      janitor::clean_names() |>
      (\(df) {
        if (sheet_name == "meta launch") {
          dplyr::rename(df, ad_name = creative)
        } else {
          df
        }
      })() |>
      fix_numeric_list_columns()
  }

  # Read, clean, combine both sheets
  meta_clean <- sheets |>
    purrr::map_dfr(~ read_and_clean_meta(.x, meta_id)) |>
    dplyr::mutate(
      reporting_starts = as.Date(reporting_starts),
      dplyr::across(
        amount_spent_usd:video_plays_at_100_percent,
        ~ ifelse(is.na(.), 0, .)
      )
    ) |>
    dplyr::group_by(reporting_starts) |>
    dplyr::summarise(
      media_cost = sum(amount_spent_usd),
      impressions = sum(impressions, na.rm = TRUE),
      clicks = sum(link_clicks, na.rm = TRUE),
      video_plays = sum(video_plays, na.rm = TRUE),
      video_completes = sum(video_plays_at_100_percent, na.rm = TRUE),
      leads = sum(leads, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      site_cm360 = "meta",
      type = "Social",
      advertiser = "KAWASAKI MOTORS CORP",
      placement_type = NA,
      campaign = dplyr::case_when(
        vehicle == "NAV" & reporting_starts < as.Date("2025-04-01") ~
          "FY25Q1_Kawasaki_NAV_Awareness_Brand",
        vehicle == "NAV" ~ "FY25_Kawasaki_NAV_Sustain_Campaign",
        vehicle == "TeryxH2" & reporting_starts >= as.Date("2025-08-11") ~
          "FY25_Kawasaki_5525_Awareness_Brand",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::rename(
      date = reporting_starts,
      partner = site_cm360
    )

  # MERGE CLEAN META WITH CM360 ---------------------------------------------
  # meta_dates_in_tmpdat <- tmpdat |>
  #   filter(partner == "meta") |>
  #   pull(date)

  meta_dates_in_clean <- meta_clean |>
    dplyr::pull(date)

  tmpdat <- tmpdat |>
    dplyr::ungroup() |>
    dplyr::filter(!(partner == "meta" & date %in% meta_dates_in_clean)) |>
    dplyr::bind_rows(
      meta_clean |> dplyr::filter(date %in% meta_dates_in_clean)
    ) |>
    dplyr::mutate(partner = trimws(partner)) |>
    dplyr::arrange(partner, type, date)

  return(tmpdat)
}
