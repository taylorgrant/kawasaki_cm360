# Function to get Kawasaki site data #

get_kawi_pv <- function(vehicle) {
  cat(
    "---------------- GA4 data pulled on:",
    as.character(Sys.Date()),
    "-------------------- \n"
  )
  cat(
    "---------------- PULLING GA4 DATA FOR:",
    vehicle,
    "----------------------------------- \n\n"
  )
  # read in config file
  source <- "/home/rstudio/R/kawasaki_cm360/data/kawasaki_vehicle_config.xlsx"
  sheets <- readxl::excel_sheets(source) # get sheet names
  vehicle_configs <- purrr::set_names(sheets) |>
    purrr::map(~ readxl::read_excel(source, sheet = .x))

  gs4_id <- vehicle_configs[[vehicle]] |>
    dplyr::filter(Source == "google_sheet") |>
    dplyr::pull(ID)

  ga4_path <- vehicle_configs[[vehicle]] |>
    dplyr::filter(Source == "ga4_path") |>
    dplyr::pull(ID)

  ga4_start <- vehicle_configs[[vehicle]] |>
    dplyr::filter(Source == "ga4_start") |>
    dplyr::pull(ID)

  ga4_start <- as.Date(as.numeric(ga4_start), origin = "1899-12-30")

  pacman::p_load(
    tidyverse,
    janitor,
    here,
    glue,
    googlesheets4,
    googledrive,
    googleAnalyticsR
  )
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/get_utms.R")

  # AUTHENTICATE ------------------------------------------------------------
  options(
    gargle_oauth_cache = "/home/rstudio/R/kawasaki_cm360/.secrets",
    gargle_oauth_client_type = "web",
    gargle_oauth_email = TRUE
    # gargle_verbosity = "debug"
  )
  googledrive::drive_auth()

  readRenviron("/home/rstudio/R/kawasaki_cm360/.Renviron")
  googleAnalyticsR::ga_auth(json_file = Sys.getenv("GA_AUTH_FILE"))
  my_property_id <- 249558280

  # BRING IN SOURCE/MEDIUMS -------------------------------------------------
  if (vehicle == "NAV") {
    # utms for multiple vehicles are in the same google sheet; need to parse individually here
    launch_utms <- get_utms("FY24 Launch KPIs")
    sustain_q1_utms <- get_utms("FY25 Q1 KPIs")
    sustain_q2_utms <- get_utms("FY25 Q2 KPIs")
    gsp_source_med <- unique(c(
      launch_utms$gsp_source_medium,
      sustain_q1_utms$gsp_source_medium,
      sustain_q2_utms$gsp_source_medium
    ))
  } else {
    q2_utms <- get_utms("FY25 5525 Q2 UTM & KPIs")
    gsp_source_med <- unique(c(q2_utms$gsp_source_medium))
  }

  # summarize page views pulled
  summarize_page_views <- function(data, source_medium) {
    fuse_sess_med <- c("google / cpc")

    # nav_pv summary (always present)
    nav_pv <- data$nav_pv |>
      dplyr::mutate(
        traffic_type = dplyr::case_when(
          sessionSourceMedium %in% c(source_medium, fuse_sess_med) ~ "paid",
          stringr::str_detect(sessionSourceMedium, "socialpaid") ~ "paid",
          TRUE ~ "organic"
        )
      ) |>
      dplyr::group_by(date, traffic_type) |>
      dplyr::summarise(nav_pv = sum(screenPageViews), .groups = "drop")

    # sub_pv summary (optional)
    if (!is.null(data$sub_pv)) {
      sub_pv <- data$sub_pv |>
        dplyr::mutate(
          traffic_type = dplyr::case_when(
            sessionSourceMedium %in% c(source_medium, fuse_sess_med) ~ "paid",
            stringr::str_detect(sessionSourceMedium, "socialpaid") ~ "paid",
            TRUE ~ "organic"
          )
        ) |>
        dplyr::group_by(date, traffic_type) |>
        dplyr::summarise(sub_pv = sum(screenPageViews), .groups = "drop")
    } else {
      sub_pv <- nav_pv |>
        dplyr::mutate(sub_pv = 0) |>
        dplyr::select(date, traffic_type, sub_pv)
    }

    pv_by_type <- nav_pv |>
      dplyr::left_join(sub_pv, by = c("date", "traffic_type")) |>
      dplyr::mutate(dplyr::across(
        c(nav_pv, sub_pv),
        ~ ifelse(is.na(.), 0, .)
      )) |>
      dplyr::mutate(pv = nav_pv + sub_pv)

    organic_pv <- pv_by_type |>
      dplyr::filter(traffic_type == "organic") |>
      dplyr::group_by(date) |>
      dplyr::summarise(pv = sum(pv))

    paid_pv <- pv_by_type |>
      dplyr::filter(traffic_type == "paid") |>
      dplyr::group_by(date) |>
      dplyr::summarise(pv = sum(pv))

    list(pv_by_type = pv_by_type, organic_pv = organic_pv, paid_pv = paid_pv)
  }

  get_page_views <- function(
    vehicle,
    my_property_id,
    page_path,
    start_date,
    end_date = Sys.Date() - 1
  ) {
    # Always fetch nav_pv
    nav_pv <- googleAnalyticsR::ga_data(
      my_property_id,
      metrics = c("screenPageViews"),
      dimensions = c("date", "sessionSourceMedium", "pagePath"),
      date_range = c(as.character(start_date), as.character(end_date)),
      dim_filters = googleAnalyticsR::ga_data_filter(
        "pagePath" %contains% page_path
      ),
      limit = -1
    ) |>
      dplyr::mutate(sessionSourceMedium = tolower(sessionSourceMedium))

    # Only fetch sub_pv for NAV
    sub_pv <- if (vehicle == "NAV") {
      googleAnalyticsR::ga_data(
        my_property_id,
        metrics = c("screenPageViews"),
        dimensions = c("date", "hostname", "sessionSourceMedium", "pagePath"),
        date_range = c(as.character(start_date), as.character(end_date)),
        dim_filters = googleAnalyticsR::ga_data_filter(
          "hostname" %contains% "nav"
        ),
        limit = -1
      ) |>
        dplyr::mutate(sessionSourceMedium = tolower(sessionSourceMedium))
    } else {
      NULL
    }

    list(nav_pv = nav_pv, sub_pv = sub_pv)
  }

  page_views <- get_page_views(
    vehicle,
    my_property_id,
    page_path = ga4_path,
    start_date = ga4_start
  )
  out <- summarize_page_views(page_views, gsp_source_med)

  # New vs Returning page views by date (paid only)
  new_v_returning <- function(page_path, source_medium) {
    # get pageviews for nav pages
    nav_pv <- googleAnalyticsR::ga_data(
      my_property_id,
      metrics = c("screenPageViews"),
      dimensions = c(
        "date",
        "newVsReturning",
        "sessionSourceMedium",
        "pagePath"
      ),
      date_range = c(
        as.character(Sys.Date() - 1),
        as.character(Sys.Date() - 1)
      ),
      dim_filters = googleAnalyticsR::ga_data_filter(
        "pagePath" %contains% page_path
      ),
      limit = -1
    ) |>
      dplyr::mutate(sessionSourceMedium = tolower(sessionSourceMedium))

    # categorizing paid media
    fuse_sess_med <- c("google / cpc")

    # split out New v Returning per day
    nav_pv |>
      dplyr::mutate(
        traffic_type = dplyr::case_when(
          sessionSourceMedium %in% c(source_medium, fuse_sess_med) ~ 'paid',
          stringr::str_detect(sessionSourceMedium, "socialpaid") ~ "paid",
          TRUE ~ "organic"
        ),
        newVsReturning = ifelse(
          is.na(newVsReturning),
          "(not set)",
          newVsReturning
        ),
        newVsReturning = ifelse(
          newVsReturning == "",
          "(not set)",
          newVsReturning
        )
      ) |>
      dplyr::group_by(date, traffic_type, newVsReturning) |>
      dplyr::summarise(nav_pv = sum(screenPageViews)) |>
      dplyr::group_by(date, traffic_type) |>
      dplyr::mutate(frac = nav_pv / sum(nav_pv))
  }
  newvreturn <- new_v_returning(ga4_path, gsp_source_med)

  geo_region <- googleAnalyticsR::ga_data(
    my_property_id,
    dimensions = c("date", "city", "region", "sessionSourceMedium"),
    metrics = c("sessions", "averageSessionDuration"),
    date_range = c(as.character(Sys.Date() - 1), as.character(Sys.Date() - 1)),
    limit = -1
    # dim_filters = ga_data_filter("sessionSourceMedium" %contains% gsp_source_med),
  ) |>
    dplyr::mutate(sessionSourceMedium = tolower(sessionSourceMedium)) |>
    dplyr::filter(sessionSourceMedium %in% gsp_source_med) |>
    dplyr::group_by(sessionSourceMedium) |>
    dplyr::slice_max(sessions, n = 20, with_ties = FALSE)

  out_list <- c(out, list(newvreturn = newvreturn, geo_region = geo_region))

  # writing to Google Sheets

  # create sheet for the first run
  googlesheets4::sheet_write(
    ss = gs4_id,
    out_list$pv_by_type,
    sheet = "pv_by_type"
  )
  googlesheets4::sheet_write(
    ss = gs4_id,
    out_list$organic_pv,
    sheet = "organic_pv"
  )
  googlesheets4::sheet_write(ss = gs4_id, out_list$paid_pv, sheet = "paid_pv")
  if (nrow(out_list$newvreturn) > 0) {
    googlesheets4::sheet_append(
      ss = gs4_id,
      out_list$newvreturn,
      sheet = "newvreturn"
    )
  }
  if (nrow(out_list$geo_region) > 0) {
    googlesheets4::sheet_append(
      ss = gs4_id,
      out_list$geo_region,
      sheet = "geo_region"
    )
  }
}
