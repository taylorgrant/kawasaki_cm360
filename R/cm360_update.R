# CM360 Google Cloud Project Workflow # 
cm360_update <- function(vehicle) {
  
  cat("---------------------  Data updated on:", format(Sys.time(), tz = "America/Los_Angeles"), "------------------------ \n")
  
  # read in config file
  source <- "/home/rstudio/R/kawasaki_cm360/data/kawasaki_vehicle_config.xlsx"
  sheets <- readxl::excel_sheets(source) # get sheet names
  vehicle_configs <- purrr::set_names(sheets) |>
    purrr::map(~ readxl::read_excel(source, sheet = .x))
  
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
  pacman::p_load(tidyverse, janitor, here, glue, reticulate, fuzzyjoin, googlesheets4, googledrive, dynamac)
  # googlesheets auth
  options(gargle_oauth_cache = "/home/rstudio/R/kawasaki_cm360/.secrets",
          gargle_oauth_client_type = "web",
          gargle_oauth_email = TRUE
          # gargle_verbosity = "debug"
  )
  drive_auth()

  source("/home/rstudio/R/kawasaki_cm360/R/helpers/get_utms.R")
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/merge_meta.R")
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/merge_search.R")
  
  # GET CAMPAIGN METADATA ---------------------------------------------------
  
  if (vehicle == "NAV") {
    # utms for multiple vehicles are in the same google sheet; need to parse individually here
    launch_utms <- get_utms("FY24 Launch KPIs")
    sustain_q1_utms <- get_utms("FY25 Q1 KPIs")
    sustain_q2_utms <- get_utms("FY25 Q2 KPIs")
    # get spend thresholds and flight dates out of the UTM data
    thresholds <- select(launch_utms, c(partner = source, type = medium, threshold = planned_budget,
                                        flight_start, flight_end)) |> 
      bind_rows(select(sustain_q1_utms, c(partner = source, type = medium, threshold = planned_budget,
                                          flight_start, flight_end))) |> 
      bind_rows(select(sustain_q2_utms, c(partner = source, type = medium, threshold = planned_budget,
                                          flight_start, flight_end))) |> 
      mutate(partner = tolower(partner))
  } else {
    q2_utms <- get_utms("FY25 5525 Q2 UTM & KPIs")
    # get spend thresholds and flights dates out of the UTM data
    thresholds <- select(q2_utms, c(partner = source, type = medium, threshold = planned_budget,
                                    flight_start, flight_end)) |>
      mutate(partner = tolower(partner)) |> 
      filter(!is.na(threshold))
  }
  
  # GET FULL PERFORMANCE ----------------------------------------------------
  performance <- read_sheet(ss = gs4_id,
                               sheet = "performance")
  
  opv <- read_sheet(ss = gs4_id,
                    sheet = "organic_pv")
  
  # MERGE SITE DIRECT PARTNERS AND CLEAN SPEND ------------------------------
  clean_media <- merge_meta(vehicle, performance) |> 
    mutate(partner = trimws(partner))
  
  clean_media <- merge_search(clean_media)
  
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
    arrange(date, partner)
  
  if (any(is.na(capped_media$threshold))) {
    warning("Some rows have missing thresholds — check for unmatched partner/type combos.")
  }
  
  # EQUILIBRIUM MODEL (ECM) -------------------------------------------------
  if (vehicle == "NAV") {
    events <- readr::read_csv("/home/rstudio/R/kawasaki_cm360/data/NAV_events_list_daily.csv") |> 
      janitor::clean_names() |> 
      select(date = dates, event) |> 
      mutate(date = lubridate::mdy(date))
    
    channel_dat <- capped_media |> 
      group_by(date, type) |> 
      summarise(spend = sum(media_cost)) |> 
      pivot_wider(names_from = type, values_from = spend) |> 
      mutate(across(CTV:Search, ~ ifelse(is.na(.), 0, .)),
             date = as.Date(date)) |> 
      left_join(opv) |> 
      mutate(date = as.Date(date)) |> 
      left_join(events) |> 
      mutate(event = ifelse(is.na(event), 0, event))
    
    # channel model 
    res <- dynardl(
      pv ~ CTV + OLV + Social + Digital, 
      data = channel_dat,
      lags = list("pv" = 1, "CTV" = 1, "OLV" = 1, 
                  "Social" = 1, Digital = 1),
      diffs = c("CTV", "OLV", "Social", "Digital"),
      # lagdiffs = list(pv = 1),
      ec = TRUE, simulate = FALSE)
    
    print(broom::tidy(summary(res)))
    
    # predict levels for day
    predict_today_traffic_levels <- function(model, data_today, data_yesterday, dv = "pv", pval_cutoff = 0.1) {
      library(broom)
      
      # Extract model summary
      model_summary <- tidy(summary(model))
      
      # Intercept and ECT
      intercept <- model_summary |> filter(term == "(Intercept)") |> pull(estimate)
      ect <- model_summary |> filter(term == paste0("l.1.", dv)) |> pull(estimate)
      
      # Short-run terms
      sr_terms <- model_summary |>
        filter(grepl("^d\\.1\\.", term)) |>
        mutate(variable = gsub("d.1.", "", term)) |>
        mutate(estimate = ifelse(p.value <= pval_cutoff, estimate, 0)) |>
        select(variable, estimate)
      
      # Long-run terms
      lr_terms <- model_summary |>
        filter(grepl(paste0("^l\\.1\\.(?!", dv, ")"), term, perl = TRUE)) |>
        mutate(variable = gsub("l.1.", "", term)) |>
        mutate(estimate = ifelse(p.value <= pval_cutoff, estimate, 0)) |>
        select(variable, estimate)
      
      # Short-run contribution by channel
      short_run_contrib <- sapply(sr_terms$variable, function(v) {
        delta <- data_today[[v]] - data_yesterday[[v]]
        coef <- sr_terms |> filter(variable == v) |> pull(estimate)
        coef * delta
      })
      
      short_run_total <- sum(short_run_contrib)
      
      # Long-run contribution by channel (to equilibrium)
      long_run_contrib <- sapply(lr_terms$variable, function(v) {
        coef <- lr_terms |> filter(variable == v) |> pull(estimate)
        coef * data_yesterday[[v]]
      })
      
      equilibrium <- intercept + sum(long_run_contrib)
      deviation <- data_yesterday[[dv]] - equilibrium
      adjustment <- ect * deviation
      
      # Final prediction
      predicted_pv <- data_yesterday[[dv]] + short_run_total + adjustment
      
      return(list(
        predicted_pv = predicted_pv,
        short_run_total = short_run_total,
        adjustment = adjustment,
        deviation = deviation,
        equilibrium = equilibrium,
        short_run_contrib = short_run_contrib,
        long_run_contrib = long_run_contrib
      ))
    }
    
    # build the dataframe 
    create_attribution_df <- function(model, data, dv = "pv", start_row = 2) {
      results <- map_dfr(start_row:nrow(data), function(t) {
        data_today <- data[t, ]
        data_yesterday <- data[t - 1, ]
        
        pred <- predict_today_traffic_levels(
          model = model,
          data_today = data_today,
          data_yesterday = data_yesterday,
          dv = dv
        )
        
        # Format per-channel contributions
        sr_contrib <- pred$short_run_contrib %>% 
          setNames(paste0(names(.), "_short_run")) |> 
          as_tibble_row()
        
        lr_contrib <- pred$long_run_contrib %>% 
          setNames(paste0(names(.), "_long_run")) |> 
          as_tibble_row()
        
        # Combine into one row
        tibble(
          date = data_today$date,
          actual_pv = data_today[[dv]],
          predicted_pv = pred$predicted_pv,
          deviation = pred$deviation,
          adjustment = pred$adjustment,
          short_run_total = pred$short_run_total
        ) %>%
          bind_cols(sr_contrib, lr_contrib)
      })
    }
    
    # Example usage
    attribution_df <- create_attribution_df(model = res, data = channel_dat, start_row = 30)
    
    # write attribution to NAV Media
    sheet_write(ss = gs4_id,
                attribution_df,
                sheet = "attribution_df")
  }
  
  
  # WRITE TO SHEETS ---------------------------------------------------------
  
  # write to Looker sheet
  sheet_write(ss = "1cNopOsZBrl0jT5yP1ghkLlDYoY_uGoS5cOD1hE4Zrcg",
              select(capped_media, c(date, advertiser, campaign, partner, 
                                     type, placement_type, impressions, clicks, 
                                     media_cost = media_cost_adjusted)),
              sheet = looker_id)
  
  # write to NAV Media
  sheet_write(ss = gs4_id,
              arrange(capped_media, partner, date),
              sheet = "daily_cleaned")
  
  # WRITE RDS TO GOOGLE DRIVE -----------------------------------------------
  
  # get all of the data 
  sheet_names <- sheet_names(gs4_id)
  all_data <- set_names(sheet_names) |> # Read some sheets into a named list
    map(~ read_sheet(gs4_id, sheet = .x))
  
  # temporary to save to google drive folder 
  tmp <- tempfile(fileext = ".rds")
  saveRDS(all_data, tmp)
  
  # Upload and overwrite the existing file
  drive_upload(
    media = tmp,
    name = paste0(vehicle, "_data.rds"),
    path = as_dribble("kawasaki_campaign"),
    overwrite = TRUE
  )
}
