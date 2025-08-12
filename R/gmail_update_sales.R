# Update daily sales via Kawi email #

gmail_update_sales <- function(vehicle) {
  # pacman::p_load(
  #   tidyverse,
  #   janitor,
  #   here,
  #   glue,
  #   googlesheets4,
  #   gmailr,
  #   base64enc,
  #   lubridate,
  #   googlesheets4,
  #   googledrive
  # )

  options(
    gargle_oauth_cache = "/home/rstudio/R/kawasaki_cm360/.secrets",
    gargle_oauth_client_type = "web",
    gargle_oauth_email = TRUE
    # gargle_verbosity = "debug"
  )
  googledrive::drive_auth()

  # VEHICLE CONFIG FILE -----------------------------------------------------
  source <- "/home/rstudio/R/kawasaki_cm360/data/kawasaki_vehicle_config.xlsx"
  sheets <- readxl::excel_sheets(source) # get sheet names
  vehicle_configs <- purrr::set_names(sheets) |>
    purrr::map(~ readxl::read_excel(source, sheet = .x))

  # get google sheet id
  gs4_id <- vehicle_configs[[vehicle]] |>
    dplyr::filter(Source == "google_sheet") |>
    dplyr::pull(ID)

  vehicle_subject <- vehicle_configs[[vehicle]] |>
    dplyr::filter(Source == "gmail_subject") |>
    dplyr::pull(ID)

  # GMAIL SET UP ------------------------------------------------------------
  token <- readRDS(
    "/home/rstudio/.gmail-oauth/07c67c617ffee7cf1e64b4ff4ac85739_gspanalytics21@gmail.com"
  )
  gmailr::gm_auth_configure(
    path = "/home/rstudio/R/kawasaki_cm360/credentials/gsp21-gmail.json"
  )
  gmailr::gm_auth(token = token)

  # GMAIL - SEARCH FOR UNREAD EMAIL FROM KAWI WITH CSV ----------------------
  # query <- glue(
  #   'from:Brandon.Flanders@kmc-usa.com has:attachment is:unread filename:csv subject:"{vehicle_subject}"'
  # )
  query <- glue::glue(
    'from:taylor_grant@gspsf.com has:attachment is:unread filename:csv subject:"{vehicle_subject}"'
  )
  msgs <- gmailr::gm_messages(search = query, num_results = 1)

  if (length(msgs[[1]]$messages) == 0) {
    base::message("No unread email with .csv attachment found.")
    return(invisible(NULL))
  }

  msg_id <- msgs[[1]]$messages[[1]]$id
  msg <- gmailr::gm_message(msg_id)

  # -- Helper to extract all parts recursively --
  find_all_parts <- function(part) {
    if (!is.null(part$parts)) {
      return(c(
        list(part),
        unlist(lapply(part$parts, find_all_parts), recursive = FALSE)
      ))
    } else {
      return(list(part))
    }
  }

  # EXTRACT CSV -------------------------------------------------------------
  all_parts <- find_all_parts(msg$payload)
  csv_part <- Filter(
    function(p) {
      !is.null(p$filename) &&
        grepl("\\.csv$", p$filename, ignore.case = TRUE) &&
        !is.null(p$body$attachmentId)
    },
    all_parts
  )[[1]]

  if (is.null(csv_part)) {
    message("No CSV attachment found in the latest email.")
    return(invisible(NULL))
  }

  # -- Decode base64url attachment --
  normalize_base64 <- function(x) {
    x <- gsub("-", "+", x, fixed = TRUE)
    x <- gsub("_", "/", x, fixed = TRUE)
    pad_len <- 4 - (nchar(x) %% 4)
    if (pad_len < 4) {
      x <- paste0(x, strrep("=", pad_len))
    }
    x
  }

  att <- gmailr::gm_attachment(msg_id, id = csv_part$body$attachmentId)
  raw_data <- base64enc::base64decode(normalize_base64(att$data))
  tmp_file <- tempfile(fileext = ".csv")
  writeBin(raw_data, tmp_file)

  # READ DAILY FILE ---------------------------------------------------------
  daily <- tryCatch(
    {
      readr::read_delim(
        file = tmp_file,
        delim = "\t",
        locale = locale(encoding = "UTF-16LE"),
        show_col_types = FALSE
      ) |>
        janitor::clean_names() |>
        dplyr::mutate(
          date = as.Date(lubridate::mdy(date)),
          dealer_number = as.numeric(dealer_number),
          dealer_store_name = trimws(dealer_store_name)
        )
    },
    error = function(e) {
      stop("Failed to read daily dealer file: ", e$message)
    }
  )

  # GOOGLE SHEETS  ----------------------------------------------------------
  # Read in current data
  df <- tryCatch(
    {
      googlesheets4::read_sheet(ss = gs4_id, sheet = "dealer_sales") |>
        dplyr::mutate(
          date = as.Date(date),
          dealer_number = as.numeric(dealer_number)
        )
    },
    error = function(e) {
      stop("Failed to read dealer sales from Google Sheets: ", e$message)
    }
  )

  processed_df <- tryCatch(
    {
      googlesheets4::read_sheet(
        ss = gs4_id,
        sheet = "dealer_sales_processed"
      ) |>
        dplyr::mutate(date = as.Date(date))
    },
    error = function(e) {
      stop("Failed to read dealer sales from Google Sheets: ", e$message)
    }
  )

  # Check for daily sales
  daily_sales <- daily |> dplyr::filter(!is.na(retail_unit_count))

  if (nrow(daily_sales) > 0) {
    df <- dplyr::bind_rows(
      df,
      dplyr::select(daily_sales, -dealer_inventory_unit_count)
    )
  } else {
    base::message(
      "Daily file contains no new sales. Dealer sales table not updated, but daily sheet will still be refreshed."
    )
  }

  agg_sales <- df |>
    dplyr::group_by(dealer_number) |>
    dplyr::summarise(
      retail_unit_count = sum(retail_unit_count),
      .groups = "drop"
    )

  daily_updated <- daily |>
    dplyr::select(-retail_unit_count) |>
    dplyr::left_join(agg_sales, by = "dealer_number") |>
    dplyr::mutate(retail_unit_count = dplyr::coalesce(retail_unit_count, 0))

  # Mark off the day as processed
  mark_today_processed <- function(df, today) {
    df %>%
      dplyr::mutate(processed = ifelse(date == today, TRUE, processed))
  }

  processed_df <- mark_today_processed(
    processed_df,
    today = daily_updated$date[1]
  )

  # Push to Google Sheets
  googlesheets4::sheet_write(df, ss = gs4_id, sheet = "dealer_sales")
  googlesheets4::sheet_write(
    daily_updated,
    ss = gs4_id,
    sheet = "daily_dealer_sales"
  )
  googlesheets4::sheet_write(
    processed_df,
    ss = gs4_id,
    sheet = "dealer_sales_processed"
  )

  # Mark the message as read
  gmailr::gm_modify_message(msg_id, remove_labels = "UNREAD")

  base::message("Sales data updated successfully.")
}
