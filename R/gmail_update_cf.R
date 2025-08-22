# Update daily data from Channel Factory #

gmail_update_cf <- function(vehicle) {
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

  cat(
    "---------------------", vehicle,"-- CF Data updated on:",
    format(Sys.time(), tz = "America/Los_Angeles"),
    "------------------------ \n"
  )
  
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

  # define the META ID for the google sheet
  meta_id <- vehicle_configs[[vehicle]] |>
    dplyr::filter(Source == "meta_daily") |>
    dplyr::pull(ID)

  vehicle_subject <- vehicle_configs[[vehicle]] |>
    dplyr::filter(Source == "meta_gmail_subject") |>
    dplyr::pull(ID)

  # GMAIL SET UP ------------------------------------------------------------
  token <- readRDS(
    "/home/rstudio/.gmail-oauth/07c67c617ffee7cf1e64b4ff4ac85739_gspanalytics21@gmail.com"
  )
  gmailr::gm_auth_configure(
    path = "/home/rstudio/R/kawasaki_cm360/credentials/gsp21-gmail.json"
  )
  gmailr::gm_auth(token = token)

  # GMAIL - SEARCH FOR UNREAD EMAIL FROM DATORAMA WITH XLSX ----------------------
  query <- glue::glue(
    'from:noreply@datorama.com has:attachment is:unread filename:xlsx subject:"{vehicle_subject}"'
  )
  msgs <- gmailr::gm_messages(search = query, num_results = 1)

  if (length(msgs[[1]]$messages) == 0) {
    message("No unread email with .xlsx attachment found.")
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

  # EXTRACT XLSX -------------------------------------------------------------
  all_parts <- find_all_parts(msg$payload)
  xlsx_part <- Filter(
    function(p) {
      !is.null(p$filename) &&
        grepl("\\.xlsx$", p$filename, ignore.case = TRUE) &&
        !is.null(p$body$attachmentId)
    },
    all_parts
  )[[1]]

  if (is.null(xlsx_part)) {
    message("No XLSX attachment found in the latest email.")
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

  att <- gmailr::gm_attachment(msg_id, id = xlsx_part$body$attachmentId)
  raw_data <- base64enc::base64decode(normalize_base64(att$data))
  tmp_file <- tempfile(fileext = ".xlsx")
  writeBin(raw_data, tmp_file)

  # READ DAILY META FILE ---------------------------------------------------------
  if (vehicle == "NAV") {
    daily_meta_nav <- tryCatch(
      {
        readxl::read_excel(tmp_file, skip = 3, sheet = "Kawasaki NAV - META") |>
          janitor::clean_names() |>
          dplyr::mutate(
            `Reporting starts` = as.Date(as.numeric(day), origin = "1899-12-30"),
            Creative = creative_name,
            `Campaign name` = "Leads_Auction_Kawasaki GS&P US PTV H1'25 Sustain_Q2'25 OP050409_META CPA KAWASAKI SUSTAIN LEAD GEN Q2 PL184209",
            `Ad Set Name` = paste0(
              "META CPA KAWASAKI SUSTAIN LEAD GEN Q2 PL184209_Leads_States_INT_ALL_",
              targeting_tactic
            ),
            leads = as.numeric(leads)
          ) |>
          dplyr::filter(`Reporting starts` == Sys.Date() - 1) |>
          dplyr::group_by(
            `Reporting starts`,
            Creative,
            `Campaign name`,
            `Ad Set Name`
          ) |>
          dplyr::summarise(
            `Amount spent (USD)` = sum(media_cost, na.rm = TRUE),
            Impressions = sum(impressions, na.rm = TRUE),
            `Link clicks` = sum(clicks_links, na.rm = TRUE),
            `Video plays` = sum(video_plays, na.rm = TRUE),
            `Video plays at 100%` = sum(video_fully_played, na.rm = TRUE),
            `Post engagements` = NA,
            Leads = sum(leads, na.rm = TRUE)
          )
      },
      error = function(e) {
        stop("Failed to read daily meta file: ", e$message)
      }
    )
    # GOOGLE SHEETS  ----------------------------------------------------------
    if (nrow(daily_meta_nav) > 0) {
      googlesheets4::sheet_append(ss = meta_id, daily_meta_nav, sheet = "meta sustain")
    }
  } else {
    
    # META TERYXH2
    daily_meta_teryx <- tryCatch(
      {
        readxl::read_excel(tmp_file, skip = 3, sheet = "Kawasaki 5525 - META") |>
          janitor::clean_names() |>
          dplyr::mutate(
            `Reporting starts` = as.Date(as.numeric(day), origin = "1899-12-30"),
            Creative = creative_name,
            `Campaign name` = campaign_name,
            `Ad Set Name` = NA,
            leads = as.numeric(leads)
            ) |> 
          dplyr::filter(`Reporting starts` == Sys.Date() - 1) |>
          dplyr::group_by(
            `Reporting starts`,
            Creative,
            `Campaign name`,
            `Ad Set Name`
          ) |>
          dplyr::summarise(
            `Amount spent (USD)` = sum(media_cost, na.rm = TRUE),
            Impressions = sum(impressions, na.rm = TRUE),
            `Link clicks` = sum(clicks_links, na.rm = TRUE),
            `Video plays` = sum(video_plays, na.rm = TRUE),
            `Video plays at 100%` = sum(video_fully_played, na.rm = TRUE),
            `Post engagements` = NA,
            Leads = sum(leads, na.rm = TRUE)
          )
      },
      error = function(e) {
        stop("Failed to read daily meta file: ", e$message)
      }
    )
    # GOOGLE SHEETS  ----------------------------------------------------------
    if (nrow(daily_meta_teryx) > 0) {
      googlesheets4::sheet_append(ss = meta_id, daily_meta_teryx, sheet = "meta")
    }
    
    # YOUTUBE TERYX
    daily_youtube_teryx <- tryCatch(
      {
        readxl::read_excel(tmp_file, skip = 3, sheet = "Kawasaki 5525 - YT") |>
          janitor::clean_names() |>
          dplyr::mutate(
            `Reporting starts` = as.Date(as.numeric(day), origin = "1899-12-30"),
            Creative = creative,
            `Campaign name` = campaign,
            `Ad Set Name` = placement
          ) |> 
          dplyr::filter(`Reporting starts` == Sys.Date() - 1) |>
          dplyr::group_by(
            `Reporting starts`,
            Creative,
            `Campaign name`,
            `Ad Set Name`
          ) |>
          dplyr::summarise(
            `Amount spent (USD)` = sum(media_spend, na.rm = TRUE),
            Impressions = sum(impressions, na.rm = TRUE),
            `Link clicks` = sum(clicks, na.rm = TRUE),
            `Video plays` = sum(video_plays, na.rm = TRUE),
            `Video plays at 100%` = sum(video_fully_played, na.rm = TRUE),
            `Post engagements` = sum(engagements)
          )
      },
      error = function(e) {
        stop("Failed to read daily meta file: ", e$message)
      }
    )
    # GOOGLE SHEETS  ----------------------------------------------------------
    if (nrow(daily_meta_teryx) > 0) {
      googlesheets4::sheet_append(ss = meta_id, daily_youtube_teryx, sheet = "youtube")
    }
  }
  # TeryxH2 runs after NAV, so we'll only mark as read for Teryx
  if (vehicle == "TeryxH2") {
    # Mark the message as read
    gmailr::gm_modify_message(msg_id, remove_labels = "UNREAD")
  }
  

  base::message("Daily CF data updated successfully.")
}
