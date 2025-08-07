# CM360 Google Cloud Project Workflow # 
# For 5525 - Analytics, but no paid media yet # 

cm360_short_workflow <- function(vehicle) {
  
  cat("---------------------  Data pulled on:", as.character(Sys.Date()), "------------------------ \n")
  
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
  pacman::p_load(tidyverse, janitor, here, glue, reticulate, fuzzyjoin, googlesheets4, googledrive)
  # googlesheets auth
  options(gargle_oauth_cache = "/home/rstudio/R/kawasaki_cm360/.secrets",
          gargle_oauth_client_type = "web",
          gargle_oauth_email = TRUE
          # gargle_verbosity = "debug"
  )
  drive_auth()
  
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
