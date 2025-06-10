# Get UTMs from Sheets #
get_utms <- function(sheet){
  pacman::p_load(tidyverse, janitor, glue, googlesheets4, googledrive)
  
  options(gargle_oauth_cache = ".secrets",
          gargle_oauth_client_type = "web",
          gargle_oauth_email = TRUE
          # gargle_verbosity = "debug"
  )
  drive_auth()
  
  # Read the entire sheet without using column names
  sheet_data <- read_sheet("1n22hGOOTeCwbYljXcbe1PeAHentosKPbCZzgpReES10",
                           sheet = sheet, 
                           col_names = FALSE)
  
  # Find the row number where the first column reads "Campaign Name & Term"
  header_row <- which(trimws(sheet_data[[1]]) == "Campaign Name & Term")
  
  if (length(header_row) == 0) {
    stop("Header row with 'Campaign Name & Term' not found.")
  }
  
  # Read again using detected header row
  data <- read_sheet("1n22hGOOTeCwbYljXcbe1PeAHentosKPbCZzgpReES10",
                     sheet = sheet,
                     skip = header_row - 1) |> 
    clean_names()
  
  # Check required columns exist
  required_cols <- c("source", "medium", "budget_needed", "flight")
  missing_cols <- setdiff(required_cols, names(data))
  if (length(missing_cols) > 0) {
    stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  }
  
  # Read the sheet again, this time using that row as the header
  data <- data |>  
    select(source, medium, planned_budget = budget_needed, flight) |> 
    mutate(gsp_source_medium = glue::glue("{tolower(source)} / {tolower(medium)}"),
           flight_start = mdy(trimws(gsub("\\-.*", "", flight))),
           flight_end = mdy(trimws(gsub(".*\\-", "", flight))))
  return(data)
}