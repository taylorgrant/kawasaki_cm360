get_latest_cm360_report <- function(files_df, credentials) {
  pacman::p_load(tidyverse, reticulate, httr, janitor)
  
  # Ensure the data frame is not empty
  if (nrow(files_df) == 0) {
    warning("No available files found. Returning empty tibble.")
    return(tibble())
  }
  
  # Find the most recent file based on last_modified timestamp
  latest_file <- files_df |> 
    dplyr::arrange(desc(last_modified)) |> 
    dplyr::slice(1)  # Select the most recent file
  
  # Extract the API URL for downloading
  download_url <- latest_file$api_url
  
  # Import Python requests module
  requests <- reticulate::import("requests")
  
  # Attach OAuth token manually
  headers <- list(
    "Authorization" = paste("Bearer", credentials$token),
    "Accept" = "application/json"
  )
  
  # Make an authenticated request
  response <- requests$get(download_url, headers = headers)
  
  # Check if the request was successful
  if (response$status_code != 200) {
    stop("Failed to download the report. Check API access and permissions.")
  }
  
  # Convert Python response to R raw vector
  raw_vector <- py_to_r(response$content) |> as.raw()
  
  # Read CSV from raw content (handling errors)
  return(tryCatch({
  lines <- readLines(rawConnection(raw_vector))
  
  header_line <- which(str_detect(lines, "^Date,"))[1]
  if (is.na(header_line)) stop("Could not find header row starting with 'Date'.")
  
  report_data <- read_csv(rawConnection(raw_vector), skip = header_line - 1) |>
    janitor::clean_names() |>
    filter(date != "Grand Total:")
  
  return(report_data)
  
}, error = function(e) {
  warning(paste("Error reading the report file:", e$message))
  return(NULL)
}))
  
  
}