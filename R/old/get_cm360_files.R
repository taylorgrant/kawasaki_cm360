get_cm360_files <- function(profile_id, report_id, credentials = NULL) {
  pacman::p_load(tidyverse, reticulate)
  
  # If credentials are not provided, refresh them
  if (is.null(credentials)) {
    credentials <- gcp_reauth()
  }
  
  # Import Google API client
  googleapiclient <- import("googleapiclient.discovery")
  cm360 <- googleapiclient$build("dfareporting", "v4", credentials = credentials)
  
  # List all files for the report
  files <- cm360$reports()$files()$list(profileId = profile_id, reportId = report_id)$execute()
  
  # If no files exist, return an empty data frame
  if (is.null(files$items) || length(files$items) == 0) {
    warning("No files found for this report.")
    return(tibble(
      file_id = character(), report_id = character(), etag = character(), status = character(),
      file_name = character(), format = character(), last_modified = POSIXct(),
      start_date = character(), end_date = character(), browser_url = character(),
      api_url = character()
    ))
  }
  
  # Convert nested list to a data frame
  files_df <- files$items |> 
    map_df(~ tibble(
      file_id = .x$id,
      report_id = .x$reportId,
      etag = .x$etag,
      status = .x$status,
      file_name = .x$fileName,
      format = .x$format,
      last_modified = as.numeric(.x$lastModifiedTime) / 1000,  # Convert milliseconds to seconds
      start_date = .x$dateRange$startDate,
      end_date = .x$dateRange$endDate,
      browser_url = .x$urls$browserUrl,
      api_url = .x$urls$apiUrl
    )) |> 
    mutate(last_modified = as.POSIXct(last_modified, 
                                      origin = "1970-01-01", 
                                      tz = "America/Los_Angeles"))
  
  return(files_df)
}