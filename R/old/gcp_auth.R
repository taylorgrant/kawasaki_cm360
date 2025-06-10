# funtion to authenticate credentials # 
gcp_auth <- function(creds_loc) {
  pacman::p_load(tidyverse, reticulate, jsonlite, here, reticulate)
  
  use_python("/home/rstudio/.local/share/r-miniconda/envs/r-reticulate/bin/python", required = TRUE)
  # test it worked 
  py_config()
  
  # Prevent unnecessary re-authentication
  if (file.exists("~/.cm360_token.json")) {
    stop("Authentication already completed. Use `reauthorize_cm360()` to refresh tokens.")
  }
  
  # Import Python authentication libraries
  google_auth_oauthlib <- import("google_auth_oauthlib.flow")
  googleapiclient <- import("googleapiclient.discovery")
  
  # Path to your OAuth client secret JSON file (from Google Cloud)
  path_to_client_secrets_file <- creds_loc
  
  # Define OAuth scopes for CM360
  OAUTH_SCOPES <- c(
    "https://www.googleapis.com/auth/dfareporting",
    "https://www.googleapis.com/auth/dfatrafficking"
  )
  
  # Initialize the OAuth flow
  flow <- google_auth_oauthlib$InstalledAppFlow$from_client_secrets_file(
    path_to_client_secrets_file, OAUTH_SCOPES
  )
  
  # Run authentication (opens a browser)
  credentials <- flow$run_local_server(port = 8080L)
  
  # Convert credentials to a list, including `expiry`
  credentials_list <- list(
    token = credentials$token,
    refresh_token = credentials$refresh_token,
    token_uri = credentials$token_uri,
    client_id = credentials$client_id,
    client_secret = credentials$client_secret,
    scopes = credentials$scopes,
    expiry = as.character(credentials$expiry)  # Convert expiry to string
  )
  
  # Save directly as a JSON file
  jsonlite::write_json(credentials_list, "~/.cm360_token.json", pretty = TRUE)
  
  return(credentials)
}
