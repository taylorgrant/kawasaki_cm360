gcp_reauth <- function(credentials_path = "/home/rstudio/.cm360_token.json") {
  library(reticulate)
  library(jsonlite)
  
  google_auth <- import("google.oauth2.credentials")
  google_auth_requests <- import("google.auth.transport.requests")
  
  # Ensure credentials file exists
  if (!file.exists(credentials_path)) {
    stop("Credentials file not found. Please authenticate first.")
  }
  
  # Load credentials from JSON
  credentials_list <- fromJSON(credentials_path)
  
  # Ensure expiry is handled properly
  if (is.null(credentials_list$expiry) || credentials_list$expiry == "" || is.na(credentials_list$expiry)) {
    warning("Expiry is missing in credentials! Forcing refresh.")
    expiry_datetime <- as.POSIXct("1970-01-01 00:00:00", tz = "UTC")  # Default to expired
  } else {
    # Convert expiry string to POSIXct (force UTC)
    expiry_datetime <- as.POSIXct(credentials_list$expiry, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
    attr(expiry_datetime, "tzone") <- "UTC"  # Ensure UTC timezone
  }
  
  # Convert expiry to Unix timestamp (seconds)
  expiry_epoch <- as.numeric(expiry_datetime)
  
  # Convert to Python credentials object
  credentials <- google_auth$Credentials(
    token = credentials_list$token,
    refresh_token = credentials_list$refresh_token,
    token_uri = credentials_list$token_uri,
    client_id = credentials_list$client_id,
    client_secret = credentials_list$client_secret,
    scopes = credentials_list$scopes,
    expiry = expiry_epoch  # Pass Unix timestamp (seconds)
  )
  
  # Check if the token is expired
  if (expiry_epoch < as.numeric(Sys.time(), tz = "UTC")) { 
    cat("Token expired or missing. Attempting refresh...\n")
    
    # Create request object
    request <- google_auth_requests$Request()
    
    # Refresh token
    credentials$refresh(request)
    
    # Update JSON with new token and expiry
    credentials_list$token <- credentials$token
    credentials_list$expiry <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")  # Ensure stored as UTC
    
    # Save updated credentials back to JSON
    write_json(credentials_list, credentials_path, pretty = TRUE)
    
    cat("Token refreshed and saved. New expiry:", credentials_list$expiry, "\n")
  } else {
    cat("Token is still valid. Expiry:", credentials_list$expiry, "\n")
  }
  
  return(credentials)
}
