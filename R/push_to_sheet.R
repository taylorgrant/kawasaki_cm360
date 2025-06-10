push_to_sheet <- function(data, sheet_id, sheet, type = c("append", "write")) {
  pacman::p_load(googlesheets4)
  
  # Authentication for googlesheets4 only
  options(
    gargle_oauth_cache = ".secrets",
    gargle_oauth_client_type = "web",
    gargle_oauth_email = TRUE
  )
  gs4_auth()
  
  # Enforce valid type
  type <- match.arg(type)
  
  if (type == "append") {
    sheet_append(ss = sheet_id, data, sheet)
  } else {
    sheet_write(data = data, ss = sheet_id, sheet)
  }
}
