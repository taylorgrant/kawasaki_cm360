# clean the spend report and push to Google Sheets # 
clean_cm360_spend <- function(data){
  pacman::p_load(tidyverse, janitor, here, glue, googledrive, googlesheets4)
  
  tmpdat <- data |> 
    filter(str_detect(campaign, "Sustain")) |> 
    mutate(across(impressions:media_cost, as.numeric)) |> 
    mutate(partner = stringr::str_to_title(sapply(stringr::str_split(placement, "_"), "[[", 1)),
           placement_type = NA,
           type = sapply(str_split(string, "_"), "[[", 10),
           type = case_when(type == "SOC" ~ "Social",
                            TRUE ~  type),
           campaign = str_replace_all(campaign, "FY26", "FY25")) |> 
    group_by(date, advertiser, campaign, partner, type, placement_type) |> 
    summarise(impressions = sum(impressions),
              clicks = sum(clicks),
              media_cost = round(sum(media_cost)))
  
  # EMAIL AUTHENTICATION --
  options(gargle_oauth_cache = ".secrets",
          gargle_oauth_client_type = "web",
          gargle_oauth_email = TRUE
          # gargle_verbosity = "debug"
  )
  drive_auth()
  id <- "1cNopOsZBrl0jT5yP1ghkLlDYoY_uGoS5cOD1hE4Zrcg" # this is the Looker spend report
  sheet_append(ss = id, tmpdat, sheet = "daily")
  
  return(tmpdat)
  
}
