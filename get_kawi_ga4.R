# Function to get Kawasaki site data # 

get_kawi_pv <- function(){
  
  cat("---------------- GA4 data pulled on:", as.character(Sys.Date()), "-------------------- \n")
  
  pacman::p_load(tidyverse, janitor, here, glue, googlesheets4, googledrive, googleAnalyticsR)
  source("/home/rstudio/R/kawasaki_cm360/R/helpers/get_utms.R")
  
  # AUTHENTICATE ------------------------------------------------------------
  options(gargle_oauth_cache = ".secrets",
          gargle_oauth_client_type = "web",
          gargle_oauth_email = TRUE
          # gargle_verbosity = "debug"
  )
  drive_auth()
  my_property_id <- 249558280
  

  # BRING IN SOURCE/MEDIUMS -------------------------------------------------
  launch_utms <- get_utms("Launch KPIs")
  sustain_utms <- get_utms("Sustain KPIs")
  gsp_source_med <- unique(c(launch_utms$gsp_source_medium, sustain_utms$gsp_source_medium))
  
  # summarize page views pulled
  summarize_page_views <- function(data, source_medium) {
    # paid session/medium
    fuse_sess_med <- c("google / cpc")
    
    nav_pv <- data$nav_pv |> 
      mutate(traffic_type = case_when(sessionSourceMedium %in% c(source_medium, fuse_sess_med) ~ 'paid',
                                      str_detect(sessionSourceMedium, "socialpaid") ~ 'paid',
                                      TRUE ~ 'organic')) |> 
      group_by(date, traffic_type) |> 
      summarise(nav_pv = sum(screenPageViews))
    
    sub_pv <- data$sub_pv |> 
      mutate(traffic_type = case_when(sessionSourceMedium %in% c(source_medium, fuse_sess_med) ~ 'paid',
                                      str_detect(sessionSourceMedium, "socialpaid") ~ 'paid',
                                      TRUE ~ 'organic')) |> 
      group_by(date, traffic_type) |> 
      summarise(sub_pv = sum(screenPageViews))
    
    pv_by_type <- nav_pv |> 
      left_join(sub_pv) |> 
      mutate(pv = nav_pv + sub_pv)
    
    organic_pv <- pv_by_type |> 
      filter(traffic_type == "organic") |> 
      summarise(pv = sum(pv))
    
    paid_pv <- pv_by_type |> 
      filter(traffic_type == "paid") |> 
      summarise(pv = sum(pv)) 
    
    list(pv_by_type = pv_by_type, organic_pv = organic_pv, paid_pv = paid_pv)
  }
  
  get_page_views <- function() {
    # subdomain
    sub_pv <- ga_data(
      my_property_id,
      metrics = c("screenPageViews"),
      dimensions = c("date", "hostname", "sessionSourceMedium", "pagePath"),
      date_range = c(as.character(Sys.Date()-1), as.character(Sys.Date()-1)),
      dim_filters = ga_data_filter("hostname" %contains% "nav"),
      limit = -1
    ) |> 
      mutate(sessionSourceMedium = tolower(sessionSourceMedium))
    
    # main site
    nav_pv <- ga_data(
      my_property_id,
      metrics = c("screenPageViews"),
      dimensions = c("date", "sessionSourceMedium", "pagePath"),
      date_range = c(as.character(Sys.Date()-1), as.character(Sys.Date()-1)),
      dim_filters = ga_data_filter("pagePath" %contains% c("/en-us/nav-ptv/nav/4-passenger/nav-4e")),
      limit = -1
    ) |> 
      mutate(sessionSourceMedium = tolower(sessionSourceMedium))
    
    list(nav_pv = nav_pv, sub_pv = sub_pv)
  }
  page_views <- get_page_views()
  out <- summarize_page_views(page_views, gsp_source_med)
  
  # New vs Returning page views by date (paid only)
  new_v_returning <- function(source_medium) {
    # get pageviews for nav pages
    nav_pv <- ga_data(
      my_property_id,
      metrics = c("screenPageViews"),
      dimensions = c("date", "newVsReturning", "sessionSourceMedium",  "pagePath"),
      date_range = c(as.character(Sys.Date()-1), as.character(Sys.Date()-1)),
      dim_filters = ga_data_filter("pagePath" %contains% "/en-us/nav-ptv/nav/4-passenger/nav-4e"),
      limit = -1
    ) |> 
      mutate(sessionSourceMedium = tolower(sessionSourceMedium))
    
    # categorizing paid media 
    fuse_sess_med <- c("google / cpc")
    
    # split out New v Returning per day
    nav_pv |> 
      mutate(traffic_type = case_when(sessionSourceMedium %in% c(source_medium, fuse_sess_med) ~ 'paid',
                                      str_detect(sessionSourceMedium, "socialpaid") ~ "paid",
                                      TRUE ~ "organic"),
             newVsReturning = ifelse(is.na(newVsReturning), "(not set)", newVsReturning),
             newVsReturning = ifelse(newVsReturning == "", "(not set)", newVsReturning)) |> 
      group_by(date, traffic_type, newVsReturning) |> 
      summarise(nav_pv = sum(screenPageViews)) |> 
      group_by(date, traffic_type) |> 
      mutate(frac = nav_pv/sum(nav_pv))
  }
  newvreturn <- new_v_returning(gsp_source_med)
  
  geo_region <- ga_data(
    my_property_id,
    dimensions = c("date","city", "region", "sessionSourceMedium"),
    metrics = c("sessions", "averageSessionDuration"),
    date_range = c(as.character(Sys.Date()-1), as.character(Sys.Date()-1)),
    limit = -1
    # dim_filters = ga_data_filter("sessionSourceMedium" %contains% gsp_source_med),
  ) |> 
    mutate(sessionSourceMedium = tolower(sessionSourceMedium)) |> 
    filter(sessionSourceMedium %in% gsp_source_med) |> 
    group_by(sessionSourceMedium) |> 
    slice_max(sessions, n = 20, with_ties = FALSE)

  out_list <- c(out, list(newvreturn = newvreturn, geo_region = geo_region))
  
  # writing to Google Sheets

  # create sheet for the first run 
  # gs4_create("kawasaki_site_analytics", 
  #            sheets = out_list)
  id <- "1dc-SL4KNa9v89CE4lGxR1ZAdoyW1SbepHzKFf7I9__k"
  sheet_append(ss = id, out_list$pv_by_type, sheet = "pv_by_type")
  sheet_append(ss = id, out_list$organic_pv, sheet = "organic_pv")
  sheet_append(ss = id, out_list$paid_pv, sheet = "paid_pv")
  sheet_append(ss = id, out_list$newvreturn, sheet = "newvreturn")
  sheet_append(ss = id, out_list$geo_region, sheet = "geo_region")
}



