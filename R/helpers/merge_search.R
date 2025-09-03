# Merge search data #

merge_search <- function(data, vehicle) {
  
  if (vehicle == "NAV") {
    search <- googlesheets4::read_sheet(
      ss = "17ioPxO_FgV8DjT8hHT5O8DMa8JEtRokKsTmoJoW0uKM",
      sheet = "search sustain"
    )
  } else {
    search <- googlesheets4::read_sheet(
      ss = "1lfSZD2eR1Qozc9dkrhp2pUftvlIFmdr3Z_SyDjxNH6E",
      sheet = "search"
    )
  }
  veh <- switch(vehicle, 
                "NAV" = "NAV", 
                "TeryxH2" = "5525"
                )
  
  search <- search |> 
    janitor::clean_names() |>
    dplyr::mutate(
      date = as.Date(date),
      advertiser = "KAWASAKI MOTORS CORP",
      campaign = glue::glue("FY25_Kawasaki_{veh}_Sustain_Campaign"),
      partner = "google",
      type = "Search",
      placement_type = NA
    ) |>
    dplyr::group_by(
      date,
      advertiser,
      campaign,
      partner,
      type,
      placement_type
    ) |>
    dplyr::summarise(
      impressions = sum(impressions, na.rm = TRUE),
      clicks = sum(clicks, na.rm = TRUE),
      media_cost = sum(spend, na.rm = TRUE),
      video_plays = NA,
      video_completes = NA
    )

  # MERGE CLEAN NEXTDOOR WITH CM360 ---------------------------------------------
  search_dates_in_dat <- search |>
    # filter(partner == "nextdoor") |>
    dplyr::pull(date)

  tmpdat <- data |>
    dplyr::ungroup() |>
    dplyr::filter(
      !(partner == "google" & type == "Search" & date %in% search_dates_in_dat)
    ) |>
    dplyr::bind_rows(search |> dplyr::filter(date %in% search_dates_in_dat)) |>
    dplyr::arrange(partner, type, date)
  return(tmpdat)
}
