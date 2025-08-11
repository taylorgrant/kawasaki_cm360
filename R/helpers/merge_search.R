# Merge search data #

merge_search <- function(data) {
  # Search sustain
  search_sustain <- googlesheets4::read_sheet(
    ss = "17ioPxO_FgV8DjT8hHT5O8DMa8JEtRokKsTmoJoW0uKM",
    sheet = "search sustain"
  ) |>
    dplyr::clean_names() |>
    dplyr::mutate(
      date = as.Date(date),
      advertiser = "KAWASAKI MOTORS CORP",
      campaign = "FY25_Kawasaki_NAV_Sustain_Campaign",
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
  search_dates_in_dat <- search_sustain |>
    # filter(partner == "nextdoor") |>
    dplyr::pull(date)

  tmpdat <- data |>
    dplyr::ungroup() |>
    dplyr::filter(
      !(partner == "google" & type == "Search" & date %in% search_dates_in_dat)
    ) |>
    dplyr::bind_rows(search_sustain |> filter(date %in% search_dates_in_dat)) |>
    dplyr::arrange(partner, type, date)
  return(tmpdat)
}
