merge_youtube <- function(data) {
  # Search sustain
  teryx_youtube <- googlesheets4::read_sheet(
    ss = "1lfSZD2eR1Qozc9dkrhp2pUftvlIFmdr3Z_SyDjxNH6E",
    sheet = "youtube"
  ) |>
    janitor::clean_names() |>
    dplyr::mutate(
      date = as.Date(reporting_starts),
      advertiser = "KAWASAKI MOTORS CORP",
      campaign = "FY25_Kawasaki_5525_Awareness_Brand",
      partner = "youtube",
      type = "OLV",
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
      clicks = sum(link_clicks, na.rm = TRUE),
      media_cost = sum(amount_spent_usd, na.rm = TRUE),
      video_plays = NA,
      video_completes = NA
    )
  
  # MERGE CLEAN NEXTDOOR WITH CM360 ---------------------------------------------
  youtube_dates_in_dat <- teryx_youtube |>
    dplyr::pull(date)
  
  tmpdat <- data |>
    dplyr::ungroup() |>
    dplyr::filter(
      !(partner == "youtube" & type == "OLV" & date %in% youtube_dates_in_dat)
    ) |>
    dplyr::bind_rows(teryx_youtube |> dplyr::filter(date %in% youtube_dates_in_dat)) |>
    dplyr::arrange(partner, type, date)
  return(tmpdat)
}