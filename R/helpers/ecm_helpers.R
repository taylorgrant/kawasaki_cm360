# ECM Model Helper Functions # 
pacman::p_load(tidyverse)

# predict levels for day
predict_today_traffic_levels <- function(
    model,
    data_today,
    data_yesterday,
    dv = "pv",
    pval_cutoff = 0.1
) {
  library(broom)
  
  # Extract model summary
  model_summary <- broom::tidy(summary(model))
  
  # Intercept and ECT
  intercept <- model_summary |>
    dplyr::filter(term == "(Intercept)") |>
    dplyr::pull(estimate)
  ect <- model_summary |>
    dplyr::filter(term == paste0("l.1.", dv)) |>
    dplyr::pull(estimate)
  
  # Short-run terms
  sr_terms <- model_summary |>
    dplyr::filter(grepl("^d\\.1\\.", term)) |>
    dplyr::mutate(variable = gsub("d.1.", "", term)) |>
    dplyr::mutate(estimate = ifelse(p.value <= pval_cutoff, estimate, 0)) |>
    dplyr::select(variable, estimate)
  
  # Long-run terms
  lr_terms <- model_summary |>
    dplyr::filter(grepl(
      paste0("^l\\.1\\.(?!", dv, ")"),
      term,
      perl = TRUE
    )) |>
    dplyr::mutate(variable = gsub("l.1.", "", term)) |>
    dplyr::mutate(estimate = ifelse(p.value <= pval_cutoff, estimate, 0)) |>
    dplyr::select(variable, estimate)
  
  # Short-run contribution by channel
  short_run_contrib <- sapply(sr_terms$variable, function(v) {
    delta <- data_today[[v]] - data_yesterday[[v]]
    coef <- sr_terms |>
      dplyr::filter(variable == v) |>
      dplyr::pull(estimate)
    coef * delta
  })
  
  short_run_total <- sum(short_run_contrib)
  
  # Long-run contribution by channel (to equilibrium)
  long_run_contrib <- sapply(lr_terms$variable, function(v) {
    coef <- lr_terms |>
      dplyr::filter(variable == v) |>
      dplyr::pull(estimate)
    coef * data_yesterday[[v]]
  })
  
  equilibrium <- intercept + sum(long_run_contrib)
  deviation <- data_yesterday[[dv]] - equilibrium
  adjustment <- ect * deviation
  
  # Final prediction
  predicted_pv <- data_yesterday[[dv]] + short_run_total + adjustment
  
  return(list(
    predicted_pv = predicted_pv,
    short_run_total = short_run_total,
    adjustment = adjustment,
    deviation = deviation,
    equilibrium = equilibrium,
    short_run_contrib = short_run_contrib,
    long_run_contrib = long_run_contrib
  ))
}

# build the dataframe
create_attribution_df <- function(model, data, dv = "pv", start_row = 2) {
  results <- purrr::map_dfr(start_row:nrow(data), function(t) {
    data_today <- data[t, ]
    data_yesterday <- data[t - 1, ]
    
    pred <- predict_today_traffic_levels(
      model = model,
      data_today = data_today,
      data_yesterday = data_yesterday,
      dv = dv
    )
    
    # Format per-channel contributions
    sr_contrib <- pred$short_run_contrib %>%
      stats::setNames(paste0(names(.), "_short_run")) |>
      tibble::as_tibble_row()
    
    lr_contrib <- pred$long_run_contrib %>%
      stats::setNames(paste0(names(.), "_long_run")) |>
      tibble::as_tibble_row()
    
    # Combine into one row
    tibble::tibble(
      date = data_today$date,
      actual_pv = data_today[[dv]],
      predicted_pv = pred$predicted_pv,
      deviation = pred$deviation,
      adjustment = pred$adjustment,
      short_run_total = pred$short_run_total
    ) |> 
      dplyr::bind_cols(sr_contrib, lr_contrib)
  })
}