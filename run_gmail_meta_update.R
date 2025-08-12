source('/home/rstudio/R/kawasaki_cm360/R/gmail_update_meta.R')
if (Sys.Date() > as.Date("2025-08-11")) {
  gmail_update_meta(vehicle = "NAV")
  gmail_update_meta(vehicle = "5525")
} else {
  gmail_update_meta(vehicle = "NAV")
}