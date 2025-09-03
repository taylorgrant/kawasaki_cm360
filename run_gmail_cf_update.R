source('/home/rstudio/R/kawasaki_cm360/R/gmail_update_cf.R')
if (Sys.Date() > as.Date("2025-08-19")) {
  # gmail_update_cf(vehicle = "NAV")
  gmail_update_cf(vehicle = "TeryxH2")
} else {
  gmail_update_cf(vehicle = "NAV")
}
