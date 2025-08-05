source('/home/rstudio/R/kawasaki_cm360/R/cm360_update.R')
if (Sys.Date() > as.Date("2025-08-11")) {
  cm360_update(vehicle = "NAV")
  cm360_update(vehicle = "5525")
} else {
  cm360_update(vehicle = "NAV")
}
