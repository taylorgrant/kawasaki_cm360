source('/home/rstudio/R/kawasaki_cm360/R/cm360_workflow.R')
source('/home/rstudio/R/kawasaki_cm360/R/cm360_short_workflow.R')

if (Sys.Date() <= as.Date("2025-08-19")) {
  cm360_workflow(vehicle = "NAV")
  cm360_short_workflow(vehicle = "TeryxH2")
} else {
  # cm360_workflow(vehicle = "NAV")
  cm360_workflow(vehicle = "TeryxH2")
}


