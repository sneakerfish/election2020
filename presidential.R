#!/usr/bin/env Rscript
# presidential.R
# 
# Take year and optional state and produce presidential election results. 

library(tidyverse)
library(politicaldata)
library(optparse)

args = commandArgs(trailingOnly=TRUE)
# test if there is at least one argument: if not, return an error
if (length(args)==0) {
  stop("At least one argument must be supplied (year).", call.=FALSE)
}
year <- args[1]
if (! (year %in% unique(pres_results_by_cd$year))) {
  stop("Year provided is not a presidential election year.")
}
if (length(args) == 2) {
  state = args[2]
  filename = paste(year, state, "presidential", "csv", sep=".")
  # Check the state argument.
  if (! state %in% unique(pres_results_by_cd$state_abb)) {
    stop("State provided is not in the list of states.")
  }
} else {
  filename = paste(year, "all", "presidential", "csv", sep=".")
}

if (length(args) != 2) {
  data <- pres_results_by_cd[pres_results_by_cd$year == year,
                             c("state_abb", "district", "total_votes", "dem", 
                               "other", "rep")]
} else {
  data <- pres_results_by_cd[pres_results_by_cd$year == year & 
                               pres_results_by_cd$state_abb == state,
                             c("state_abb", "district", "total_votes", "dem", 
                               "other", "rep")]
}
print(paste("Writing filename: ", filename))
write_csv(data, filename)


