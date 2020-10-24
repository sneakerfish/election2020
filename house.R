#!/usr/bin/env Rscript
# house.R
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
if (! (year %in% unique(house_results$year))) {
  stop("Year provided is not a house election year.")
}
if (length(args) == 2) {
  state = args[2]
  filename = paste(year, state, "house", "csv", sep=".")
  if (! state %in% unique(house_results$state_abb)) {
    stop("State provided is not in the list of states.")
  }
} else {
  filename = paste(year, "all", "house", sep=".")
}

if (length(args) != 2) {
  data <- house_results[house_results$year == year,
                        c("state_abb", "district", "total_votes", "dem", 
                          "other", "rep")]
} else {
  data <- house_results[house_results$year == year & 
                          house_results$state_abb == state,
                        c("state_abb", "district", "total_votes", "dem", 
                          "other", "rep")]
}
print(paste("Writing filename: ", filename))
write_csv(data, filename)


