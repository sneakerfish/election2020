#!/usr/bin/env Rscript
                                        # presidential.R
                                        #
                                        # Take year and optional state and produce presidential election results.

library(tidyverse)
library(politicaldata)
library(optparse)

data_dir = "data"
if (! dir.exists(data_dir)) {
  stop(paste("The data directory doesn't exist: ", data_dir))
}

args = commandArgs(trailingOnly=TRUE)
                                        # test if there is at least one argument: if not, return an error
if (length(args)==0) {
    stop("At least one argument must be supplied (year).", call.=FALSE)
}


year <- args[1]
if (! (year %in% unique(pres_results_by_cd$year))) {
      years = paste(unique(pres_results_by_cd$year), collapse=", ")
  stop(paste("Year provided is not a presidential election year.",
             years))
}

if (length(args) == 2) {
    state = args[2]
    filename = paste(state, year, "csv", sep=".")
    if (! state %in% unique(pres_results_by_cd$state_abb)) {
        stop("State provided is not in the list of states.")
    }
    data <- pres_results_by_cd[pres_results_by_cd$year == year &
                               pres_results_by_cd$state_abb == state,
                               c("state_abb", "district", "total_votes", "dem",
                                 "other", "rep")]
} else {
    filename = paste("all", year, "csv", sep=".")
    data <- pres_results_by_cd[pres_results_by_cd$year == year,
                               c("state_abb", "district", "total_votes", "dem",
                                 "other", "rep")]
}

print(paste("Writing filename: ", paste(data_dir, filename, sep="/")))
write_csv(data, paste(data_dir, filename, sep="/"))
