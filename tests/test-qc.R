# DIRECTIONS:
# Copy and paste the following line of code into the console and press enter.
# Also define db_file_path below if it needs to be updated.
# testthat::test_dir(here::here("tests"))

# PURPOSE:
# This file contains the automated testing suite for our quality control of water chemistry data as well as
# the formatting of CCAL and Test America deliverables into the EDD format for EQuIS.
# Specifically, we include tests for:
  # flag_replicates()
  # flag_blanks()
  # flag_tot_vs_dissolved()
  # attach_ta_flags()

# Load packages & functions
library(imdccal)
library(tidyverse)
library(readxl)
library(lubridate)
library(openxlsx)
library(here)
library(testthat)
library(fpCompare)
library(DBI)
library(odbc)
source(here("code/format_edd.R"))

# Define constants
db_file_path <- here("data/databases/SEI_ROMN_WQLab_Processing_20250131_LSmithCopy.accdb")

# flag_replicates
test_that("Field dup flagging routine works", {
  # Read in data
  results_ccal <- read_csv(here("tests/test-data/results_replicates_ccal.csv"))
  results_ta <- read_csv(here("tests/test-data/results_replicates_test_america.csv"))
  activities <- read_csv(here("tests/test-data/activities.csv"))
  SEI_DB <- DBI::dbConnect(odbc::odbc(), .connection_string = paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=", 
                                                                     db_file_path))
  limits <- DBI::dbReadTable(SEI_DB, "tlu_CCAL_limits")
  units <- DBI::dbReadTable(SEI_DB, "Xwalk_Parameter_SampleFraction_Medium_Unit")
  DBI::dbDisconnect(SEI_DB)
  
  # Apply replicate flagging routine to CCAL test data
  # (used for all cases except metals >= LQL)
  test_data_ccal <- results_ccal %>% 
    flag_replicates(activities, 
                    "CCAL", 
                    limits)
  
  # Apply replicate flagging routine to Test America test data
  # (only used for case of metals >= LQL)
  test_data_ta <- results_ta %>% 
    flag_replicates(activities, 
                    "Test America", 
                    units)
    
  # Both results >= LQL, Major
  major_message <- "SUS: Result value is defined as suspect by data owner because replicate samples exceed the 15% relative percent difference permitted for major constituents; "
  expect_equal(test_data_ccal %>%
                 filter(str_detect(Note, "both >= LQL, major")) %>% # filter for relevant observations
                 pull(Dup_Flag), 
               c("", "", "", "", major_message, major_message))
  
  # Both results >= LQL, Nutrient
  nutrient_message <- "SUS: Result value is defined as suspect by data owner because replicate samples exceed the 30% relative percent difference permitted for nutrients; "
  expect_equal(test_data_ccal %>%
                 filter(str_detect(Note, "both >= LQL, nutrient")) %>% # filter for relevant observations
                 pull(Dup_Flag), 
               c(nutrient_message, nutrient_message, nutrient_message, nutrient_message, "", ""))
  
  # Both results >= LQL, Metal
  metal_message <- "SUS: Result value is defined as suspect by data owner because replicate samples exceed the 30% relative percent difference permitted for metals; "
  expect_equal(test_data_ta %>%
                 pull(Dup_Flag), 
               c("", "", metal_message, "", metal_message, "",
                 "", "", metal_message, "", metal_message, ""))
  
  # One result >= LQL
  over_twice_LQL_message <- "SUS: Result value is defined as suspect by data owner because either the replicate or the sample is not quantified, but the other is greater than twice the lower quantitation limit; "
  expect_equal(test_data_ccal %>%
                 filter(str_detect(Note, "one >= LQL")) %>% # filter for relevant observations
                 pull(Dup_Flag), 
               c("", "", "", "", over_twice_LQL_message, over_twice_LQL_message, over_twice_LQL_message, over_twice_LQL_message))
  
  # Neither result >= LQL
  one_nondetect_message <- "SUS: Result value is defined as suspect by data owner because either the replicate or the sample concentration is not detected, but the other is detected above method detection limit though not quantified; "
  expect_equal(test_data_ccal %>%
                 filter(str_detect(Note, "both < LQL")) %>% # filter for relevant observations
                 pull(Dup_Flag), 
               c("", "", "", "", one_nondetect_message, one_nondetect_message))
})
  

# flag_blanks
test_that("Field blank flagging routine works", {
  # Read in data
  results <- read_csv(here("tests/test-data/results_blanks.csv")) %>%
    mutate(Note = if_else(is.na(Note), "", paste0(Note, " ")))
  activities <- read_csv(here("tests/test-data/activities.csv"))
  
  # Apply blank flagging routine to test data
  test_data <- results %>% 
    flag_blanks(activities)
  
  # Fail test if there are any differences between Note (manually entered flag) and Blank_Flag (automated flag)
  expect_equal(test_data %>%
                 filter(Note != Blank_Flag) %>%
                 nrow(),
               0)
})

# flag_tot_vs_dissolved
test_that("Tot vs dissolved flagging routine works", {
  # Read in data
  results <- read_csv(here("tests/test-data/results_tot_vs_dissolved.csv")) %>%
    mutate(Note = if_else(is.na(Note), "", paste0(Note, " ")))
  activities <- read_csv(here("tests/test-data/activities.csv"))
  
  # Apply tot vs dissolved flagging routine to test data
  test_data <- results %>%
    flag_tot_vs_dissolved(activities,
                          dissolved_characteristic_name = c("Phosphorus, phosphate (PO4) as P",
                                                            "Phosphorus, phosphate (PO4) as P",
                                                            "Phosphorus",
                                                            "Nitrogen",
                                                            "Totally Legit Analyte",
                                                            "Totally Legit Analyte",
                                                            "Fake Chemical",
                                                            "Fake Chemical"),
                          dissolved_filtered_fraction = c("Dissolved",
                                                          "Dissolved",
                                                          "Dissolved",
                                                          "Dissolved",
                                                          "Dissolved",
                                                          "Dissolved",
                                                          "Dissolved",
                                                          "Dissolved"),
                          total_characteristic_name = c("Phosphorus",
                                                        "Phosphorus",
                                                        "Phosphorus",
                                                        "Nitrogen",
                                                        "Totally Legit Analyte",
                                                        "Totally Legit Analyte",
                                                        "Fake Chemical",
                                                        "Fake Chemical"),
                          total_filtered_fraction = c("Dissolved",
                                                      "Total",
                                                      "Total",
                                                      "Total",
                                                      "Total",
                                                      "Total",
                                                      "Total",
                                                      "Total"))
  
  # Fail test if there are any differences between Note (manually entered flag) and Tot_vs_Dissolved_Flag (automated flag)
  expect_equal(test_data %>%
                 filter(Note != Tot_vs_Dissolved_Flag) %>%
                 nrow(),
               0)
})

# attach_ta_flags
test_that("Flags from Test America are crosswalked correctly", {
  # Read in data
  results <- read_csv(here("tests/test-data/results_attach_ta_flags.csv")) %>%
    mutate(Note = if_else(is.na(Note), "", paste0(Note, " "))) %>%
    mutate(Note = str_replace_all(Note, "�", "–"))
  SEI_DB <- DBI::dbConnect(odbc::odbc(), .connection_string = paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=", 
                                                                     db_file_path))
  ta_flags <- DBI::dbReadTable(SEI_DB, "tlu_TestAmerica_flags")
  DBI::dbDisconnect(SEI_DB)
  
  # Apply Test America flag crosswalk
  test_data <- results %>% 
    attach_ta_flags(ta_flags) %>%
    mutate(All_Flags = paste0(`4`,
                              `"`, 
                              HIB, 
                              IQCOL,
                              HICC,
                              FBK,
                              SDROL, 
                              `SD%EL`,
                              `SD%SS`,
                              H))
  
  # Fail test if there are any differences between Note (manually entered flag) and All_Flags (automated flag)
  expect_equal(test_data %>%
                 filter(Note != All_Flags) %>%
                 nrow(),
               0)
  
  # Check that error is raised if new flag is encountered
  expect_error(results %>%
                 mutate(Source_Flags = "new_flag") %>%
                 attach_ta_flags(ta_flags))
})