# PURPOSE:
# This file contains the automated testing suite for our quality control of water chemistry data as well as
# the formatting of CCAL's deliverables into the EDD format for EQuIS.
# Specifically, we include tests for:
  # assign_detection_flags(): a function in the imdccal package that raises the "J-R" flag when values are 
      # greater than the MDL but less than or equal to the LQL.
  # flag_replicates(): a function for raising the "SUS" flag when the relative percent difference between
      # an observation and its field replicate is greater than a group (Nutrient/Major/Metal) specific threshold
  # flag_blanks(): a function for raising the "FBK" flag when a field blank has a detected concentration.
      # Also includes a comment about how much larger the environmental sample was in comparison to the blank,
      # which may be useful for further processing.
  # flag_tot_vs_dissolved(): a function for raising the "NFNSI" and "NFNSU" flags when a dissolved fraction
      # has a higher concentration than a total fraction.
  # format_edd_ccal(): a function for formatting a CCAL deliverable into the EDD format required for EQuIS.
      # While the other functions test for correctness, this function tests for consistency with prior results.
      # (can change the stored data if/when updates are needed)

# DIRECTIONS:
# Copy and paste the following line of code into the console and press enter.
# testthat::test_dir(here::here("tests"))

# NOTE:
# If we ever convert this to an R package, we should revise the testing suite to conform to best practices.
# For example, instead of loading the packages as we do below we could remove this and use load_all().
# We might also use a different function instead of test_dir().
# See chapters 13-15 at https://r-pkgs.org/ for best practices on testing during package development.

# Load packages
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
limits <- readRDS(here('data/raw/limits.rds')) # maybe put this inside of the tests

# assign_detection_flags
test_that("Flagging observations greater than the MDL but less than or equal to the LQL works", {
  # Read in data
  results <- readRDS(here("tests/test-data/results.Rd"))
  activities <- readRDS(here("tests/test-data/activities.Rd"))
  
  # Apply J-R flagging routine to test data
  test_data <- results %>%
    # Filter for relevant observations in test data
    filter(Activity_ID == "nondetect_jr_0") %>%
    # Feels somewhat backward but need to add in the analyte_code in order to run assign_detection_flags()
    left_join(limits %>% select(Characteristic_Name, Filtered_Fraction, analyte_code, StartDate, EndDate),
              by = dplyr::join_by(Characteristic_Name,
                                  Filtered_Fraction,
                                  Analysis_Start_Date >= StartDate,
                                  Analysis_Start_Date <= EndDate)) %>%
    # Now rename and remove fields to prepare data into a state the function accepts
    rename(value = Result_Text,
           parameter = analyte_code,
           date = Analysis_Start_Date) %>%
    select(-StartDate, -EndDate) %>%
    # Run the function
    assign_detection_flags() 
  
  # Less than or equal to MDL
  expect_equal(test_data %>%
                 filter(row_number() %in% 1:6) %>%
                 pull(flag), 
               c(NA_character_, NA_character_, NA_character_, 
                 NA_character_, NA_character_, NA_character_))
  
  # Greater than MDL but less than LQL
  expect_equal(test_data %>%
                 filter(row_number() %in% 7:9) %>%
                 pull(flag), 
               c("J-R", "J-R", "J-R"))
  
  # Equal to LQL
  expect_equal(test_data %>%
                 filter(row_number() %in% 10:11) %>%
                 pull(flag), 
               c("J-R", "J-R"))
  
  # Greater than LQL
  expect_equal(test_data %>%
                 filter(row_number() %in% 12:14) %>%
                 pull(flag), 
               c(NA_character_, NA_character_, NA_character_))
})

# flag_replicates
test_that("Field dup flagging routine works", {
  # Read in data
  results <- readRDS(here("tests/test-data/results.Rd"))
  activities <- readRDS(here("tests/test-data/activities.Rd"))
  
  # Apply replicate flagging routine to test data
  test_data <- results %>% 
    # Filter for relevant observations in test data
    filter(Activity_ID %in% c("replicates_0", "replicates_1")) %>%
    # Join in Method_Detection_Limit
    left_join(limits %>% select(Characteristic_Name, Filtered_Fraction, 
                                         Method_Detection_Limit, StartDate, EndDate),
              by = dplyr::join_by(Characteristic_Name,
                                  Filtered_Fraction,
                                  Analysis_Start_Date >= StartDate,
                                  Analysis_Start_Date <= EndDate)) %>%
    rename(Method_Detection_Limit = Method_Detection_Limit) %>%
    select(-StartDate, -EndDate) %>%
    # Censor values less than or equal to the MDL
    mutate(Result_Detection_Condition = if_else(Result_Text %<=% Method_Detection_Limit, "Not Detected", "Detected And Quantified"),
           Result_Text = if_else(Result_Detection_Condition == "Detected And Quantified", Result_Text, NA)) %>%
    # Run the function
    flag_replicates(activities, limits)
    
  # Nutrient 
  nutrient_message <- "SUS (because replicate samples exceed the 30% relative percent difference permitted for nutrients), "
  expect_equal(test_data %>%
                 filter(row_number() %in% c(3:7, 11:14, 17:21, 25:28)) %>% # nutrients
                 pull(Dup_Flag), 
               c("", "", "", "", nutrient_message, nutrient_message, "", nutrient_message, nutrient_message, 
                                         "", "", "", "", nutrient_message, nutrient_message, "", nutrient_message, nutrient_message))
  # Major
  major_message <- "SUS (because replicate samples exceed the 15% relative percent difference permitted for major constituents), "
  expect_equal(test_data %>%
                 filter(row_number() %in% c(1, 2, 8, 9, 10, 15, 16, 22, 23, 24)) %>% # majors
                 pull(Dup_Flag), 
               c("", major_message, "", major_message, major_message,
                 "", major_message, "", major_message, major_message))
  # Metal
  # add in this test when it becomes relevant
})

# flag_blanks
test_that("Field blank flagging routine works", {
  # Read in data
  results <- readRDS(here("tests/test-data/results.Rd"))
  activities <- readRDS(here("tests/test-data/activities.Rd"))
  
  # Apply blank flagging routine to test data
  test_data <- results %>% 
    # Filter for relevant observations in test data
    filter(Activity_ID %in% c("blanks_0", "blanks_1")) %>%
    # Join in the MDL
    left_join(limits %>% select(Characteristic_Name, Filtered_Fraction,
                                         Method_Detection_Limit, StartDate, EndDate),
              by = dplyr::join_by(Characteristic_Name,
                                  Filtered_Fraction,
                                  Analysis_Start_Date >= StartDate,
                                  Analysis_Start_Date <= EndDate)) %>%
    rename(Method_Detection_Limit = Method_Detection_Limit) %>%
    # Censor values less than or equal to the MDL
    mutate(Result_Detection_Condition = if_else(Result_Text %<=% Method_Detection_Limit, "Not Detected", "Detected And Quantified"),
           Result_Text = if_else(Result_Detection_Condition == "Detected And Quantified", Result_Text, NA)) %>%
    # Apply FBK flags
    flag_blanks(activities)
  
  # Case 1: reg non-detect, blank non-detect
  expect_equal(test_data %>%
                 filter(row_number() %in% c(1, 2, 15, 16)) %>%
                 pull(Blank_Flag), 
               c("", "", "", ""))
  
  # Case 2: reg detect, blank non-detect
  expect_equal(test_data %>%
                 filter(row_number() %in% c(3, 4, 17, 18)) %>%
                 pull(Blank_Flag), 
              c("", "", "", ""))
  
  # Case 3: reg non-detect, blank detect
  message <- "FBK (Analyte detected in blank but not detected in environmental sample), "
  expect_equal(test_data %>%
                 filter(row_number() %in% c(5, 6, 19, 20)) %>%
                 pull(Blank_Flag), 
               c(message, message, message, message))
  
  # Case 4-a: reg detect, blank detect, reg less than 2x blank
  message <- "FBK (Environmental sample less than twice the blank's level), "
  expect_equal(test_data %>%
                 filter(row_number() %in% c(7:9, 21:23)) %>%
                 pull(Blank_Flag),
               c(message, message, message, message, message, message))
  
  # Case 4-b: reg detect, blank detect, reg 2-5x blank
  message <- "FBK (Environmental sample at least twice but less than 5 times the blank's level), "
  expect_equal(test_data %>%
                 filter(row_number() %in% c(10:12, 24:26)) %>%
                 pull(Blank_Flag),
               c(message, message, message, message, message, message))
  
  # Case 4-c: reg detect, blank detect, reg over 5x blank
  message <- "FBK (Environmental sample at least 5 times the blank's level), "
  expect_equal(test_data %>%
                 filter(row_number() %in% c(13, 14, 27, 28)) %>%
                 pull(Blank_Flag), 
              c(message, message, message, message))
})

# flag_tot_vs_dissolved
test_that("Tot vs dissolved flagging routine works", {
  # Read in data
  results <- readRDS(here("tests/test-data/results.Rd"))
  activities <- readRDS(here("tests/test-data/activities.Rd"))
  
  # Apply tot vs dissolved flagging routine to test data
  test_data <- results %>%
    # Filter for relevant observations in test data
    filter(Activity_ID %in% c("tot_vs_dissolved_noflag_0", "tot_vs_dissolved_nfnsi_0", 
                              "tot_vs_dissolved_nfnsu_0")) %>%
    # Join in the MDL
    left_join(limits %>% select(Characteristic_Name, Filtered_Fraction,
                                         Method_Detection_Limit, StartDate, EndDate),
              by = dplyr::join_by(Characteristic_Name,
                                  Filtered_Fraction,
                                  Analysis_Start_Date >= StartDate,
                                  Analysis_Start_Date <= EndDate)) %>%
    rename(Method_Detection_Limit = Method_Detection_Limit) %>%
    # Apply NFNSI and NFNSU flags
    flag_tot_vs_dissolved(dissolved_characteristic_name = c("Phosphorus, phosphate (PO4) as P",
                                                            "Phosphorus, phosphate (PO4) as P",
                                                            "Phosphorus",
                                                            "Nitrogen"),
                          dissolved_filtered_fraction = c("Dissolved",
                                                          "Dissolved",
                                                          "Dissolved",
                                                          "Dissolved"),
                          total_characteristic_name = c("Phosphorus",
                                                        "Phosphorus",
                                                        "Phosphorus",
                                                        "Nitrogen"),
                          total_filtered_fraction = c("Dissolved",
                                                      "Total",
                                                      "Total",
                                                      "Total"))
  
  # Case 1: no flag
  expect_equal(test_data %>%
                 filter(Activity_ID == "tot_vs_dissolved_noflag_0") %>%
                 pull(Tot_vs_Dissolved_Flag), 
               c("", "", "", "", "", "", "",
                 "", "", "", "", "", "", ""))
  
  # Case 2: NFNSI
  expect_equal(test_data %>%
                 filter(Activity_ID == "tot_vs_dissolved_nfnsi_0") %>%
                 pull(Tot_vs_Dissolved_Flag), 
               c("", "", "NFNSI, ", "NFNSI, ", "", "NFNSI, ", "",
                 "", "", "", "", "", "", ""))
  
  # Case 3: NFNSU
  expect_equal(test_data %>%
                 filter(Activity_ID == "tot_vs_dissolved_nfnsu_0") %>%
                 pull(Tot_vs_Dissolved_Flag), 
               c("", "", "NFNSU, ", "NFNSU, ", "", "NFNSU, ", "",
                 "", "", "", "", "", "", ""))
})

# format_edd_ccal
# Note that this check doesn't necessarily check for correctness.
# Rather, it checks for consistency with prior output, which may have issues of its own.
test_that("The formatting produced by format_edd_ccal has not changed", {
  # Read in prior EDDs
  old_edd_2019 <- readRDS(here("tests/test-data/edd_2019.Rd"))
  old_edds_post_2019 <- readRDS(here("tests/test-data/edds_post_2019.Rd"))
  
  # Format EDDs
  new_edd_2019 <- format_edd_ccal(ccal_file_paths = here("data/raw/ROMN_103119.xlsx"),
                              db_file_path = here("data/databases/SEI_ROMN_WQLab_Processing_20250131_LSmithCopy.accdb"),
                              pre_2020 = here("data/EDD_examples/ROMN_SEI_in_EQuIS.xlsx"))
  
  new_edds_post_2019 <- format_edd_ccal(ccal_file_paths = c(here("data/raw/ROMN_101823.xlsx"),
                                              here("data/raw/ROMN_102920.xlsx"),
                                              here("data/raw/ROMN_081021.xlsx"),
                                              here("data/raw/ROMN_102221.xlsx"),
                                              here("data/raw/ROMN_100620.xlsx")),
                          db_file_path = here("data/databases/SEI_ROMN_WQLab_Processing_20250131_LSmithCopy.accdb"),
                          pre_2020 = FALSE)

  # Test equality on EDD before 2020
  expect_equal(old_edd_2019,
               new_edd_2019)
  
  # Test equality on EDDs after 2019
  expect_equal(old_edds_post_2019,
               new_edds_post_2019)
})
