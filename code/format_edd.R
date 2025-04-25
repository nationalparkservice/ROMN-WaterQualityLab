# Handy function to determine if an item is not in a vector
`%notin%` <- Negate(`%in%`)

#' Obtain metadata from database used to format our EDD.
#'
#' @param file_path File path to a copy of the SEI water quality lab processing database.
#' @param lab The name of the lab that we are loading tables for. Valid entries are "CCAL" and "Test America".
#' @param pre_2020 By default set to false. If you are using data from before 2020, instead set this 
#' to a file path to ROMN_SEI_in_EQuIS.xlsx, which includes a crosswalk between EventName and SampleNumber.
#'
#' @return A list containing the required tables from the database.
load_db <- function(file_path, lab, pre_2020 = FALSE) {
  
  # Stop if invalid lab
  if (lab %notin% c("Test America", "CCAL")) {
    stop("Invalid lab argument for load_db(). Valid inputs are 'CCAL' and 'Test America'")
  }
  
  # Create list to return
  return_list <- list()
  
  # Connect to database
  SEI_DB <- DBI::dbConnect(odbc::odbc(), .connection_string = paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=", 
                                                                     file_path))
  # Load water quality metadata
  return_list$Site <- DBI::dbReadTable(SEI_DB,"dbo_Site")
  return_list$SitesMetadata <- DBI::dbReadTable(SEI_DB,"dbo_SitesMetadata")
  dbo_CollectionEvent <- DBI::dbReadTable(SEI_DB,"dbo_CollectionEvent")
  dbo_WaterSediment <- DBI::dbReadTable(SEI_DB,"dbo_WaterSediment")
  
  # Load Test America Crosswalks
  if(lab == "Test America") {
    lab_id <- "TESTAMC_AZ"
    return_list$flags <- DBI::dbReadTable(SEI_DB,"tlu_TestAmerica_flags")
    return_list$units <- DBI::dbReadTable(SEI_DB, "Xwalk_Parameter_SampleFraction_Medium_Unit")
    return_list$methods <- DBI::dbReadTable(SEI_DB, "IMPORT_Water_Lab_TestAmerica_Analytical_Methods")
  }
  
  # Load CCAL crosswalk
  if(lab == "CCAL") {
    lab_id <- "CCAL_LAB"
    return_list$limits <- DBI::dbReadTable(SEI_DB, "tlu_CCAL_limits")
  }

  # Disconnect from database
  DBI::dbDisconnect(SEI_DB)
  
  # Here we perform a join that we will need when formatting the EDD.
  # Essentially, we need to bring EventName into the Activities EDD, and EventName is stored in
  # dbo_CollectionEvent. In order to join, we must use the SampleNumber field, but this is not present
  # in dbo_CollectionEvent. Hence we perform a join here to get both pieces of information in one table
  if (is.character(pre_2020)) {
    # Routine for pre-2020 data
    # Need to use a different source for some data due to changes in our database
    return_list$Joined <- read_excel(pre_2020, na = "ND") %>%
      filter(Lab_ID == lab_id) %>%
      select(Custody_ID, Additional_Location_Info, Activity_Comment) %>%
      distinct() %>%
      rename("SampleNumber" = "Custody_ID",
             "EventName" = "Additional_Location_Info",
             "Notes" = "Activity_Comment")
  }
  else {
    # Regular routine using the database
    # Applicable for 2020 and on
    return_list$Joined <- dbo_CollectionEvent %>%
      left_join(dbo_WaterSediment, by = c("ID" = "CollectionEventID")) %>%
      select(ID, SampleNumber, EventName, Notes)
  }
  
  return(return_list)
}

#' Extract projects table from Access database for the EQuIS EDD.
#'
#' @param db_file_path File path to a copy of the SEI water quality lab processing database.
#'
#' @returns The projects table for the EDD.
create_projects_table <- function(db_file_path) {
  SEI_DB <- DBI::dbConnect(odbc::odbc(), .connection_string = paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=", 
                                                                     db_file_path))
  projects <- DBI::dbReadTable(SEI_DB, "tbl_Projects_EDD") %>%
    rename(`#Org_Code` = X.Org_Code)
  
  DBI::dbDisconnect(SEI_DB)
  
  return(projects)
}

#' Create locations table for the EQuIS EDD.
#'
#' @param activities_or_results The activities or results table corresponding to the relevant lab deliverables.
#' As long as this table has correct and complete `#Org_Code` and `Activity_ID` columns, the output will be as expected (code accounts for duplicate Activity_IDs).
#' @param dbo The database tables returned by load_db().
#'
#' @returns The locations table for the EDD.
create_locations_table <- function(activities_or_results, dbo) {
  activities_or_results %>%
    mutate(Park_Code = str_extract(Activity_ID, "(?<=_)[:alpha:]*(?=_)"), # first instance of a group of letters after and before an underscore
           Location_ID = str_extract(Activity_ID, ".*(?=_\\d{8})"), # everything before the underscore before the 8 digit date
           Name = str_extract(Location_ID, "(?<=_).*")) %>% # everything after the first underscore
    select(`#Org_Code`, Park_Code, Location_ID, Name) %>%
    unique() %>%
    left_join(dbo$Site, by = c("Name")) %>%
    rename("Location_Name" = "Alias") %>%
    mutate(Location_Type = "River/Stream", 
           Lat_Lon_Method = "GPS-Unspecified", 
           Lat_Lon_Datum = "NAD83") %>% 
    left_join(dbo$SitesMetadata, by = c("Name" = "SiteName")) %>%
    mutate(Source_Map_Scale_Numeric = NA,
           Lat_Lon_Accuracy = NA,
           Lat_Lon_Accuracy_Unit = NA,
           Location_Description = NA,
           Travel_Directions = NA,
           Location_Purpose = NA,
           Establishment_Date = NA) %>%
    rename("HUC8_Code" = "HUC8",
           "HUC12_Code" = "HUC12") %>%
    mutate(Alternate_Location_ID = NA,
           Alternate_Location_ID_Context = NA,
           Elevation = NA,
           Elevation_Unit = NA,
           Elevation_Method = NA,
           Elevation_Datum = NA,
           Elevation_Accuracy = NA,
           Elevation_Accuracy_Unit = NA,
           Country_Code = NA) %>%
    rename("State_Code" = "StateCode",
           "County_Code" = "CountyName") %>%
    mutate(Drainage_Area = NA,
           Drainage_Area_Unit = NA,
           Contributing_Area = NA,
           Contributing_Area_Unit = NA,
           Tribal_Land_Indicator = NA,
           Tribal_Land_Name = NA,
           Well_ID = NA,
           Well_Type = NA,
           Aquifer_Name = NA,
           Formation_Type = NA,
           Well_Hole_Depth = NA,
           Well_Hole_Depth_Unit = NA,
           Well_Status = NA,
           Public_Latitude = NA,
           Public_Longitude = NA) %>%
    select(`#Org_Code`, Park_Code, Location_ID, Location_Name, Location_Type, # order for EDD
           Latitude, Longitude, Lat_Lon_Method, Lat_Lon_Datum, Source_Map_Scale_Numeric,
           Lat_Lon_Accuracy, Lat_Lon_Accuracy_Unit, Location_Description, Travel_Directions,
           Location_Purpose,	Establishment_Date, HUC8_Code, HUC12_Code, 
           Alternate_Location_ID,	Alternate_Location_ID_Context,	Elevation,	Elevation_Unit,
           Elevation_Method,	Elevation_Datum,	Elevation_Accuracy,	Elevation_Accuracy_Unit,	Country_Code,
           State_Code,	County_Code,	Drainage_Area,	Drainage_Area_Unit,	Contributing_Area,
           Contributing_Area_Unit,	Tribal_Land_Indicator,	Tribal_Land_Name,	Well_ID,	Well_Type,
           Aquifer_Name,	Formation_Type,	Well_Hole_Depth,	Well_Hole_Depth_Unit,	Well_Status,
           Public_Latitude,	Public_Longitude)
}

#' Function for formatting our EDD for CCAL
#'
#' @param ccal_file_paths Path to .xlsx file delivered by CCAL. Use a character vector to specify multiple files.
#' @param db_file_path File path to a copy of the SEI water quality lab processing database.
#' @param pre_2020 By default set to false. If you are using data from before 2020, instead set this 
#' to a file path to ROMN_SEI_in_EQuIS.xlsx, which includes a crosswalk between EventName and SampleNumber.
#' @param concat If concat is set to TRUE, the tables are concatenated together for each input file.
#' By default, concat is set to FALSE, so the output contains separate tables for each file.
#' If only one file path is supplied to the file_paths argument, this parameter does not affect the output.
#'
#' @return A fully formatted EQuIS deliverable ready for the ecologist's manual review.
#' Specifically, this function returns a list of deliverables, each of which is a list of the Projects, Locations, Activities, and Results tables.
format_edd_ccal <- function(ccal_file_paths, db_file_path, pre_2020 = FALSE, concat = FALSE){
  
  # Load database tables
  dbo <- load_db(db_file_path, "CCAL", pre_2020)
  
  # Clean data, join in some helpful fields
  package_data <- read_ccal(ccal_file_paths, concat) %>%
    lapply(function(x) {
      x[[1]] %>%
        remove_ccal_duplicates() %>%
        dplyr::left_join(dbo$limits %>% dplyr::select(-analysis), # join MDL and ML to data
                         by = dplyr::join_by(parameter == analyte_code,
                                             date >= StartDate,
                                             date <= EndDate))
    })
  
  # ------------------------
  # CREATE ACTIVITIES TABLE
  # ------------------------
  edd_activities <- package_data %>%
    lapply(function(x) {
      x %>%
        rename("#Org_Code" = "project_code") %>%
        mutate(Project_ID = paste(`#Org_Code`, "SEI", sep = "_"),
               Location_ID = paste(`#Org_Code`, 
                                   str_extract(remark, "^.*(?=,)"), # everything before the first comma
                                   sep = "_"),
               remark_date = str_extract(`remark`, "(?<=, ).{0,11}(?= )"), # any 0-11 characters between ", " and " "
               remark_date = if_else(remark_date == "", str_extract(remark, "(?<=, ).*"), remark_date), # deals with different possible format from CCAL
               Activity_Start_Date = format(mdy(remark_date), "%Y%m%d"),
               Activity_ID_wDups = paste(Location_ID, Activity_Start_Date, "C", Medium, sep = "_"),
               Custody_ID = str_extract(site_id, "\\d{2}[A-Z]{3}\\d{7}")) %>% # extract bottle number (2 digits, 3 characters, 7 digits)
        select(`#Org_Code`, Project_ID, Location_ID, Activity_ID_wDups, Medium, Activity_Start_Date, Custody_ID) %>%
        unique() %>%
        left_join(dbo$Joined, by = c("Custody_ID" = "SampleNumber")) %>%
        rename("Additional_Location_Info" = "EventName",
               "Activity_Comment" = "Notes") %>%
        mutate(Activity_Type = case_when(str_detect(Additional_Location_Info, "_FD_W$") ~ 
                                           "Quality Control Sample-Field Replicate",
                                         str_detect(Additional_Location_Info, "_FB_W$") ~
                                           "Quality Control Sample-Field Blank",
                                         TRUE ~ "Sample-Routine"), # the suffixes are how we denote QC samples internally
               Medium_Subdivision = NA,
               Assemblage_Sampled_Name = NA,
               Activity_Start_Time = NA,
               Activity_Start_Time_Zone = NA,
               Activity_End_Date =  NA,
               Activity_End_Time = NA,
               Activity_End_Time_Zone = NA,
               Activity_Relative_Depth = NA,
               Activity_Depth = NA,
               Activity_Depth_Unit = NA,
               Activity_Upper_Depth = NA,
               Activity_Lower_Depth = NA, 
               Activity_Depth_Reference = NA,
               Activity_Sampler = NA,
               Activity_Recorder = NA,
               Activity_Conducting_Organization = NA,
               Station_Visit_Comment = NA,
               Collection_Method_ID = if_else(str_detect(Activity_Type, "[S|s]ample"), "ROMN_GRAB", NA),
               Collection_Equipment_Name = if_else(str_detect(Activity_Type, "[S|s]ample"), "Water bottle", NA),
               Collection_Equipment_Description = NA,
               Gear_Deployment = NA,
               Container_Type	= NA,
               Container_Color = NA,	
               Container_Size = NA,
               Container_Size_Unit = NA,
               Preparation_Method_ID = NA,
               Chemical_Preservative = NA,
               Thermal_Preservative = NA,
               Transport_Storage_Description = NA,	
               Activity_Group_ID = NA,
               Activity_Group_Name = NA,
               Activity_Group_Type = NA,
               Collection_Duration = NA,
               Collection_Duration_Unit = NA,
               Sampling_Component_Name = NA,
               Sampling_Component_Place_In_Series = NA,	
               Reach_Length = NA,
               Reach_Length_Unit = NA,	
               Reach_Width = NA,
               Reach_Width_Unit = NA,
               Pass_Count = NA,
               Net_Type = NA,
               Net_Surface_Area = NA,	
               Net_Surface_Area_Unit = NA,	
               Net_Mesh_Size = NA,
               Net_Mesh_Size_Unit = NA,
               Boat_Speed = NA,
               Boat_Speed_Unit = NA,	
               Current_Speed = NA,
               Current_Speed_Unit = NA,
               Toxicity_Test_Type = NA,
               Collection_Effort = NA,
               Collection_Effort_Gear_Type = NA,
               Org_Source_Act_ID = NA) %>%
        group_by(Activity_ID_wDups) %>%
        arrange(Custody_ID) %>%
        mutate(count = row_number() - 1,
               Activity_ID = paste(Activity_ID_wDups, count, sep = "_")) %>%
        ungroup() %>%
        select(`#Org_Code`, Project_ID, Location_ID, Activity_ID_wDups, Activity_ID, Activity_Type, Medium,
               Medium_Subdivision, Assemblage_Sampled_Name, Activity_Start_Date, Activity_Start_Time,
               Activity_Start_Time_Zone, Activity_End_Date, Activity_End_Time, Activity_End_Time_Zone,
               Activity_Relative_Depth, Activity_Depth, Activity_Depth_Unit, Activity_Upper_Depth,
               Activity_Lower_Depth, Activity_Depth_Reference, Additional_Location_Info, Activity_Sampler,
               Activity_Recorder, Custody_ID, Activity_Conducting_Organization, Station_Visit_Comment,
               Activity_Comment, Collection_Method_ID, Collection_Equipment_Name, 
               Collection_Equipment_Description, Gear_Deployment, Container_Type, Container_Color, 
               Container_Size, Container_Size_Unit, Preparation_Method_ID, Chemical_Preservative, 
               Thermal_Preservative, Transport_Storage_Description, Activity_Group_ID, Activity_Group_Name,
               Activity_Group_Type, Collection_Duration, Collection_Duration_Unit, Sampling_Component_Name,
               Sampling_Component_Place_In_Series, Reach_Length, Reach_Length_Unit, Reach_Width,
               Reach_Width_Unit, Pass_Count, Net_Type, Net_Surface_Area, Net_Surface_Area_Unit, Net_Mesh_Size,
               Net_Mesh_Size_Unit, Boat_Speed, Boat_Speed_Unit, Current_Speed, Current_Speed_Unit,
               Toxicity_Test_Type, Collection_Effort, Collection_Effort_Gear_Type, Org_Source_Act_ID)
      })
    
  # ------------------------
  # CREATE RESULTS TABLE
  # ------------------------
  edd_results <- pmap(list(format_equis_results(ccal_file_paths, limits = dbo$limits, concat = concat), 
                           package_data, edd_activities), 
                      function(results, package_data, activities) {
      results %>%
        select(-Activity_ID) %>%
        inner_join(package_data %>% 
                     select(lab_number, Characteristic_Name, Filtered_Fraction, remark, Medium, site_id),
                   by = c("Lab_Batch_ID" = "lab_number",
                          "Characteristic_Name" = "Characteristic_Name",
                          "Filtered_Fraction" = "Filtered_Fraction")) %>%
        mutate(remark_date = str_extract(remark, "(?<=, ).{0,11}(?= )"), # any 0-11 characters between ", " and " "
               remark_date = if_else(remark_date == "", str_extract(remark, "(?<=, ).*"), remark_date), # deals with different possible format from CCAL
               remark_date_reformatted = format(mdy(remark_date), "%Y%m%d"),
               Activity_ID_wDups = paste(`#Org_Code`, str_extract(remark, "^.*(?=,)"), #everything before last ", "
                                         remark_date_reformatted, "C", Medium, sep = "_"),
               Custody_ID = str_extract(site_id, "\\d{2}[A-Z]{3}\\d{7}")) %>% # extract bottle number (2 digits, 3 characters, 7 digits)
        inner_join(activities %>% select(Activity_ID_wDups, Custody_ID, Activity_ID),
                   by = c("Activity_ID_wDups", "Custody_ID")) %>%
        select(-Medium, -Custody_ID) %>%
        flag_replicates(activities, "CCAL", dbo$limits) %>% 
        flag_blanks(activities) %>%
        flag_tot_vs_dissolved(activities,
                              dissolved_characteristic_name = c("Phosphorus, phosphate (PO4) as P",
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
                                                          "Total")) %>%
        mutate(Result_Qualifier = case_when(Blank_Flag != "" ~ "FBK", 
                                            Dup_Flag != "" ~ "SUS", 
                                            Tot_vs_Dissolved_Flag != "" ~ "SUS", 
                                            TRUE ~ NA), # this implements our flag hierarchy by filling Result_Qualifier with the
                                                        # highest priority flag whose flag column (which we remove to conform to EDD format) isn't blank
               Reportable_Result = "Y") %>%
        mutate(Result_Comment = paste0(if_else(is.na(Result_Comment), "", paste0(Result_Comment, " ")),
                                       Blank_Flag, 
                                       Dup_Flag,
                                       Tot_vs_Dissolved_Flag)) %>%
        mutate(Result_Comment = if_else(Result_Comment == "", NA, str_trim(Result_Comment))) %>%
        select(`#Org_Code`, Activity_ID, Characteristic_Name, Method_Speciation,  # ORDER TO MATCH EDD
               Filtered_Fraction, Result_Detection_Condition, Result_Text, 
               Result_Unit, Result_Qualifier, Result_Status, Result_Type, Result_Comment,
               Method_Detection_Limit, Lower_Quantification_Limit, Upper_Quantification_Limit, Limit_Comment,
               Temperature_Basis, Statistical_Basis, Time_Basis, Weight_Basis, Particle_Size_Basis,
               Precision, Bias, Confidence_Interval, Upper_Confidence_Limit,
               Lower_Confidence_Limit, Result_Sampling_Point_Name, Result_Depth_Height_Measure,
               Result_Depth_Height_Measure_Unit, Result_Depth_Altitude_Reference_Point,
               Analytical_Method_ID, Analytical_Remark, Lab_ID, Lab_Remark_Code, Analysis_Start_Date,
               Analysis_Start_Time, Analysis_Start_Time_Zone, Lab_Accreditation_Indicator,
               Lab_Accreditation_Authority_Name, Lab_Batch_ID, Lab_Sample_Preparation_ID,
               Lab_Sample_Preparation_Start_Date, Lab_Sample_Preparation_Start_Time,
               Lab_Sample_Preparation_Start_Time_Zone, Dilution_Factor, Num_of_Replicates,
               Data_Logger_Line_Name, Biological_Intent, Biological_Individual_ID, Subject_Taxon,
               Unidentified_Species_ID, Tissue_Anatomy, Group_Summary_Count_or_Weight,
               Group_Summary_Count_or_Weight_Unit, Cell_Form, Cell_Shape, Habit_Name_1,
               Habit_Name_2, Habit_Name_3, Voltinism, Pollution_Tolerance, Pollution_Tolerance_Scale,
               Trophic_Level, Functional_Feeding_Group_1, Functional_Feeding_Group_2,
               Functional_Feeding_Group_3, File_Name_or_Resource_ID, Resource_Date,
               Resource_Title_Name, Resource_Creator_Name, Resource_Publisher_Name,
               Resource_Publication_Year, Resource_Volume_Pages, Resource_Subject_Text,
               Frequency_Class_Descriptor_1, Frequency_Class_Bounds_Unit_1,
               Frequency_Class_Lower_Bound_1, Frequency_Class_Upper_Bound_1,
               Frequency_Class_Descriptor_2, Frequency_Class_Bounds_Unit_2,
               Frequency_Class_Lower_Bound_2, Frequency_Class_Upper_Bound_2,
               Frequency_Class_Descriptor_3, Frequency_Class_Bounds_Unit_3,
               Frequency_Class_Lower_Bound_3, Frequency_Class_Upper_Bound_3,
               Taxonomist_Accreditation_Indicator, Taxonomist_Accreditation_Authority_Name,
               Result_File_Name, Lab_Reported_Result, Reportable_Result, Source_Flags,
               Logger_Standard_Difference, Logger_Percent_Error, Analytical_Method_ID_Context,
               Predicted_Result) 
      })
  # ------------------------
  # FIX ACTIVITIES
  # ------------------------
  
  # We kept a column in edd_activities earlier for the purposes of performing a join with
  # edd_results. Now, we can get rid of that column
  edd_activities <- edd_activities %>%
    lapply(function(x) {
      x %>%
        select(-Activity_ID_wDups)
    })
  
  # ------------------------
  # CREATE LOCATIONS TABLE
  # ------------------------
  
  edd_locations <- edd_results %>%
    lapply(create_locations_table, dbo = dbo)
  
  # ------------------------
  # CREATE PROJECTS TABLE
  # ------------------------
  
  edd_projects <- create_projects_table(db_file_path)
 
  # ------------------------
  # FORMAT LIST TO RETURN
  # ------------------------
  
  # Get length of component lists
  n <- length(edd_locations)
  
  # Create list containing all data
  all_data <- list("Projects" = rep(list(edd_projects), n),
                   "Locations" = edd_locations, 
                   "Activities" = edd_activities, 
                   "Results" = edd_results)
  
  # Rearrange list of lists
  edd <- lapply(seq_len(n), function(i) {
    sapply(all_data, `[[`, i)
  })
  
  # Adjust names depending on concat
  if (concat) {
    # Edit name of item in list
    names(edd) <- ccal_file_paths %>%
      basename() %>%
      paste(collapse = "_") %>%
      stringr::str_remove_all(".xlsx") %>%
      paste0(".xlsx")
  }
  else {
    # Assign names to lists
    names(edd) <- basename(ccal_file_paths)
  }
  
  return(edd)
}

#' Function for formatting our EDD for TestAmerica.
#'
#' @param test_america_file_paths Path to .csv files delivered by Test America. Use a character vector to specify multiple files.
#' @param db_file_path File path to a copy of the SEI water quality lab processing database.
#' @param pre_2020 By default set to false. If you are using data from before 2020, instead set this 
#' to a file path to ROMN_SEI_in_EQuIS.xlsx, which includes a crosswalk between EventName and SampleNumber.
#' @param qualifiers Table with flags and their meanings. By default, uses the version in the imdccal package. User-defined versions must have the same columns.
#'
#' @return A fully formatted EQuIS deliverable ready for the ecologist's manual review.
#' Specifically, this function a list containing 4 tables: Projects, Locations, Activities, and Results.
format_edd_test_america <- function(test_america_file_paths, db_file_path, pre_2020 = FALSE,
                                    qualifiers = imdccal::equis_qualifiers){
  
  # Load database tables
  dbo <- load_db(db_file_path, "Test America", pre_2020)
  
  # Load TestAmerica deliverables and remove extraneous rows
  deliverables <- read_csv(test_america_file_paths) %>%
    filter(`Sample Type Desc` %notin% c("Lab Control Sample", "Lab Control Sample Duplicate", 
                                        "Method Blank", "Matrix Spike", "Matrix Spike Duplicate"))
  
  # ------------------------
  # CREATE ACTIVITIES TABLE
  # ------------------------
  
  activities <- deliverables %>%
    mutate(`#Org_Code` = "ROMN",
           Project_ID = paste(`#Org_Code`, "SEI", sep = "_"),
           Activity_Start_Date = format(mdy_hm(`Collection Date`), "%Y%m%d"),
           Custody_ID = `Client Sample ID`,
           Medium = if_else(Matrix == "Solid", "Sediment", Matrix)) %>%
    { if (is.character(pre_2020)) {
      left_join(., dbo$Joined, by = c("Custody_ID" = "SampleNumber"))
    } else{
      mutate(., Custody_ID_Join = str_extract(`Client Sample ID`, "\\d{2}[A-Z]{3}\\d{7}")) %>% # extract bottle number (2 digits, 3 characters, 7 digits)
      left_join(., dbo$Joined, by = c("Custody_ID_Join" = "SampleNumber"))
    }} %>%
    mutate(Location_ID = paste(`#Org_Code`, 
                               str_extract(EventName, "(?i)[a-z]*_[a-z0-9]*"), # extracts site name from event name
                               sep = "_"),
           Activity_ID_wDups = paste(Location_ID, Activity_Start_Date, "T", Medium, sep = "_")) %>%
    select(`#Org_Code`, EventName, Project_ID, Location_ID, Activity_ID_wDups, Medium, Activity_Start_Date, Custody_ID, Notes) %>%
    unique() %>%
    rename(Additional_Location_Info = EventName,
           Activity_Comment = Notes) %>%
    mutate(Activity_Type = case_when(str_detect(Additional_Location_Info, "_FD_W$") ~ 
                                       "Quality Control Sample-Field Replicate",
                                     str_detect(Additional_Location_Info, "_FB_W$") ~
                                       "Quality Control Sample-Field Blank",
                                     TRUE ~ "Sample-Routine"), # the suffixes are how we denote QC samples internally
           Medium_Subdivision = NA,
           Assemblage_Sampled_Name = NA,
           Activity_Start_Time = NA,
           Activity_Start_Time_Zone = NA,
           Activity_End_Date =  NA,
           Activity_End_Time = NA,
           Activity_End_Time_Zone = NA,
           Activity_Relative_Depth = NA,
           Activity_Depth = NA,
           Activity_Depth_Unit = NA,
           Activity_Upper_Depth = NA,
           Activity_Lower_Depth = NA, 
           Activity_Depth_Reference = NA,
           Activity_Sampler = NA,
           Activity_Recorder = NA,
           Activity_Conducting_Organization = NA,
           Station_Visit_Comment = NA,
           Collection_Method_ID = if_else(str_detect(Activity_Type, "[S|s]ample"), "ROMN_GRAB", NA),
           Collection_Equipment_Name = if_else(str_detect(Activity_Type, "[S|s]ample"), "Water bottle", NA),
           Collection_Equipment_Description = NA,
           Gear_Deployment = NA,
           Container_Type	= NA,
           Container_Color = NA,	
           Container_Size = NA,
           Container_Size_Unit = NA,
           Preparation_Method_ID = NA,
           Chemical_Preservative = NA,
           Thermal_Preservative = NA,
           Transport_Storage_Description = NA,	
           Activity_Group_ID = NA,
           Activity_Group_Name = NA,
           Activity_Group_Type = NA,
           Collection_Duration = NA,
           Collection_Duration_Unit = NA,
           Sampling_Component_Name = NA,
           Sampling_Component_Place_In_Series = NA,	
           Reach_Length = NA,
           Reach_Length_Unit = NA,	
           Reach_Width = NA,
           Reach_Width_Unit = NA,
           Pass_Count = NA,
           Net_Type = NA,
           Net_Surface_Area = NA,	
           Net_Surface_Area_Unit = NA,	
           Net_Mesh_Size = NA,
           Net_Mesh_Size_Unit = NA,
           Boat_Speed = NA,
           Boat_Speed_Unit = NA,	
           Current_Speed = NA,
           Current_Speed_Unit = NA,
           Toxicity_Test_Type = NA,
           Collection_Effort = NA,
           Collection_Effort_Gear_Type = NA,
           Org_Source_Act_ID = NA) %>%
    group_by(Activity_ID_wDups) %>%
    arrange(Custody_ID) %>%
    mutate(count = row_number() - 1,
           Activity_ID = paste(Activity_ID_wDups, count, sep = "_")) %>%
    ungroup() %>%
    select(`#Org_Code`, Project_ID, Location_ID, Activity_ID_wDups, Activity_ID, Activity_Type, Medium,
           Medium_Subdivision, Assemblage_Sampled_Name, Activity_Start_Date, Activity_Start_Time,
           Activity_Start_Time_Zone, Activity_End_Date, Activity_End_Time, Activity_End_Time_Zone,
           Activity_Relative_Depth, Activity_Depth, Activity_Depth_Unit, Activity_Upper_Depth,
           Activity_Lower_Depth, Activity_Depth_Reference, Additional_Location_Info, Activity_Sampler,
           Activity_Recorder, Custody_ID, Activity_Conducting_Organization, Station_Visit_Comment,
           Activity_Comment, Collection_Method_ID, Collection_Equipment_Name, 
           Collection_Equipment_Description, Gear_Deployment, Container_Type, Container_Color, 
           Container_Size, Container_Size_Unit, Preparation_Method_ID, Chemical_Preservative, 
           Thermal_Preservative, Transport_Storage_Description, Activity_Group_ID, Activity_Group_Name,
           Activity_Group_Type, Collection_Duration, Collection_Duration_Unit, Sampling_Component_Name,
           Sampling_Component_Place_In_Series, Reach_Length, Reach_Length_Unit, Reach_Width,
           Reach_Width_Unit, Pass_Count, Net_Type, Net_Surface_Area, Net_Surface_Area_Unit, Net_Mesh_Size,
           Net_Mesh_Size_Unit, Boat_Speed, Boat_Speed_Unit, Current_Speed, Current_Speed_Unit,
           Toxicity_Test_Type, Collection_Effort, Collection_Effort_Gear_Type, Org_Source_Act_ID)
  
  # ------------------------
  # CREATE RESULTS TABLE
  # ------------------------

  results <- deliverables %>%
    mutate(`#Org_Code` = "ROMN",
           Activity_Start_Date = format(mdy_hm(`Collection Date`), "%Y%m%d"),
           Custody_ID = `Client Sample ID`, 
           Medium = if_else(Matrix == "Solid", "Sediment", Matrix)) %>%
    { if (is.character(pre_2020)) {
      left_join(., dbo$Joined, by = c("Custody_ID" = "SampleNumber"))
    } else{
      mutate(., Custody_ID_Join = str_extract(`Client Sample ID`, "\\d{2}[A-Z]{3}\\d{7}")) %>% # extract bottle number (2 digits, 3 characters, 7 digits)
      left_join(., dbo$Joined, by = c("Custody_ID_Join" = "SampleNumber"))
    }} %>%
    mutate(Location_ID = paste(`#Org_Code`, 
                               str_extract(EventName, "(?i)[a-z]*_[a-z0-9]*"), # extracts site name from event name
                               sep = "_"),
           Activity_ID_wDups = paste(Location_ID, Activity_Start_Date, "T", Medium, sep = "_")) %>%
    inner_join(activities %>% select(Activity_ID_wDups, Custody_ID, Activity_ID),
               by = c("Activity_ID_wDups", "Custody_ID")) %>%
    mutate(Characteristic_Name = if_else(Analyte == "Percent Moisture", "Moisture content", Analyte),
           Method_Speciation = NA,
           Filtered_Fraction = if_else(`Prep Type` == "Total/NA", "Total", `Prep Type`)) %>%
    left_join(dbo$units,
              by = join_by(Unit == ResultValueUnits,
                           Medium == Medium,
                           Filtered_Fraction == SampleFraction,
                           Characteristic_Name == ParameterName)) %>%
    mutate(Result_Numeric = as.numeric(if_else(Result == "ND", NA, Result)),
           Result_Text = case_when(Conversion == "Yes" & Operation == "*" ~ Result_Numeric * Factor,
                                   Conversion == "Yes" & Operation == "/" ~ Result_Numeric / Factor,
                                   TRUE ~ Result_Numeric),
           Lab_Reported_Result = if_else(Result == "ND", "ND", as.character(Result_Text)),
           Method_Detection_Limit = case_when(Conversion == "Yes" & Operation == "*" ~ `Low Limit` * Factor,
                                              Conversion == "Yes" & Operation == "/" ~ `Low Limit` / Factor,
                                              TRUE ~ `Low Limit`),
           Lower_Quantification_Limit = case_when(Conversion == "Yes" & Operation == "*" ~ `High Limit` * Factor,
                                                  Conversion == "Yes" & Operation == "/" ~ `High Limit` / Factor,
                                                  TRUE ~ `High Limit`),
           Result_Detection_Condition = case_when(Result_Text %<<% Method_Detection_Limit | Lab_Reported_Result == "ND" ~ "Not Detected",
                                                  Result_Text %<<% Lower_Quantification_Limit ~ "Present Below Quantification Limit",
                                                  TRUE ~ "Detected And Quantified"), # set detection condition according to comparisons between the measurement, MDL, and LQL
           Result_Comment = case_when(Result_Text %<<% Method_Detection_Limit
                                      ~ paste0("Lab reported a value of ", Result_Text, " which was below the MDL;"),
                                      Result_Text %<<% Lower_Quantification_Limit
                                      ~ paste0("Lab reported a value of ", Result_Text, " which was below the LQL;"),
                                      TRUE ~ NA), # include value in Result_Comment if censored from Result_Text
           Result_Text = if_else(Result_Detection_Condition == "Detected And Quantified", Result_Text, NA),
           Result_Unit = FinalValueUnits,
           Result_Qualifier = NA,
           Result_Status = "Pre-Cert",
           Result_Type = "Actual",
           Upper_Quantification_Limit = NA,
           Limit_Comment = "Detection Limit Type = MDL",
           Temperature_Basis = NA,
           Statistical_Basis = NA,
           Time_Basis = NA,
           Weight_Basis = NA,
           Particle_Size_Basis = NA,
           Precision = NA,
           Bias = NA,
           Confidence_Interval = NA,
           Upper_Confidence_Limit = NA,
           Lower_Confidence_Limit = NA,
           Result_Sampling_Point_Name = NA,
           Result_Depth_Height_Measure = NA,
           Result_Depth_Height_Measure_Unit = NA,
           Result_Depth_Altitude_Reference_Point = NA,
           Characteristic_Join = str_to_lower(Characteristic_Name)) %>%
    left_join(dbo$methods %>%
                mutate(Analyte_Join = str_to_lower(Analyte)),
              by = join_by(Characteristic_Join == Analyte_Join,
                           Medium == Matrix,
                           Filtered_Fraction == PrepType,
                           `Analysis Method` == Analysis_Method)) %>%
    mutate(Analytical_Method_ID = EQuIS_Method,
           Analytical_Remark = NA,
           Lab_ID = "EUROFINS_DEN",
           Lab_Remark_Code = NA,
           Analysis_Start_Date = format(mdy_hm(`Analysis Date`), "%Y%m%d"),
           Analysis_Start_Time = format(mdy_hm(`Analysis Date`), "%H:%M:%S"),
           Analysis_Start_Time_Zone = NA,
           Lab_Accreditation_Indicator = NA,
           Lab_Accreditation_Authority_Name = NA) %>%
    rename(Lab_Batch_ID = `Lab Sample ID`) %>%
    mutate(Lab_Sample_Preparation_ID =  NA,
           Lab_Sample_Preparation_Start_Date = NA,
           Lab_Sample_Preparation_Start_Time = NA,
           Lab_Sample_Preparation_Start_Time_Zone = NA,
           Dilution_Factor = NA,
           Num_of_Replicates = NA,
           Data_Logger_Line_Name = NA,
           Biological_Intent = NA,
           Biological_Individual_ID = NA,
           Subject_Taxon = NA,
           Unidentified_Species_ID = NA,
           Tissue_Anatomy = NA,
           Group_Summary_Count_or_Weight = NA,
           Group_Summary_Count_or_Weight_Unit = NA,
           Cell_Form = NA,
           Cell_Shape = NA,
           Habit_Name_1 = NA,
           Habit_Name_2 = NA,
           Habit_Name_3 = NA,
           Voltinism = NA,
           Pollution_Tolerance = NA,
           Pollution_Tolerance_Scale = NA,
           Trophic_Level = NA,
           Functional_Feeding_Group_1 = NA,
           Functional_Feeding_Group_2 = NA,
           Functional_Feeding_Group_3 = NA,
           File_Name_or_Resource_ID = NA,
           Resource_Date = NA,
           Resource_Title_Name = NA,
           Resource_Creator_Name = NA,
           Resource_Publisher_Name = NA,
           Resource_Publication_Year = NA,
           Resource_Volume_Pages = NA,
           Resource_Subject_Text = NA,
           Frequency_Class_Descriptor_1 = NA,
           Frequency_Class_Bounds_Unit_1 = NA,
           Frequency_Class_Lower_Bound_1 = NA,
           Frequency_Class_Upper_Bound_1 = NA,
           Frequency_Class_Descriptor_2 = NA,
           Frequency_Class_Bounds_Unit_2 = NA,
           Frequency_Class_Lower_Bound_2 = NA,
           Frequency_Class_Upper_Bound_2 = NA,
           Frequency_Class_Descriptor_3 = NA,
           Frequency_Class_Bounds_Unit_3 = NA,
           Frequency_Class_Lower_Bound_3 = NA,
           Frequency_Class_Upper_Bound_3 = NA,
           Taxonomist_Accreditation_Indicator = NA,
           Taxonomist_Accreditation_Authority_Name = NA,
           Result_File_Name = NA,
           Reportable_Result = "Y",
           Source_Flags = Flag, 
           Logger_Standard_Difference = NA,
           Logger_Percent_Error = NA,
           Analytical_Method_ID_Context = NA,
           Predicted_Result = NA) %>%
    select(-Medium, -Group, -Custody_ID) %>%
    flag_replicates(activities, "Test America", dbo$units) %>%
    flag_blanks(activities) %>%
    flag_tot_vs_dissolved(activities,
                          dissolved_characteristic_name = c("Arsenic",
                                                            "Barium",
                                                            "Cadmium",
                                                            "Copper",
                                                            "Lead",
                                                            "Selenium",
                                                            "Silver",
                                                            "Zinc"),
                          dissolved_filtered_fraction = c("Dissolved",
                                                          "Dissolved",
                                                          "Dissolved",
                                                          "Dissolved",
                                                          "Dissolved",
                                                          "Dissolved",
                                                          "Dissolved",
                                                          "Dissolved"),
                          total_characteristic_name = c("Arsenic",
                                                        "Barium",
                                                        "Cadmium",
                                                        "Copper",
                                                        "Lead",
                                                        "Selenium",
                                                        "Silver",
                                                        "Zinc"),
                          total_filtered_fraction = c("Total",
                                                      "Total",
                                                      "Total",
                                                      "Total",
                                                      "Total",
                                                      "Total",
                                                      "Total",
                                                      "Total")) %>%
    attach_ta_flags(dbo$flags, qualifiers) %>%
    mutate(Result_Qualifier = case_when(Blank_Flag != "" ~ "FBK",
                                        FBK != "" ~ "FBK",
                                        Dup_Flag != "" ~ "SUS",
                                        Tot_vs_Dissolved_Flag != "" ~ "SUS",
                                        `4` != "" ~ "4", 
                                        `"` != "" ~ '"', 
                                        HIB != "" ~ "HIB", 
                                        IQCOL != "" ~ "IQCOL", 
                                        HICC != "" ~ "HICC",
                                        SDROL != "" ~ "SDROL", 
                                        `SD%EL` != "" ~ "SD%EL", 
                                        `SD%SS` != "" ~ "SD%SS",
                                        H != "" ~ "H", 
                                        TRUE ~ NA), # this implements our flag hierarchy by filling Result_Qualifier with the
                                                    # highest priority flag whose flag column (which we remove to conform to EDD format) isn't blank
           Reportable_Result = "Y") %>%
    mutate(Result_Comment = paste0(if_else(is.na(Result_Comment), "", paste0(Result_Comment, " ")),
                                   Blank_Flag, 
                                   FBK,
                                   Dup_Flag,
                                   Tot_vs_Dissolved_Flag, 
                                   `4`,
                                   `"`, 
                                   HIB, 
                                   IQCOL,
                                   HICC,
                                   SDROL, 
                                   `SD%EL`,
                                   `SD%SS`,
                                   H)) %>%
    mutate(Result_Comment = if_else(Result_Comment == "", NA, str_trim(Result_Comment))) %>%
    select(`#Org_Code`, Activity_ID, Characteristic_Name, Method_Speciation,  # ORDER TO MATCH EDD
           Filtered_Fraction, Result_Detection_Condition, Result_Text,
           Result_Unit, Result_Qualifier, Result_Status, Result_Type, Result_Comment,
           Method_Detection_Limit, Lower_Quantification_Limit, Upper_Quantification_Limit, Limit_Comment,
           Temperature_Basis, Statistical_Basis, Time_Basis, Weight_Basis, Particle_Size_Basis,
           Precision, Bias, Confidence_Interval, Upper_Confidence_Limit,
           Lower_Confidence_Limit, Result_Sampling_Point_Name, Result_Depth_Height_Measure,
           Result_Depth_Height_Measure_Unit, Result_Depth_Altitude_Reference_Point,
           Analytical_Method_ID, Analytical_Remark, Lab_ID, Lab_Remark_Code, Analysis_Start_Date,
           Analysis_Start_Time, Analysis_Start_Time_Zone, Lab_Accreditation_Indicator,
           Lab_Accreditation_Authority_Name, Lab_Batch_ID, Lab_Sample_Preparation_ID,
           Lab_Sample_Preparation_Start_Date, Lab_Sample_Preparation_Start_Time,
           Lab_Sample_Preparation_Start_Time_Zone, Dilution_Factor, Num_of_Replicates,
           Data_Logger_Line_Name, Biological_Intent, Biological_Individual_ID, Subject_Taxon,
           Unidentified_Species_ID, Tissue_Anatomy, Group_Summary_Count_or_Weight,
           Group_Summary_Count_or_Weight_Unit, Cell_Form, Cell_Shape, Habit_Name_1,
           Habit_Name_2, Habit_Name_3, Voltinism, Pollution_Tolerance, Pollution_Tolerance_Scale,
           Trophic_Level, Functional_Feeding_Group_1, Functional_Feeding_Group_2,
           Functional_Feeding_Group_3, File_Name_or_Resource_ID, Resource_Date,
           Resource_Title_Name, Resource_Creator_Name, Resource_Publisher_Name,
           Resource_Publication_Year, Resource_Volume_Pages, Resource_Subject_Text,
           Frequency_Class_Descriptor_1, Frequency_Class_Bounds_Unit_1,
           Frequency_Class_Lower_Bound_1, Frequency_Class_Upper_Bound_1,
           Frequency_Class_Descriptor_2, Frequency_Class_Bounds_Unit_2,
           Frequency_Class_Lower_Bound_2, Frequency_Class_Upper_Bound_2,
           Frequency_Class_Descriptor_3, Frequency_Class_Bounds_Unit_3,
           Frequency_Class_Lower_Bound_3, Frequency_Class_Upper_Bound_3,
           Taxonomist_Accreditation_Indicator, Taxonomist_Accreditation_Authority_Name,
           Result_File_Name, Lab_Reported_Result, Reportable_Result, Source_Flags,
           Logger_Standard_Difference, Logger_Percent_Error, Analytical_Method_ID_Context,
           Predicted_Result)
  
  # ------------------------
  # FIX ACTIVITIES
  # ------------------------
  
  # We kept a column in activities earlier for the purposes of performing a join with
  # results. Now, we can get rid of that column
  activities <- activities %>%
    select(-Activity_ID_wDups)
  
  # ------------------------
  # CREATE LOCATIONS TABLE
  # ------------------------
  
  locations <- create_locations_table(results, dbo)
  
  # ------------------------
  # CREATE PROJECTS TABLE
  # ------------------------
  
  projects <- create_projects_table(db_file_path)
  
  # ------------------------
  # FORMAT LIST TO RETURN
  # ------------------------
  
  return(list("Projects" = projects,
              "Locations" = locations, 
              "Activities" = activities, 
              "Results" = results))
}
  
#' Write the EDD to csv or xlsx
#'
#' @param deliverable_file_paths Paths to .xlsx files from CCAL or .csv files from Test America. 
#' Use a character vector to specify multiple files.
#' @inheritParams openxlsx::write.xlsx
#' @param db_file_path File path to a copy of the SEI water quality lab processing database.
#' @param lab The name of the lab that we are loading tables for. Valid entries are "CCAL" and "Test America".
#' @param pre_2020 By default set to false. If you are using data from before 2020, instead set this 
#' to a file path to ROMN_SEI_in_EQuIS.xlsx, which includes a crosswalk between EventName and SampleNumber.
#' @param format File format to export EDD to - either "xlsx" or "csv".
#' @param destination_folder Folder to save the data in. Defaults to current working directory. Folder must already exist.
#' @param qualifiers Table with flags and their meanings. By default, uses the version in the imdccal package. User-defined versions must have the same columns.
#' @param concat For CCAL only. Value of concat will not impact the Test America processing.
#' If concat is set to TRUE, the function creates one file rather than one for every input file.
#' By default, concat is set to FALSE, so the output contains separate files for each CCAL deliverable.
#' If only one file path is supplied to the file_paths argument, this parameter does not affect the output.
#'
#' @return Invisibly returns a list containing the data that were written to file.
write_edd <- function(deliverable_file_paths, db_file_path, lab, pre_2020 = FALSE, format = c("xlsx", "csv"), destination_folder = "./", 
                      overwrite = FALSE, qualifiers = imdccal::equis_qualifiers, concat = FALSE) {
  format <- match.arg(format)
  destination_folder <- normalizePath(destination_folder, winslash = .Platform$file.sep)
  
  # Call function to format EDD
  if (lab == "CCAL") {
    all_data <- format_edd_ccal(deliverable_file_paths, db_file_path, pre_2020, concat)
  }
  else if (lab == "Test America") {
    all_data <- list(format_edd_test_america(deliverable_file_paths, db_file_path, pre_2020, qualifiers))
    
    year <- all_data[[1]]$Activities %>%
      mutate(Year = substring(Activity_Start_Date, 1, 4)) %>%
      distinct(Year) %>%
      pull(Year)
    
    names(all_data) <- paste0("ROMN_TestAmerica_", year)
  }
  else {
    stop("Invalid lab argument for write_edd(). Valid inputs are 'CCAL' and 'Test America'")
  }
  
  # Call function from imdccal package to write EDD
  write_ccal(all_data, format, destination_folder, overwrite, suffix = "_edd", num_tables = 4)
  
  return(invisible(all_data))
}

#' Identify observations to flag depending on relative percent difference between samples and their field duplicates
#'
#' @param results An appropriately formatted results table. Specifically, the following fields are required:
#' Activity_ID, Characteristic_Name, Filtered_Fraction, Result_Text, Result_Detection_Condition, Method_Detection_Limit, 
#' Lower_Quantification_Limit, Analysis_Start_Date.
#' @param activities An appropriately formatted activities table. Specifically, the following fields are required:
#' Activity_ID, Activity_Type, Additional_Location_Info, Medium.
#' @param lab Either "CCAL" or "Test America"
#' @param limits Table with detection limits. By default, uses the version in the imdccal package. User-defined versions must have the same columns.
#'
#' @return The results table with the addition of a "Dup_Flag" column of character type which can be further
#' processed to apply quality control flags and create associated descriptive fields.
flag_replicates <- function(results, activities, lab, limits = imdccal::detection_limits) {
  
  # Identify field duplicates
  dups <- results %>%
    left_join(activities, by = "Activity_ID") %>%
    filter(Activity_Type == "Quality Control Sample-Field Replicate") %>%
    mutate(EventName_Common = str_remove(Additional_Location_Info, "_FD_W$"))
  
  # Identify corresponding original samples
  regs <- results %>%
    left_join(activities, by = "Activity_ID") %>%
    filter(Additional_Location_Info %in% dups$EventName_Common)
  
  # Join regs and dups
  joined <- dups %>%
    inner_join(regs %>%
                 select(Activity_ID, Additional_Location_Info, Characteristic_Name, Filtered_Fraction,
                        Medium, Result_Text, Result_Detection_Condition, Method_Detection_Limit, Lower_Quantification_Limit),
               by = c("EventName_Common" = "Additional_Location_Info", "Characteristic_Name", 
                      "Filtered_Fraction", "Medium"),
               suffix = c("_Dup", "_Reg")) %>% # include a helpful suffix
    mutate(RPD = abs( (Result_Text_Reg - Result_Text_Dup) / ((Result_Text_Reg + Result_Text_Dup)/2) * 100 ))
  
  # Join group into table
  if (lab == "CCAL") {
    joined <- joined %>%
      left_join(limits %>% select(Characteristic_Name, Filtered_Fraction, Group, StartDate, EndDate),
                by = dplyr::join_by(Characteristic_Name, 
                                    Filtered_Fraction,
                                    Analysis_Start_Date >= StartDate,
                                    Analysis_Start_Date <= EndDate)) 
  } else if (lab == "Test America") {
    joined <- joined %>%
      left_join(limits %>% distinct(ParameterName, SampleFraction, Group),
                by = dplyr::join_by(Characteristic_Name == ParameterName, 
                                    Filtered_Fraction == SampleFraction)) 
  }
  else {
    stop("Invalid lab argument for flag_replicates(). Valid inputs are 'CCAL' and 'Test America'")
  }
  
  # Determine if RPD is less than group specific threshold for flagging purposes
  # Create various flag comments according to comparisons between the reg and dup
  joined <- joined %>% 
    mutate(Dup_Flag = case_when(Result_Detection_Condition_Reg == "Detected And Quantified" & Result_Detection_Condition_Dup == "Detected And Quantified" &
                                  RPD > 15.0 & Group %notin% c("Metal", "Nutrient") ~ 
                                  "SUS: Result value is defined as suspect by data owner because replicate samples exceed the 15% relative percent difference permitted for analytes that are neither nutrients nor metals; ",
                                Result_Detection_Condition_Reg == "Detected And Quantified" & Result_Detection_Condition_Dup == "Detected And Quantified" &
                                  RPD > 30.0 & Group == "Metal" ~ 
                                  "SUS: Result value is defined as suspect by data owner because replicate samples exceed the 30% relative percent difference permitted for metals; ",
                                Result_Detection_Condition_Reg == "Detected And Quantified" & Result_Detection_Condition_Dup == "Detected And Quantified" &
                                  RPD > 30.0 & Group == "Nutrient" ~ 
                                  "SUS: Result value is defined as suspect by data owner because replicate samples exceed the 30% relative percent difference permitted for nutrients; ",
                                is.na(RPD) & ((is.na(Result_Text_Dup) & Result_Text_Reg > 2*Lower_Quantification_Limit_Reg) |
                                                (is.na(Result_Text_Reg) & Result_Text_Dup > 2*Lower_Quantification_Limit_Dup)) ~ 
                                  "SUS: Result value is defined as suspect by data owner because either the replicate or the sample is not quantified, but the other is greater than twice the lower quantitation limit; ",
                                Result_Detection_Condition_Reg == "Not Detected" & Result_Detection_Condition_Dup == "Present Below Quantification Limit" |
                                  Result_Detection_Condition_Reg == "Present Below Quantification Limit" & Result_Detection_Condition_Dup == "Not Detected" ~
                                  "SUS: Result value is defined as suspect by data owner because either the replicate or the sample concentration is not detected, but the other is detected above method detection limit though not quantified; ",
                                TRUE ~ ""))
  
  # Pivot longer (one row per observation)
  pivoted <- joined %>%
    select(Activity_ID_Dup, Activity_ID_Reg, Characteristic_Name, Filtered_Fraction, Group, Dup_Flag) %>%
    pivot_longer(cols = c(Activity_ID_Dup, Activity_ID_Reg)) %>%
    rename("Activity_ID" = "value") %>%
    select(-name)
  
  # Join to results and assign flag 
  results <- results %>%
    left_join(pivoted,
              by = c("Activity_ID", "Characteristic_Name", "Filtered_Fraction")) %>%
    mutate(Dup_Flag = if_else(is.na(Dup_Flag), "", Dup_Flag)) %>%
    select(-Group)
  return(results)
}

#' Identify observations to flag depending on the detection condition and magnitudes of samples and their field blanks 
#'
#' @param results An appropriately formatted results table. Specifically, the following fields are required:
#' Activity_ID, Characteristic_Name, Filtered_Fraction, Result_Text, Result_Detection_Condition.
#' @param activities An appropriately formatted activities table. Specifically, the following fields are required:
#' Activity_ID, Activity_Type, Additional_Location_Info, Medium.
#'
#' @return The results table with the addition of a "Blank_Flag" column of character type which can be further
#' processed to apply quality control flags and create associated descriptive fields.
flag_blanks <- function(results, activities) {
  
  # Identify field blanks
  blanks <- results %>%
    left_join(activities, by = "Activity_ID") %>%
    filter(Activity_Type == "Quality Control Sample-Field Blank") %>%
    mutate(EventName_Common = str_remove(Additional_Location_Info, "_FB_W$"))
  
  # Identify corresponding original samples
  regs <- results %>%
    left_join(activities, by = "Activity_ID") %>%
    filter(Additional_Location_Info %in% blanks$EventName_Common)
  
  # Join regs and blanks
  joined <- blanks %>%
    inner_join(regs %>%
                 select(Activity_ID, Additional_Location_Info, Characteristic_Name, Filtered_Fraction, Medium,
                        Activity_Type, Result_Text, Result_Detection_Condition),
               by = c("EventName_Common" = "Additional_Location_Info", "Characteristic_Name", 
                      "Filtered_Fraction", "Medium"),
               suffix = c("_Blank", "_Reg")) # include a helpful suffix
  
  # Create various flag comments for blanks that detected analytes
  joined <- joined %>%
    mutate(Blank_Flag = case_when(Result_Detection_Condition_Blank == "Present Below Quantification Limit" ~
                                    "FBK: Analyte detected in field blank but not quantified as it is less than the Lower Quantification Limit; ",
                                  Result_Detection_Condition_Blank == "Detected And Quantified" & 
                                    Result_Detection_Condition_Reg != "Detected And Quantified" ~ 
                                    "FBK: Analyte detected and quantified in field blank but not quantified in environmental sample; ",
                                  Result_Detection_Condition_Blank == "Detected And Quantified" & 
                                    Result_Text_Reg < 5*Result_Text_Blank ~
                                    "FBK: Analyte detected and quantified in field blank. Environmental sample analyte concentration less than five times the field blank's concentration; ",
                                  Result_Detection_Condition_Blank == "Detected And Quantified" &
                                    Result_Text_Reg >= 5*Result_Text_Blank ~
                                    "FBK: Analyte detected and quantified in field blank. Environmental sample analyte concentration at least 5 times the field blank's concentration; ",
                                  TRUE ~ ""))
  
  # Pivot longer (one row per observation)
  pivoted <- joined %>%
    select(Activity_ID_Blank, Activity_ID_Reg, Characteristic_Name, Filtered_Fraction, Blank_Flag) %>%
    pivot_longer(cols = c(Activity_ID_Blank, Activity_ID_Reg)) %>%
    rename("Activity_ID" = "value",
           "Type" = "name")
  
  # Join to results and assign flag 
  results <- results %>%
    left_join(pivoted,
              by = c("Activity_ID", "Characteristic_Name", "Filtered_Fraction")) %>%
    mutate(Blank_Flag = if_else(is.na(Blank_Flag), "", Blank_Flag)) %>%
    select(-Type)
  return(results)
}

#' Identify observations to flag depending on comparisons between different fractions of specified analytes 
#'
#' @param results An appropriately formatted results table. Specifically, the following fields are required:
#' Activity_ID, Characteristic_Name, Filtered_Fraction, Result_Text, Method_Detection_Limit.
#' @param activities An appropriately formatted activities table. Specifically, the following fields are required:
#' Activity_ID, Medium.
#' @param dissolved_characteristic_name The characteristic names of the dissolved fractions to be compared.
#' Must be a character vector where the i-th element corresponds to the i-th element of dissolved_filtered_fraction, 
#' total_characteristic_name, and total_filtered_fraction.
#' @param dissolved_filtered_fraction The filtered fractions of dissolved fractions to be compared.
#' Must be a character vector where the i-th element corresponds to the i-th element of dissolved_characteristic_name, 
#' total_characteristic_name, and total_filtered_fraction.
#' @param total_characteristic_name The characteristic names of total fractions to be compared.
#' Must be a character vector where the i-th element corresponds to the i-th element of dissolved_characteristic_name,
#' dissolved_filtered_fraction, and total_filtered_fraction.
#' @param total_filtered_fraction The filtered fractions of total fractions to be compared.
#' Must be a character vector where the i-th element corresponds to the i-th element of dissolved_characteristic_name,
#' dissolved_filtered_fraction, and total_characteristic_name.
#'
#' @return The results table with the addition of a "Tot_vs_Dissolved_Flag" column of character type which can be further
#' processed to apply quality control flags and create associated descriptive fields.
flag_tot_vs_dissolved <- function(results, activities, dissolved_characteristic_name, dissolved_filtered_fraction,
                                  total_characteristic_name, total_filtered_fraction) {
  # Join results and activities
  results <- results %>%
    left_join(activities %>% 
                select(Activity_ID, Medium, Custody_ID) %>%
                rename(Medium_in_function = Medium),
              by = "Activity_ID")
  
  # Create empty data tables with the same columns as results
  dissolved <- results[0,]
  total <- results[0,]
  
  # Row bind the relevant parts of the dissolved and total tables together
  for (i in 1:length(dissolved_characteristic_name)) {
    dissolved <- bind_rows(dissolved, 
                           results %>%
                             filter(Characteristic_Name == dissolved_characteristic_name[i],
                                    Filtered_Fraction == dissolved_filtered_fraction[i],
                                    Medium_in_function == "Water"))
    total <- bind_rows(total,
                       results %>%
                         filter(Characteristic_Name == total_characteristic_name[i],
                                Filtered_Fraction == total_filtered_fraction[i],
                                Medium_in_function == "Water")) 
  }
  
  # Handle error where there is no match between analytes intended to be compared
  if (dim(dissolved)[1] != dim(total)[1]) {
    # Create bottle number that is the same for dissolved and total fractions (Activity_ID can differ)
    dissolved <- dissolved %>%
      mutate(Custody_ID = str_extract(Custody_ID, "\\d{2}[A-Z]{3}\\d{7}"))
    
    total <- total %>%
      mutate(Custody_ID = str_extract(Custody_ID, "\\d{2}[A-Z]{3}\\d{7}"))
    
    # Identify any combinations of Custody_ID and Characteristic_Name that do not appear in both total and dissolved
    issues <- symdiff(dissolved %>% select(Custody_ID, Characteristic_Name),
                      total %>% select(Custody_ID, Characteristic_Name))
    
    # Warn the user about these rows
    inspect_issues <- paste(issues$Custody_ID, issues$Characteristic_Name, sep = ": ")
    warning <- c("!" = "Number of 'dissolved' fraction rows does not equal number of corresponding 'total' fraction rows.",
                 "!" = "Removing {inspect_issues} from comparison.",
                 "i" = "You may want to inspect these observations in your data.")
    
    cli::cli_warn(warning)

    # Remove problem rows
    dissolved <- dissolved %>%
      anti_join(issues,
                by = join_by(Characteristic_Name,
                             Custody_ID))
    
    total <- total %>%
      anti_join(issues,
                by = join_by(Characteristic_Name,
                             Custody_ID))
    
    if (dim(dissolved)[1] != dim(total)[1]) {
      stop("Number of 'dissolved' fraction rows STILL does not equal number of corresponding 'total' fraction rows.
           Please inspect your data and/or your code to troubleshoot this issue.")
    }
  }
  
  # Column bind the total to the dissolved
  # We column bind instead of joining due to some analytes (orthophosphate) needing multiple comparisons
  compare <- bind_cols(dissolved %>%
                         rename_with(~ paste0("dissolved_", .x)), 
                       total %>%
                         rename_with(~ paste0("total_", .x)))
  
  # Create various flag comments depending on comparison between dissolved and total fractions
  compare <- compare %>%
    mutate(Tot_vs_Dissolved_Flag = case_when(dissolved_Result_Text - total_Result_Text > total_Lower_Quantification_Limit ~
                                               paste0("SUS: Result value is defined as suspect by data owner due to comparison of filtered fractions. Dissolved fraction (",
                                                      dissolved_Characteristic_Name, 
                                                      ", ",
                                                      dissolved_Filtered_Fraction,
                                                      ") was found to be greater than the total fraction (",
                                                      total_Characteristic_Name,
                                                      ", ",
                                                      total_Filtered_Fraction,
                                                      ") plus the lower quantitation limit; "),
                                             dissolved_Result_Detection_Condition == "Detected And Quantified" &
                                               total_Result_Detection_Condition != "Detected And Quantified" ~
                                               paste0("SUS: Result value is defined as suspect by data owner due to comparison of filtered fractions. Dissolved fraction (",
                                                      dissolved_Characteristic_Name, 
                                                      ", ",
                                                      dissolved_Filtered_Fraction,
                                                      ") was detected and quantified but total fraction (",
                                                      total_Characteristic_Name,
                                                      ", ",
                                                      total_Filtered_Fraction,
                                                      ") was not quantified; "),
                                             dissolved_Result_Detection_Condition == "Present Below Quantification Limit" &
                                               total_Result_Detection_Condition == "Not Detected" ~
                                               paste0("SUS: Result value is defined as suspect by data owner due to comparison of filtered fractions. Dissolved fraction (",
                                                      dissolved_Characteristic_Name, 
                                                      ", ",
                                                      dissolved_Filtered_Fraction,
                                                      ") was present below lower quantitation limit, but total fraction (",
                                                      total_Characteristic_Name,
                                                      ", ",
                                                      total_Filtered_Fraction,
                                                      ") was not detected; "),
                                             TRUE ~ ""))
  
  # Select the relevant total columns and qc note
  total <- compare %>%
    select(total_Activity_ID,
           total_Characteristic_Name, 
           total_Filtered_Fraction, 
           total_Medium_in_function,
           Tot_vs_Dissolved_Flag) %>%
    rename(Activity_ID = total_Activity_ID,
           Characteristic_Name = total_Characteristic_Name,
           Filtered_Fraction = total_Filtered_Fraction,
           Medium = total_Medium_in_function)
  
  # Select the relevant dissolved columns and qc note
  dissolved <- compare %>%
    select(dissolved_Activity_ID, 
           dissolved_Characteristic_Name, 
           dissolved_Filtered_Fraction, 
           dissolved_Medium_in_function,
           Tot_vs_Dissolved_Flag) %>%
    rename(Activity_ID = dissolved_Activity_ID,
           Characteristic_Name = dissolved_Characteristic_Name,
           Filtered_Fraction = dissolved_Filtered_Fraction,
           Medium = dissolved_Medium_in_function)
  
  # Row bind the total and dissolved together, use a group by + summarize to combine comments for repeat analytes (total phosphorus, dissolved phosphorus, orthophosphate)
  longer <- total %>%
    bind_rows(dissolved) %>%
    group_by(Activity_ID, 
             Characteristic_Name, 
             Filtered_Fraction,
             Medium) %>%
    summarize(Tot_vs_Dissolved_Flag = paste(Tot_vs_Dissolved_Flag, collapse = ""),
              .groups = 'drop')
  
  # Join flag information into results, the flag description, and handle NAs
  results <- results %>%
    left_join(longer,
              by = c("Activity_ID" = "Activity_ID",
                     "Characteristic_Name" = "Characteristic_Name", 
                     "Filtered_Fraction" = "Filtered_Fraction",
                     "Medium_in_function" = "Medium")) %>%
    mutate(Tot_vs_Dissolved_Flag = if_else(is.na(Tot_vs_Dissolved_Flag), "", Tot_vs_Dissolved_Flag)) %>%
    select(-Medium_in_function, -Custody_ID)
    
  return(results)
}

#' Translate flags provided by Test America into acceptable flags in EQuIS
#'
#' @param results An appropriately formatted results table. Specifically, the following fields are required:
#' Activity_ID, Characteristic_Name, Filtered_Fraction, Source_Flags.
#' @param ta_flags The tlu_TestAmerica_flags table in the water chemistry lab deliverable processing database. This defines the crosswalk between individual Test America and EQuIS flags.
#' @param qualifiers Table with flags and their meanings. By default, uses the version in the imdccal package. User-defined versions must have the same columns.
#'
#' @returns The results table with the addition of several columns of character type for each possible EQuIS flag resulting from 
#' flags historically provided by Test America. Throws an error if a new flag is encountered, and requests that the user update the crosswalk accordingly.
#' These flag columns are further processed to apply quality control flags and create associated descriptive fields in format_edd_test_america().
attach_ta_flags <- function(results, ta_flags, qualifiers = imdccal::equis_qualifiers) {
  # Filter for rows with a Test America flag
  # Expand so each TA flag is on its own row
  # Join in EQuIS flags
  results_with_flags <- results %>%
    select(Activity_ID, Characteristic_Name, Filtered_Fraction, Source_Flags) %>%
    filter(!is.na(Source_Flags)) %>%
    separate_rows(Source_Flags, sep = " ") %>%
    left_join(ta_flags %>%
                select(TA_flag, EQuIS_flag),
              by = join_by(Source_Flags == TA_flag)) 
  
  # Filter for any NA EQuIS flags 
  undefined_flags <- results_with_flags %>%
    filter(is.na(EQuIS_flag)) %>%
    distinct(Source_Flags)
  
  # Stop process and request the user define the new flag
  if(nrow(undefined_flags) > 0) {
    stop("Undefined flag(s)! Define the translation from Test America flag(s) ", undefined_flags, 
         " to valid EQuIS flag(s) in the flag crosswalk (tlu_TestAmerica_flags in the water chemistry processing database), incorporate them in the flag hierarchy, then try again") 
  }
  
  # Expand so each EQuIS flag is on its own row
  # Pivot wider to create a column for each flag
  results_with_flags <- results_with_flags %>%
    separate_rows(EQuIS_flag, sep = ", ") %>%
    mutate(value = 1) %>%
    select(-Source_Flags) %>%
    pivot_wider(names_from = EQuIS_flag, values_from = value, values_fn = mean)
  
  # Join the flags to the results table
  results <- results %>%
    left_join(results_with_flags,
              by = c("Activity_ID", "Characteristic_Name", "Filtered_Fraction")) %>%
    mutate(across(-1:-ncol(results),
                  function(x) {
                    if_else(is.na(x), "", paste0(cur_column(), 
                                                 ": ", 
                                                 qualifiers %>%
                                                   filter(lookup_code == cur_column()) %>%
                                                   pull(remark),
                                                 "; "))
                  }))
  
  # Create a table with a column for each EQuIS flag, and one row with an empty string in each column
  cols <- ta_flags %>%
    select(EQuIS_flag) %>%
    separate_rows(EQuIS_flag, sep = ", ") %>%
    mutate(value = 0) %>%
    pivot_wider(names_from = EQuIS_flag, values_from = value, values_fn = mean) %>%
    mutate(across(everything(),  ~ ""))
  
  # Create a column for each column in cols that is not yet a column in results
  results <- results %>%
    add_column(!!!cols[!names(cols) %in% names(.)]) %>%
    select(-`J-R`) %>% # remove J-R because we account for this ourselves
    mutate(FBK = if_else(FBK == "", FBK, "FBK: According to lab, analyte detected in lab method blank; "))
  
  return(results)
}