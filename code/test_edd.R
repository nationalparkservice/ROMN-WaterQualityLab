# Function to compare tables from our historical EDDs and the new process in R
same_table <- function(table_old, table_new, table_type) {
  
  # drop deprecated columns from old EDD
  table_old <- table_old %>%
    select(-starts_with("Deprecated"))
  
  # drop columns so that the two tables have the same columns
  table_new <- table_new %>%
    select(colnames(table_old))
  
  # deal with table specific issues
  if (tolower(table_type) == "projects") {
    # seemingly harmless difference where some entries have different line breaks
    table_new <- table_new %>%
      mutate(across(c(Project_Description, Design_Sampling_Summary, Study_Area_Description), 
                    function(x) {str_replace_all(x, "\r\r\n\r\r\n", "\r\n\r\n")}))
    
    table_old <- table_old %>%
          mutate(across(c(Project_Description, Design_Sampling_Summary, Study_Area_Description), 
                        function(x) {str_replace_all(x, "\r\r\n\r\r\n", "\r\n\r\n")}))
  }
  else if (tolower(table_type) == "locations") {
    table_old <- table_old %>% 
      filter(Location_ID %in% table_new$Location_ID) %>%
      arrange(Location_ID)
    
    table_new <- table_new %>%
      arrange(Location_ID)
  }
  else if (tolower(table_type) == "activities") {
    table_new <- table_new %>%
      mutate(Activity_ID = toupper(Activity_ID)) %>%
      mutate(Activity_Comment = str_trim(Activity_Comment)) %>%
      arrange(Activity_ID)
    
    table_old <- table_old %>%  
      mutate(Activity_ID = toupper(Activity_ID)) %>%
      mutate(Activity_Comment = str_trim(Activity_Comment)) %>%
      filter(Activity_ID %in% table_new$Activity_ID) %>%
      arrange(Activity_ID)
  }
  else if (tolower(table_type) == "results") {
    table_new <- table_new %>%
      mutate(Activity_ID = toupper(Activity_ID),
             Characteristic_Name = toupper(Characteristic_Name)) %>%
      arrange(Activity_ID, Characteristic_Name, Filtered_Fraction, Lab_Batch_ID)
    
    table_old <- table_old %>%  
      mutate(Activity_ID = toupper(Activity_ID),
             Characteristic_Name = toupper(Characteristic_Name),
             Source_Flags = as.character(Source_Flags)) %>% # to match data type
      filter(Activity_ID %in% table_new$Activity_ID) %>%
      arrange(Activity_ID, Characteristic_Name, Filtered_Fraction, Lab_Batch_ID)
  }
  else {
    stop("Invalid table_type")
  }
  
  # stop function if table dimensions are still not the same
  if (nrow(table_old) != nrow(table_new)) { 
    stop("Table dimensions disagree")
  }
  else if (ncol(table_old) != ncol(table_new)) {
    stop("Table dimensions disagree")
  }
  
  # create test table of same shape but all NA
  table_test <- table_old
  is.na(table_test) <- TRUE
  
  # set entries equal to true where the two tables disagree
  for(j in 1:ncol(table_old)) {
    # deal with dates
    if (class(pull(table_new, j)) == "Date") {
      table_new[,j] <- ymd(pull(table_new, j))
      table_old[,j] <- ymd(pull(table_old, j))
      for(i in 1:nrow(table_old)) {
        if(is.na(table_old[i,j]) & is.na(table_new[i,j])) {
          table_test[i,j] <- FALSE
        }
        else if ( (table_old[i,j] != table_new[i,j]) || xor(is.na(table_old[i,j]), is.na(table_new[i,j])) ) {
          table_test[i,j] <- TRUE
        }
        else {table_test[i,j] <- FALSE}
      }
    }
    # deal with matching classes
    else if (class(pull(table_old, j)) == class(pull(table_new, j))) {
      for(i in 1:nrow(table_old)) {
        if(is.na(table_old[i,j]) & is.na(table_new[i,j])) {
          table_test[i,j] <- FALSE
        }
        else if ( xor(is.na(table_old[i,j]), is.na(table_new[i,j])) || (toupper(table_old[i,j]) != toupper(table_new[i,j])) ) { # don't care about capitalization differences
          table_test[i,j] <- TRUE
        }
        else {table_test[i,j] <- FALSE}
      }
    }
    else {
      if (is.numeric(pull(table_new, j)) | is.numeric(pull(table_old, j))) {
        table_old[,j] <- as.numeric(pull(table_old, j))
        table_new[,j] <- as.numeric(pull(table_new, j))
      }
      for(i in 1:nrow(table_old)) {
        if(is.na(table_old[i,j]) & is.na(table_new[i,j])) {
          table_test[i,j] <- FALSE
        }
        else if ( (toupper(table_old[i,j]) != toupper(table_new[i,j])) || xor(is.na(table_old[i,j]), is.na(table_new[i,j])) ) {
          table_test[i,j] <- TRUE
        }
        else {table_test[i,j] <- FALSE}
      }
    }
  }
  # return information to user
  if (sum(table_test) == 0) {
    print("Identical tables")
  }
  else {
    col_sums <- colSums(table_test)
    for (i in 1:length(table_test)) {
      if (col_sums[i] > 0) {
        # Print column name and the number of differences
        print(col_sums[i])
        
        # Find and print the differences between the two datasets for a given column
        print(table_old %>%
                select(names(col_sums[i])) %>%
                rename("Historical EDD" = names(col_sums[i])) %>%
                cbind(table_new %>% 
                        select(names(col_sums[i]))) %>%
                rename("New EDD (work in progress)" = names(col_sums[i])) %>%
                mutate(index = row_number()) %>%
                filter(`Historical EDD` != `New EDD (work in progress)` | xor(is.na(`Historical EDD`), is.na(`New EDD (work in progress)`))))
      }
    }
  }
  return (list("test" = table_test, "old" = table_old, "new" = table_new))
}

# function to wrap all of the comparisons for an EDD into one place
compare_edd <- function(edd_old_file_path, edd_new) {
  # Read in old EDD
  projects_old <- read_excel(edd_old_file_path, sheet = "Projects")
  locations_old <- read_excel(edd_old_file_path, sheet = "Locations")
  activities_old <- read_excel(edd_old_file_path, sheet = "Activities")
  results_old <- read_excel(edd_old_file_path, sheet = "Results")
  
  # Test equivalence
  print("----------------------------Projects----------------------------")
  t_projects <- same_table(projects_old, edd_new$Projects, 'projects')
  print("----------------------------Locations----------------------------")
  t_locations <- same_table(locations_old, edd_new$Locations, 'locations')
  print("----------------------------Activities----------------------------")
  t_activities <- same_table(activities_old, edd_new$Activities, 'activities')
  print("----------------------------Results----------------------------")
  t_results <- same_table(results_old, edd_new$Results, 'results')
  
  # Return
  return (list("projects" = t_projects, "locations" = t_locations, 
               "activities" = t_activities, "results" = t_results))
}
