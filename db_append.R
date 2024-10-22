# Print a message to indicate the script has started
print("Script is starting...")

# Set current date variables
current_date <- Sys.Date()  # Get today's date
week_prior <- current_date - 3  # Calculate the date three days prior
week_prior_pairing_date <- current_date - 7  # Calculate the date seven days prior

# Ensure the 'librarian' package is installed and loaded
if (!require(librarian)) {
  install.packages("librarian")  # Install the 'librarian' package if it is not already installed
  library(librarian)  # Load the 'librarian' package
}

# Use librarian to manage and load packages
librarian::shelf(tidyverse, here, DBI, odbc)  # Load required packages for data manipulation and database connectivity


### Database Connection: Connect to `ENTERPRISE` database using Snowflake
tryCatch({
  db_connection <- DBI::dbConnect(odbc::odbc(),  # Establish a database connection using ODBC
                                  Driver = "SnowflakeDSIIDriver",  # Specify the Snowflake ODBC driver
                                  Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",  # Server address
                                  WAREHOUSE = "DATA_LAKE_READER",  # Specify the Snowflake warehouse
                                  Database = "ENTERPRISE",  # Specify the database name
                                  UID = "jacob.eisaguirre@hawaiianair.com",  # User ID for authentication (replace with your email)
                                  authenticator = "externalbrowser")  # Use external browser for authentication
  print("Database Connected!")  # Print success message if connection is established
}, error = function(cond) {
  print("Unable to connect to Database.")  # Print error message if connection fails
})

# Set the schema for the session
dbExecute(db_connection, "USE SCHEMA CREW_ANALYTICS")  # Set the database schema to 'CREW_ANALYTICS'

# Query flight history data for the last 3 days
q_flighthistory <- paste0("SELECT * 
                          FROM CT_FLIGHT_HISTORY 
                          WHERE FLIGHT_DATE BETWEEN '", week_prior, "' AND '", current_date, "';")  # SQL query to fetch flight history

# Fetch flight history data
view_flighthistory <- dbGetQuery(db_connection, q_flighthistory)  # Execute the query and store the results

# Clean the flight history data to keep unique records
clean_flighthistory <- view_flighthistory %>%
  filter(SEGMENT_STATUS == "A") %>%  # Filter for active segments
  rename(SCHED_DEPARTURE_TIME = SCHED_DEPARTURE_TIME_RAW) %>%  # Rename raw departure time for clarity
  mutate(updated_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  # Combine date and time into a single datetime column
  group_by(FLIGHT_NO, FLIGHT_DATE, DEPARTING_CITY, ARRIVAL_CITY, SCHED_DEPARTURE_TIME) %>%  # Group by relevant fields
  filter(updated_dt == max(updated_dt)) %>%  # Keep only the most recent update for each group
  mutate(temp_id = cur_group_id()) %>%  # Create a temporary ID for grouping
  filter(!duplicated(temp_id))  # Remove duplicate records based on temporary ID

# Free memory by removing the unneeded object and running garbage collection
rm(view_flighthistory)  # Remove the original fetched data to free up memory
gc()  # Run garbage collection to clear memory

# Query flight leg data for the last 7 days
q_flightleg <- paste0("SELECT * 
                      FROM CT_FLIGHT_LEG 
                      WHERE PAIRING_DATE BETWEEN '", week_prior_pairing_date, "' AND '", current_date, "';")  # SQL query to fetch flight leg data

# Fetch flight leg data
view_flightleg <- dbGetQuery(db_connection, q_flightleg)  # Execute the query and store the results

# Clean flight leg data and identify crew assignments (DEADHEAD coded as "C")
flight_leg_join <- view_flightleg %>%
  mutate(updated_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " "),  # Combine update date and time
         shed_dept_hour = hour(SCHED_DEPARTURE_TIME),  # Extract hour from scheduled departure time
         DEADHEAD = if_else(is.na(DEADHEAD), "C", DEADHEAD)) %>%  # Replace NA DEADHEAD values with "C"
  relocate(updated_dt, .after = PAIRING_NO) %>%  # Move updated_dt column next to PAIRING_NO
  group_by(PAIRING_POSITION, DEPARTING_CITY, ARRIVAL_CITY, SCHED_DEPARTURE_DATE, shed_dept_hour, PAIRING_NO, PAIRING_DATE) %>%  # Group by relevant fields
  filter(DEADHEAD == "C", updated_dt == max(updated_dt)) %>%  # Filter for DEADHEAD entries with the most recent update
  mutate(temp_id = cur_group_id(), .after = CREW_INDICATOR) %>%  # Create a temporary ID for grouping
  filter(!duplicated(temp_id))  # Remove duplicate records based on temporary ID

# Clean up the memory again
rm(view_flightleg)  # Remove the original fetched data to free up memory
gc()  # Run garbage collection to clear memory

# Join cleaned flight history and flight leg data
correct_fh <- clean_flighthistory %>%
  inner_join(flight_leg_join, by = join_by(FLIGHT_NO, DEPARTING_CITY, ARRIVAL_CITY, SCHED_DEPARTURE_DATE, SCHED_ARRIVAL_DATE),  # Perform inner join on specified columns
             relationship = "many-to-many")  # Specify the relationship type for the join

# Query master pairing data for the past week
q_masterpairing <- paste0("SELECT * 
                          FROM CT_MASTER_PAIRING 
                          WHERE PAIRING_DATE BETWEEN '", week_prior_pairing_date, "' AND '", current_date, "';")  # SQL query to fetch master pairing data

# Fetch master pairing data
view_masterpairing <- dbGetQuery(db_connection, q_masterpairing)  # Execute the query and store the results

# Clean the master pairing data to retain the most recent updates
join_masterpairing <- view_masterpairing %>%
  mutate(updated_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%  # Combine update date and time
  relocate(updated_dt, .after = PAIRING_NO) %>%  # Move updated_dt column next to PAIRING_NO
  group_by(CREW_ID, PAIRING_DATE) %>%  # Group by crew ID and pairing date
  filter(PAIRING_STATUS == "A", updated_dt == max(updated_dt))  # Keep only active pairings with the most recent update

# Clean up the memory
rm(view_masterpairing)  # Remove the original fetched data to free up memory
gc()  # Run garbage collection to clear memory

# Merge flight history with master pairing data, filter out specific CREW_IDs
int_prob_final_pairing <- correct_fh %>%
  inner_join(join_masterpairing, by = join_by(PAIRING_NO, PAIRING_DATE, PAIRING_POSITION), relationship = "many-to-many") %>%  # Perform inner join with master pairing data
  relocate(c(PAIRING_NO:CREW_ID), .before = FLIGHT_NO) %>%  # Move specified columns before FLIGHT_NO
  filter(!CREW_ID %in% c("6", "8", "10", "11", "35", "21", "7", "18", "1", "2", "3", "4", "5", "9", "12", "13", "14", "15", "17", "19", "20", "25", "31", "32", "33", "34", "36", "37")) %>%  # Exclude specific crew IDs
  group_by(PAIRING_POSITION, PAIRING_NO, DEPARTING_CITY, ARRIVAL_CITY, SCHED_DEPARTURE_DATE, SCHED_DEPARTURE_TIME.x, PAIRING_DATE, CREW_ID) %>%  # Group by relevant fields
  mutate(temp_id = cur_group_id()) %>%  # Create a temporary ID for grouping
  filter(!duplicated(temp_id))  # Remove duplicate records based on temporary ID

# Query master schedule data and clean it
q_masterschedule <- "SELECT * FROM CT_MASTER_SCHEDULE WHERE BID_DATE BETWEEN '2018-12' AND '2024-09';"  # SQL query to fetch master schedule data
view_masterschedule <- dbGetQuery(db_connection, q_masterschedule)  # Execute the query and store the results

clean_base <- view_masterschedule %>%
  select(CREW_ID, BID_DATE, BASE) %>%  # Select relevant columns
  distinct() %>%  # Remove duplicate rows
  filter(!CREW_ID %in% c("6", "8", "10", "11", "35", "21", "7", "18", "1", "2", "3", "4", "5", "9", "12", "13", "14", "15", "17", "19", "20", "25", "31", "32", "33", "34", "36", "37"))  # Exclude specific crew IDs

# Define a helper function to identify columns with all missing values
Cols_AllMissing <- function(final_pairing) {  # Function to find columns with all NA values
  as.vector(which(colSums(is.na(final_pairing)) == nrow(final_pairing)))  # Return column indices where all values are NA
}

# Finalize the pairing by joining with clean base, removing unnecessary columns
final_pairing <- int_prob_final_pairing %>%
  mutate(filter_group = case_when(EQUIPMENT == "717" & CREW_INDICATOR == "P" ~ "int_717_p",  # Create filter group based on conditions
                                  EQUIPMENT == "717" & CREW_INDICATOR == "FA" ~ "fa_717",
                                  TRUE ~ "all_other_craft")) %>%
  group_by(PAIRING_POSITION, PAIRING_NO, DEPARTING_CITY,  # Group by relevant fields
           ARRIVAL_CITY, SCHED_DEPARTURE_DATE, 
           SCHED_DEPARTURE_TIME.x, PAIRING_DATE,  # Add to scheduled departure time .x
           filter_group) %>%
  filter(
    (filter_group == "int_717_p" & updated_dt == max(updated_dt)) |  # Filter conditions based on filter group
      (filter_group != "int_717_p")) %>%
  ungroup() %>%
  select(!c(filter_group, temp_id)) %>%  # Remove filter group and temporary ID
  group_by(PAIRING_NO, PAIRING_DATE, PAIRING_POSITION, CREW_ID, FLIGHT_NO, FLIGHT_DATE.x, DEPARTING_CITY,  # Group by relevant fields
           ARRIVAL_CITY, SCHED_DEPARTURE_DATE) %>% 
  mutate(temp_id = cur_group_id()) %>%  # Create a temporary ID for grouping
  filter(!duplicated(temp_id)) %>%  # Remove duplicate records based on temporary ID
  select(!c(temp_id, updated_dt)) %>%  # Remove temporary ID and update date
  select(!c(CREW_INDICATOR.x, updated_dt.y, updated_dt.x, SCHED_DEPARTURE_TIME.y,  # Remove unnecessary columns
            SCHED_DEPARTURE_GMT_VAR.y, SCHED_ARRIVAL_TIME.y, SCHED_ARRIVAL_GMT_VAR.y, UPDATED_BY.y, UPDATED_BY.x,
            UPDATE_DATE.y, UPDATE_DATE.x, UPDATE_TIME.y, UPDATE_TIME.x, temp_id.x, temp_id.y, UNIQUE_ID,
            DEADHEAD, FLIGHT_DATE.x, UPDATED_BY, UPDATE_DATE, UPDATE_TIME, CREW_INDICATOR)) %>% 
  ungroup() %>% 
  select(!FLIGHT_DATE.x) %>%  # Remove FLIGHT_DATE.x
  rename(SCHED_DEPARTURE_TIME=SCHED_DEPARTURE_TIME.x,  # Rename columns for clarity
         SCHED_DEPARTURE_GMT_VAR=SCHED_DEPARTURE_GMT_VAR.x,
         SCHED_ARRIVAL_TIME = SCHED_ARRIVAL_TIME.x,
         SCHED_ARRIVAL_GMT_VAR = SCHED_ARRIVAL_GMT_VAR.x,
         FLIGHT_DATE=FLIGHT_DATE.y,
         CREW_INDICATOR=CREW_INDICATOR.y) %>% 
  relocate(c(CREW_ID, CREW_INDICATOR), .after = PAIRING_DATE) %>%  # Move CREW_ID and CREW_INDICATOR columns next to PAIRING_DATE
  relocate(c(PAIRING_POSITION, FLIGHT_NO, FLIGHT_DATE, DEPARTING_CITY, ARRIVAL_CITY, SCHED_DEPARTURE_DATE, SCHED_DEPARTURE_TIME,
             SCHED_ARRIVAL_DATE, SCHED_ARRIVAL_TIME, EQUIPMENT, EQUIPMENT_CODE, FIN_NO), .after=PAIRING_DATE) %>%  # Move additional columns for clarity
  select(-Cols_AllMissing(.))  # Remove columns with all missing values

# Connect to the `PLAYGROUND` database and append data if necessary
tryCatch({
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),  # Establish a database connection using ODBC for the playground database
                                     Driver = "SnowflakeDSIIDriver",  # Specify the Snowflake ODBC driver
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",  # Server address
                                     WAREHOUSE = "DATA_LAKE_READER",  # Specify the Snowflake warehouse
                                     Database = "PLAYGROUND",  # Specify the database name
                                     UID = "jacob.eisaguirre@hawaiianair.com",  # User ID for authentication
                                     authenticator = "externalbrowser")  # Use external browser for authentication
  print("Database Connected!")  # Print success message if connection is established
}, error = function(cond) {
  print("Unable to connect to Database.")  # Print error message if connection fails
})

# Set schema and retrieve data from `AA_FINAL_PAIRING` table
dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS")  # Set the database schema to 'CREW_ANALYTICS'
present_fp <- dbGetQuery(db_connection_pg, "SELECT * FROM AA_FINAL_PAIRING")  # Fetch current final pairing data

# Find matching columns between the present and final pairings
matching_cols <- dplyr::intersect(colnames(present_fp), colnames(final_pairing))  # Identify common columns in both datasets

# Filter both datasets to have matching columns and append new records
match_present_fo <- present_fp %>%
  select(matching_cols)  # Select only the matching columns from present pairing data

final_append_match_cols <- final_pairing %>%
  select(matching_cols)  # Select only the matching columns from final pairing data

final_append <- anti_join(final_append_match_cols, match_present_fo, by = c("PAIRING_NO", "PAIRING_DATE", "PAIRING_POSITION", "FLIGHT_NO", "FLIGHT_DATE", "DEPARTING_CITY", "ARRIVAL_CITY", "SCHED_DEPARTURE_DATE", "SCHED_ARRIVAL_DATE", "CREW_ID", "CREW_INDICATOR"))  # Identify new records in final pairing data not present in current pairing data

# Append new records to the `AA_FINAL_PAIRING` table
dbAppendTable(db_connection_pg, "AA_FINAL_PAIRING", final_append)  # Append new records to the table in the database

# Print the number of rows added and a success message
print(paste(nrow(final_append), "rows added"))  # Print the number of rows added
Sys.sleep(5)  # Pause for 5 seconds
print("Script finished successfully!")  # Print success message
Sys.sleep(10)  # Pause for 10 seconds

# End of script
