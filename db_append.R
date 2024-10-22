# Print a message to indicate the script has started
print("Script is starting...")



# Set current date variables
current_date <- Sys.Date()  # Today's date
week_prior <- current_date - 3  # Date three days prior
week_prior_pairing_date <- current_date - 7  # Date seven days prior

# Ensure the 'librarian' package is installed and loaded
if (!require(librarian)) {
  install.packages("librarian")  # Install if not available
  library(librarian)  # Load the package
}

# Use librarian to manage and load packages
librarian::shelf(tidyverse, here, DBI, odbc)  # Load required packages

source(here("funcs.R"))

### Database Connection: Connect to `ENTERPRISE` database using Snowflake
tryCatch({
  db_connection <- DBI::dbConnect(odbc::odbc(),
                                  Driver = "SnowflakeDSIIDriver",
                                  Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                  WAREHOUSE = "DATA_LAKE_READER",
                                  Database = "ENTERPRISE",
                                  UID = "jacob.eisaguirre@hawaiianair.com",  # Replace Sys.getenv("UID") with your email
                                  authenticator = "externalbrowser")
  print("Database Connected!")  # Success message
}, error = function(cond) {
  print("Unable to connect to Database.")  # Error handling
})

# Set the schema for the session
dbExecute(db_connection, "USE SCHEMA CREW_ANALYTICS")

# Query flight history data for the last 3 days
q_flighthistory <- paste0("SELECT * 
                          FROM CT_FLIGHT_HISTORY 
                          WHERE FLIGHT_DATE BETWEEN '", week_prior, "' AND '", current_date, "';")

# Fetch flight history data
view_flighthistory <- dbGetQuery(db_connection, q_flighthistory)

# Clean the flight history data to keep unique records
clean_flighthistory <- view_flighthistory %>%
  filter(SEGMENT_STATUS == "A") %>%  # Filter for active segments
  rename(SCHED_DEPARTURE_TIME = SCHED_DEPARTURE_TIME_RAW) %>%
  mutate(updated_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
  group_by(FLIGHT_NO, FLIGHT_DATE, DEPARTING_CITY, ARRIVAL_CITY, SCHED_DEPARTURE_TIME) %>%
  filter(updated_dt == max(updated_dt)) %>%  # Keep the most recent update
  mutate(temp_id = cur_group_id()) %>%
  filter(!duplicated(temp_id))  # Remove duplicate records

# Free memory by removing the unneeded object and running garbage collection
rm(view_flighthistory)
gc()

# Query flight leg data for the last 7 days
q_flightleg <- paste0("SELECT * 
                      FROM CT_FLIGHT_LEG 
                      WHERE PAIRING_DATE BETWEEN '", week_prior_pairing_date, "' AND '", current_date, "';")

# Fetch flight leg data
view_flightleg <- dbGetQuery(db_connection, q_flightleg)

# Clean flight leg data and identify crew assignments (DEADHEAD coded as "C")
flight_leg_join <- view_flightleg %>%
  mutate(updated_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " "),
         shed_dept_hour = hour(SCHED_DEPARTURE_TIME),
         DEADHEAD = if_else(is.na(DEADHEAD), "C", DEADHEAD)) %>%
  relocate(updated_dt, .after = PAIRING_NO) %>%
  group_by(PAIRING_POSITION, DEPARTING_CITY, ARRIVAL_CITY, SCHED_DEPARTURE_DATE, shed_dept_hour, PAIRING_NO, PAIRING_DATE) %>%
  filter(DEADHEAD == "C", updated_dt == max(updated_dt)) %>%
  mutate(temp_id = cur_group_id(), .after = CREW_INDICATOR) %>%
  filter(!duplicated(temp_id))  # Remove duplicate records

# Clean up the memory again
rm(view_flightleg)
gc()

# Join cleaned flight history and flight leg data
correct_fh <- clean_flighthistory %>%
  inner_join(flight_leg_join, by = join_by(FLIGHT_NO, DEPARTING_CITY, ARRIVAL_CITY, SCHED_DEPARTURE_DATE, SCHED_ARRIVAL_DATE),
             relationship = "many-to-many")

# Query master pairing data for the past week
q_masterpairing <- paste0("SELECT * 
                          FROM CT_MASTER_PAIRING 
                          WHERE PAIRING_DATE BETWEEN '", week_prior_pairing_date, "' AND '", current_date, "';")

# Fetch master pairing data
view_masterpairing <- dbGetQuery(db_connection, q_masterpairing)

# Clean the master pairing data to retain the most recent updates
join_masterpairing <- view_masterpairing %>%
  mutate(updated_dt = paste(UPDATE_DATE, UPDATE_TIME, sep = " ")) %>%
  relocate(updated_dt, .after = PAIRING_NO) %>%
  group_by(CREW_ID, PAIRING_DATE) %>%
  filter(PAIRING_STATUS == "A", updated_dt == max(updated_dt))  # Only active pairings

# Clean up the memory
rm(view_masterpairing)
gc()

# Merge flight history with master pairing data, filter out specific CREW_IDs
int_prob_final_pairing <- correct_fh %>%
  inner_join(join_masterpairing, by = join_by(PAIRING_NO, PAIRING_DATE, PAIRING_POSITION), relationship = "many-to-many") %>%
  relocate(c(PAIRING_NO:CREW_ID), .before = FLIGHT_NO) %>%
  filter(!CREW_ID %in% c("6", "8", "10", "11", "35", "21", "7", "18", "1", "2", "3", "4", "5", "9", "12", "13", "14", "15", "17", "19", "20", "25", "31", "32", "33", "34", "36", "37")) %>%
  group_by(PAIRING_POSITION, PAIRING_NO, DEPARTING_CITY, ARRIVAL_CITY, SCHED_DEPARTURE_DATE, SCHED_DEPARTURE_TIME.x, PAIRING_DATE, CREW_ID) %>%
  mutate(temp_id = cur_group_id()) %>%
  filter(!duplicated(temp_id))

# Query master schedule data and clean it
q_masterschedule <- "SELECT * FROM CT_MASTER_SCHEDULE WHERE BID_DATE BETWEEN '2018-12' AND '2024-09';"
view_masterschedule <- dbGetQuery(db_connection, q_masterschedule)

clean_base <- view_masterschedule %>%
  select(CREW_ID, BID_DATE, BASE) %>%
  distinct() %>% 
  filter(!CREW_ID %in% c("6", "8", "10", "11", "35", "21", "7", "18", "1", "2", "3", "4", "5", "9", "12", "13", "14", "15", "17", "19", "20", "25", "31", "32", "33", "34", "36", "37"))

  
Cols_AllMissing <- function(final_pairing){ # helper function
  as.vector(which(colSums(is.na(final_pairing)) == nrow(final_pairing)))
}


# Finalize the pairing by joining with clean base, removing unnecessary columns
  final_pairing <- int_prob_final_pairing %>%
  mutate(filter_group = case_when(EQUIPMENT == "717" & CREW_INDICATOR == "P" ~ "int_717_p",
                                  EQUIPMENT == "717" & CREW_INDICATOR == "FA" ~ "fa_717",
                                  TRUE ~ "all_other_craft")) %>%
  group_by(PAIRING_POSITION, PAIRING_NO, DEPARTING_CITY, 
           ARRIVAL_CITY, SCHED_DEPARTURE_DATE, 
           SCHED_DEPARTURE_TIME.x, PAIRING_DATE, # add to sched departure time .x
           filter_group) %>%
  filter(
    (filter_group == "int_717_p" & updated_dt == max(updated_dt)) |
      (filter_group != "int_717_p")) %>%
  ungroup() %>%
  select(!c(filter_group, temp_id)) %>% 
  group_by(PAIRING_NO, PAIRING_DATE, PAIRING_POSITION, CREW_ID, FLIGHT_NO, FLIGHT_DATE.x, DEPARTING_CITY, # add to flight data .x
           ARRIVAL_CITY, SCHED_DEPARTURE_DATE) %>% 
  mutate(temp_id = cur_group_id()) %>% 
  filter(!duplicated(temp_id)) %>% 
  select(!c(temp_id, updated_dt)) %>% 
  select(!c(CREW_INDICATOR.x, updated_dt.y, updated_dt.x, SCHED_DEPARTURE_TIME.y,
            SCHED_DEPARTURE_GMT_VAR.y, SCHED_ARRIVAL_TIME.y, SCHED_ARRIVAL_GMT_VAR.y, UPDATED_BY.y, UPDATED_BY.x,
            UPDATE_DATE.y, UPDATE_DATE.x, UPDATE_TIME.y, UPDATE_TIME.x, temp_id.x, temp_id.y, UNIQUE_ID,
            DEADHEAD, FLIGHT_DATE.x, UPDATED_BY, UPDATE_DATE, UPDATE_TIME, CREW_INDICATOR)) %>% 
  ungroup() %>% 
  select(!FLIGHT_DATE.x) %>% 
  rename(SCHED_DEPARTURE_TIME=SCHED_DEPARTURE_TIME.x,
         SCHED_DEPARTURE_GMT_VAR=SCHED_DEPARTURE_GMT_VAR.x,
         SCHED_ARRIVAL_TIME = SCHED_ARRIVAL_TIME.x,
         SCHED_ARRIVAL_GMT_VAR = SCHED_ARRIVAL_GMT_VAR.x,
         FLIGHT_DATE=FLIGHT_DATE.y,
         CREW_INDICATOR=CREW_INDICATOR.y) %>% 
  relocate(c(CREW_ID, CREW_INDICATOR), .after = PAIRING_DATE) %>% 
  relocate(c(PAIRING_POSITION, FLIGHT_NO, FLIGHT_DATE, DEPARTING_CITY, ARRIVAL_CITY, SCHED_DEPARTURE_DATE, SCHED_DEPARTURE_TIME,
             SCHED_ARRIVAL_DATE, SCHED_ARRIVAL_TIME, EQUIPMENT, EQUIPMENT_CODE, FIN_NO), .after=PAIRING_DATE)%>%
    select(-Cols_AllMissing(.))




# Connect to the `PLAYGROUND` database and append data if necessary
tryCatch({
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),
                                     Driver = "SnowflakeDSIIDriver",
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                     WAREHOUSE = "DATA_LAKE_READER",
                                     Database = "PLAYGROUND",
                                     UID = "jacob.eisaguirre@hawaiianair.com",
                                     authenticator = "externalbrowser")
  print("Database Connected!")
}, error = function(cond) {
  print("Unable to connect to Database.")
})

# Set schema and retrieve data from `AA_FINAL_PAIRING` table
dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS")
present_fp <- dbGetQuery(db_connection_pg, "SELECT * FROM AA_FINAL_PAIRING")

# Find matching columns between the present and final pairings
matching_cols <- dplyr::intersect(colnames(present_fp), colnames(final_pairing))

# Filter both datasets to have matching columns and append new records
match_present_fo <- present_fp %>%
  select(matching_cols)

final_append_match_cols <- final_pairing %>%
  select(matching_cols)

final_append <- anti_join(final_append_match_cols, match_present_fo, by = c("PAIRING_NO", "PAIRING_DATE", "PAIRING_POSITION", "FLIGHT_NO", "FLIGHT_DATE", "DEPARTING_CITY", "ARRIVAL_CITY", "SCHED_DEPARTURE_DATE", "SCHED_ARRIVAL_DATE", "CREW_ID", "CREW_INDICATOR"))

# Append new records to the `AA_FINAL_PAIRING` table
dbAppendTable(db_connection_pg, "AA_FINAL_PAIRING", final_append)

# Print the number of rows added and a success message
print(paste(nrow(final_append), "rows added"))
Sys.sleep(5)  # Pause for 10 seconds
print("Script finished successfully!")
Sys.sleep(10)  # Pause for 10 seconds

# End of script
# 
