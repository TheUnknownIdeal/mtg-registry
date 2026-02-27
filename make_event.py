#!/usr/bin/env python3

from pathlib import Path
import pandas as pd

import utils_df as ud
import utils_input as ui
 
import sys
import re
import os

def main():

    # GET CONFIG PARAMETERS

    config_file = sys.argv[1]
    config_path = Path(config_file)
    registry_dir = config_path.parent
    inputs = ui.get_parameters(config_file)

    # LOAD CARD DATABASES

    vault_file = inputs["vault_file"]
    vault_columns = inputs["vault_columns"]
    csv_config = inputs["csv_config"]
    vault_path = registry_dir / vault_file
    vault = ud.load_collection_to_df(vault_path, vault_columns, csv_config)
    
    archive_file = inputs["archive_file"]
    archive_path = registry_dir / archive_file
    archive = ud.load_collection_to_df(archive_path, vault_columns, csv_config)

    activity_file = inputs["activity_file"]
    activity_columns = inputs["activity_columns"]
    activity_path = registry_dir / activity_file
    
    if os.path.exists(activity_path):
        activity = ud.load_collection_to_df(activity_path, activity_columns, csv_config)
    else:
        activity = pd.DataFrame(columns=activity_columns.keys()).astype(activity_columns)

    
    # Get all preexisting event ids
    event_ids = activity["id"]


    # BEGIN EVENT REGISTRATION
    while True:

        # Create new event
        new_id = ud.generate_next_pid(event_ids,"e")

        print(f"Beginning event {new_id}...")
        event_entry = { "id": new_id }

        
        # ========== INBOUND ==========

        # Get card sequence
        search_prompt = "INBOUND: Provide a search term."
        inbound_pid_list, inbound_string, inbound_df = make_card_sequence([vault, archive], vault_columns, search_prompt=search_prompt)
        event_entry["in"] = inbound_string

        # Get event date from inbound card data
        event_date = None
        if inbound_df is not None:
            modes = inbound_df["in date"].mode()
            if not modes.empty:
                event_date = modes.iloc[0]

        
        # Get names of inbound cards
        inbound_name_list = [inbound_df.loc[inbound_df['pid'] == pid, 'name'].iloc[0] for pid in inbound_pid_list]

        inbound_name_list = []
        if inbound_df is not None:
            inbound_name_list = [inbound_df.loc[inbound_df['pid'] == pid, 'name'].iloc[0] 
                                for pid in inbound_pid_list if pid in inbound_df['pid'].values]

        # Get a dataframe containing the prior inbound activity of these cards
        # Returns None if there is not prior activity
        inbound_activity = get_prior_activity(activity, "in", inbound_pid_list, inbound_name_list)
 

        # ========== OUTBOUND ==========
        

        search_prompt = "OUTBOUND: Provide a search term."
        outbound_pid_list, outbound_string, outbound_df = make_card_sequence([archive], vault_columns, search_prompt=search_prompt)
        event_entry["out"] = outbound_string

        # Get event date from outbound card data
        if outbound_df is not None:
            modes = outbound_df["out date"].mode()
            if not modes.empty:
                event_date = modes.iloc[0]
                

        outbound_name_list = []
        if outbound_df is not None:
            outbound_name_list = [outbound_df.loc[outbound_df['pid'] == pid, 'name'].iloc[0] 
                                for pid in outbound_pid_list if pid in outbound_df['pid'].values]

        # Get a dataframe containing the prior inbound activity of these cards
        outbound_activity = get_prior_activity(activity, "out", outbound_pid_list, outbound_name_list)


        # DO NOT REGISTER IF BOTH INBOUND AND OUTBOUND ARE EMPTY
        if inbound_string == "" and outbound_string == "":
            print("Both inbound and outbound are empty. Discarding event.")

        else:
            # ========== COMPUTE EVENT DATE ==========
            if event_date is None:
                prompt = f"Give a date for the event {new_id} (YYYY-MM-DD)"
                event_date = ui.get_typed_input(prompt, target_type="date", default=pd.Timestamp.today().normalize())

            event_entry["date"] = event_date


            # ========== DISPLAY EVENT SUMMARY ==========

            

            event_parts = []
            peek_cols=[
                "direction",
                "name", 
                "finish", 
                "language", 
                "condition", 
                "comment",
                "in date",
                "out date"
                ]            

            # Create complete event df from inbound and outbound dfs
            if inbound_df is not None:
                inbound_df["direction"] = "in"
                event_parts.append(inbound_df)

            if outbound_df is not None:
                outbound_df["direction"] = "out"
                event_parts.append(outbound_df)

            # Display complete event df
            if event_parts:
                event_df = pd.concat(event_parts, ignore_index=True)
                actual_cols = [c for c in peek_cols if c in event_df.columns]
                
                title = f"Event {new_id} - {event_date.strftime('%Y-%m-%d')}:"
                ud.display_dynamic_df(ud.peek_df(event_df, columns=actual_cols),
                                        title=title,
                )


            # ========== DISPLAY PRIOR EVENTS ==========

            print("These prior events will be modified:")

            prior_events_parts = []
            peek_cols=[
                "subject_name", 
                "direction", 
                "id",
                "date", 
                "comment"
                ]
            
            if inbound_activity is not None:
                inbound_activity["direction"] = "in"
                prior_events_parts.append(inbound_activity)

            if outbound_activity is not None:
                outbound_activity["direction"] = "out"
                prior_events_parts.append(outbound_activity)

            # Display prior events
            if event_parts:

                prior_df = pd.concat(prior_events_parts, ignore_index=True)
                # Filter columns only if they exist to prevent errors
                actual_cols = [c for c in peek_cols if c in prior_df.columns]
                
                title = f"Prior events containing cards of event {new_id}."
                ud.display_dynamic_df(ud.peek_df(prior_df, columns=actual_cols),
                                        title=title,
                )

            # ========== Comment or Discard ==========

            prompt = f"Give a comment, or input '--q' to discard event {new_id}."
            usr_input = ui.get_typed_input(prompt, target_type="str", default="")

            # Condition for event to be accepted
            if usr_input != "--q":

                # === Deleting Cards from prior events ===

                # Delete event cards from prior inbound events
                for pid in inbound_pid_list:
                    remove_pid_from_events(activity, "in", pid)

                # Delete event cards from prior outbound events
                for pid in outbound_pid_list:
                    remove_pid_from_events(activity, "out", pid)

                # ========================================

                # Log new entry
                event_entry["comment"] = usr_input
                # Add the new event to activity database
                new_event_df = pd.DataFrame([event_entry]).astype(activity_columns)

                new_event_df['id'] = new_event_df['id'].astype(str)
                
                activity = pd.concat([activity, new_event_df], ignore_index=True)

                # UPDATE THE SERIES HERE
                # We re-assign event_ids to the updated 'id' column of your activity DF
                event_ids = activity["id"]

        print("=====")

        # ========== CONTINUE PROMPT ==========

        prompt = f"Create a new event (y/n)?"
        usr_input = ui.get_typed_input(prompt, target_type="str", default="y")

        if usr_input == "n" or usr_input == "no": break

    prompt = f"Do you want to save activity?"
    save = ui.get_typed_input(prompt, target_type="str", default="no")
    if save.lower().strip() in ["yes", "y"]:
        # Remove ghost events, and sort by date
        activity = activity_cleanup(activity)

        activity.to_csv(activity_path, index=False, **csv_config)
        print(f"Activity saved to '{activity_path}'.")



# Generates pid list and string from user queries
def make_card_sequence(dfs, df_col_types, cols=None, search_prompt=None):
    # 'dfs' needs to be a list of DataFrames
    # Each dataframe in 'dfs' must contain columns that are specified in 'config.json'
    # 'df_col_types' is a dictionary containing column names as keys, and the corresponding dtypes as values
    default_cols = [
        "name", 
        "set_name", 
        "finish", 
        "language", 
        "condition", 
        "comment",
        "in date",
        "out date"
        ]
    if cols is None: cols = default_cols
    
    pid_list = []
    pid_string = ""
    selected_rows = []

    query_prompt = "Provide a search term."
    if search_prompt is not None:
        query_prompt = search_prompt
    
    # Loop until user commands protocol to stop querying for cards
    while True:
        prompt =  query_prompt + " Input nothing or '--q' to move on."
        query = ui.get_typed_input(query_prompt, target_type="str", default="--q")

        # Break outbound card search with "--q" 
        if query == "--q": break

        # Get query results for outbound card
        hits_list = []

        for df in dfs:
            hits = ud.str_search_col(df, query)
            if hits.empty: continue
            hits_list.append(hits)

        if len(hits_list) == 0:
            print(f"No hits.")
            continue

        all_hits = pd.concat(hits_list, ignore_index=True, sort=False)

        # If a display column is missing from hits, add it
        for col in cols:
            if col not in all_hits.columns:
                all_hits[col] = pd.Series(dtype=df_col_types[col])

        view_hits = ud.peek_df(all_hits, columns=cols)

        if len(view_hits) == 1:
            index=0
        else:
            print("Query hits:")
            ud.display_dynamic_df(view_hits)

            prompt = f"Select index."
            index = ui.get_typed_input(prompt, target_type="int")

            if index is None:
                print("Faulty input.")
                continue

        # Make sure the index is within bounds of dataframe
        if 0 <= index < len(all_hits):
            # Get pid from index given by user
            target_pid = all_hits.iloc[index]['pid']

            # Prevent duplicates
            if target_pid not in pid_list: 
                pid_list.append(target_pid)

                selected_row = all_hits.iloc[[index]].copy()
                selected_rows.append(selected_row)
            else:
                print("Card has already been selected.")

        else:
            print("Failed to select card.")
            
    pid_string = " ".join(map(str, pid_list))

    # ... and at the very end of search_cards:
    return pid_list, pid_string, pd.concat(selected_rows) if selected_rows else None




def get_prior_activity(event_df, col, pid_list, name_list):

    # 1. Search and tag each result with the PID that found it
    prior_activity_list = []
    for i, pid in enumerate(pid_list):
        search_result = ud.str_search_col(event_df, pid, col)
        
        if search_result is not None and not search_result.empty:
            # Create a copy so we don't modify the original 'event_df' df
            search_result = search_result.copy()
            
            # Add the 'subject_pid' column
            search_result['subject_name'] = name_list[i]
            prior_activity_list.append(search_result)

    
    # Check if there are any prior events
    if len(prior_activity_list) > 0:

        # Dataframe that stores prior inbound activity of specified cards
        return pd.concat(prior_activity_list, ignore_index=True)
    else:
        return None

def remove_pid_from_events(event_df, col, target_pid):
    pattern = rf'\b{target_pid}\b'
    
    # Remove the PID
    event_df[col] = event_df[col].str.replace(pattern, '', regex=True)
    
    # The "Magic" line: cleans all double spaces, leading, and trailing spaces at once
    # and handles your "-" placeholder logic.
    event_df[col] = event_df[col].apply(lambda x: ' '.join(x.split()) if isinstance(x, str) and x.strip() else "-")


# Function that removes "ghost" events (events with both empty inbound and empty outbound).
# It also arranges the events in chronological order
def activity_cleanup(df, verbose=False):

    if "in" not in df.columns or "out" not in df.columns:
        print(f"Activity doesn't contain 'in' and 'out' columns. Skipping...")
        return df

    # 1. REMOVE EMPTY EVENTS
    # We look for rows where both 'in' and 'out' are the placeholder '-'
    empty_mask = (df['in'] == "-") & (df['out'] == "-")
    removed_count = empty_mask.sum()

    if removed_count > 0:
        df = df[~empty_mask].copy()
        if verbose:
            print(f"Cleanup: Removed {removed_count} empty events with no card data.")

    # 2. SORT BY DATE
    # Ensure 'date' is a datetime objects for proper chronological sorting
    if "date" in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
        if verbose:
            print("Activity log sorted by date.")

    return df




if __name__ == '__main__':
    main()