#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
import numpy as np

import utils_df as ud
import utils_input as ui

from make_event import make_card_sequence
from make_event import activity_cleanup
 
import sys

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

    

    # Get id Series
    event_ids = activity["id"]

    
    # Add column for storing card exit dates to vault if such is in archive.
    outdate_col, outdate_flag = "out date", False
    if outdate_col in archive.columns and outdate_col not in vault.columns:
        # Use None and object dtype
        vault[outdate_col] = pd.Series(dtype=archive[outdate_col].dtype)
        outdate_flag = True


    # First register all new cards
    print("CARD REGISTER:")
    new_pids = ud.register_new_cards(vault, [archive])

    unassigned_inbound_df = None

    # Create a new dataframe with only these pids found in "new_pids"
    if len(new_pids) > 0:

        # Create a new dataframe with only specified PIDs
        unassigned_inbound_df = vault[vault['pid'].isin(new_pids)].copy()

        # Add and "out date" for the sake of viewing when displaying summaries
        if "out date" not in unassigned_inbound_df.columns:
            unassigned_inbound_df["out date"] = pd.Series(dtype=vault_columns["out date"])

    while True:

        # Create new event
        new_id = ui.generate_next_pid(event_ids,"e")

        event_entry = { "id": new_id }

        peek_cols = [
            "name", 
            "set_name", 
            "finish", 
            "language", 
            "condition", 
            "comment",
            "in date",
            "out date"
            ]

        accepted_pids = []
        inbound_df = None
        outbound_df = None
        
        # Check if there are any unassigned cards
        if unassigned_inbound_df is not None:

            # ========== INBOUND ==========

            # Display unassigned new cards
            ud.display_dynamic_df(ud.peek_df(unassigned_inbound_df, columns=peek_cols))
        
            # Get user input of new cards
            prompt = f"INBOUND - Select cards (e.g., 'all', '0-15', or '1 3 5'): "
            usr_input = ui.get_typed_input(prompt, target_type="str", default="--q")

            # "--q" is the exit command
            if usr_input == "--q": break

            # 2. Get the indices using the smart parser
            selected_indices = ui.parse_smart_selection(usr_input, unassigned_inbound_df)

            # 3. Convert indices to actual PIDs
            accepted_pids = unassigned_inbound_df.iloc[selected_indices]['pid'].tolist()
            #print(f"Accepted {len(accepted_pids)} cards.")

            # Make selected PIDs dataframe
            inbound_df = unassigned_inbound_df[unassigned_inbound_df['pid'].isin(accepted_pids)].copy()

            # Create inbound storage string and add it to event
            inbound_string = " ".join(map(str, accepted_pids))


            # ========== COMPUTE EVENT DATE ==========
            # Date computed only from inbound cards

            # Check if "in date" in inbound data
            if "in date" in inbound_df.columns:
                # 1. Filter for the PIDs and grab the 'in date' column
                target_dates = inbound_df['in date']

                # Compute the date of the event based
                if target_dates.empty:
                    prompt = f"Give a date for the event {new_id} (YYYY-MM-DD)"
                    event_date = ui.get_typed_input(prompt, target_type="date", default=date.today())

                else:
                    # 2. Calculate the mode(s)
                    # .mode() returns a Series of all values that tied for 'most frequent'
                    modes = target_dates.mode()

                    # Check if there is any data (nan)
                    if not modes.empty:
                        event_date = modes.iloc[0]
                    else:
                        # If all dates were NaN (common for fresh booster packs)
                        prompt = f"No dates found in card data. Give a date for event {new_id} (YYYY-MM-DD)"
                        event_date = ui.get_typed_input(prompt, target_type="date", default=pd.Timestamp.today().normalize())

            else:
                prompt = f"No candidate dates found. Give a date for the event {new_id} (YYYY-MM-DD)"
                event_date = ui.get_typed_input(prompt, target_type="date", default=pd.Timestamp.today().normalize())

        else:
            print("No unassigned cards available. Skipping inbound...")
            inbound_string = "-"

            prompt = f"Give a date for the event {new_id} (YYYY-MM-DD)"
            event_date = ui.get_typed_input(prompt, target_type="date", default=pd.Timestamp.today().normalize())
     

        event_entry["in"] = inbound_string
        event_entry["date"] = event_date


        # ========== OUTBOUND ==========

        search_prompt = "OUTBOUND: Provide a search term."
        outbound_list, outbound_string, outbound_df = make_card_sequence([vault], vault_columns, search_prompt=search_prompt)
        event_entry["out"] = outbound_string


        # ========== DISPLAY EVENT SUMMARY ==========

        print(f"Event {new_id} - {event_date.strftime('%Y-%m-%d')}:")

        event_parts = []
        event_cols=[
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
        if inbound_df is not None and not inbound_df.empty:
            inbound_df["direction"] = "in"
            inbound_df["in date"] = event_date

            event_parts.append(inbound_df)

        if outbound_df is not None and not outbound_df.empty:
            outbound_df["direction"] = "out"
            outbound_df["out date"] = event_date
            
            event_parts.append(outbound_df)

        # Display complete event df
        if event_parts:

            event_df = pd.concat(event_parts, ignore_index=True)
            # Filter columns only if they exist to prevent errors
            actual_cols = [c for c in event_cols if c in event_df.columns]
            
            title = f"Event {new_id} - {event_date.strftime('%Y-%m-%d')}:"
            ud.display_dynamic_df(ud.peek_df(event_df, columns=actual_cols),
                                    title=title,
            )

        # ========== Comment or Discard ==========

        prompt = f"Give a comment, or input '--q' to discard event {new_id}."
        usr_input = ui.get_typed_input(prompt, target_type="str", default="")

        # Condition for event to be accepted
        if usr_input != "--q":

            # Add comment to event entry
            event_entry["comment"] = usr_input

            # ========== TRANSFER CARDS ==========
            
            # Add exit date to outbound cards
            if outdate_col in archive.columns:
                # Update only the 'out date' column for rows matching your PIDs
                vault.loc[vault['pid'].isin(outbound_list), 'out date'] = event_date
            
            # Move outbound cards to "archive"
            vault, archive = ud.transfer_cards(vault, archive, outbound_list)

            # Also set the "in date" of the new cards
            if accepted_pids:
                vault.loc[vault['pid'].isin(accepted_pids), 'in date'] = event_date

            
            # ========== ADD EVENT TO ACTIVITY ==========

            # Add the new event to "activity"
            new_event_df = pd.DataFrame([event_entry]).astype(activity_columns)

            # Ensure the ID column remains an string to match your 'event_ids' set logic
            new_event_df['id'] = new_event_df['id'].astype(str)

            activity = pd.concat([activity, new_event_df], ignore_index=True)
            
            event_ids = activity["id"] # Refresh the tracker

            # ============ UPDATE UNASSIGNED NEW PIDS ============

            # 1. Update the master list of PIDs remaining
            new_pids = [p for p in new_pids if p not in accepted_pids]

            # Check if there are unassigned pids remaining
            if new_pids:
                # 2. Update the DataFrame by filtering for those remaining PIDs
                # Using .isin() is the safest way to "shrink" the DF while keeping all columns
                unassigned_inbound_df = unassigned_inbound_df[unassigned_inbound_df['pid'].isin(new_pids)].copy()

            # 3. Optional: Break the loop if everything is assigned
            else:
                print("All new cards have been assigned to events.")
                unassigned_inbound_df = None

        print("=====")

        # ========== CONTINUE PROMPT ==========

        prompt = f"Create a new event (y/n)?"
        usr_input = ui.get_typed_input(prompt, target_type="str", default="y")

        if usr_input == "n" or usr_input == "no": break

    
    # Remove the outdate column from vault
    if outdate_flag:
        vault.drop(columns=outdate_col, inplace=True)

    # Save prompt
    prompt = f"Do you want to save databases?"
    save = ui.get_typed_input(prompt, target_type="str", default="no")

    if save.lower().strip() in ["yes", "y"]:
        # Remove ghost events, and sort by date
        activity = activity_cleanup(activity)

        vault.to_csv(vault_path, index=False, **csv_config)
        archive.to_csv(archive_path, index=False, **csv_config)
        activity.to_csv(activity_path, index=False, **csv_config)
        
        print(f"Vault saved to '{vault_path}'.")
        print(f"Archive saved to '{archive_path}'.")
        print(f"Activity saved to '{activity_path}'.")

    return 0

    

if __name__ == '__main__':
    main()