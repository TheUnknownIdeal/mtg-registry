#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
from numpy import nan

import utils_df as ud
import utils_input as ui
 
import sys

import os


def main():

    # GET CONFIG PARAMETERS    
    config_file = sys.argv[1]
    inputs = ui.get_parameters(config_file)

    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = BASE_DIR / inputs["data_folder"]
    

    # LOAD CARD DATABASES
    # Current collection
    vault_file = inputs["vault_file"]
    vault_columns = inputs["vault_columns"]
    csv_config = inputs["csv_config"]
    vault_path = DATA_DIR / vault_file
    vault = ud.load_collection_to_df(vault_path, vault_columns, csv_config)

    # Archived cards
    archive_file = inputs["archive_file"]
    archive_path = DATA_DIR / archive_file
    archive = ud.load_collection_to_df(archive_path, vault_columns, csv_config)

    # History of total collection size and value
    timeline_file = inputs["timeline_file"]
    timeline_columns = inputs["timeline_columns"]
    timeline_path = DATA_DIR / timeline_file

    if os.path.exists(timeline_path):
        timeline = ud.load_collection_to_df(timeline_path, timeline_columns, csv_config)
    else:
        timeline = pd.DataFrame(columns=timeline_columns.keys()).astype(timeline_columns)
    
    
    #ud.register_new_cards(vault, [archive])

    # Updating lists
    print(f"Updating '{vault_file}'...")
    ud.update_collection(vault)

    print(f"Updating '{archive_file}'...")
    ud.update_collection(archive)

    today = pd.Timestamp.now().normalize()

    number_of_cards = len(vault.drop_duplicates(subset=['pid']))
    total_value_usd = round(vault['price trend usd'].sum(), 2)
    total_value_eur = round(vault['price trend eur'].sum(), 2)

    new_entry = {
        "date": pd.Timestamp.now().normalize(), # Today's date (no time)
        "card count": number_of_cards,
        "price usd": total_value_usd,
        "price eur": total_value_eur,
        "price change % usd": 0.0,
        "price change % eur": 0.0,
        "comment": nan  
        }


    # 3. Check if history is empty
    if timeline.empty:
        timeline = pd.DataFrame([new_entry]).astype(timeline_columns)

    else:
        # 4. Get the date of the last entry
        # Ensure it's a datetime object for comparison
        last_date = pd.to_datetime(timeline["date"].iloc[-1])

        
        if last_date.normalize() == today:
            print("Last timeline entry is from today. Updating existing row...")
            # Overwrite the last row
            # We use .index[-1] to make sure we hit the correct position
            for column, value in new_entry.items():
                timeline.loc[timeline.index[-1], column] = value
        else:
            print("Last timeline entry is older. Appending new row...")
            # Append new row
            new_row_df = pd.DataFrame([new_entry])
            timeline = pd.concat([timeline, new_row_df], ignore_index=True)


        # Compute percentage change in total value from previous entry
        if len(timeline) > 1:
            # Get the actual numeric values (No pd.to_datetime here!)
            last_usd = timeline["price usd"].iloc[-2]
            last_eur = timeline["price eur"].iloc[-2]    

            current_usd = timeline["price usd"].iloc[-1]
            current_eur = timeline["price eur"].iloc[-1] 

            # Calculate percentage change
            # We check if last_usd > 0 to avoid "Division by Zero" errors
            if last_usd and last_usd > 0:
                change_usd = round(100 * (current_usd / last_usd - 1), 2)
                timeline.loc[timeline.index[-1], "price change % usd"] = change_usd
                
            if last_eur and last_eur > 0:
                change_eur = round(100 * (current_eur / last_eur - 1), 2)
                timeline.loc[timeline.index[-1], "price change % eur"] = change_eur


    save = True
    if save:
        vault.to_csv(vault_path, index=False, **csv_config)
        archive.to_csv(archive_path, index=False, **csv_config)
        timeline.to_csv(timeline_path, index=False, **csv_config)
        print(f"Vault saved to '{vault_path}'.")
        print(f"Archive saved to '{archive_path}'.")
        print(f"Timeline saved to '{timeline_path}'.")

    return 0



if __name__ == '__main__':
    main()