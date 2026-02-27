#!/usr/bin/env python3

import time

import pandas as pd
import numpy as np

import utils_input as ui

from exchange_rates_module import get_eur_usd_rate
import scryfall_module as scryfall

def load_collection_to_df(file_path, header_type_dict, config=None):

    # Read only the header row to get the headers
    current_sep = config["sep"] if config else ","
    headers = pd.read_csv(file_path, nrows=0, sep=current_sep).columns
    
    # Create a mapping of header and dtype, using pre-exiting header-dtype dictionary
    header_dtypes = {k: v for k, v in header_type_dict.items() if k in headers}
    
    # define df dtypes, exclude datetimes
    df_dtypes = {k: v for k, v in header_dtypes.items() if v != "datetime64[ns]"}
    
    # separate datetime headers here
    df_date_cols = [k for k, v in header_dtypes.items() if v == "datetime64[ns]"]

    # read df from csv file
    if config:
        df = pd.read_csv(
            file_path,
            sep=config["sep"], 
            decimal=config["decimal"],
            dtype=df_dtypes,
            parse_dates=df_date_cols,
            dayfirst=True,
            date_format=config["date_format"],
            encoding=config.get("encoding", "utf-8"))

    else:
        df = pd.read_csv(file_path,
                            dtype=df_dtypes,
                            parse_dates=df_date_cols)

    # 3. Safety Pass: Force any missed date columns to datetime
    for col in df_date_cols:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = pd.to_datetime(
                df[col],
                format= config["date_format"] if config else None,
                dayfirst=True,
                errors='coerce'
                )

    # Handle finish.
    if "finish" in df:
        df["finish"] = df["finish"].fillna("non-foil")
        valid_finishes = ['non-foil','foil', 'etched']
        df.loc[~df['finish'].isin(valid_finishes), 'finish'] = 'non-foil'


    # Delete bloat columns and columns that are not specified in the config file
    cleanup_dataframe(df, header_type_dict)

    return df

# Updates all the info in the cards using the scryfall id
def update_collection(df):

    # Scryfall only allows 75 cards at a time
    BATCH_SIZE = 75


    # Each card on scryfall has id (uuid) identifier
    unique_id_df = df.drop_duplicates(subset=['id'])

    # Extra price columns
    price_cols = ["usd_reg", "usd_foil", "usd_etched", "eur_reg", "eur_foil", "eur_etched"]
    
    # Add extra price columns
    for col in price_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Set look-up index
    df.set_index('id', inplace=True)

    # Get exchange rates
    eur_to_usd,_ = get_eur_usd_rate()

    
    # Timimg required so that we don't flood api with requests
    post_time = time.time()

    # Iterate through the unique list of cards by 75 card Batches
    for i in range(0, len(unique_id_df), BATCH_SIZE):

        # Creates a batch of card ids (uuids)
        batch_df = unique_id_df.iloc[i : i + BATCH_SIZE]
        ids_for_api = batch_df['id'].dropna().tolist()
        payload = {"identifiers": [{"id": id} for id in ids_for_api]}

        # Sends batch to scryfall to get card data back
        cards_data, _, post_time = scryfall.get_card_batch(payload, post_time)
    
        # Iterate through each object (card) in the api return JSON
        for card in cards_data:
            fill_prices(card, eur_to_usd)

        # Set "id" as the root for mapping df1 to update_chunk
        update_chunk = pd.DataFrame(cards_data).set_index("id")
        

        # Maps df1 to the downloaded data of update_chunk
        df.update(update_chunk)

        # Mask all cards not connected to the batch
        current_ids = batch_df['id'].tolist()
        mask = df.index.isin(current_ids)

        # update price trend columns
        mass_price_select(df, mask)

        ui.progress_bar(i + len(update_chunk),len(unique_id_df))

    print("\n")
    
    df.drop(columns=price_cols, inplace=True)

    # reset root index
    df.reset_index(inplace=True)

    return df

def fill_prices(card_json, eur_usd_xrate):

    # Append date for each card
    # Iso format is 2026-02-16
    card_json["current date"] = pd.Timestamp.now().normalize()

    # Raw prices are strings
    raw_prices = card_json.get("prices", {})

    # Convert prices to floats
    p = {k: safe_float(v) for k,v in raw_prices.items()}

    usd_eur_xrate = 1.0/eur_usd_xrate

    # Regular version prices
    if p["usd"] is None and p["eur"] is not None: p["usd"] =  eur_usd_xrate*p["eur"]
    if p["eur"] is None and p["usd"] is not None: p["eur"] =  usd_eur_xrate*p["usd"]

    # Foil prices
    if p["usd_foil"] is None and p["eur_foil"] is not None: p["usd_foil"] =  eur_usd_xrate*p["eur_foil"]
    if p["eur_foil"] is None and p["usd_foil"] is not None: p["eur_foil"] =  usd_eur_xrate*p["usd_foil"]
    
    # Etched prices
    if p["usd_etched"] is not None: p["eur_etched"] = usd_eur_xrate*p["usd_etched"]

    # List of keys to process
    keys = ["usd_reg", "usd_foil", "usd_etched", "eur_reg", "eur_foil", "eur_etched"]
    # Map internal price dict 'p' keys to 'card_json' keys
    source_keys = ["usd", "usd_foil", "usd_etched", "eur", "eur_foil", "eur_etched"]

    for target, source in zip(keys, source_keys):
        val = p.get(source)
        # Only round if the value is not None
        card_json[target] = round(val, 2) if val is not None else None

    return card_json


# Converts a variable, "val", to a float
def safe_float(val):
    if val is None: return None
    try:
        # Removes commas often found in thousands or as decimal separators
        clean_val = str(val).replace(',', '.') 
        return float(clean_val)
    except:
        return None
    
# Select the correct price from auxiliary columns
def mass_price_select(df, mask=None):

    required_columns = ["usd_reg", "usd_foil", "usd_etched", "eur_reg", "eur_foil", "eur_etched",
                        "price trend usd", "price trend eur"]

    for col in required_columns:
        if col not in df.columns:
            print(f"Error with updating prices. '{col}' column missing")
            return df

    # Define foil conditions
    conditions = [(df["finish"] == "etched"), (df["finish"] == "foil")]
    
    usd_choices = [df["usd_etched"], df["usd_foil"]]
    eur_choices = [df["eur_etched"], df["eur_foil"]]

    if mask is None:

        df["price trend usd"] = np.select(conditions, usd_choices, default=df["usd_reg"])
        df["price trend eur"] = np.select(conditions, eur_choices, default=df["eur_reg"])

    else:

        df.loc[mask, "price trend usd"] = np.select(conditions, usd_choices, default=df["usd_reg"])[mask]
        df.loc[mask, "price trend eur"] = np.select(conditions, eur_choices, default=df["eur_reg"])[mask]
        
    return df

# Define a function to register new cards
# New cards are rows that contain no PID but a query string in the "name" column
# Returns None if there is an error
def register_new_cards(main_df, dfs=[]):

    # df: the dataframe where the rows will be added
    # dfs: A list of other dataframes containing cards, with unique pids

    # Find rows that represent new cards
    mask = main_df['pid'].isna() & main_df['name'].notna()

    if not mask.any():
        print("All cards are already registered. Skipping...")
        return []


    # Get all pids
    pid_col_list = [df["pid"] for df in dfs]
    pid_col_list.append(main_df["pid"]) # Ensure main_df's existing IDs are included!
    reserved_pids = pd.concat(pid_col_list, ignore_index=True, sort=False)


    # Get exchange rates
    eur_to_usd,_ = get_eur_usd_rate()


    # A list of scryfall data from the soon to be added cards
    cards_data = []
    pid_list = []
    
    for index, row in main_df[mask].iterrows():
        query = row['name']
        print(f"Registering: {query}...")
        
        # Search SCRYFALL
        card_json = scryfall.query_name(query)

        # Generate pid, put it into data, and add to list
        if card_json:
            new_pid = ui.generate_next_pid(reserved_pids,"p")

            card_json["pid"] = new_pid
            card_json["index"] = index

            fill_prices(card_json, eur_to_usd)

            cards_data.append(card_json)

            reserved_pids = pd.concat([reserved_pids, pd.Series([new_pid])], ignore_index=True, sort=False)
            pid_list.append(new_pid)

        else:
            print("Error registering. Exiting protocol.")           
            return []

    # 3. UPDATE THE MAIN DATAFRAME
    if cards_data:

        # Set "index" as the root for mapping df1 to update_chunk
        update_chunk = pd.DataFrame(cards_data).set_index("index")


        # Auxiliary headers
        price_cols = ["usd_reg", "usd_foil", "usd_etched", "eur_reg", "eur_foil", "eur_etched"]

        # Add axiliary price columns
        for col in price_cols:
            if col not in main_df.columns:
                main_df[col] = np.nan

        # Maps main_df to the downloaded data of update_chunk
        main_df.update(update_chunk)

        # LOCK IN THE TYPE: Ensure pids stay strings after the update
        main_df['pid'] = main_df['pid'].astype(str)

        # update price trend columns
        mass_price_select(main_df, mask)

        # Drop auxiliary columns
        main_df.drop(columns=price_cols, inplace=True)

        print(f"Successfully registered {len(cards_data)} cards.")

        return pid_list


# This function deletes columns that shouldn't be in the data, but somehow got there
def cleanup_dataframe(df, header_source):

    # If it's a dictionary, we want the keys. If it's a list, we use it as is.
    if isinstance(header_source, dict):
        allowed_columns = set(header_source.keys())
    elif isinstance(header_source, list):
        allowed_columns = set(header_source)
    else:
        print("Error: header_source must be a list or dict")
        return
    
    # 2. Identify the "junk" columns
    bloat_columns = [col for col in df.columns if col not in header_source]
    
    # errors='ignore' ensures it won't crash if the column is already gone
    df.drop(columns=bloat_columns, axis=1, errors='ignore', inplace=True)
    

# Show a part of the dataframe "df"
def peek_df(df, columns=None, pids=[], rows=None, last=False, char_limit=20):
    """
    Returns a view of the dataframe with selected columns, 
    a limited number of rows, and truncated string lengths.
    """

    
    #default_columns = ["pid", "name", "set_name", "finish", "language", "condition", "comment"]
    #cols = [col for col in default_columns if col in df.columns]

    # 1. Select specific columns and the first N rows
    if columns:
        cols = [col for col in columns if col in df.columns]
    else:
        cols = df.columns


    # Create a new dataframe with only specified PIDs
    if len(pids) > 0:
        view = df[df['pid'].isin(pids)].copy()

    # Pick first or last columns
    elif rows is not None:
        if last:
            view = df.tail(rows).copy()
        else:
            view = df.head(rows).copy()

    else:
        view = df.copy()

    # 3. NOW apply the column filter to the resulting view
    # We do this after row filtering so it applies to every scenario
    view = view[cols]

    # Format datetime columns to show only the date
    for col in view.columns:
        if pd.api.types.is_datetime64_any_dtype(view[col]):
            # dt.strftime converts it to a clean string format
            view[col] = view[col].dt.strftime('%Y-%m-%d')

    # 2. Truncate all strings to the char_limit
    # We use 'astype(str)' to ensure we can slice, then slice [:char_limit]
    # The 'if isinstance(x, str)' check keeps numbers as numbers
    view = view.map(lambda x: (str(x)[:char_limit] + '..') if isinstance(x, str) and len(str(x)) > char_limit else x)

    return view

def str_search_col(df, search_term, col='name', verbose=False):
    """
    Searches the dataframe for card names containing the search_term.
    Displays results using the dynamic formatter.
    """
    # 1. Perform the case-insensitive search
    mask = df[col].str.contains(search_term, case=False, na=False)
    results = df[mask]

    if not verbose: return results

    # 2. Feedback for the user
    if results.empty:
        print(f"No cards found matching: '{search_term}'")
    else:
        print(f"Found {len(results)} matches for '{search_term}':")
        # Use the dynamic display function from the previous step
        #display_dynamic_df(results)
    return results

def display_dynamic_df(df, title=None, description=None):
    if df is None or df.empty:
        print("\n[ Empty DataFrame ]\n")
        return

    # Convert everything to string for display calculations
    df_str = df.astype(str)
    
    # Calculate column widths based on headers and data
    col_widths = {col: max(len(col), df_str[col].map(len).max()) for col in df.columns}
    
    # Build the table components
    header_row = " | ".join(f"{col:<{col_widths[col]}}" for col in df.columns)
    separator = "-" * (sum(col_widths.values()) + (3 * (len(df.columns) - 1)) + 4)

    # --- 1. DISPLAY TITLE ---
    if title:
        # Center the title within the width of the table
        print(f"{separator}")
        #print(f"  {title.upper():^{len(separator)-4}}  ") # Capitalized
        print(f"  {title:^{len(separator)-4}}  ") # Non-capitalizd
    
    # --- 2. DISPLAY TABLE ---
    print(separator)
    print(f" {header_row}  |")
    print(separator)

    for _, row in df_str.iterrows():
        row_str = " | ".join(f"{row[col]:<{col_widths[col]}}" for col in df.columns)
        print(f" {row_str}  |")

    print(separator)

    # --- 3. DISPLAY DESCRIPTION ---
    if description:
        print(f" NOTE: {description}")
        print(f"{separator}")
    else:
        print("")

def transfer_cards(df_source, df_dest, id_list, id_col='pid'):
    """
    Moves rows from source to destination.
    Only transfers columns that already exist in the destination.
    """
    # 1. Identify the rows to move
    mask = df_source[id_col].isin(id_list)
    
    # 2. Slice the source data to ONLY include columns found in the destination
    # This prevents 'location' or other vault-only cols from polluting the archive
    cols_to_keep = [col for col in df_source.columns if col in df_dest.columns]
    rows_to_move = df_source.loc[mask, cols_to_keep].copy()

    if rows_to_move.empty:
        return df_source, df_dest

    # 3. Combine with destination
    updated_dest = pd.concat([df_dest, rows_to_move], ignore_index=True, sort=False)

    # 4. Remove moved rows from source (using the original mask)
    updated_source = df_source[~mask].copy()

    return updated_source, updated_dest


if __name__ == '__main__':
    print_string = "This module contains functions:\n \
                    'load_collection_to_df'\n \
                    'update_collection'\n \
                    'register_new_cards'\n \
                    'cleanup_dataframe'\n \
                    'peek_df'\n \
                    'get_parameters'"
    print(print_string) 

