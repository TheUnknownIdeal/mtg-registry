import requests
import time
import json

import pandas as pd

import utils_df as ud
import utils_input as ui

import exchange_rates_module as rates
from display_module import display_card_image


usd_to_eur = 0.0
rate_fetch_flag = False


def get_card_batch(scryfall_payload, reference_time=None):
    url = "https://api.scryfall.com/cards/collection"
    delay_between_requests = 0.1 # Scryfall requests 100 ms (0.1 s) between requests
    all_cards_data = []
    invalid_ids = []

    # Return empty lists if given an empty layload
    if not scryfall_payload: return all_cards_data, invalid_ids, reference_time


    # Compute sleep duration
    if reference_time is not None:
        time_delta = time.time() - reference_time
        sleep_duration = delay_between_requests - time_delta
    else:
        sleep_duration = delay_between_requests

    # Sleep so that request rate limit is not exceeded
    if sleep_duration > 0: time.sleep(sleep_duration)

    # Time that post request is sent
    post_time = time.time()

    # Make the POST request for the current batch
    response = requests.post(url, json=scryfall_payload)
    
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        all_cards_data.extend(data.get('data', []))  # Append valid cards

        # Collect invalid/faulty IDs if they exist
        if 'not_found' in data:
            invalid_ids.extend(data['not_found'])
    else:
        print(f"Error: {response.status_code} - {response.text}")

    return all_cards_data, invalid_ids, post_time




# A procedure to get scryfall card data from a name 
def query_name(input_string, attempts=3, params={}):
    # "query": the query as a string
    # "attempts": number of total attempt to find the card
    # "params": Any additional parameters that might need to be passed along

    query_string = input_string

    for i in range(attempts + 1):

            # Identify card and set (set is optional)
            prints_uri, prints_set = name_search(query_string, uri_flag=True)
            
            # Card has been found!
            if prints_uri is not None:

                # Select correct version and download data
                card_json = get_card_prints(prints_uri, prints_set)

                if card_json:
                    return card_json

                else:
                    error_string = f"Selection failed."
                    
            else:
                error_string = f"Search failed."

            prompt = f"{error_string} Attempt {i + 1} of {attempts}: give a new query."
            query_string = ui.get_typed_input(prompt, target_type="str")

    return {}


def uuid_fetch(uuid):
    url = f"https://api.scryfall.com/cards/{uuid}"
    return card_req(url)

    

def tcg_id_fetch(id):
    url = f"https://api.scryfall.com/cards/tcgplayer/{id}"
    return card_req(url)

def mkm_id_fetch(id):
    url = f"https://api.scryfall.com/cards/cardmarket/{id}"
    return card_req(url)

def name_search(input_name, uri_flag=False, verbose=True):

    # Construct the API URL for searching the card
    url = f"https://api.scryfall.com/cards/search?q={input_name}"

    ret_dict, ret_uri = {}, None
    selected_set = None
    
    # Call API and download data into "cards_data"
    cards_data = card_req(url, verbose=False)

    # Check that return data is not empty
    if cards_data != {}:

        
        # Check if there is more than one card option returned
        if cards_data['total_cards'] > 1:

            ret_indices = []
            ret_names = []
            ret_mana_costs = []

            for i,card in enumerate(cards_data['data']):
                card_string = str(i + 1) + ". " + card['name']
                #print(card_string)
                #print(json.dumps(card, indent=4))
                card_name = card.get('name', "?")
        
                # Robust mana_cost check:
                # If it's not at the top level (like a DFC), check the first face
                mana_cost = card.get('mana_cost')
                if mana_cost is None and 'card_faces' in card:
                    mana_cost = card['card_faces'][0].get('mana_cost', "?")
                elif mana_cost is None:
                    mana_cost = "?"

                ret_indices.append(i + 1)  
                ret_names.append(card_name)
                ret_mana_costs.append(mana_cost)

            # Create a dataframe for viewing results
            hits_df = pd.DataFrame({
                "index": ret_indices,
                "name": ret_names, 
                "mana cost": ret_mana_costs
                })

            # Display query results
            title = f"Search results for '{input_name}':"
            ud.display_dynamic_df(hits_df, title=title)

            prompt = "Enter index to select (and optional set abbreviation, e.g., '1' or '1 mh3')"
            usr_input = ui.get_typed_input(prompt, target_type="str")

            if not usr_input:
                print("No selection made.")

            else:
                words = usr_input.split()
                selection_idx = int(words[0]) - 1 # Convert back to 0-based index
                selected_set = words[1] if len(words) > 1 else None

                try:
                    ret_dict = cards_data['data'][selection_idx]
                    ret_uri = ret_dict["prints_search_uri"]

                except IndexError:
                    print(f"Error: {words[0]} is out of range.")

        # No user selection needed if exactly 1 card is found.
        elif cards_data['total_cards'] == 1:
            ret_dict = cards_data['data'][0]
            ret_uri = ret_dict["prints_search_uri"]
                

        elif verbose:
            print(f"No cards found.")
            
    elif verbose:
        print("Request failed")

    # Return Logic
    ret_uri = ret_dict.get("prints_search_uri")
    return (ret_uri, selected_set) if uri_flag else (ret_dict, selected_set)

        
def get_card_prints(url, input_set=None):

    prints_uri = url
    cards_data = card_req(prints_uri)
    if not cards_data: return {}

    #print(json.dumps(cards_data, indent=4))

    all_cards = []

    hit_ids = []

    hit_names = []

    hit_indices = []
    hit_sets = []
    hit_set_names = []
    hit_usd_prices = []
    hit_usd_foil_prices = []
    hit_usd_etched_prices = []
    hit_eur_prices = []
    hit_eur_foil_prices = []

    i = 1
    while True:
        # Iterate through first page
        for card in cards_data['data']:

            # Check for appropriate set
            if input_set is not None and card['set'] != input_set: continue

            all_cards.append(card)

            # Store card print information
            hit_ids.append(card.get("id")) # Card id

            hit_names.append(card.get("name"))

            hit_indices.append(i) 
            hit_sets.append(card.get("set"))
            hit_set_names.append(card.get("set_name"))

            prices = card.get("prices")

            if prices is not None:
                hit_usd_prices.append(prices.get("usd"))
                hit_usd_foil_prices.append(prices.get("usd_foil"))
                hit_usd_etched_prices.append(prices.get("usd_etched"))
                hit_eur_prices.append(prices.get("eur"))
                hit_eur_foil_prices.append(prices.get("eur_foil"))
            else:
                hit_usd_prices.append(None)
                hit_usd_foil_prices.append(None)
                hit_usd_etched_prices.append(None)
                hit_eur_prices.append(None)
                hit_eur_foil_prices.append(None)

            
            i += 1

            
        # Check if there are more cards
        has_more = cards_data.get("has_more")
        if has_more:
            prints_uri = cards_data.get("next_page") # Get new uri
            cards_data = card_req(prints_uri) # Download data
        else:
            # If there are no more pages of cards, break the loop
            break

    # Check if there is more than one hit
    if (len(hit_ids) > 1):

        # Create a dataframe for viewing multiple results
        hits_df = pd.DataFrame({
            "index": hit_indices,
            "set": hit_sets, 
            "set name": hit_set_names,
            "reg usd": hit_usd_prices,
            "foil usd": hit_usd_foil_prices,
            "etched usd": hit_usd_etched_prices,
            "reg eur": hit_eur_prices,
            "foil eur": hit_eur_foil_prices
            })

        # Get the name of the card by finding most common element in hit_names
        card_name = max(set(hit_names), key=hit_names.count)
        title = f"Prints for '{card_name}':"
        
        # "peek_df" will limit the length of each string
        ud.display_dynamic_df(ud.peek_df(hits_df), title=title)

        description = "Enter index to select, 'v 1 2' to view, or 'v- 1 5' for range"
        print(description)


        # 1. Create lookup maps for the user indices
        # We use strings because input() returns strings
        lookup = {str(i): card_id for i, card_id in zip(hit_indices, hit_ids)}


        while True:
            usr_input = ui.get_typed_input("", target_type="str", display_default=False)
            
            if not usr_input: 
                return {}

            parts = usr_input.split()
            cmd = parts[0].lower()

            # --- VIEW MODE (v or v-) ---
            if cmd in ["v", "v-"]:
                target_ids = []
                
                if cmd == "v":
                    # Get specific IDs: v 1 3 5
                    target_ids = [p for p in parts[1:] if p in lookup]
                
                elif cmd == "v-":
                    # Get range: v- 1 5
                    if len(parts) > 2:
                        try:
                            start, end = int(parts[1]), int(parts[2])
                            target_ids = [str(i) for i in range(start, end + 1) if str(i) in lookup]
                        except (ValueError, IndexError):
                            pass # Stay silent and let target_ids remain None

                    if not target_ids:
                        # FALLBACK: Just point to the whole list of indices.
                        # The loop below handles the "first 15" constraint.
                        target_ids = [str(i) for i in hit_indices]

                # Display the images
                for idx in target_ids[:15]: # Cap at 15 to avoid crashing
                    display_card_image(lookup[idx], card_name=f"{idx}")
                
                import matplotlib.pyplot as plt
                plt.show()

            # --- SELECTION MODE (The user just typed a number) ---
            elif cmd in lookup:
                #print(f"Selected: {hit_sets[cmd]}")
                return all_cards[int(cmd) - 1]

            else:
                #print("Invalid index or command.")
                pass
        
    elif len(hit_ids) == 1: 
            return all_cards[0]

    return {}

def card_req(url, data=None, verbose=True):
    if data is None:
        response = requests.get(url)
    else:
        response = requests.get(url, json=data)

    card = {}
    if response.status_code == 200:
        card = response.json()
    elif verbose:
        print(f"Error fetching card data: {response.status_code} - {response.json()}")
    return card


# Get right price data with price key
def get_price(json, version, currency="eur"):
    global usd_to_eur
    global eur_to_usd
    global rate_fetch_flag


    # Fetch exchange rate
    if not rate_fetch_flag:
        eur_to_usd, usd_to_eur,  = rates.get_eur_usd_rate()
        rate_fetch_flag = True

    # Compute price from version and currency (eur/usd)
    if version == "etched":
        price_string = json["prices"]["usd_etched"]
        if price_string is None: return None
        price = float(price_string)
        if currency != "usd": price *= usd_to_eur

    elif version == "foil":
        if currency != "usd":
            price_string = json["prices"]["eur_foil"]
            if price_string is None:
                price_string = json["prices"]["usd_foil"]
                if price_string is None: return None
                price = float(price_string)*usd_to_eur
                #print("usd: {}, rate: {}, eur: {}".format(price_string,usd_to_eur, price))
            else:
                price = float(price_string)
        else: # usd
            price_string = json["prices"]["usd_foil"]
            if price_string is None:
                price_string = json["prices"]["eur_foil"]
                if price_string is None: return None
                price = float(price_string)*eur_to_usd
            else:
                price = float(price_string)
    else:
        if currency != "usd":
            price_string = json["prices"]["eur"]
            if price_string is None:
                price_string = json["prices"]["usd"]
                if price_string is None: return None
                price = float(price_string)*usd_to_eur
                #print("usd: {}, rate: {}, eur: {}".format(price_string,usd_to_eur, price))
            else:
                price = float(price_string)

        else: # usd
            price_string = json["prices"]["usd"]
            if price_string is None:
                price_string = json["prices"]["eur"]
                if price_string is None: return None
                price = float(price_string)*eur_to_usd
            else:
                price = float(price_string)
    return round(price, 2)
