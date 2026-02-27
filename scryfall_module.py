import requests
import sys
import time
import json
import matplotlib.pyplot as plt

import utils_input as ui
import exchange_rates_module as rates
import view

usd_to_eur = 0.0
rate_fetch_flag = False

def main():

    card_name = ' '.join(sys.argv[1:])
    print(card_name)
    #json = mkm_id_search(card_name)
    json = name_search(card_name)

    print(json)

    return 0

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
def query_name(query_string, attempts=3, params={}):
    # "query": the query as a string
    # "attempts": number of total attempt to find the card
    # "params": Any additional parameters that might need to be passed along

    for i in range(attempts + 1):

            print (f"Searching '{query_string}'...")

            # Identify card and set (set is optional)
            prints_uri, prints_set = name_search(query_string, uri_flag=True)
            
            # Card has been found!
            if prints_uri is not None:

                # Select correct version and download data
                card_json,_,_ = get_card_prints(prints_uri, prints_set)

                if card_json:
                    return card_json

                else:
                    print(f"Card selection failed.")
                    
            else:
                print(f"Card search failed.")

            print(f"Attempt {i + 1} of {attempts} - give a new query")
            query_string = input("? ")
        
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
    ret_set = None
    
    cards_data = card_req(url, verbose=False)
    if cards_data != {}:
        
        # Check if there are cards returned
        if cards_data['total_cards'] > 1:
            for i,card in enumerate(cards_data['data']):
                card_string = str(i + 1) + ". " + card['name']
                print(card_string)


            usr_input = input("? ")
            words = usr_input.split()

            try:
                j = int(words[0]) - 1
                ret_dict = cards_data['data'][j]

                if uri_flag:
                    if len(words) > 1: ret_set = words[1]
                    ret_uri = ret_dict["prints_search_uri"]

            except ValueError as e:
                print("Bad input")

        # No user selection needed if exactly 1 card is found.
        elif cards_data['total_cards'] == 1:
            ret_dict = cards_data['data'][0]

            words = []
            if uri_flag:
                if len(words) > 1: ret_set = words[1]
                ret_uri = ret_dict["prints_search_uri"]

        elif verbose:
            print(f"No cards found.")
            
    elif verbose:
        print("Request failed")

    if uri_flag:
        return ret_uri, ret_set
    else: 
        return ret_dict, ret_set
        
def get_card_prints(url, input_set=None):

    prints_uri = url
    cards_data = card_req(prints_uri)
    if not cards_data: return {}, "", ""

    matches = {} # All query hits will be stored in "matches" dict
    match_sets = {}

    i = 1
    while True:
        # Iterate through first page
        for card in cards_data['data']:
            
            # Check for appropriate set
            if input_set is not None and card['set'] != input_set: continue

            card_string = str(i) + ". (" + card['set'] + ") "
            card_string += card['set_name'] + ": { \""
            for k,key in enumerate(card['prices']):
                card_string += f"{key}\": {card['prices'][key]}"
                if k < len(card['prices']) - 1: card_string += ", \""
            card_string += " }"
            print(card_string)
            
            matches[str(i)] = card["id"]
            match_sets[str(i)] = card["set_name"]
            i += 1
            
        # Check if there are more cards
        has_more = cards_data.get("has_more")
        if has_more:
            prints_uri = cards_data.get("next_page") # Get new uri
            cards_data = card_req(prints_uri) # Download data
        else:
            # If there are no more pages of cards, break the loop
            break

    if (len(matches) > 1):

        while True:
            usr_input = input("? ")
            input_list = usr_input.split()

            # Check that input is given
            if len(input_list) == 0: return {}, "", ""

            # Check if user want cards shown (view mode "v")
            # View individual cards by pid
            if input_list[0] == "v":

                for i,word in enumerate(input_list):
                    if i == 0: continue
                    uuid = matches.get(word)
                    if uuid is None: continue
                    fig_name = match_sets.get(word)
                    if fig_name is None:
                        fig_name = word + "."
                    else:
                        fig_name = word + ". " + fig_name
                    view.display_card_image(uuid,card_name=fig_name) # Generates figure
          
                plt.show() # Displays all figures

            # View cards in interval (e.g. 100 - 110)
            elif input_list[0] == "v-":
                max_words = 15
                try:
                    start, end = int(input_list[1]), int(input_list[2])+1
                    pids = list(range(start, end))
                    words = [str(pid) for pid in pids]

                    for i,word in enumerate(words):
                        uuid = matches.get(word)
                        if uuid is None: continue
                        fig_name = match_sets.get(word)
                        if fig_name is None:
                            fig_name = word + "."
                        else:
                            fig_name = word + ". " + fig_name
                        view.display_card_image(uuid,card_name=fig_name) # Generates figure
                        if i == max_words:
                            print("Max cards hit")
                            break

                except (ValueError, IndexError) as e:
                    for i,word in enumerate(matches):
                        uuid = matches.get(word)
                        if uuid is None: continue
                        fig_name = match_sets.get(word)
                        if fig_name is None:
                            fig_name = word + "."
                        else:
                            fig_name = word + ". " + fig_name
                        view.display_card_image(uuid,card_name=fig_name) # Generates figure
                        if i == max_words:
                            print("Max cards hit")
                            break

                plt.show() # Displays all figures
 
            else:
                break # breaks view mode

        index_version = input_list[0]
        # Get foil info
        if 'f' in index_version:
            foil = "foil"
        elif 'e' in index_version:
            foil = "etched"
        else:
            foil = ""
        # Get comment
        if  (len(input_list) > 1):
            comment = ' '.join(input_list[1:])
        else:
            comment = ""
        # Get id
        index_string = ''.join([char for char in index_version if char.isdigit()])

        uuid = matches.get(index_string)
        if uuid is None:
            print("Invalid index. Please select a number from the list.")
        else:
            selected_json = uuid_fetch(uuid) # Download data

            return selected_json, foil, comment
        
    elif cards_data['total_cards'] == 1: 
        card = cards_data["data"][0]

        return card, "", ""
    else:
        print(f"No cards found.")
    return {}, "", ""

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

if __name__ == '__main__':
    main()