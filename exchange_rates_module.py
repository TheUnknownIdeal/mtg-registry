import time
import json
import requests

def main():
    get_eur_usd_rate()



def get_eur_usd_rate():
    
    file_path = 'exchange_rates.json'

    # Get access_token
    with open(file_path, 'r') as config_file:
        config = json.load(config_file)
    access_token = config['EXCHANGERATES_API_TOKEN']

    
    minimum_time_between_calls = 10368 # seconds (250 calls a month)


    # Old Exchange rates
    usd_to_eur = config["usd_to_eur"]
    eur_to_usd = config["eur_to_usd"]


    current_timestamp = time.time()
    if "timestamp" in config and current_timestamp - config["timestamp"] < minimum_time_between_calls:
    #if False:
       
        # Return old exchange rates if they are recent enough.
        return eur_to_usd, usd_to_eur

    else:
        url = f"https://api.exchangerate.host/latest?symbols=USD&access_key={access_token}"
        response = requests.get(url)
        
        # Check if the response is OK (status code 200)
        if response.status_code == 200:
            data = response.json()

            # Get the USD to EUR rate
            eur_to_usd = data['rates']['USD']
            config["eur_to_usd"] = eur_to_usd
            # Inverse for EUR to USD
            usd_to_eur = 1 / eur_to_usd
            config["usd_to_eur"] = usd_to_eur

            # Store timestamp
            config["timestamp"] = data['timestamp']

            # Write the JSON data to the config file
            with open(file_path, 'w') as filename:
                json.dump(config, filename, indent=4)

            print("{:.2f} EUR/USD fetched".format(eur_to_usd))
            return usd_to_eur, eur_to_usd
        else:
            print(f"Error ({response.status_code}) fetching new exchange rates. Using old ones ")

            return eur_to_usd, usd_to_eur

if __name__ == '__main__':
    main()