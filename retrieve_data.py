import requests
import json
from datetime import date
from datetime import datetime
from datetime import timedelta

def get_data():
    API_KEY = 'Q5G9Y3VALS4FZYSEHRVHMQAWQ0K4FOHW'
    options_url = 'https://api.tdameritrade.com/v1/marketdata/chains'
    price_url = 'https://api.tdameritrade.com/v1/marketdata/AAPL/pricehistory'

    options_response = requests.get(options_url,params={'apikey':API_KEY,
                                                'symbol':'AAPL',
                                                'contractType':'CALL',
                                                'strikeCount':30})
    options_raw_data = options_response.json()

    # converting yesterday timestamp into ms int so api can properly read them
    y_timestamp = date.today() - timedelta(days = 1)
    y_string = y_timestamp.strftime('%Y-%m-%d')
    y_datetime = datetime.strptime(y_string, '%Y-%m-%d')
    start_date = int(y_datetime.timestamp()*1000)

    price_response = requests.get(price_url, params = {'apikey': API_KEY,
                                                 'periodType': 'ytd',
                                                 'frequencyType': 'daily',
                                                 'frequency': '1',
                                                 'startDate': start_date,
                                                 'needExtendedHoursData': 'false'})
    price_raw_data = price_response.json()

    # obtain the current date and change the datatype so it can be used as a newly created file name
    t_timestamp = date.today()
    t_string = t_timestamp.strftime('%Y-%m-%d')
    options_file_name = "raw_aapl_options_{}.json".format(t_string)
    price_file_name = "raw_aapl_price_{}.json".format(t_string)

    # saves the .json file in a file with path provided
    with open("/home/bkim/raw_options_dir/" + options_file_name, 'w') as f:
        json.dump(options_raw_data, f, indent = 4)

    with open('/home/bkim/raw_price_dir/' + price_file_name, 'w') as f:
        json.dump(price_raw_data, f, indent= 4 )
