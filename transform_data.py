import json
import pandas as pd
from datetime import date

def transform():
    t_timestamp = date.today()
    t_string = t_timestamp.strftime('%Y-%m-%d')

    options_f_name = '/home/bkim/raw_options_dir/raw_aapl_options_{}.json'.format(t_string)
    price_f_name = '/home/bkim/raw_price_dir/raw_aapl_price_{}.json'.format(t_string)

    options_pre = open(options_f_name)
    price_pre = open(price_f_name)

    options_data = json.load(options_pre)
    price_data = json.load(price_pre)

    data_append_row = {}
    index_counter = 0
    options_df = pd.DataFrame()
    wanted_data = ['expirationDate','daysToExpiration','bid','ask','strikePrice','totalVolume','volatility','delta',
                   'gamma','theta','vega']
    yearly_date_list = list(options_data['callExpDateMap'].keys()) # needed for iteration so it can access further in data in json file

    for dates in yearly_date_list:
        strike_list = list(options_data['callExpDateMap'][dates].keys())  # needed for iteration so it can access further in data in json file
        strike_dict = options_data['callExpDateMap'][dates]

        for strike in strike_list:
            options_data_dict = strike_dict[strike][0]

            for value in wanted_data:
                # converts the date (ms) unit into readable date format
                if value == 'expirationDate':
                    timestamp = pd.to_datetime(options_data_dict[value], unit='ms')
                    string_time = timestamp.strftime('%Y-%m-%d')
                    data_append_row[value] = string_time
                else:
                    data_append_row[value] = options_data_dict[value]

            row_data = pd.DataFrame(data_append_row, index = [index_counter])
            options_df = pd.concat([options_df, row_data])
            index_counter += 1

    price_df = pd.DataFrame(price_data['candles'][0], index=[0])

    combined_df = options_df.copy()
    combined_df['stock_price'] = price_df.iloc[0]['close']

    for row in range(len(combined_df)):
        if 'NaN' in list(combined_df.iloc[row]):
            cleaned_combined_df = combined_df.drop(row) #index is set to false when saving to csv so no need to reset index
        else:
            cleaned_combined_df = combined_df.copy()

    cleaned_combined_file_name = "combined_data_{}.csv".format(t_string)
    save_file_location = "/home/bkim/processed_data_dir/" + cleaned_combined_file_name
    cleaned_combined_df.to_csv(save_file_location, index=False)