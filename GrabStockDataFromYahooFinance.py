#!/usr/bin/env python
# coding: utf-8

#   Created by Michael Yen  
#   Varmeego ltd  
#   Date: 2020-9-23  
#   This is a web scrapper for yahoo finance

# In[4]:


# Grab all historical stock data which the stock now has larger or equal the specific threshold_volume
# inputs:
#     threshold_volume: int - a volume threshold for filtering
# return:
#     stock_table[][] - ['Code.HK', 'Volume'] store all stocks which fulfill the threshold

def get_stock_codes_from_yahoo(threshold_volume):
    import requests
    from bs4 import BeautifulSoup
    
    print('get_stock_codes_from_yahoo start...')
    
    stock_table = []
    table_attribute = ['Code', 'Volume']
    stock_table.append(table_attribute)
    row = []
    ticker = ''
    for i in range(1,10000): # 1, 10000
        if i >= 1 and i <= 9:
            ticker = '000' + str(i)
        elif i >= 10 and i <= 99:
            ticker = '00' + str(i)
        elif i >= 100 and i <= 999:
            ticker = '0' + str(i)
        else:
            ticker = str(i)
        print(ticker)
        url = "https://hk.finance.yahoo.com/quote/" + ticker + ".HK"
        resp = requests.get(url)
        soup = BeautifulSoup(resp.text, 'html.parser')
        l = len(soup.find_all('span'))
        for i in range(l):
            if soup.find_all('span')[i].get_text() == '成交量':
                if soup.find_all('span')[i+1].get_text() == '無':
                    continue
                value = soup.find_all('span')[i+1].get_text()
                new_value = ''
                for i in range(len(value)):
                    if value[i:i+1] == '0' or value[i:i+1] == '1' or value[i:i+1] == '2' or value[i:i+1] == '3' or value[i:i+1] == '4' or value[i:i+1] == '5' or value[i:i+1] == '6' or value[i:i+1] == '7' or value[i:i+1] == '8' or value[i:i+1] == '9':
                        new_value = new_value + value[i:i+1]
                #print(new_value)
                if int(new_value) >= threshold_volume:
                    row.append(str(ticker) + '.HK')
                    row.append(new_value)
                    stock_table.append(row)
                    row = []
        ticker = ''
    print('get_stock_codes_from_yahoo Finished...')
    return stock_table


# In[5]:


#inputPath: 'stock_codes.csv'
def read_csv(inputPath):
    stock_table = []
    with open(inputPath, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            stock_table.append(row)
    return stock_table


# In[6]:


# Get specific stock data, which are provided by stock_table, by start_of_date and end_of_date.
# The function will automatically generate 3 csv files which are 
#                                                               1. the entire grabbed data
#                                                               2. the stock codes that be grabbed
#                                                               3. the error stock codes that cannot be grabbed
# inputs:
#      stock_table[][] - [code, volume]
#      start_of_date: datetime - start of the date
#      end_of_date: datetime - end of the date
#      df_csv_outputPath: string - the outputPath of the entire grabbed data csv
#      stockcode_csv_outputPath: string - the output path of the stock code csv
#      errorStock_csv_outputPath: string - the output path of the error stock code csv
# return:
#      df: dataFrame - the entire stock data


def get_stock_data(stock_table, start_of_date, end_of_date, df_outputPath, stockcode_csv_outputPath, errorStock_csv_outputPath):
    import pandas as pd
    import pandas_datareader as dr
    get_ipython().run_line_magic('matplotlib', 'inline')
    import datetime
    import csv
    
    print('get_stock_data start...')
    
    name_arr = []
    df = pd.DataFrame()
    error_log = []
    fulfilled_stock_codes = []
    fulfilled_stock_codes_row = []
    fulfilled_stock_codes_row.append('num')
    fulfilled_stock_codes_row.append('code')
    fulfilled_stock_codes.append(fulfilled_stock_codes_row)
    fulfilled_stock_codes_row = []
    count = 1
    for i in range(1,len(stock_table)):
        code = stock_table[i][0]
        try:
            print(code)
            data = dr.data.get_data_yahoo(code, start_of_date, end_of_date)
            for i in range(len(data)):
                name_arr.append(code)
            data.insert(0, "Code", name_arr, True)
            name_arr = []
            df = df.append(data)
            fulfilled_stock_codes_row.append(count)
            fulfilled_stock_codes_row.append(code)
            fulfilled_stock_codes.append(fulfilled_stock_codes_row)
            fulfilled_stock_codes_row = []
            count = count + 1
        except:
            error_log.append(code)
            print(code + " Caught error")
            count = count + 1

    df.to_csv(df_csv_outputPath)

    with open(errorStock_csv_outputPath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(error_log)
    with open(stockcode_csv_outputPath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(fulfilled_stock_codes)
    print("get_stock_data Finished...")
    return df


# In[7]:


import datetime
import pandas as pd
get_ipython().run_line_magic('matplotlib', 'inline')
import csv
stock_table = get_stock_codes_from_yahoo(100000)

df_outputPath = './stock_data.csv'
stockcode_csv_outputPath = './fulfilled_stock_codes.csv'
errorStock_csv_outputPath = './error_log.csv'
df = get_stock_data(stock_table, datetime.datetime(2005,1,3), datetime.datetime(2020,9,23), df_outputPath, stockcode_csv_outputPath, errorStock_csv_outputPath)


# In[7]:


#==========================================================================================================


# In[ ]:




