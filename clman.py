import configparser
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from configparser import ConfigParser
from sqlalchemy import create_engine, Table, Column,  String, Integer, MetaData,  select, Numeric, DateTime, text, Float
from sqlalchemy.dialects.mysql import insert
import boto3
import json

from sqlalchemy.sql.expression import update



file = 'config.ini'
config = ConfigParser()
config.read(file)

user = config['DATABASE']['LOCAL_USERNAME']
passw = config['DATABASE']['LOCAL_PASSWORD']
dbHost = config['DATABASE']['LOCAL_HOST']
dbName = config['DATABASE']['DB']
bucket = config['S3']['BUCKET']
app_id = config['EXCHANGE']['APP_ID']
# baseurl = config['EXCHANGE']['BASE_EXCHANGE']
# symbols = config['EXCHANGE']['SYMBOLS']

# engine = create_engine(f'mysql://{user}:{passw}@localhost/sakila')
engine = create_engine(f'mysql+pymysql://{user}:{passw}@{dbHost}/{dbName}')

#Table Schema MetaData 
meta = MetaData()

daily_rates = Table(
   'daily_rates', meta, 
   Column('symbol', String(4), primary_key = True), 
   Column('rate', Float(10,5), nullable=False), 
   Column('inserted_at', DateTime(), default=datetime.now, nullable=False),
   Column('modified_at', DateTime(), default=datetime.now, onupdate=datetime.now)
)

currency_details = Table(
   'currency_details', meta, 
   Column('symbol', String(4), primary_key = True), 
   Column('descr', String(50), nullable=False),
   Column('inserted_at', DateTime(), default=datetime.now, nullable=False),
   Column('modified_at', DateTime(), default=datetime.now, onupdate=datetime.now) 
)

coinMarket_details = Table(
   'coinMarket_details', meta, 
   Column('coinRank', Integer, nullable=False), 
   Column('coinName', String(100), nullable=False),
   Column('coinSymbol', String(10), primary_key=True, nullable=False),
   Column('coinPrice', Float, nullable=False),
   Column('coin1hrChange', Float, nullable=False),
   Column('coin24hrChange', Float, nullable=False),
   Column('coin7dChange', Float, nullable=False),
   Column('coin24hrVol', Float, nullable=False),
   Column('coinMarketCap', Float, nullable=False),
   Column('fetchTime', DateTime(), default=datetime.now),
   Column('rate', Float, nullable=False),
   Column('coinPriceNaira', Float, nullable=False),
)

try:
   conn = engine.connect()
   print('DB Connected Successfully')
   print(f'connection object is :{conn}')
except:
   print('DB Connection Failed.')

meta.create_all(engine)


class fetchOn():
    def fetchCoins(pageNum=1): 

        #Instantiate an Empty list to collect the coins data in dict
        coinBase = []

        #The default URL to coingecko, the page number is appended to the base url for each request
        url_base = 'https://www.coingecko.com/en?page='

        # Loop through each page to retrieve the Page contents
        for i in range(1, pageNum+1):

            # Make a request to the website
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    # result = requests.get(url, headers=headers)
            response = requests.get(f'{url_base}{i}', headers=headers)

            # Create a soup object to parse the returned website content. 
            soup = BeautifulSoup(response.content, 'html.parser')
            results = soup.find('table', {'class': 'table-scrollable'}).find('tbody').findAll('tr')

            #Loop through the returned data of each page and extract the coin details

            for item, val in enumerate(results):
                try:
                    coinRank = int(results[item].find('td', {'class':'table-number tw-text-left text-xs'}).get_text().strip())
                except:
                    '--'
                try:
                    coinName = results[item].find('a', {'class':'tw-hidden lg:tw-flex font-bold tw-items-center tw-justify-between'}).get_text().strip()
                except:
                    '--'
                try:
                    coinSymbol = results[item].find('a', {'class':'d-lg-none font-bold'}).get_text().strip()
                except:
                    '--'
                try:
                    coinPrice = results[item].find('td', {'class':'td-price price text-right'}).get_text().strip()
                    coinPrice = float(coinPrice.replace('$','').replace('%','').replace(',',''))
                except:
                    '--'
                try:
                    coin1hrChange = results[item].find('td', {'class':'td-change1h'}).get_text().strip()
                    coin1hrChange = float(coin1hrChange.replace('$','').replace('%','').replace(',',''))
                except:
                    '--'
                try:
                    coin24hrChange = results[item].find('td', {'class':'td-change24h'}).get_text().strip()
                    coin24hrChange = float(coin24hrChange.replace('$','').replace('%','').replace(',',''))
                except:
                    '--'
                try:
                    coin7dChange = results[item].find('td', {'class':'td-change7d'}).get_text().strip()
                    coin7dChange = float(coin7dChange.replace('$','').replace('%','').replace(',',''))
                except:
                    '--'
                try:
                    coin24hrVol = results[item].find('td', {'class':'td-liquidity_score'}).get_text().strip()
                    coin24hrVol = float(coin24hrVol.replace('$','').replace('%','').replace(',',''))
                except:
                    '--'
                try:
                    coinMarketCap = results[item].find('td', {'class':'td-market_cap'}).get_text().strip()
                    coinMarketCap = float(coinMarketCap.replace('$','').replace('%','').replace(',',''))
                except:
                    '--'
                coinBag = dict(coinRank=coinRank,
                                coinName=coinName,
                                coinSymbol=coinSymbol,
                                coinPrice=coinPrice,
                                coin1hrChange=coin1hrChange,
                                coin24hrChange=coin24hrChange,
                                coin7dChange=coin7dChange,
                                coin24hrVol=coin24hrVol,
                                coinMarketCap=coinMarketCap,
                                fetchTime = datetime.now().isoformat()
                            )
                coinBase.append(coinBag)
        return coinBase

    def fetchDailyRates():
        baseurl = 'https://openexchangerates.org/api/latest.json?app_id='
        symbols = 'NGN,AUD,GBP,EUR,JPY,CNY,CAD'
        exurl = f'{baseurl}{app_id}&symbols={symbols}'
        response = requests.get(exurl).json()
        return response['rates']

    def fetchCurrencyDetails():
        baseurl = 'https://openexchangerates.org/api/currencies.json?app_id='
        exurl = f'{baseurl}{app_id}'
        response = requests.get(exurl).json()
        return response

class dbUpdate:
    def updateCurrencyDetailsTable():
        ls = fetchOn.fetchCurrencyDetails()
        with conn:
            for k, v in ls.items():
                ins_stmt = insert(currency_details).values(
                symbol = k, 
                descr = v
                )

                ins_on_duplicate_key = ins_stmt.on_duplicate_key_update(
                    descr = ins_stmt.inserted.descr,
                    modified_at = datetime.now() #ins_stmt.inserted.modified_at
                )

                conn.execute(ins_on_duplicate_key)
            return "Currency Details Update Completed Successfully"

    def updateDailyRatesTable():
        rates = fetchOn.fetchDailyRates()

        with conn:
            for k, v in rates.items():
                ins_stmt = insert(daily_rates).values(
                    symbol = k, 
                    rate = v
                )

                ins_on_duplicate_key = ins_stmt.on_duplicate_key_update(
                    rate = ins_stmt.inserted.rate,
                    modified_at = datetime.now() #ins_stmt.inserted.modified_at
                )
                conn.execute(ins_on_duplicate_key)
        return "Rates Update Completed Successfully"

# obj = fetchmap.fetchCoins()[0]
print(dbUpdate.updateCurrencyDetailsTable())
 
# s3 = boto3.client('s3')
# json_object = obj
# s3.put_object(
#      Body=json.dumps(json_object),
#      Bucket=bucket,
#      Key='filetest.json'
# )
# print('All Done')