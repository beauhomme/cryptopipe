import configparser
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date, datetime
from configparser import ConfigParser
from sqlalchemy import create_engine, Table, Column,  String, Integer, MetaData,  select, Numeric, DateTime, text, Float
from sqlalchemy.dialects.mysql import insert
import boto3
import io
from pycoingecko import CoinGeckoAPI
from sqlalchemy.sql.expression import update
import time



file = 'config.ini'
config = ConfigParser()
config.read(file)

user = config['DATABASE']['LOCAL_USERNAME']
passw = config['DATABASE']['LOCAL_PASSWORD']
dbHost = config['DATABASE']['LOCAL_HOST']
dbName = config['DATABASE']['DB']
bucket = config['S3']['BUCKET']
app_id = config['EXCHANGE']['APP_ID']
aws_dbuser = config['DATABASE']['AWS_USERNAME']
aws_dbpassw = config['DATABASE']['AWS_PASSWORD']
aws_dbHost = config['DATABASE']['AWS_HOST']
# baseurl = config['EXCHANGE']['BASE_EXCHANGE']
# symbols = config['EXCHANGE']['SYMBOLS']

# engine = create_engine(f'mysql://{user}:{passw}@localhost/sakila')
# engine = create_engine(f'mysql+pymysql://{user}:{passw}@{dbHost}/{dbName}')
engine = create_engine(f'mysql+pymysql://{aws_dbuser}:{aws_dbpassw}@{aws_dbHost}/{dbName}')

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
   Column('coinId', String(15), nullable=False),
   Column('coinName', String(100), nullable=False),
   Column('coinSymbol', String(10), primary_key=True, nullable=False),
   Column('coinLoc', String(15), nullable=False),
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

        # The function takes a page number parameter, each page returns a list of 100 different Coins.
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
                    coinLoc = results[item].find('a', attrs={'class':'tw-hidden lg:tw-flex font-bold tw-items-center tw-justify-between'}).get("href").split('/')[3]
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
                                coinLoc = coinLoc,
                                coinPrice=coinPrice,
                                coin1hrChange=coin1hrChange,
                                coin24hrChange=coin24hrChange,
                                coin7dChange=coin7dChange,
                                coin24hrVol=coin24hrVol,
                                coinMarketCap=coinMarketCap,
                                fetchTime = str(datetime.now())
                            )
                coinBase.append(coinBag)
        return coinBase
        
    def fetchDailyRates():
        baseurl = 'https://openexchangerates.org/api/latest.json?app_id='
        symbols = 'NGN,AUD,GBP,EUR,JPY,CNY,CAD'
        exurl = f'{baseurl}{app_id}&symbols={symbols}'
        response = requests.get(exurl).json()
        return response['rates']

    def fetchOnCoinsNairaRate():
        coinList = fetchOn.fetchCoins()
        nairaRate = fetchOn.fetchDailyRates()['NGN']
        for i in coinList:
            i['rate'] = float(nairaRate)
            i['coinPriceNaira'] = float(nairaRate) * i['coinPrice']
        return coinList

    def fetchCurrencyDetails():
        baseurl = 'https://openexchangerates.org/api/currencies.json?app_id='
        exurl = f'{baseurl}{app_id}'
        response = requests.get(exurl).json()
        return response

    def fetchCoinsToS3(state='clo'):  # pass 'wts' to write results to s3)
        p = fetchOn.fetchOnCoinsNairaRate()
        df_coinmarket = pd.DataFrame(p)
        df_coinmarket['name']=df_coinmarket['coinName']

        cg = CoinGeckoAPI()
        p2 = cg.get_coins_list()
        df_coinList = pd.DataFrame(p2)
        df_coinList

        df3= pd.merge(df_coinmarket, df_coinList[['id', 'name']], on='name', how='inner')
        df3.rename(columns={"id" : "coinId"}, inplace=True)
        df3 = df3[["coinRank","coinId","coinName","coinSymbol","coinLoc","coinPrice","coin1hrChange","coin24hrChange","coin7dChange","coin24hrVol","coinMarketCap","fetchTime","rate","coinPriceNaira"]]

        coinIdlist = list(df3['coinId'])

        if state == 'wts':

            s3 = boto3.client("s3")
            with io.StringIO() as json_buffer:
                df3.to_json(json_buffer, orient='records')

                response = s3.put_object(
                    Bucket=bucket, Key=f'coinmarketdetails/coinmarket-{datetime.now().strftime("%Y-%m-%d")}.json', Body=json_buffer.getvalue()
                )

                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

                if status == 200:
                    s3status =  (f"Successful S3 put_object response. Status - {status}")
                    st = dict (status=s3status,coinIdlist=coinIdlist)
                    return(st)
                else:
                    return (f"Unsuccessful S3 put_object response. Status - {status}")
        else:
            return coinIdlist

            coinIdlist = fetchCoinsToS3()

    def fetchOhlc():
        print(f'{str(datetime.now())} - fetching coinId\'s from Dataframe')
        coinIdlist = fetchOn.fetchCoinsToS3()
        combinedFrame = pd.DataFrame()
        num = 0
        max = len(coinIdlist)
        cg = CoinGeckoAPI()

        for coinid in coinIdlist[num : max+1]:
            cf = cg.get_coin_ohlc_by_id(coinid, 'usd', 1)
            df = pd.DataFrame(cf, columns=['timestamp','open','high', 'low','close'])
            df['coinId'] = coinid
            df['dateTime'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000.0))
            df['timeFrame'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000.0).strftime("%H:%M"))
            combinedFrame = combinedFrame.append(df, ignore_index=True)
            time.sleep(1)

        s3 = boto3.client("s3")

        with io.StringIO() as json_buffer:
            combinedFrame.to_json(json_buffer, orient='records')

            response = s3.put_object(
                Bucket='cryptstreamdemo', Key=f'coinohlcdetails/coinohlc-{datetime.now().strftime("%Y-%m-%d")}.json', Body=json_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print (f"Successful S3 put_object response. Status - {status}")
            else:
                print (f"Unsuccessful S3 put_object response. Status - {status}")


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


class fetchDb():

    def fetchCurrencyRateDb(symbol):
      
        with conn:
        # user_actions = Table('actor', meta2, autoload_with = conn)
            stmt = text(f"select rate from daily_rates where symbol = '{symbol}'")
            result  = conn.execute(stmt)
            rate = result.first()
        # print(rate[0])
        return rate[0]

    def fetchCoinsNairaRate():
        p = fetchOn.fetchCoins()
        nairaRate = fetchDb.fetchCurrencyRateDb('NGN')
        for i in p:
            i['rate'] = float(nairaRate)
            i['coinPriceNaira'] = float(nairaRate) * i['coinPrice']
        return p




# obj = fetchmap.fetchCoins()[0]
print(fetchOn.fetchOhlc())
 
# s3 = boto3.client('s3')
# json_object = obj
# s3.put_object(
#      Body=json.dumps(json_object),
#      Bucket=bucket,
#      Key='filetest.json'
# )
# print('All Done')