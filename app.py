import requests
import json
import os


id = 'bitcoin'
currency = 'usd'

url = f'https://api.coingecko.com/api/v3/simple/price?ids={id}&vs_currencies={currency}'

t = requests.get(url).json()

print(t)
# t = json.loads(t)
price = int((t['bitcoin']['usd']))



