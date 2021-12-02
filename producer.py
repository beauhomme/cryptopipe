from kafka import KafkaProducer
from json import dumps
import datetime
# import time

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

value_s = lambda x: dumps(x, default=json_serial).encode('utf-8')

producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                         value_serializer=lambda x: dumps(x, default=json_serial).encode('utf-8'))

KAFKA_TOPIC = 'coinstream'
bic = {'coinRank': 99,
 'coinName': 'kemkoin',
 'coinSymbol': 'KTC',
 'coinPrice': 56508.93,
 'coin1hrChange': -0.4,
 'coin24hrChange': -1.6,
 'coin7dChange': 0.2,
 'coin24hrVol': 30228018381.0,
 'coinMarketCap': 1068854353265.0,
 'fetchTime': '2021-12-02 16:58:46.655462',
 'rate': 413.70003,
 'coinPriceNaira': 23377746.036267903}



message = bic
print("Message to be sent: ", value_s(message))
producer.send(KAFKA_TOPIC, message)

print(dumps(bic, default=json_serial))
print(value_s(bic))
