import os
from sys import exit
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
from yaml import load, dump
try:
        from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
        from yaml import Loader, Dumper
from influxdb import InfluxDBClient
import datetime

try:
    f = open(os.path.dirname(os.path.realpath(__file__) + "/" + "config.yml", "r")
except IOError:
    print("Error while opening file, please check if config.yml exists.")
    exit(1)

configRaw = f.read()
configYaml = load(configRaw, Loader=Loader)
baseUrl = configYaml["coinmarketcap"]["baseUrl"]
apiToken = configYaml["coinmarketcap"]["token"]
influxDbServer = configYaml["influxdb"]["address"]
influxDbPort = configYaml["influxdb"]["port"]
username = configYaml["influxdb"]["username"]
password = configYaml["influxdb"]["password"]

headers = {
  'Accepts': 'application/json',
  'X-CMC_PRO_API_KEY': apiToken,
}

session = Session()
session.headers.update(headers)

def getMarketData():
    url = baseUrl + '/cryptocurrency/listings/latest'
    parameters = {
        'start':'1',
        'limit':'200',
        'convert':'USD'
    }
    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
    data = data["data"]
    dataDict = {
        "measurement": "price",
        "fields": {}
    }
    for i in range(len(data)):
        symbol = data[i]["symbol"]
        quote = data[i]["quote"]
        USDprice = quote["USD"]["price"]
        dataDict["fields"][symbol] = float(USDprice)
    dataDict["time"] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    return dataDict

def writeToInfluxDB(data):
    client = InfluxDBClient(username=username,password=password,host=influxDbServer,port=influxDbPort)
    version = client.ping()
    print("Connected!")
    client.write_points([data], database='coinmarketcap')
    return

if __name__ == '__main__':
    data = getMarketData()
    writeToInfluxDB(data)
