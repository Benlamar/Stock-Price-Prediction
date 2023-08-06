import json
import boto3
import sys
import yfinance as yf

import time
import random
import datetime


# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream

kinesis = boto3.client('kinesis', region_name = "us-east-1") #Modify this line of code according to your requirement.

today = datetime.date.today()
yesterday = datetime.date.today() - datetime.timedelta(1)

# Example of pulling the data between 2 dates from yfinance API
stocklist = ["MSFT", "MVIS", "GOOG", "SPOT", "INO", "OCGN", "ABML", "RLLCF", "JNJ", "PSFE"]
message ={}

## Add code to pull the data for the stocks specified in the doc
for stock in stocklist:
    data = yf.download(stock, start= "2022-04-14", end= "2022-04-16", interval = '1h')
    print("*****",stock,"*****")
    # print(data.info)
    tk = yf.Ticker(stock)
    timelist = data.index
    for i in range(len(timelist)):
        message['id'] = stock
        message['timestamp'] = timelist[i].strftime('%Y-%m-%d %X')
        message['close'] = round(data.iloc[i].Close,2)
## Add additional code to call 'info' API to get 52WeekHigh and 52WeekLow refering this this link - https://pypi.org/project/yfinance/
        high = tk.info["fiftyTwoWeekHigh"]
        low = tk.info["fiftyTwoWeekLow"]
        message['fiftyTwoWeekHigh'] = round(high,2)
        message['fiftyTwoWeekLow'] = round(low,2)
        print("Streaming to kinesis", message)
## Add your code here to push data records to Kinesis stream.
        kinesis.put_record(StreamName="StockStream",Data=json.dumps(message),PartitionKey="timestamp")
        time.sleep(1)