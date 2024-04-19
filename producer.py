from kafka import KafkaProducer
import json

# Raw Package
import numpy as np
import pandas as pd

# Yahoo Data Source 
import yfinance as yf

# Set the ticker, period and interval required  

ticker = 'AAPL'

#data = yf.download(tickers=['ORCL'], start="2023-01-01", end="2023-07-19", interval='5m')
#data = yf.download(tickers='ORCL', period="60d", interval='5m')
#data = yf.download(tickers='AAPL', period="60d", interval='5m')
#data = yf.download(tickers='MSFT', period="60d", interval='5m')
data = yf.download(tickers=ticker, period="60d", interval='5m')


# Gathering the data 
   
df = pd.DataFrame(data)

# Create a empty dictionary 
datatrade = dict()

# Set the kafka topic name 

kafka_topic="quickstart-events"

# Set the Kafka bootstrap servers

kafka_bootstrap_servers = "localhost:9092"

# Create a Kafka producer

producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

# Iterate through the "Datetime" column to get datetime value for each event in a row

# Iterate over index 

for index, values in data.iterrows():
    datatrade['Ticker'] = ticker # Adding a new key-value pair for Stock Symbol
    datatrade['Datetime'] = index.strftime('%Y-%m-%d %H:%M:%S') # Convert Timestamp to String 

    # Iterate over values for adding a new key-value pair

    for item in values:
       datatrade['Open'] = values["Open"] 
       datatrade['High'] = values["High"] 
       datatrade['Low'] = values["Low"] 
       datatrade['Close'] = values["Close"] 
       datatrade['Adj Close'] = values["Adj Close"] 
       datatrade['Volume'] = values["Volume"]
    #print(datatrade)
    #break

    # Convert datatrade dictionary to JSON  

    # Serializing json
    
    json_object = json.dumps(datatrade, indent = 4) 

    # Sending a single message as a string 

    message = str(json_object)

    print(message)

    producer.send(kafka_topic, value=message.encode("utf-8"))

    # Wait for the message to be sent and acknowledged by the broker
    producer.flush()

# Close the Kafka producer to release resources
producer.close()
