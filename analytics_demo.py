from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

# Set up MongoDB connection
mongo_uri = "mongodb+srv://farsim:database@mycluster.thlwhor.mongodb.net/"
db_name = 'kafka_stream'
collection_name = 'kafka_data'

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

# Fetching Stock Data with a Specific Price Range:
min_price = 10
max_price = 60
stock_data =  db.collection.find({'Close': {"$gte": min_price, "$lte": max_price}})
for document in stock_data:
    print(document)
    
#Fetching Stock Data with a Specific Volume Range
min_volume = 5000
max_volume = 20000
stock_data = db.collection.find({" db.olume": {"$gte": min_volume, "$lte": max_volume}})
for document in stock_data:
    print(document)

# Function to get all stock price data from MongoDB and transform it to a pandas DataFrame
def get_stock_data():
    stock_data = collection.find({})
    df = pd.DataFrame(stock_data)
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df['Close'] = df['Close'].astype(float)
    print(df)
    return df

# Function to calculate average stock price
def calculate_average_stock_price(stock_data):
    average_price = stock_data['Close'].mean()
    return average_price

# Function to find the highest stock price
def find_highest_stock_price(stock_data):
    highest_price = stock_data['Close'].max()
    return highest_price

# Function to find the lowest stock price
def find_lowest_stock_price(stock_data):
    lowest_price = stock_data['Close'].min()
    return lowest_price

# Function to calculate the total volume traded
def calculate_total_volume_traded(stock_data):
    total_volume = stock_data['Volume'].sum()
    return total_volume

# Function to plot stock prices over time
def plot_stock_prices(stock_data):
    plt.figure(figsize=(10, 6))
    plt.plot(stock_data['Datetime'], stock_data['Close'])
    plt.xlabel('Datetime')
    plt.ylabel('Close Price')
    plt.title('Stock Prices Over Time')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.show()
    
#Histogram of Stock Prices:
def Histogram(stock_data):
    plt.figure(figsize=(10, 6))
    plt.hist(stock_data['Close'], bins=50, alpha=0.6, color='blue')
    plt.xlabel('Price')
    plt.ylabel('Frequency')
    plt.title('Histogram of Stock Prices')
    plt.grid(True)
    plt.show()
    
#Scatter of Volume
def Scatter(stock_data):
    plt.figure(figsize=(10, 6))
    plt.scatter(stock_data['Volume'], stock_data['Close'], color='blue', alpha=0.3)
    plt.xlabel('Volume')
    plt.ylabel('Price')
    plt.title('Volume vs. Price')
    plt.grid(True)
    plt.show()

#Function to plot 30 Day Moving Average    
def Moving_Average(stock_data):
    stock_data['30-Day MA'] = stock_data['Close'].rolling(window=30).mean()
    plt.figure(figsize=(10, 6))
    plt.plot(stock_data['Datetime'], stock_data['30-Day MA'], label='30-Day Moving Average', color='orange')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.title('Stock Price with Moving Average')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True)
    plt.show()
    

# Main function
def main():
    stock_data = get_stock_data()
    average_price = calculate_average_stock_price(stock_data)
    highest_price = find_highest_stock_price(stock_data)
    lowest_price = find_lowest_stock_price(stock_data)
    total_volume = calculate_total_volume_traded(stock_data)

    print(f"Average stock price: {average_price}")
    print(f"Highest stock price: {highest_price}")
    print(f"Lowest stock price: {lowest_price}")
    print(f"Total volume traded: {total_volume}")

    # Plots
    plot_stock_prices(stock_data)
    Moving_Average(stock_data)
    Histogram(stock_data)
    Scatter(stock_data)

if __name__ == "__main__":
    main()
