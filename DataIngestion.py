
import warnings
from pyspark.sql.session import SparkSession

warnings.simplefilter(action='ignore', category=FutureWarning)
import requests
import time
#from pyspark.shell import spark
# Importing needed Pyspark functions
from pyspark.sql import functions as F
from pyspark.sql import Row

# Importing Struct Types
from pyspark.sql.types import StructField, StructType, DoubleType, StringType, IntegerType

API_KEY = 'UWKMNAFCSQ9J1A83v'
API_TWELVE = "8fc73ee45b444b7fa7d27e539bb53094"

spark = SparkSession.builder.appName("DataIngestion").getOrCreate()

#def get_data(ticker, size='compact', API_KEY=API_KEY):
def get_data(ticker,start_date=None,end_date = None, size='compact', API_KEY=API_KEY):
    '''
    Function to get the stock daily historic data for a ticker from the Alpha Vantage API
    Inputs:
    * Ticker = Stock code: str
    * size = 'full' for 20 years of historic data or 'compact' for the last 100 data points: str

    Returns:
    Data frame with the stock historic data
    '''
    # headers = {'Cookie':'AEC=AQTF6Hw70qZl_HKJhZzeMSZjVLYL4xzv6nQLO2uUkZzxwiR5nOR-aB6O9N8; SOCS=CAISHAgCEhJnd3NfMjAyNDA0MTUtMF9SQzUaAmVuIAEaBgiA6IaxBg; NID=513=jya9cwG3soxM-LPai4PiWaJ2uJU0tjl0CDnpHQ-zpo7OPk6XeLiW5xxn94JztwyvNfO4GeJFLGfik3cCQ15kZTRI7gaSw2srVPGtopt58T81KAVTFNwRf9nEYINZZVvboAa3q_Dq-QT0tmN-1MdiljegFgIpyQKjUKBMtn4qg0n1ZekDcj8xc--nPgUymPMnhkoQT0k-'
    #            }
    # Get Data from Alpha Vantage Open API
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&outputsize={size}&apikey={API_KEY}'
    r = requests.get(url,verify=False)
    #Get only the json file output
    data = r.json()

    # Get only the time series, discarding the meta attribute
    dtf = data['Time Series (Daily)']

    # Transform JSON data into a list of Row objects
    rows = [Row(date=key, **{k: float(v) if k != '5. volume' else int(v) for k, v in value.items()}) for key, value in
            dtf.items()]

    # Define the schema for the DataFrame
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", IntegerType(), True)
    ])

    # Create a DataFrame
    df = spark.createDataFrame(rows, schema)

    print('get_data-------',df)
    return df

def get_economic_ind(indicator,start_date ,end_date ,API_KEY=API_KEY):  #REAL_GDP_PER_CAPITA ,INFLATION
    '''
    Get Economic indicator data from Alpha Vantage API
    Indicator: str = e.g. 'REAL_GDP_PER_CAPITA', 'INFLATION', 'CPI'
    '''
    # Get Data from Alpha Vantage Open API
    url = 'https://www.alphavantage.co/query?function={indicator}&apikey={API_KEY}'
    r = requests.get(url,verify=False)
    data = r.json()
    # Transform to dataframe
    dtf = data["Technical Analysis: RSI"]

    # Transform JSON data into a list of Row objects
    rows = [Row(date=key, **{k: float(v) if k != '5. volume' else int(v) for k, v in value.items()}) for key, value in
            dtf.items()]

    # Define the schema for the DataFrame
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("value", DoubleType(), True)
    ])

    # Create a DataFrame
    df = spark.createDataFrame(rows, schema)

    print(indicator+ ' data fetched')
    df.show()
    return df

def get_RSI(ticker,start_date ,end_date, API_KEY=API_KEY):
    '''
    Get Relative Strength data from Alpha Vantage API
    ticker: str = stock code
    '''
    # Get Data from Alpha Vantage Open API
    url = 'https://www.alphavantage.co/query?function=RSI&symbol={ticker}&interval=daily&time_period=14&series_type=close&apikey={API_KEY}'
    r = requests.get(url,verify=False)
    data = r.json()
    dtf = data["Technical Analysis: RSI"]

    # Transform JSON data into a list of Row objects
    # Construct rows for DataFrame creation
    rows = [Row(date=key, value=float(value['RSI'])) for key, value in dtf.items()]

    #rows = [Row(date=key, **{k: float(v) if k != '5. volume' else int(v) for k, v in value.items()}) for key, value in
    #        dtf.items()]

    # Define the schema for the DataFrame
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("value", DoubleType(), True)
    ])

    # Create a DataFrame
    df = spark.createDataFrame(rows, schema)

    print('RSI data for' +ticker + 'was fetched')

    print('get_rsi-------',df)
    # Return
    return df


def save_data(df, ticker):  #ticker='/RSI/'+ticker)
    '''Function to save a dataframe to Databricks File System in a given file path'''

    # Error Handling
    try:
        # Create the path to save the file

        path = '/user/ec2-user/UKUSMarHDFS/Arun/Stocks/' + ticker
        #path = f'/home/ec2-user/UKUSMar/Arun/Stocks/{ticker}'
        # Write file to DBFS
        df_new=df.coalesce(1)
        df_new.show()
        df = df.orderBy(df["date"].desc())
        df.coalesce(1).write.option('header','true').csv(path,mode='overwrite')
        print('{ticker} stock successfully saved to DBFS.')

    except Exception as e:
        return 'Failed fetching {ticker} data. {e}'


# Tickers to be fetched from the Alpha Vantage API
#tickers = ['TMUS', 'T', 'VZ', 'CMCSA', 'CHTR']
tickers = ['AAPL']

# Extracting the Data from the API
for ticker in tickers:
    # Extract
    #df_extracted = get_data(ticker=ticker, size='full')
    start_date = '2023-12-01'
    end_date = '2023-12-31'
    #df_extracted = get_data(ticker=ticker, size='compact')
    df_extracted = get_data(ticker=ticker, start_date=start_date,end_date=end_date,size='compact')
    print('gfd',df_extracted)
    # Save
    save_data(df_extracted, ticker=ticker)
    time.sleep(2)


# Economic Indicators to be fetched from the Alpha Vantage API
indicators = ['REAL_GDP_PER_CAPITA', 'INFLATION', 'CPI']

# Extracting the Data from the API
for indicator in indicators:
    # Extract
    start_date = '2023-12-01'
    end_date = '2023-12-31'
    df_extracted = get_economic_ind(indicator=indicator,start_date = start_date,end_date=end_date)  #REAL_GDP_PER_CAPITA,INFLATION
    # Save
    save_data(df_extracted, ticker=indicator)
    time.sleep(4)


# Tickers to be fetched from the Alpha Vantage API
#tickers = ['TMUS', 'T', 'VZ', 'CMCSA', 'CHTR']
tickers = ['AAPL']
# Extracting the Data from the API
for ticker in tickers:
    # Extract
    start_date = '2023-12-01'
    end_date = '2023-12-31'
    df_extracted = get_RSI(ticker= ticker)
    # Save
    save_data(df_extracted, ticker='/RSI/'+ticker)
    time.sleep(4)

    # Tickers to be fetched from the Alpha Vantage API
    #telco_index = 'DJUSTL'
    # Get Data from Alpha Vantage Open API
    #url = 'https://api.twelvedata.com/time_series?apikey={API_TWELVE}&interval=1day&symbol={telco_index}&format=JSON&outputsize=5000'
    #r = requests.get(url,verify=False)
    #data = r.json()
    # Create dataframe
    #df = spark.createDataFrame(data['values'])

    # Save to the Raw Folder
    # path = f'/FileStore/tables/raw/{telco_index}'
    ##path = f'C:/Users/Public/Documents/PySpark/Telecom/{telco_index}'
    #path = '/user/ec2-user/UKUSMarHDFS/Arun/Telecom/{telco_index}'
    #path = f'C:/Users/Public/Documents/PySpark/Telecom/{telco_index}'
    # Write file to DBFS
    # df.coalesce(1).write.format('csv').mode('overwrite').save(path)
    #df.coalesce(1).write.csv(path, mode='overwrite')
    # df.coalesce(1).write.format('json').mode('overwrite').save(path)