# Importing needed Pyspark functions

from pyspark.shell import spark
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

def transform_data(ticker):
    '''
    Function that (1) loads a dataset;
    (2) adds a ticker column for the stock dataset;
    (3) Transform the column to datetime;
    (4) Drop data before 2019-01-01
    Inputs:
    * ticker: str = code to be added to all obervations
    '''

    # File path to be loaded
    #path = 'C:/Users/Public/Documents/{ticker}'
    path = '/user/ec2-user/UKUSMarHDFS/Arun/Stocks/RAW/'+ticker+'/*'
    #need to load data into Server as HDFS location

    # As the dataset for the ETF "DJUSTL" was pulled from a different API, it has the column date named as datetime. It needs to be corrected
    if ticker == "DJUSTL":
        df = (
            #spark.read.parquet(path)
            spark.read.csv(path)
            .withColumn('ticker', F.lit(ticker))
            .withColumnRenamed('datetime', 'date')
        )

    else:
        print("tansform_data started in else statment :")
        # (2) Add ticker column
        df = (
            # spark.read.parquet(path)
            spark.read.option('header','true').csv(path)
            .withColumn('ticker', F.lit(ticker))
        )

    # Steps (3) and (4)
    # If working with economic indices, adapt for less columns
    if ticker in ['INFLATION', 'REAL_GDP_PER_CAPITA', 'CPI']:
        print("tansform_data step 3 and 4 :")
        df = (df
            .select( 'ticker', 'value',
                      F.to_date('date').alias('date')  )
            .filter( col('date') >= '2021-12-31' ) #data cleanup, drop data before 2019
          )
    else:
        print("tansform_data in step4 :")
        df = (df
            .select( 'ticker', 'open', 'high', 'low', 'close', 'volume',
                      F.to_date('date').alias('date')  )
            .filter( col('date') >= '2021-12-31' ) #data cleanup, drop data before 2019
          )

    # Return transformed data
    return df

# Function to add RSI to the stock dataset
def add_RSI(df, ticker):
    '''Function to add RSI column to the stocks
    inputs:
    ticker: str = stock code'''

    # File path to be loaded
    #path =  f'C:/Users/Public/Documents/PySpark/Telecom/Files/{ticker}'
    path = '/user/ec2-user/UKUSMarHDFS/Arun/Stocks/RSI/'+ticker
    #path = f'/FileStore/tables/raw/RSI/{ticker}'
    #rsi = spark.read.parquet(path)
    rsi = spark.read.option('header','true').csv(path)  ### need to change

    # Transform date to datetime format
    rsi = (rsi
            .select( F.to_date('date').alias('date'),
                    col('value').alias('RSI')  )
            .filter( col('date') >= '2021-12-31' ) #data cleanup, drop data before 2019
    )

    # Add RSI to the dataset
    df = (df
          .join(rsi, on='date', how= 'inner')
          )

    # Return transformed data
    return df

names = ['CHTR', 'CMCSA', 'T', 'TMUS', 'VZ']


# Save transformed datasets
for folder in names:
    print("tansform_data started :")
    stock = transform_data(ticker=folder)
    print("tansform_data finished :")
    stock_bronze = add_RSI(stock, ticker=folder)
    print("add_RSI finished :")
    path = '/user/ec2-user/UKUSMarHDFS/Arun/Stocks/bronze/'+folder
    stock_bronze.coalesce(1).write.option('header','true').format('csv').mode('overwrite').save(path)
    print(folder + ' transformed')

# indicators = ['INFLATION', 'REAL_GDP_PER_CAPITA', 'CPI', 'DJUSTL']
#Save transformed datasets
# for folder in indicators:
#     df_ind = transform_data(ticker=folder)
#     path = '/user/ec2-user/UKUSMarHDFS/Arun/Stocks/bronze/'+folder
#     path = f'/FileStore/tables/bronze/{folder}'
    # df_ind.coalesce(1).write.format('parquet').mode('overwrite').save(path)
    # print(folder + 'transformed')
