# Importing needed Pyspark functions
from pyspark.shell import spark
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.window import Window

# Function to save datasets to the Silver Layer
def save_gold(df, name):
    '''Save dataset to the silver folder
    Inputs:
    name: str = name of the folder in the silver layer'''

    # Build path in the Silver Layer
    file_path = '/user/ec2-user/UKUSMarHDFS/Arun/Stocks/gold/'+name
    #file_path = f'/FileStore/tables/gold/{name}'
    df.coalesce(1).write.option('header','true').format('csv').mode('overwrite').save(file_path)

    #df.coalesce(1).write.format('parquet').mode('overwrite').save(file_path)
    print('File'+name+ 'saved successfuly.')


# Open the Stocks table
stocks = spark.read.option('header','true').csv("/user/ec2-user/UKUSMarHDFS/Arun/Stocks/silver/stocks/")
#stocks = spark.read.parquet('/FileStore/tables/silver/stocks')

# Creating dataset of stock prices by week
weekly_stocks = (
    stocks # dataset
    .groupBy('ticker', F.year('date').alias('year'), F.weekofyear('date').alias('week')) # group by ticker, year and week
    .agg( F.first('open').alias('open'), # Get open value
          F.max('high').alias('high'), # get high for the week
          F.min('low').alias('low'), # get low for the week
          F.last('close').alias('close'), # get close for the week
          F.sum('volume').alias('volume') ) # calc total volume for the week
    .filter( F.concat(col('year'), col('week')) != '20191' ) # remove first week 2019, as 2019-12-31 is being considered as week 1 in 2020 and it's calculating wrong the high open and close
    .sort('ticker', 'year', 'week')
    )
weekly_stocks.write.mode('overwrite').saveAsTable('ukusmar.weekly_stocks')
# Save dataset to the Gold Layer
save_gold(weekly_stocks, 'weekly_stocks')

# Creating dataset of stock prices by month
monthly_stocks = (
    stocks # dataset
    .groupBy('ticker', F.year('date').alias('year'), F.month('date').alias('month')) # group by ticker, year and month
    .agg( F.first('open').alias('open'), # Get open value
          F.max('high').alias('high'), # get high for the month
          F.min('low').alias('low'), # get low for the month
          F.last('close').alias('close'), # get close for the month
          F.sum('volume').alias('volume') ) # calc total volume for the month
    .sort('ticker', 'year', 'month')
    )
monthly_stocks.write.mode('overwrite').saveAsTable('ukusmar.monthly_stocks')

# Save dataset to the Gold Layer
save_gold(monthly_stocks, 'monthly_stocks')