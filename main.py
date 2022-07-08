# importing necessary libraries and initializing spark
from pyspark.sql import SparkSession
import shutil
from clean import Clean
from create import Create


def create_spark_session():
    spark = SparkSession.builder\
        .master("local")\
        .appName("Udacity")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()
    return spark

def read_file(file,spark):
    df=spark.read.csv(file,header=True)
    df.dropDuplicates()
    return df

def delete_parquet(self,file):
    shutil.rmtree(file)

def main():
    # initializing a spark session 
    spark = create_spark_session()
    
    # reading csv files
    df_traffic=read_file("TrafficEvents_Aug16_Dec20_Publish.csv",spark)
    df_weather=read_file("WeatherEvents_Aug16_Dec20_Publish.csv",spark)
    df_airport= read_file("airport-codes_csv.csv",spark)
    
    # removing parquet files if any 
    shutil.rmtree('Traffic.parquet')
    shutil.rmtree('Weather.parquet')
    #shutil.rmtree('Fact.parquet')
    
    # cleaning weather, traffic and airport tables 
    item_clean=Clean(spark,df_traffic)
    df_truncate=item_clean.clean_traffic_data()
    item_weather=Clean(spark,df_weather)
    df_weather_truncate=item_weather.clean_weather_data()
    item_airport=Clean(spark,df_airport)
    df_airport=item_airport.clean_airport_data()
    
    # writing traffic and weather tables on parquet
    df_truncate.write.parquet("Traffic.parquet") 
    df_truncate=spark.read.parquet("Traffic.parquet")
    df_weather_truncate.write.parquet("Weather.parquet") 
    df_weather_truncate=spark.read.parquet("Weather.parquet")
    
    # creating time table
    item_time=Create(spark,df_truncate)
    time_table=item_time.create_time_table()
    time_table.createOrReplaceTempView("temp_time")
    
    # creating address table
    item_address=Create(spark,df_truncate)
    address_table=item_address.create_address_table()
    address_table.createOrReplaceTempView("temp_address")
    
    # creating airport table
    item_airport=Create(spark,df_airport)
    airport_table=item_airport.create_airport_table()
    airport_table.createOrReplaceTempView("temp_airport")
    
    # creating weather table
    item_weather=Create(spark,df_weather_truncate)
    weather_table=item_weather.create_weather_table()
    weather_table.createOrReplaceTempView("temp_weather")
    
    # creating fact table
    item_fact=Create(spark,df_truncate)
    fact_table=item_fact.create_weather_table()
    fact_table.createOrReplaceTempView("temp_fact")
    
    #sanity checks: check primary keys
    check_primary_keys(airport_table)
    check_primary_keys(time_table)
    check_primary_keys(address_table)
    check_primary_keys(weather_table)
    check_primary_keys(fact_table)
    
    # check rows
    check_exists_row(airport_table)
    check_exists_row(time_table)
    check_exists_row(address_table)
    check_exists_row(weather_table)
    check_exists_row(fact_table)
    
    
if __name__ == "__main__":
    main()