from  pyspark.sql.functions import split,col,isnan, when, count

class Clean():
    """This class cleans staging tables"""
    def __init__(self,spark,table):
        self.spark=spark
        self.table=table

    def clean_traffic_data(self):
        self.table.withColumnRenamed("StartTime(UTC)","Start_time")\
         .withColumnRenamed("EndTime(UTC)","End_time")\
         .withColumnRenamed("Distance(mi)","Distance")\
         .createOrReplaceTempView("temp")
        self.df_truncate=self.spark.sql("""select * from temp where
                         Start_time>'2019-01-01' """)
        return self.df_truncate
    
    def clean_weather_data(self):
        self.table.withColumnRenamed("StartTime(UTC)","Start_time")\
              .withColumnRenamed("EndTime(UTC)","End_time")\
              .createOrReplaceTempView("temp_weather")
            
        self.df_weather_truncate=self.spark.sql("""select * from temp_weather where 
                           Start_time>'2019-01-01' """)
        return self.df_weather_truncate
    
    def clean_airport_data(self):
        split_coordinates = split(self.table['coordinates'], ',')
        split_region = split(self.table['iso_region'], '-')
        self.df_airport =self.table.select(self.table['*'],
                                           split_coordinates.getItem(0).
                                           alias('airport_longitude'),
                                           split_coordinates.getItem(1).
                                           alias('airport_latitude'),
                                           split_region.getItem(1).
                                           alias('region'))
        self.df_airport = self.df_airport.drop('coordinates','iso_region')
        return self.df_airport
