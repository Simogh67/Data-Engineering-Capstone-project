class Create():
    """This class creates all necessary tables"""
    def __init__(self,spark,table):
        self.spark=spark
        self.table=table
        
    def create_time_table(self):
        self.table.createOrReplaceTempView("temp")
        self.table=self.spark.sql(""" select                                                   monotonically_increasing_id()
                         as time_id,
                         Start_time as start_time, 
                         End_time as end_time,
                         TimeZone as time_zone 
                         from temp""")
        self.table=self.table.dropDuplicates(['start_time',                                                         'end_time','time_zone'])
        return self.table
    def create_address_table(self):
        self.table.createOrReplaceTempView("temp")
        self.table=self.spark.sql("""select                                                          monotonically_increasing_id()
                             as address_id,
                             State as state,
                             City as city,
                             County as county,
                             Street as street,
                             Side as side,
                             ZipCode as zipcode
                             from temp""")
        self.table=self.table.dropDuplicates(['state','city',
                                            'county','street','side'])
        return self.table
    
    def create_airport_table(self):
        self.table.createOrReplaceTempView("temp_airport")
        self.table=self.spark.sql("""select                                                           monotonically_increasing_id()
                             as airport_id, 
                             ident as airport_code,
                             a.type,
                             region,
                             municipality,
                             gps_code,
                             iata_code,
                             local_code,
                             elevation_ft as elevation,
                             airport_longitude,
                             airport_latitude
                             from temp_airport a
                             join temp t
                             on t.AirportCode=a.ident""")
        self.table=self.table.dropDuplicates(['airport_code'])
        return self.table
    
    def create_weather_table(self):
        self.table.createOrReplaceTempView("temp_weather")
        self.table=self.spark.sql("""select w.EventId as weather_id,
                              w.Type as type,
                              w.Severity as severity,
                              w.City as city,
                              w.County as county,
                              w.Start_time as start_time,
                              w.End_time as end_time
                              from temp_weather as w
                              join temp as t
                              on w.City=t.City and 
                              w.County=t.County and
                              t.Start_time between w.Start_time and                                         w.End_time 
                              """)
        self.table=self.table.dropDuplicates(['weather_id'])
        return self.table
    
    def create_fact_table(self):
        self.table.createOrReplaceTempView("temp")
        self.table=self.spark.sql(""" select t.EventId as event_id,
                              t.Type as type,
                              t.Severity as severity,
                              TMC as tmc,
                              Description as description,
                              LocationLat as location_latitude,
                              LocationLng as location_longtitude,
                              Distance as distance,
                              Number as number,
                              elevation,
                              airport_longitude,
                              airport_latitude,
                              time_id,
                              address_id,
                              airport_id,
                              weather_id
                              from temp t
                              left join temp_time t_t
                              on t.Start_time=t_t.start_time and
                              t.End_time=t_t.end_time and
                              t.TimeZone=t_t.time_zone
                              left join temp_airport a
                              on a.airport_code=t.AirportCode
                              left join temp_address ad
                              on t.city=ad.city and 
                              t.county=ad.county and 
                              t.street=ad.street and
                              t.side=ad.side
                              left join temp_weather w
                              on t.city=w.city and 
                              t.county=w.county and 
                              t.start_time between 
                              w.start_time and w.end_time
                              """)
        self.table=self.table.dropDuplicates(['event_id'])
        return self.table