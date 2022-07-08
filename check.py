from  pyspark.sql.functions import col, when, count
class Check():
    """This class checks that if the dataframe is empty or not, and if there is any null values in the primary keys"""
    
    def __init__(self,table):
        self.table=table
    def check_exists_row(self):
        print('number of rows:{}'.format(self.table.count()))
        assert self.table.count()>0, 'Error! There is no row'
    
    def check_primary_keys(self):
        check_table=self.table.select([count(when(col(self.table.columns[0])
                                                 .isNull(), self.table.columns[0])).alias(self.table.columns[0])])
        if check_table.take(1)[0][0]>0:
            raise ValueError(f"Data quality check failed! Found NULL values  in the primary key!")
        else:
            print('Data quality check passed')