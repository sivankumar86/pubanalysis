import sys
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
import argparse, sys
from pyspark.sql.functions import udf
import math

"""
  Pub analysis in England
"""
class Qantas_exam(object):
    def __init__(self,target,df):
        self.target=target
        self.df=df
    ##1.Which is the most isolated pub in England ?
    def isolated_pub(self):
        """
         - Find the center location 
         - Calcualte distance from center and print which has maximum distance
         
        """
        center=self.df.where(col('latitude').isNotNull()& col('longitude').isNotNull()) \
                .select(avg('latitude').alias('latitude'),avg('longitude').alias('longitude')) \
                .rdd.map(lambda l:(l.latitude,l.longitude)).collect()
        #print(center)
        out=self.df.where(col('latitude').isNotNull()& col('longitude').isNotNull()) \
             .withColumn('distance',distance_udf(col('latitude'),col('longitude'),lit(center[0][0]),lit(center[0][1]))) \
             .sort(desc("distance")).select('name').limit(1)
        out.withColumnRenamed('name','most isolated pub in England') \
            .write.mode("overwrite").csv('{}isolatedpub/'.format(self.target))
        return out.rdd.map(lambda row:row.name).collect()


    ##2) Which local_authority has the least number of pubs?

    def least_num_pubBylocal_auth(self):
        """
        - total pub by each local_authority
        - return local_authority which has minimum value 
        """
        out=self.df.groupby('local_authority').agg(count('name').alias('lcount')) \
            .sort(asc("lcount")).select("local_authority").limit(1) 
        out.withColumnRenamed('local_authority','local_authority who has the least number of pubs') \
            .write.mode("overwrite").csv('{}local_authority/'.format(self.target))
        return out.rdd.map(lambda l:l.local_authority).collect()
         

    

    ##3) Which 5 words are the most common words used in the English pub names ?

    def topCommonwords(self,value=5):
        """
        - count the words in name column after removing non alphanumeric value
        - sort in decending and limit 5 results. 
        - ToDo remove the stop words 
        """
        out=self.df.withColumn('word', explode(split(col('name'), ' '))) \
            .withColumn('norm_word',trim(regexp_replace('word','[^a-zA-Z0-9 ]', ''))) \
            .filter(col('norm_word') !='')\
            .groupBy('norm_word')\
            .count()\
            .sort('count', ascending=False)\
            .select('norm_word').limit(value)
        out.withColumnRenamed('norm_word','Top english name in pubname').write \
            .mode("overwrite").csv('{}pubname/'.format(self.target))

        return out.rdd.map(lambda l:l.norm_word).collect()


    ## 4) Which Street in England has the highest number of pubs?
    def topstreet_pub(self):
        """
         - Extract street from address (assume splitted by comma)
         - find the street which has more pub
        """
        out=self.df.withColumn('street', split(col('address'), ',')[0]) \
                .groupby('street').agg(count('name').alias('st_count')) \
                .sort(desc("st_count")).select("street").limit(1) 
        out.withColumnRenamed('street','street which has highest number of pubs') \
            .write.mode("overwrite").csv('{}streetname/'.format(self.target))

        return out.rdd.map(lambda l:l.street).collect()


class Driver(object):
    def __init__(self):
        parser=argparse.ArgumentParser()
        parser.add_argument('--input', help='pass your s3 input path')
        parser.add_argument('--output', help='pass your s3 output path')
        args=parser.parse_args()
        self.spark = SparkSession.builder.appName('qantastest').getOrCreate()
        self.source=args.input ##'s3://athenaiad/aqantas/input/'
        self.target=args.output ##'s3://athenaiad/aqantas/output/'
        
    def transform(self):
        #customSchema = StructType([StructField('fas_id', IntegerType(),True), \
         #                          StructField('name', StringType(), True),StructField('address', StringType(), True), \
          #                         StructField('postcode', IntegerType(), True),StructField('easting', IntegerType(), True), \
           #                        StructField('northing', IntegerType(), True), StructField('latitude', DoubleType(), True), \
            #                       StructField('longitude', DoubleType(), True),StructField('local_authority', StringType(), True)])
        df= self.spark.read.option('header','true').option("quote", "\"").option("escape", "\"") \
                    .option("inferschema", "true").csv(self.source)
        df=df.withColumn('latitude',col("latitude").cast(DoubleType())) \
             .withColumn('longitude',col("longitude").cast(DoubleType())) 
        df.persist()        
        qantas_interview=Qantas_exam(self.target,df)
        print('Which is the most isolated pub in England ? \n')
        for i in qantas_interview.isolated_pub():
            print(i)
            print('\n')
        print('Which local_authority has the least number of pubs? \n')
        for i in qantas_interview.least_num_pubBylocal_auth():
            print(i)
            print('\n')
        print('Which 5 words are the most common words used in the English pub names ? \n')
        for i in qantas_interview.topCommonwords():
            print(i)
            print('\n')
        print('Which Street in England has the highest number of pubs?')
        for i in qantas_interview.topstreet_pub():
            print(i)
            print('\n')

        

        
        
def distance(lat1,lon1,lat2, lon2):
    """
    haversine formula
    www.movable-type.co.uk/scripts/latlong.html
    """
    radius = 6371 # km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    dist = radius * c

    return dist

distance_udf = udf(distance, DoubleType())


def main():
    driver=Driver()
    driver.transform()

if __name__== "__main__":
    main()
