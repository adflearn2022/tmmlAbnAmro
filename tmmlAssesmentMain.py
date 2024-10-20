"""
Environment: 
    Python 3.11.9, pip 24.0
    PySpark 3.5.1 (Scala 2.12.18, OpenJDK 17.0.10)

"""


from pyspark.sql import SparkSession
from transactionProcessor import transactionProcessor

#spark config changes requires while its impacting the processing time
"""
    .config("spark.executor.memory", "4g")\
    .config("spark.driver.memory", "2g")\
    .config("spark.executor.cores","4")\
    .config("spark.sql.suffle.partitions","200")\
    .config("spark.sql.autoBroadCastJoinThreshold", "10485760")\ """

def tmml_assessment():
  
  spark = SparkSession.builder \
    .appName("abnAmroPoc") \
    .getOrCreate()
    
  cluster_url = spark.sparkContext.master
  print("cluster url : ", cluster_url)
  print(spark.sparkContext.getConf().getAll())
  print("spark version : ", spark.version)
  
  configPath = "/home/user/data/configuration.json"
  
  transactionProcessor.transactionProcessor(spark, configPath)
  

if __name__ == '__main__':
  tmml_assessment()
