"""
Environment: 
    Python 3.11.9, pip 24.0
    PySpark 3.5.1 (Scala 2.12.18, OpenJDK 17.0.10)

"""


from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, regexp_replace, coalesce, lit, when
from pyspark.sql import functions as F
from datetime import datetime
from Utils import utils
from readUtils import readUtils
from actionUtils import actionUtils
from transformationUtils import transformationUtils

"""spark config changes requires while it impacting the processing time
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
  
  config = utils.load_config("/home/user/data/configuration.json")
  src_config = config['source']
  tgt_config = config['target']
  log_config = config['logging']
  
  logging = utils.logging(datetime, log_config['basepath'], log_config['logFileName'])
  logging.info("job started")
  
  # Data Ingestion:
  df_acnt =  readUtils.readSrcAsPdAndReturnAsDf(spark,src_config['basepath'], "accounts", src_config['type'],logging)
  df_cust =  readUtils.readSrcAsPdAndReturnAsDf(spark,src_config['basepath'], "customers", src_config['type'],logging)
  df_trans = readUtils.readSrcAsPdAndReturnAsDf(spark,src_config['basepath'], "transactions", src_config['type'],logging)
   
  # Data Transformation:
  #clear null values and considering null may occur in any of the columns
  df_clnsd_acnt = transformationUtils().dropNullValuesAndReturnDf(df_acnt,"accounts", logging)
  df_clnsd_cust = transformationUtils().dropNullValuesAndReturnDf(df_cust,"customers",logging)
  df_clnsd_trans = transformationUtils().dropNullValuesAndReturnDf(df_trans,"transactions",logging)
  
  #convert date column to datatime objects
  df_cust_dttime = df_clnsd_cust.withColumn("join_date", col("join_date").cast("timestamp"))
  df_trans_dttime = df_clnsd_trans.withColumn("transaction_date", col("transaction_date").cast("timestamp"))
  
  #create and format year month column in transactions
  df_trans_frmt = df_trans_dttime.withColumn("year_month", F.date_format(col("transaction_date"),"yyyy-MM"))
  
  #merge transactions dataset with the account dataset        
  df_trans_jn_acnt = transformationUtils().joinTransformation(df_trans_frmt, df_clnsd_acnt, "inner",\
                    [{'parentKey':'account_id','childKey':'account_id','operator':'==','use_and':False}], ["*"], ["customer_id","account_type","balance"], True)
  
  #merge updated transaction dataset with the customer dataset:
  df_trans_jn_cust = transformationUtils().joinTransformation(df_trans_jn_acnt, df_clnsd_cust, "inner", \
                    [{'parentKey':'customer_id','childKey':'customer_id','operator':'==','use_and':False}], ["*"], ["customer_name","join_date"], True)
                  
  #Data Analysis:
  #the total deposit and withdrawal amount for each account for each `year_month`
  df_sum_wd = df_trans_jn_cust.filter(col("transaction_type") == lit("withdrawal")).groupBy("account_id", "year_month").agg(F.sum("amount").alias("total_withdraw"))
  df_sum_dp = df_trans_jn_cust.filter(col("transaction_type") == lit("deposit")).groupBy("account_id", "year_month").agg(F.sum("amount").alias("total_deposit"))
  #the total balance for each account type for each `year_month`
  df_sum_ttl_bal = df_trans_jn_cust.groupBy("account_type","year_month").agg(F.sum("balance").alias("total_balance"))
  #average transaction amount per customer
  df_avg_amt_cust = df_trans_jn_cust.groupBy("customer_id").agg(F.avg("amount").alias("avg_transaction_amount"))
  
  # Data Export:
  actionUtils.writeOutputFile(spark,df_sum_wd,tgt_config['basepath'],tgt_config['loadMode'],"totalWithdrawal",tgt_config['type'],logging)
  actionUtils.writeOutputFile(spark,df_sum_dp,tgt_config['basepath'],tgt_config['loadMode'],"totalWithDeposit",tgt_config['type'],logging)
  actionUtils.writeOutputFile(spark,df_sum_ttl_bal,tgt_config['basepath'],tgt_config['loadMode'],"totalBalance",tgt_config['type'],logging)
  actionUtils.writeOutputFile(spark,df_avg_amt_cust,tgt_config['basepath'],tgt_config['loadMode'],"avgTransactionAmount",tgt_config['type'],logging)
  
  # Data Enrichment:
  #classifies transactions as either 'high', 'medium', or 'low' based on the transaction amount
  df_classifies = df_trans_frmt.withColumn("classifies", when(col("amount") > lit(1000), lit("high"))\
                               .otherwise(\
                                 when((col("amount") >= lit(500)) & (col("amount") <= lit(1000)),lit("medium"))\
                                 .otherwise(\
                                   when(col("amount") < lit(500), lit("low")).otherwise(lit("None"))
                                   )\
                                )\
                               )
  actionUtils.writeOutputFile(spark,df_classifies,tgt_config['basepath'],tgt_config['loadMode'],"enrichedClassifiedTransaction",tgt_config['type'],logging)
  #the number of transactions per classification per week
  df_week_number = df_classifies.withColumn("week_number", F.weekofyear(col("transaction_date"))- F.weekofyear(F.date_format(col("transaction_date"), "yyyy-MM-01"))+1)
  df_no_of_trans = df_week_number.groupBy("year_month", "week_number", "classifies").agg(F.count("*").alias("number_of_transactions"))
  actionUtils.writeOutputFile(spark,df_no_of_trans,tgt_config['basepath'],tgt_config['loadMode'],"enrichedTransactionCountPerWeek",tgt_config['type'],logging)
  #the transactions with the largest amount per classification
  df_classifies.groupBy("classifies").agg(F.max("amount").alias("max_trans_amount")).show()
  
  logging.info("job done")
  
  spark.stop()
  

if __name__ == '__main__':
  tmml_assessment()
