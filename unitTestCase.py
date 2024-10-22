from pyspark.sql import SparkSession
from datetime import datetime
from Utils import utils
from readUtils import readUtils
from actionUtils import actionUtils
from transformationUtils import transformationUtils
from transactionProcessor import transactionProcessor
import pytest
from deltalake import DeltaTable
from pyspark.sql.functions import *
 

def testCase():

  spark = SparkSession.builder \
    .appName("testTransactionProcessor") \
    .getOrCreate()
     
  configPath = "/home/user/data/testCaseConfiguration.json"
  
  #case 1:- Reading the specific configuration File using the generic load_config function from the utils class
  config = utils.load_config(configPath)
  src_config = config['source']
  log_config = config['logging']
  tgt_config = config['target']
  print("read Config")
  print(src_config)
  
  #case 2:-  Using logging utiliy function, Configure the logging properties and 
  #           check the logging folder and files created dynamically based upon the session time along with the logging details 
  logging = utils.logging(datetime, log_config['basepath'], log_config['logFileName'])
  logging.info("job started")
  
  #case 3:- Using read utility function check data ingested from an input source to the dataframe using pandas along with logging details by using the src config properties.
  df_acnt =  readUtils.readSrcAsPdAndReturnAsDf(spark,src_config['basepath'], "accounts", src_config['type'],logging)
  #check if dataframe is not empty
  assert df_acnt.count() > 0, "DataFrame is empty"
  #check schema of the dataframe
  expected_columns = ["account_id","customer_id","account_type","balance"]
  assert df_acnt.columns == expected_columns, f"Expected columns {expected_columns} but got {df_acnt.columns}"
  
  def test_dataframes_are_equal_with_exception(df_expect,df_input):
    try:
      assert df_expect.exceptAll(df_input).isEmpty()
      assert df_input.exceptAll(df_expect).isEmpty()
    except AssertionError as e:
      pytest.fail("Dataframes are not equal -->> " + str(e))
      
  df_case4_expected = spark.createDataFrame(data=[
    ("acc_01","cust_01","checking",65.56),
    ("acc_02","cust_02","checking",48.38),
    ("acc_03","cust_01","savings",None),
    ("acc_04",None,"savings",47.11)], schema=["account_id","customer_id","account_type","balance"])
  #case 4:- Using transform replaceNanToNullValues utility function and check the NaN values to be replaced to null values.FileExistsError
  df_nan_null = transformationUtils().replaceNanToNullValues(df_acnt)
  test_dataframes_are_equal_with_exception(df_case4_expected,df_nan_null)
  
  df_case5_expected = spark.createDataFrame(data=[
    ("acc_01","cust_01","checking",65.56),
    ("acc_02","cust_02","checking",48.38)], schema=["account_id","customer_id","account_type","balance"])
  #case5:- Using transform dropNullValuesAndReturnDf Utility function and check all the rows conatining nulls to be dropped/cleared
  #        Also logged the total row count values for a dataset i.e., dropped/cleared 
  df_dropNull = transformationUtils().dropNullValuesAndReturnDf(df_acnt,"account", logging)
  test_dataframes_are_equal_with_exception(df_case5_expected,df_dropNull)
  
  df_case6_expected = spark.createDataFrame(data=[
    (1,"acc_01","2024-07-05",28.89,"deposit","cust_01","checking",65.56),
    (2,"acc_02","2024-01-11",35.75,"withdrawal","cust_02","checking",48.38),
    (5,"acc_01","2024-07-28",87.48,"withdrawal","cust_01","checking",65.56),
    (6,"acc_02","2024-03-06",77.63,"deposit","cust_02","checking",48.38),
    (9,"acc_01","2024-01-08",75.04,"deposit","cust_01","checking",65.56)], 
    schema=["transaction_id","account_id","transaction_date","amount","transaction_type","customer_id","account_type","balance"]) 
  #case6:- Using transform joinTransformation Utility function and check datassets has been merged properly using the required parameters
  #       parameter details
  """
    1. parentDf -> parent dataframe
    2. ChildDf -> child dataframe
    3. joinType -> leftOuter/fullOuter/Inner
    4. joinCondDetails -> in an json format, here you can add multiple join conditions along with the conditional operator and also group conditional operator is optional
    5. parentDfSelectedColumns -> here you can mention * inorder to select all the columns or you can specify the selective columns that you have retrive, based upon the input it will automically retrive all the selected columns
    6. childDfSelectedColuns -> it will operate as like parentDFSelectedColumns
    7. isBroadcastJoin -> True/false i.e., based upon boolean condition the broadcast join performed on the child dataset
  """
  df_trans =  readUtils.readSrcAsPdAndReturnAsDf(spark,src_config['basepath'], "transactions", src_config['type'],logging)
  df_trans = transformationUtils().dropNullValuesAndReturnDf(df_trans,"account", logging)

  df_trans_jn_acnt = transformationUtils().joinTransformation(df_trans, df_dropNull, "inner",\
                      [{'parentKey':'account_id','childKey':'account_id','operator':'==','use_and':False}], ["*"], ["customer_id","account_type","balance"], True)
  test_dataframes_are_equal_with_exception(df_case6_expected,df_trans_jn_acnt)    
  
  df_trans_dttime = df_trans_jn_acnt.withColumn("transaction_date", col("transaction_date").cast("timestamp"))
  #create and format year month column in transactions
  df_trans_frmt = df_trans_dttime.withColumn("year_month", date_format(col("transaction_date"),"yyyy-MM"))
  df_total_wd_expected = spark.createDataFrame(
    data=[
      ("acc_02","2024-01",35.75),
      ("acc_01","2024-07",87.48)
    ],
    schema=["account_id","year_month","total_withdraw"]
  )
  df_sum_wd = df_trans_frmt.filter(col("transaction_type") == lit("withdrawal")).groupBy("account_id", "year_month").agg(sum("amount").alias("total_withdraw"))
  test_dataframes_are_equal_with_exception(df_total_wd_expected,df_sum_wd)   
  
  df_classifies_expected = spark.createDataFrame(data=[
    (1,"acc_01","2024-07-05 00:00:00",28.89,"deposit","cust_01","checking",65.56,"2024-07","low"),
    (2,"acc_02","2024-01-11 00:00:00",35.75,"withdrawal","cust_02","checking",48.38,"2024-01","low"),
    (5,"acc_01","2024-07-28 00:00:00",87.48,"withdrawal","cust_01","checking",65.56,"2024-07","low"),
    (6,"acc_02","2024-03-06 00:00:00",77.63,"deposit","cust_02","checking",48.38,"2024-03","low"),
    (9,"acc_01","2024-01-08 00:00:00",75.04,"deposit","cust_01","checking",65.56,"2024-01","low")], 
    schema=["transaction_id","account_id","transaction_date","amount","transaction_type","customer_id","account_type","balance","year_month","classifies"]) 
  #classifies transactions as either 'high', 'medium', or 'low' based on the transaction amount
  df_classifies = df_trans_frmt.withColumn("classifies", when(col("amount") > lit(1000), lit("high"))\
                              .otherwise(\
                                when((col("amount") >= lit(500)) & (col("amount") <= lit(1000)),lit("medium"))\
                                .otherwise(\
                                  when(col("amount") < lit(500), lit("low")).otherwise(lit("None"))
                                  )\
                                )\
                              ) 
  test_dataframes_are_equal_with_exception(df_classifies_expected,df_classifies)         
  
  df_weekNumber_expected = spark.createDataFrame(data=[
    (1,"acc_01","2024-07-05 00:00:00",28.89,"deposit","cust_01","checking",65.56,"2024-07","low",1),
    (2,"acc_02","2024-01-11 00:00:00",35.75,"withdrawal","cust_02","checking",48.38,"2024-01","low",2),
    (5,"acc_01","2024-07-28 00:00:00",87.48,"withdrawal","cust_01","checking",65.56,"2024-07","low",4),
    (6,"acc_02","2024-03-06 00:00:00",77.63,"deposit","cust_02","checking",48.38,"2024-03","low",2),
    (9,"acc_01","2024-01-08 00:00:00",75.04,"deposit","cust_01","checking",65.56,"2024-01","low",2)], 
    schema=["transaction_id","account_id","transaction_date","amount","transaction_type","customer_id","account_type","balance","year_month","classifies","week_number"])
  df_week_number = df_classifies.withColumn("week_number", weekofyear(col("transaction_date"))- weekofyear(date_format(col("transaction_date"), "yyyy-MM-01"))+1)
  test_dataframes_are_equal_with_exception(df_weekNumber_expected,df_week_number)  
  
  df_no_trans_expected = spark.createDataFrame(data=[
    ("2024-07",1,"low",1),
    ("2024-01",2,"low",2),
    ("2024-07",4,"low",1),
    ("2024-03",2,"low",1)], schema=["year_month","week_number","classifies","number_of_transactions"])  
  df_no_of_trans = df_week_number.groupBy("year_month", "week_number", "classifies").agg(count("*").alias("number_of_transactions"))
  test_dataframes_are_equal_with_exception(df_no_trans_expected,df_no_of_trans)
  
  #case7:- Using action utility write the required dataset into the specific format into the specific target location i.e., from the target config details from the config json 
  #       check error handling and logging has been captured properly
  actionUtils.writeOutputFile(spark,df_trans_jn_acnt,tgt_config['basepath'],tgt_config['loadMode'],"DummyTransaction",tgt_config['type'],logging)

  delta_table = DeltaTable(table_uri=tgt_config['basepath']+"DummyTransaction").to_pandas()
  df_tgt = spark.createDataFrame(delta_table)
  df_tgt_upd = transformationUtils().replaceNanToNullValues(df_tgt)
  assert df_tgt_upd.count() > 0, "DataFrame is empty"
  
  spark.stop()
 
  
if __name__ == '__main__':
  testCase()