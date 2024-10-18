import pandas as pd

class readUtils:
  
  def readSrcAsPdAndReturnAsDf(spark,basepath, filename, fileFormat, logging):
    try:
      df_pd_src = pd.read_csv(basepath+filename+"."+fileFormat)
      df_src = spark.createDataFrame(df_pd_src)
      logging.info("Successfully read the input file i.e., "+ filename)
      return df_src
    except Exception as e:
      print("Failed to read the input file i.e., " + filename + " " + str(e))
      logging.exception("Failed to read the input file i.e., " + filename + " " + str(e))