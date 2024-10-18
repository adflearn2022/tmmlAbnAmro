
from deltalake.writer import write_deltalake

class actionUtils:
  
  def writeOutputFile(spark, dataframe, baspath, load_mode, filename, fileformat, logging):
    try:
      if fileformat == "delta" :
        df = dataframe.toPandas()
        write_deltalake(baspath+filename, df, mode=load_mode)
      else :
        dataframe.write.mode(load_mode).format(fileformat).save(baspath+filename)
      logging.info("Successfully load the data for output file i.e., "+ filename)
    except Exception as e:
      print("Failed to write the output " +fileformat+" file i.e., " + filename + " " + str(e))
      logging.exception("Failed to write the output " +fileformat+" file i.e., " + filename + " " + str(e))