import json
import logging
from pathlib import Path

class utils:
  
  def load_config(file_path):
    try:
      with open(file_path, 'r') as f:
        return json.load(f)
    except Exception as e:
      print("While reading the Configuration File " + str(e))
      
  def logging(datetime, basepath, logFileName):
    try:
      path_obj = Path(basepath)
      if not path_obj.exists():
        path_obj.mkdir(parents=True)
        
      logging.basicConfig(
        filename=basepath+logFileName+"_"+str(datetime.now()).replace(" ","").replace("-","").replace(":","").replace(".","")+".log",
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
      )
    except Exception as e:
      print("Failed while creating the logging objt " + str(e))
      
    return logging
  
      