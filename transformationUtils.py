from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, regexp_replace, coalesce, lit, when, expr
from pyspark.sql import functions as F

class transformationUtils:
  
  def replaceNanToNullValues(self,dataframe):
    df = dataframe
    for column in df.columns:
      df = (
          # usage of "isnan()" function to test and replace values
          df.withColumn(column,
                    when(F.isnan(col(column)), lit(None))
                    .otherwise(col(column)))
      ) 
    return df
    
  def dropNullValuesAndReturnDf(self, df, datasetname, logging):
    df_clnsd = self.replaceNanToNullValues(df).na.drop("any")
    df_na = self.replaceNanToNullValues(df).filter(F.greatest(*[F.col(i).isNull() for i in self.replaceNanToNullValues(df).columns]))
    logging.info("Total " + str(df_na.count()) +" rows containing null values and cleared in the return dataset i.e., "+ datasetname)
    return df_clnsd
    
  def joinTransformation(self,dfParent, dfChild, joinType, joinColumns , dfParentSelectedColumns, dfChildSelectedColumns, isBroadcast):
   
    joinConds = []
    for i, cond in enumerate(joinColumns):
      parentCol = cond['parentKey']
      childCol = cond['childKey']
      operator = cond['operator']
     
      if operator:
        joinConds.append(f"dfParent.{parentCol} {operator} dfChild.{childCol}")
        
        if cond.get('use_and', False) and i < len(join_columns) - 1:
          joinConds.append("AND")
    
    join_cond = ' '.join(joinConds)
    
    if join_cond:
      join_expr = expr(join_cond)
    else:
      raise ValueError("No Valid join condition")
   
    if ((len(dfParentSelectedColumns) == 1) and (dfParentSelectedColumns[0] == "*")) and ((len(dfChildSelectedColumns) == 1) and (dfChildSelectedColumns[0] == "*")):
      selected_columns = [f"dfParent.{col}" for col in dfParent.columns] + [f"dfChild.{col}" for col in dfChild.columns]
    elif ((len(dfParentSelectedColumns) == 1) and (dfParentSelectedColumns[0] == "*")) and ((len(dfChildSelectedColumns) >= 1) and (dfChildSelectedColumns[0] != "*")):
      selected_columns = [f"dfParent.{col}" for col in dfParent.columns] + [f"dfChild.{col}" for col in dfChildSelectedColumns]
    elif ((len(dfParentSelectedColumns) >= 1) and (dfParentSelectedColumns[0] != "*")) and ((len(dfChildSelectedColumns) == 1) and (dfChildSelectedColumns[0] == "*")):
      selected_columns = [f"dfParent.{col}" for col in dfParentSelectedColumns] + [f"dfChild.{col}" for col in dfChild.columns]
    else:
      selected_columns = [f"dfParent.{col}" for col in dfParentSelectedColumns] + [f"dfChild.{col}" for col in dfChildSelectedColumns]
   
    if isBroadcast :
      df_join = dfParent.alias("dfParent").join(F.broadcast(dfChild.alias("dfChild")), join_expr, how=joinType)\
                    .select(*selected_columns)
    else:
      df_join = dfParent.join(dfChild, on=join_expr, how=joinType)\
                    .select(*selected_columns)
             
    return df_join
    