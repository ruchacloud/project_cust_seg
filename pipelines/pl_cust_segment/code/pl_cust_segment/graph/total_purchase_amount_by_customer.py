from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_segment.config.ConfigStore import *
from pl_cust_segment.udfs.UDFs import *

def total_purchase_amount_by_customer(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = in0\
               .withColumn("TotalPurchase", col("Quantity") * col("Price"))\
               .groupBy("Cust_Id")\
               .agg(sum("TotalPurchase").alias("TotalPurchase"))

    return out0
