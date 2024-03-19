from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_segment.config.ConfigStore import *
from pl_cust_segment.udfs.UDFs import *

def Frequency(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import count
    out0 = in0.groupBy("CustomerID").agg(count("OrderID").alias("Frequency"))

    return out0
