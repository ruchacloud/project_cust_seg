from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_segment.config.ConfigStore import *
from pl_cust_segment.udfs.UDFs import *

def avg_basket_size_by_order_id(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import avg
    out0 = in0.groupBy("OrderID").agg(avg("Quantity").alias("avg_basket_size"))

    return out0
