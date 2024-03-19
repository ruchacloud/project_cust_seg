from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_segment.config.ConfigStore import *
from pl_cust_segment.udfs.UDFs import *

def customers_projection(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = in0.select(col("Cust_Id"), col("Age").cast("int"), col("Gender"), col("Location"))

    return out0
