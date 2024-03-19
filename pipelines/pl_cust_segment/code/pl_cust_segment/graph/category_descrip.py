from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_cust_segment.config.ConfigStore import *
from pl_cust_segment.udfs.UDFs import *

def category_descrip(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(StructType([StructField("Category", StringType(), True), StructField("Description", StringType(), True)]))\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/POC_Rucha/Category_Description.txt")
