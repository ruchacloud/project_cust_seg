from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pl_cust_segment.config.ConfigStore import *
from pl_cust_segment.udfs.UDFs import *
from prophecy.utils import *
from pl_cust_segment.graph import *

def pipeline(spark: SparkSession) -> None:
    df_product_order_data = product_order_data(spark)
    df_cutomer_data = cutomer_data(spark)
    df_rename_customer_id_to_cust_id = rename_customer_id_to_cust_id(spark, df_cutomer_data)
    df_by_customer_id_inner_join = by_customer_id_inner_join(
        spark, 
        df_rename_customer_id_to_cust_id, 
        df_product_order_data
    )
    df_total_purchase_amount_by_customer = total_purchase_amount_by_customer(spark, df_by_customer_id_inner_join)
    ds_total_purchase_amount(spark, df_total_purchase_amount_by_customer)
    df_product_details = product_details(spark)
    df_by_product_id_left_outer_join = by_product_id_left_outer_join(spark, df_product_order_data, df_product_details)
    df_category_descrip = category_descrip(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pl_cust_segment")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pl_cust_segment", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/pl_cust_segment")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
