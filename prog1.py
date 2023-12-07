from pyspark.sql import SparkSession





spark = SparkSession.\
builder.\
config('spark.shuffle.useOldFetchProtocol','true').\
config("spark.sql.warehouse.dir", "/user/itv009490/warehouse").\
enableHiveSupport().\
getOrCreate()

order_schema = 'order_id  long, order_date string, customer_id long, order_status string '

df = spark.read.format("csv").\
schema(order_schema).\
load("/public/trendytech/orders/orders_1gb.csv")

print(df.rdd.getNumPartitions())

df.createOrReplaceTempView("orders")

df =spark.sql("""select order_status,count(*) from orders group by order_status""")

df.write.mode("overwrite").save("pivot_assignment_result")

spark.stop()