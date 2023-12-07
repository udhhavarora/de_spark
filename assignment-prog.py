from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import * #for window
from pyspark.sql.types import *

spark = SparkSession\
.builder\
.enableHiveSupport()\
.config("spark.shuffle.useOldFetchProtocol",'true')\
.config("spark.sql.warehouse.dir","/user/itv009490/warehouse")\
.getOrCreate()


users_schema=StructType([StructField("user_id",IntegerType(),nullable=False),
                         StructField("user_first_name",StringType(),nullable=False),
                         StructField("user_last_name",StringType(),nullable=False),
                         StructField("user_email",StringType(),nullable=False),
                         StructField("user_gender",StringType(),nullable=False),
                         StructField("user_phone_numbers",ArrayType(StringType()),nullable=True),
                         StructField("user_address",StructType([
                             StructField("street",StringType(),nullable=False),
                             StructField("city",StringType(),nullable=False),
                             StructField("state",StringType(),nullable=False),
                             StructField("postal_code",StringType(),nullable=False),]),nullable=False)
                        ])

df = spark.read.format("json").schema(users_schema).\
load("/public/sms/users/")

print("Partitions = ",df.rdd.getNumPartitions())

print("Total Count = ",df.count())

df2=df.withColumn("street",col("user_address.street"))\
.withColumn("city",col("user_address.city"))\
.withColumn("state",col("user_address.state"))\
.withColumn("postal_code",col("user_address.postal_code"))\
.withColumn("num_phn_numbers",size(col("user_phone_numbers")))

df2.createOrReplaceTempView("users")

spark.sql("select count(*) from users where state = 'New York'").show()
ny_count = df2.filter("state='New York'").count()
print("NY counts: ",ny_count)

spark.sql("""select state,count(postal_code) as cnt from users group by state order by cnt desc""").show(1)

spark.sql("""select city,count(user_id) as cnt from users where city is not null group by city order by cnt desc""").show(1)

bizjournals=df2.where("user_email like '%bizjournals.com'").count()
print("bizjournals counts: ",bizjournals)

num_phn_numbers = df2.filter(col("num_phn_numbers")==4).select("user_id").count()

print("num_phn_numbers=4 counts: ",num_phn_numbers)

spark.stop()