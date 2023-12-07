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

df2=df.withColumn("street",col("user_address.street"))\
.withColumn("city",col("user_address.city"))\
.withColumn("state",col("user_address.state"))\
.withColumn("postal_code",col("user_address.postal_code"))\
.withColumn("num_phn_numbers",size(col("user_phone_numbers")))

df2.createOrReplaceTempView("users")

df3 = spark.sql("""
select state,sum(male_cnt) as Males, sum(fmale_cnt) as Females from (
select state, case when user_gender ='Male' then count(*) end as male_cnt,
case when user_gender ='Female' then count(*) end as fmale_cnt from users
where state is not null and user_phone_numbers is not null
group by state , user_gender
)
group by state
order by state
""")

df3.write.mode("overwrite").save("pivot_assignment_result")

spark.stop()