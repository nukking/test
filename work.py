from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = (
    SparkSession
    .builder
    .appName("Data Engineer Training Course")
    .config("spark.sql.session.timeZone", "Asia/Seoul")
    .getOrCreate()
)

# 노트북에서 테이블 형태로 데이터 프레임 출력을 위한 설정을 합니다

from IPython.display import display, display_pretty, clear_output, JSON
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # display enabled
spark.conf.set("spark.sql.repl.eagerEval.truncate", 100) # display output columns size
spark

user25 = spark.read.parquet("user/20201025")
user25.printSchema()
user25.show(truncate=False)
display(user25)

purchase25 = spark.read.parquet("purchase/20201025")
purchase25.createOrReplaceTempView("purchase25")
purchase25.printSchema()
display(purchase25)

accesslog = spark.read.option("inferSchema", "true").json("access/20201025")
accesslog.createOrReplaceTempView("accesslog")
access25 = spark.sql("select a_id, a_tag, a_time, a_timestamp, a_uid from accesslog")
access25.createOrReplaceTempView("access25")
access25.printSchema()
display(access25)

user25.createOrReplaceTempView("user25")
purchase25.createOrReplaceTempView("purchase25")
access25.createOrReplaceTempView("access25")
spark.sql("show tables '*25'")

u_signup_condition = "u_signup >= '20201025' and u_signup < '20201026'"
user = spark.sql("select u_id, u_name, u_gender from user25").where(u_signup_condition)
user.createOrReplaceTempView("user")
display(user)

p_time_condition = "p_time >= '2020-10-25 00:00:00' and p_time < '2020-10-26 00:00:00'"
purchase = spark.sql("select from_unixtime(p_time) as p_time, p_uid, p_id, p_name, p_amount from purchase25").where(p_time_condition)
purchase.createOrReplaceTempView("purchase")
display(purchase)

access = spark.sql("select a_id, a_tag, a_timestamp, a_uid from access25")
access.createOrReplaceTempView("access")
display(access)

spark.sql("show tables")

spark.sql("select * from user")
whereCondition = "u_gender = '남'"
spark.sql("select * from user").where(whereCondition)

spark.sql("select * from purchase")
selectClause = "select * from purchase where p_amount > 2000000"
spark.sql(selectClause)

spark.sql("select * from access")
groupByClause="select a_id, count(a_id) from access group by a_id"
spark.sql(groupByClause)

display(access)
distinctAccessUser = "select count(distinct a_uid) as DAU from access"
dau = spark.sql(distinctAccessUser)
display(dau)
v_dau = dau.collect()[0]["DAU"]

display(purchase)
distinctPayingUser = "select count(distinct p_uid) as PU from purchase"
pu = spark.sql(distinctPayingUser)
display(pu)
v_dpu = pu.collect()[0]["PU"]

display(purchase)
sumOfDailyRevenue = "select sum(p_amount) as DR from purchase"
dr = spark.sql(sumOfDailyRevenue)
display(dr)
v_dr = dr.collect()[0]["DR"]

print("+------------------+")
print("|             ARPU |")
print("+------------------+")
print("|        {} |".format(v_dr / v_dau))
print("+------------------+")

print("+------------------+")
print("|            ARPPU |")
print("+------------------+")
print("|        {} |".format(v_dr / v_dpu))
print("+------------------+")

access.printSchema()
countOfAccess = "select a_uid, count(a_uid) as a_count from access where a_id = 'login' group by a_uid"
accs = spark.sql(countOfAccess)
display(accs)

access.printSchema()
countOfAccess = "select a_uid, count(a_uid) as a_count from access where a_id = 'login' group by a_uid"
accs = spark.sql(countOfAccess)
display(accs)

purchase.printSchema()
sumOfCountAndAmount = "select p_uid, sum(p_amount) as p_amount, count(p_uid) as p_count from purchase group by p_uid"
amts = spark.sql(sumOfCountAndAmount)
display(amts)

accs.printSchema()
amts.printSchema()
joinCondition = accs["a_uid"] == amts["p_uid"]
joinHow = "left_outer"
dim1 = accs.join(amts, joinCondition, joinHow)
dim1.printSchema()
display(dim1.orderBy(asc("a_uid")))

dim1.printSchema()
user.printSchema()
joinCondition = dim1["a_uid"] == user["u_id"]
joinHow = "left_outer"
dim2 = dim1.join(user, joinCondition, joinHow)
dim2.printSchema()
display(dim2.orderBy(asc("a_uid")))

dim2.printSchema()
dim3 = dim2.drop("p_uid", "u_id")
fillDefaultValue = {"p_amount":0, "p_count":0}
dim4 = dim3.na.fill(fillDefaultValue)
dim4.printSchema()
display(dim4.orderBy(asc("a_uid")))

dim4.printSchema()
dim5 = (
    dim4
    .withColumnRenamed("a_uid", "d_uid")
    .withColumnRenamed("a_count", "d_acount")
    .withColumnRenamed("p_amount", "d_pamount")
    .withColumnRenamed("p_count", "d_pcount")
    .withColumnRenamed("u_name", "d_name")
    .withColumnRenamed("u_gender", "d_gender")
   .drop("a_uid", "a_count", "p_amount", "p_count", "u_name", "u_gender")
   .select("d_uid", "d_name", "d_gender", "d_acount", "d_pamount", "d_pcount")
)
display(dim5.orderBy(asc("d_uid")))

purchase.printSchema()
selectFirstPurchaseTime = "select p_uid, min(p_time) as p_time from purchase group by p_uid"

first_purchase = spark.sql(selectFirstPurchaseTime)
dim6 = dim5.withColumn("d_first_purchase", lit(None))
dim6.printSchema()

exprFirstPurchase = expr("case when d_first_purchase is null then p_time else d_first_purchase end")

dim7 = (
    dim6.join(first_purchase, dim5.d_uid == first_purchase.p_uid, "left_outer")
    .withColumn("first_purchase", exprFirstPurchase)
    .drop("d_first_purchase", "p_uid", "p_time")
    .withColumnRenamed("first_purchase", "d_first_purchase")
)
    
dimension = dim7.orderBy(asc("d_uid"))
dimension.printSchema()
display(dimension)

dimension.printSchema()
target_dir="dimension/dt=20220928"
dimension.write.mode("overwrite").parquet(target_dir)

newDimension = spark.read.parquet(target_dir)
newDimension.printSchema()
display(newDimension)

print("+------------------+")
print("|              DAU |")
print("+------------------+")
print("|                {} |".format(v_dau))
print("+------------------+")
print("+------------------+")
print("|               PU |")
print("+------------------+")
print("|                {} |".format(v_dpu))
print("+------------------+")
print("+------------------+")
print("|                DR |")
print("+------------------+")
print("|        {} |".format(v_dr))
print("+------------------+")
print("+------------------+")
print("|             ARPU |")
print("+------------------+")
print("|        {} |".format(v_dr / v_dau))
print("+------------------+")
print("+------------------+")
print("|            ARPPU |")
print("+------------------+")
print("|        {} |".format(v_dr / v_dpu))
print("+------------------+")
