# starter_script.py
from pyspark.sql import SparkSession, functions as F
import os

spark = (
    SparkSession.builder
    .appName("Taxi-Rideshare-Recommendation")
    .getOrCreate()
)

# Step 0: 读取数据
# 优先使用环境变量 TAXI_PATH；若未设置则使用相对本文件的本地示例数据路径
taxi_path = os.environ.get(
    "TAXI_PATH",
    os.path.join(os.path.dirname(__file__), "datasets", "sample", "*.parquet"),
)
df_raw = spark.read.parquet(taxi_path)

#TODO: 可根据lab1 的版本修改，即是否需要数据清洗
df = (
    df_raw
    .filter(F.col("pickup_datetime").isNotNull())
    .filter(F.col("dropoff_datetime").isNotNull())
    .filter(F.col("pickup_longitude").isNotNull())
    .filter(F.col("pickup_latitude").isNotNull())
)

print("Count after basic filter:", df.count())

# Step 1: 特征生成（含 UDF）
@F.udf("int")
def get_hour(ts): return int(ts.hour) if ts else None

@F.udf("string")
def bucket_distance(d):
    if d is None:
        return "unknown"
    elif d < 2:
        return "short"
    elif d < 8:
        return "medium"
    else:
        return "long"

df_feat = (
    df
    .withColumn("pickup_ts", F.to_timestamp("pickup_datetime"))
    .withColumn("pickup_hour", get_hour(F.col("pickup_ts")))
    .withColumn("pickup_grid_x", (F.col("pickup_longitude") * 100).cast("int"))
    .withColumn("pickup_grid_y", (F.col("pickup_latitude") * 100).cast("int"))
    .withColumn("pickup_zone", F.concat_ws("_", "pickup_grid_x", "pickup_grid_y"))
    .withColumn("distance_bucket", bucket_distance(F.col("trip_distance")))
)

print("Sample rows:", df_feat.limit(5).collect())

# Step 2: 候选拼车对构造
df_small = (
    df_feat
    .withColumn("pickup_date", F.to_date("pickup_ts"))
    .select(
        "pickup_date", "pickup_hour", "pickup_zone",
        "pickup_latitude", "pickup_longitude",
        "trip_distance", "passenger_count", "pickup_datetime"
    )
)

candidates = (
    df_small.alias("a")
    .join(
        df_small.alias("b"),
        on=[
            F.col("a.pickup_date") == F.col("b.pickup_date"),
            F.col("a.pickup_hour") == F.col("b.pickup_hour")
        ],
        how="inner"
    )
    .where(F.col("a.pickup_datetime") < F.col("b.pickup_datetime"))
)

@F.udf("double")
def geo_distance(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return 9999.0
    return ((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2) ** 0.5

candidates = candidates.withColumn(
    "pickup_distance",
    geo_distance(
        F.col("a.pickup_latitude"),
        F.col("a.pickup_longitude"),
        F.col("b.pickup_latitude"),
        F.col("b.pickup_longitude"),
    )
)

print("Candidate count:", candidates.count())

# Step 3: 模型训练
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

train_df = (
    candidates
    .withColumn("label", (F.col("pickup_distance") < 0.02).cast("int"))
    .select(
        "label",
        F.col("a.passenger_count").alias("passenger_a"),
        F.col("b.passenger_count").alias("passenger_b"),
        F.col("a.trip_distance").alias("trip_a"),
        F.col("b.trip_distance").alias("trip_b"),
        "pickup_distance",
    )
)

print("Training rows:", train_df.count())

assembler = VectorAssembler(
    inputCols=["passenger_a", "passenger_b", "trip_a", "trip_b", "pickup_distance"],
    outputCol="features"
)
train_vec = assembler.transform(train_df)

lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(train_vec)
pred = model.transform(train_vec)

print(pred.groupBy("label").count().collect())

spark.stop()
