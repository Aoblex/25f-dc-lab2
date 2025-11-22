import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression


def create_spark(app_name="Taxi-Rideshare-Recommendation"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_and_clean_data(spark, taxi_path):
    df_raw = spark.read.parquet(taxi_path)
    df = (
        df_raw
        .select(
            "pickup_datetime", "dropoff_datetime",
            "pickup_latitude", "pickup_longitude",
            "trip_distance", "passenger_count",
        )
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("pickup_longitude").isNotNull())
        .filter(F.col("pickup_latitude").isNotNull())
    )
    print("Count after basic filter:", df.count())
    return df


def feature_engineering(df):
    @F.udf("int")
    def get_hour(ts):
        return int(ts.hour) if ts else None

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
    return df_feat


def build_candidates(df_feat):
    df_small = (
        df_feat
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .select(
            "pickup_date", "pickup_hour", "pickup_zone",
            "pickup_latitude", "pickup_longitude",
            "trip_distance", "passenger_count", "pickup_datetime"
        )
    )

    # 按 join key 重新分区，减少 shuffle
    repart_keys = ["pickup_date", "pickup_hour", "pickup_zone"]
    df_small = df_small.repartition(*repart_keys).persist()

    a = df_small.alias("a")
    b = df_small.alias("b")

    join_cond = (
        (F.col("a.pickup_date") == F.col("b.pickup_date")) &
        (F.col("a.pickup_hour") == F.col("b.pickup_hour")) &
        (F.col("a.pickup_zone") == F.col("b.pickup_zone")) &
        (F.col("a.pickup_datetime") < F.col("b.pickup_datetime"))
    )

    candidates = a.join(b, on=join_cond, how="inner")

    dx = F.col("a.pickup_latitude") - F.col("b.pickup_latitude")
    dy = F.col("a.pickup_longitude") - F.col("b.pickup_longitude")
    candidates = candidates.withColumn("pickup_distance", F.sqrt(F.pow(dx, 2) + F.pow(dy, 2)))

    return candidates



def train_model(candidates):
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
    return model


def main():
    parser = argparse.ArgumentParser(description="Taxi Rideshare Recommendation Pipeline")
    parser.add_argument("--taxi_path", type=str, required=True, help="Path to taxi parquet data")
    args = parser.parse_args()

    spark = create_spark()
    df = read_and_clean_data(spark, args.taxi_path)
    df_feat = feature_engineering(df)
    candidates = build_candidates(df_feat)
    model = train_model(candidates)
    spark.stop()


if __name__ == "__main__":
    main()
