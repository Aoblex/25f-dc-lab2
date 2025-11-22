#!/usr/bin/env python3
"""
Optimized version of taxi rideshare recommendation system
Key optimizations:
1. Eliminate UDFs using Spark SQL built-in functions
2. Reduce number of actions to minimize recomputation
3. Optimize join operations with partitioning and bucketing
4. Use appropriate caching for reused DataFrames
"""

import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
import time


def create_spark_session(app_name="Taxi-Rideshare-Recommendation-Optimized"):
    """Create SparkSession with optimized configuration"""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    return spark


def read_and_clean_data(spark, taxi_path):
    """Read and clean raw taxi data with minimal actions"""
    print("\n[Step 0] Reading and cleaning data...")
    print(f"  Taxi data path: {taxi_path}")

    df_raw = spark.read.parquet(taxi_path)
    initial_count = df_raw.count()

    df = df_raw.filter(
        F.col("pickup_datetime").isNotNull() &
        F.col("dropoff_datetime").isNotNull() &
        F.col("pickup_longitude").isNotNull() &
        F.col("pickup_latitude").isNotNull()
    )
    filtered_count = df.count()

    print(f"  Initial row count: {initial_count:,}")
    print(f"  After filtering: {filtered_count:,} " +
          f"(removed {initial_count - filtered_count:,} rows)")

    return df


def optimize_feature_engineering(df):
    """
    Feature engineering using built-in Spark functions (no UDFs)
    Optimization: Use Spark SQL expressions instead of Python UDFs
    """
    print("\n[Step 1] Feature Engineering - Using built-in functions (no UDFs)")

    df_feat = (
        df
        .withColumn("pickup_ts", F.to_timestamp("pickup_datetime"))
        .withColumn("pickup_hour", F.hour(F.col("pickup_ts")))
        .withColumn("pickup_grid_x", (F.col("pickup_longitude") * 100).cast("int"))
        .withColumn("pickup_grid_y", (F.col("pickup_latitude") * 100).cast("int"))
        .withColumn("pickup_zone", F.concat_ws("_", "pickup_grid_x", "pickup_grid_y"))
        .withColumn("distance_bucket",
            F.when(F.col("trip_distance").isNull(), "unknown")
             .when(F.col("trip_distance") < 2, "short")
             .when(F.col("trip_distance") < 8, "medium")
             .otherwise("long")
        )
        .withColumn("pickup_date", F.to_date("pickup_ts"))
    )

    sample_df = df_feat.select(
        "pickup_datetime", "pickup_hour", "pickup_zone",
        "pickup_latitude", "pickup_longitude",
        "trip_distance", "distance_bucket", "passenger_count"
    ).limit(5)

    print("  Sample rows after feature engineering:")
    sample_df.show(truncate=False)

    return df_feat


def build_optimized_candidates(df_feat):
    """
    Build ride-share candidates with optimized self-join
    Optimization: Pre-partition data to reduce shuffle, add zone filtering,
    and use built-in functions for distance calculation
    """
    print("\n[Step 2] Building candidates with optimized self-join")

    candidate_cols = [
        "pickup_date", "pickup_hour", "pickup_zone",
        "pickup_latitude", "pickup_longitude",
        "trip_distance", "passenger_count", "pickup_datetime"
    ]

    df_small = df_feat.select(candidate_cols)

    print("  Pre-partitioning data for optimized join...")
    df_partitioned = df_small.repartition(
        200,
        "pickup_date",
        "pickup_hour"
    ).cache()

    count_partitioned = df_partitioned.count()
    print(f"  Partitioned data count: {count_partitioned:,}")

    print("  Performing optimized self-join...")
    candidates = (
        df_partitioned.alias("a")
        .join(
            df_partitioned.alias("b"),
            on=[
                F.col("a.pickup_date") == F.col("b.pickup_date"),
                F.col("a.pickup_hour") == F.col("b.pickup_hour"),
                F.col("a.pickup_zone") == F.col("b.pickup_zone"),
                F.col("a.pickup_datetime") < F.col("b.pickup_datetime")
            ],
            how="inner"
        )
    )

    print("  Calculating distances using built-in functions...")
    candidates = candidates.withColumn(
        "pickup_distance",
        F.sqrt(
            F.pow(F.col("a.pickup_latitude") - F.col("b.pickup_latitude"), 2) +
            F.pow(F.col("a.pickup_longitude") - F.col("b.pickup_longitude"), 2)
        )
    ).filter(F.col("pickup_distance") < 0.1)

    filtered_count = candidates.count()
    print(f"  Candidates after distance filtering (< 0.1): {filtered_count:,}")

    return candidates


def train_optimized_model(candidates):
    """
    Train logistic regression model with optimized data preparation
    """
    print("\n[Step 3] Preparing training data and training model")

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

    train_count = train_df.count()
    positive_count = train_df.filter("label = 1").count()
    negative_count = train_df.filter("label = 0").count()

    print(f"  Total training rows: {train_count:,}")
    print(f"  Positive samples (label=1): {positive_count:,}")
    print(f"  Negative samples (label=0): {negative_count:,}")

    print("  Assembling feature vectors...")
    assembler = VectorAssembler(
        inputCols=["passenger_a", "passenger_b", "trip_a", "trip_b", "pickup_distance"],
        outputCol="features"
    )
    train_vec = assembler.transform(train_df)

    print("  Training Logistic Regression model...")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)
    model = lr.fit(train_vec)

    pred = model.transform(train_vec)

    print("\n  Prediction distribution:")
    pred.groupBy("label", "prediction").count().orderBy("label", "prediction").show()

    print("\n[Model Summary]")
    print(f"  Coefficients: {model.coefficients}")
    print(f"  Intercept: {model.intercept}")

    return model


def run_optimized_pipeline(spark, taxi_path):
    """
    Run the complete optimized pipeline
    Returns: Total execution time in seconds
    """
    print("=" * 70)
    print("OPTIMIZED TAXI RIDESHARE RECOMMENDATION SYSTEM")
    print("Optimizations: No UDFs | Pre-partitioning | Early filtering | AQE")
    print("=" * 70)

    start_time = time.time()

    df = read_and_clean_data(spark, taxi_path)
    df_feat = optimize_feature_engineering(df)
    candidates = build_optimized_candidates(df_feat)
    model = train_optimized_model(candidates)

    end_time = time.time()
    total_time = end_time - start_time

    print("\n" + "=" * 70)
    print(f"✓ Pipeline completed successfully!")
    print(f"  Total execution time: {total_time:.2f} seconds")
    print("=" * 70)

    return total_time


def main():
    parser = argparse.ArgumentParser(
        description="Optimized Taxi Rideshare Recommendation Pipeline"
    )
    parser.add_argument(
        "--taxi_path",
        type=str,
        required=True,
        help="Path to taxi parquet data (e.g., ./datasets/*.parquet)"
    )

    args = parser.parse_args()

    spark = create_spark_session()
    try:
        run_optimized_pipeline(spark, args.taxi_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
