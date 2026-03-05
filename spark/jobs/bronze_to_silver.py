"""
Bronze to Silver Transformation Job
Reads raw data from GCS Bronze bucket, transforms and cleans it, writes to Silver bucket
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    datediff,
    dayofweek,
    hour,
    isnan,
    isnull,
    lit,
    regexp_replace,
    round as spark_round,
    to_timestamp,
    udf,
    when,
)
from pyspark.sql.types import DoubleType
from datetime import datetime
import math


# Haversine formula UDF for distance calculation
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two coordinates in km"""
    R = 6371  # Earth radius in km
    try:
        lat1_rad = math.radians(float(lat1))
        lon1_rad = math.radians(float(lon1))
        lat2_rad = math.radians(float(lat2))
        lon2_rad = math.radians(float(lon2))

        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c
    except (ValueError, TypeError):
        return None


haversine_udf = udf(haversine_distance, DoubleType())


def read_bronze_data(spark, bronze_path, table_name):
    """Read raw data from Bronze bucket
    bronze_path should be like: gs://ride-analytics/bronze
    table_name: drivers, passengers, or rides
    """
    print(f"Reading {table_name} from {bronze_path}")
    try:
        if table_name == "rides":
            # Rides are in subdirectories by date: bronze/rides/YYYY-MM-DD/rides.csv
            df = spark.read.option("header", "true").csv(f"{bronze_path}/rides/*/*.csv")
        else:
            # Drivers and passengers are in folders: bronze/drivers/drivers.csv
            df = spark.read.option("header", "true").csv(
                f"{bronze_path}/{table_name}/*.csv"
            )
        print(f"✓ Read {df.count()} records from {table_name}")
        return df
    except Exception as e:
        print(f"Error reading {table_name}: {str(e)}")
        raise


def transform_rides(spark, rides_df):
    """Transform rides data"""
    print("Transforming rides data...")

    # Convert timestamps
    rides_df = rides_df.withColumn(
        "pickup_datetime", to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "dropoff_datetime",
        when(
            col("dropoff_datetime").isNotNull(),
            to_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"),
        ).otherwise(None),
    )

    # Convert coordinates to double
    for col_name in [
        "pickup_latitude",
        "pickup_longitude",
        "dropoff_latitude",
        "dropoff_longitude",
    ]:
        rides_df = rides_df.withColumn(col_name, col(col_name).cast(DoubleType()))

    # Convert fare and tip to double
    rides_df = (
        rides_df.withColumn("fare_amount", col("fare_amount").cast(DoubleType()))
        .withColumn("tip_amount", col("tip_amount").cast(DoubleType()))
        .withColumn("surge_multiplier", col("surge_multiplier").cast(DoubleType()))
    )

    # Calculate ride distance using Haversine formula
    rides_df = rides_df.withColumn(
        "ride_distance_km",
        when(
            (col("pickup_latitude").isNotNull())
            & (col("pickup_longitude").isNotNull())
            & (col("dropoff_latitude").isNotNull())
            & (col("dropoff_longitude").isNotNull()),
            haversine_udf(
                col("pickup_latitude"),
                col("pickup_longitude"),
                col("dropoff_latitude"),
                col("dropoff_longitude"),
            ),
        ).otherwise(None),
    )

    # Calculate ride duration in minutes
    rides_df = rides_df.withColumn(
        "ride_duration_minutes",
        when(
            col("dropoff_datetime").isNotNull(),
            datediff(col("dropoff_datetime"), col("pickup_datetime")) * 24 * 60
            + (hour(col("dropoff_datetime")) - hour(col("pickup_datetime"))) * 60,
        ).otherwise(None),
    )

    # Calculate fare per km
    rides_df = rides_df.withColumn(
        "fare_per_km",
        when(
            (col("ride_distance_km").isNotNull())
            & (col("ride_distance_km") > 0)
            & (col("fare_amount").isNotNull()),
            col("fare_amount") / col("ride_distance_km"),
        ).otherwise(None),
    )

    # Calculate total fare
    rides_df = rides_df.withColumn(
        "total_fare",
        when(
            col("fare_amount").isNotNull(),
            col("fare_amount")
            + when(col("tip_amount").isNotNull(), col("tip_amount")).otherwise(0.0),
        ).otherwise(None),
    )

    # Calculate surge impact
    rides_df = rides_df.withColumn(
        "surge_impact",
        when(
            (col("surge_multiplier").isNotNull())
            & (col("surge_multiplier") > 1.0)
            & (col("fare_amount").isNotNull()),
            col("fare_amount") * (col("surge_multiplier") - 1.0),
        ).otherwise(0.0),
    )

    # Extract time-based features
    rides_df = (
        rides_df.withColumn("pickup_hour", hour(col("pickup_datetime")))
        .withColumn("pickup_day_of_week", dayofweek(col("pickup_datetime")))
        .withColumn(
            "is_peak_hour",
            when(
                ((col("pickup_hour") >= 7) & (col("pickup_hour") <= 9))
                | ((col("pickup_hour") >= 17) & (col("pickup_hour") <= 19)),
                True,
            ).otherwise(False),
        )
    )

    # Extract date for partitioning
    rides_df = rides_df.withColumn("pickup_date", col("pickup_datetime").cast("date"))

    # Add processing metadata
    rides_df = rides_df.withColumn(
        "processed_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    # Round numeric columns
    for col_name in [
        "ride_distance_km",
        "fare_per_km",
        "total_fare",
        "surge_impact",
    ]:
        rides_df = rides_df.withColumn(col_name, spark_round(col(col_name), 2))

    print(f"✓ Transformed {rides_df.count()} ride records")
    return rides_df


def transform_drivers(drivers_df):
    """Transform drivers data"""
    print("Transforming drivers data...")

    # Convert dates
    drivers_df = drivers_df.withColumn(
        "registration_date",
        to_timestamp(col("registration_date"), "yyyy-MM-dd").cast("date"),
    )

    # Convert rating to double
    drivers_df = drivers_df.withColumn("rating", col("rating").cast(DoubleType()))

    # Convert is_active to boolean
    drivers_df = drivers_df.withColumn(
        "is_active",
        when(col("is_active").isin(["True", "true", "1", "True"]), True).otherwise(
            False
        ),
    )

    # Add processing metadata
    drivers_df = drivers_df.withColumn(
        "processed_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    print(f"✓ Transformed {drivers_df.count()} driver records")
    return drivers_df


def transform_passengers(passengers_df):
    """Transform passengers data"""
    print("Transforming passengers data...")

    # Convert dates
    passengers_df = passengers_df.withColumn(
        "registration_date",
        to_timestamp(col("registration_date"), "yyyy-MM-dd").cast("date"),
    )

    # Convert rating to double
    passengers_df = passengers_df.withColumn("rating", col("rating").cast(DoubleType()))

    # Add processing metadata
    passengers_df = passengers_df.withColumn(
        "processed_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    print(f"✓ Transformed {passengers_df.count()} passenger records")
    return passengers_df


def write_silver_data(df, silver_path, table_name, partition_col=None):
    """Write processed data to Silver bucket"""
    print(f"Writing {table_name} to {silver_path}")

    try:
        if partition_col:
            df.write.mode("overwrite").partitionBy(partition_col).parquet(
                f"{silver_path}/{table_name}",
            )
        else:
            df.write.mode("overwrite").parquet(f"{silver_path}/{table_name}")

        print(f"✓ Successfully wrote {table_name} to Silver bucket")
    except Exception as e:
        print(f"Error writing {table_name}: {str(e)}")
        raise


def main():
    """Main transformation job"""
    # Get arguments
    if len(sys.argv) < 3:
        print(
            "Usage: bronze_to_silver.py <bronze_path> <silver_path> <processing_date>"
        )
        sys.exit(1)

    bronze_path = sys.argv[1]
    silver_path = sys.argv[2]
    processing_date = sys.argv[3] if len(sys.argv) > 3 else None

    print(f"Starting Bronze to Silver transformation...")
    print(f"Bronze path: {bronze_path}")
    print(f"Silver path: {silver_path}")
    print(f"Processing date: {processing_date}")

    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("BronzeToSilverTransformation")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        # Read bronze data
        rides_df = read_bronze_data(spark, bronze_path, "rides")
        drivers_df = read_bronze_data(spark, bronze_path, "drivers")
        passengers_df = read_bronze_data(spark, bronze_path, "passengers")

        # Transform data
        rides_processed = transform_rides(spark, rides_df)
        drivers_processed = transform_drivers(drivers_df)
        passengers_processed = transform_passengers(passengers_df)

        # Write to silver
        write_silver_data(
            rides_processed, silver_path, "rides_processed", "pickup_date"
        )
        write_silver_data(drivers_processed, silver_path, "drivers_processed")
        write_silver_data(passengers_processed, silver_path, "passengers_processed")

        print("\n✅ Bronze to Silver transformation completed successfully!")

    except Exception as e:
        print(f"\n❌ Error in transformation: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
