"""
Silver to Gold Transformation Job
Reads processed data from Silver bucket and loads to BigQuery
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    to_date,
    when,
)
from datetime import datetime


def read_silver_data(spark, silver_path, table_name):
    """Read processed data from Silver bucket"""
    print(f"Reading {table_name} from {silver_path}")
    try:
        df = spark.read.parquet(f"{silver_path}/{table_name}")
        print(f"✓ Read {df.count()} records from {table_name}")
        return df
    except Exception as e:
        print(f"Error reading {table_name}: {str(e)}")
        raise


def create_fact_rides(spark, rides_df):
    """Create fact_rides table"""
    print("Creating fact_rides table...")

    fact_rides = rides_df.select(
        col("ride_id"),
        col("driver_id"),
        col("passenger_id"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_latitude"),
        col("pickup_longitude"),
        col("dropoff_latitude"),
        col("dropoff_longitude"),
        col("fare_amount"),
        col("tip_amount"),
        col("surge_multiplier"),
        col("ride_status"),
        col("ride_distance_km"),
        col("ride_duration_minutes"),
        col("fare_per_km"),
        col("total_fare"),
        col("surge_impact"),
        col("pickup_hour"),
        col("pickup_day_of_week"),
        col("is_peak_hour"),
        to_date(col("pickup_datetime")).alias("pickup_date"),
    ).filter(
        col("ride_status") == "completed"  # Only completed rides
    )

    print(f"✓ Created fact_rides with {fact_rides.count()} records")
    return fact_rides


def create_dim_drivers_scd2(spark, drivers_df, existing_dim_drivers=None):
    """Create dim_drivers with SCD Type 2"""
    print("Creating dim_drivers (SCD Type 2)...")

    # Add effective date and end date
    current_date = datetime.now().date()
    drivers_scd2 = drivers_df.select(
        col("driver_id"),
        col("name"),
        col("email"),
        col("phone"),
        col("registration_date"),
        col("vehicle_type"),
        col("license_plate"),
        col("city"),
        col("rating"),
        col("is_active"),
        lit(current_date).alias("effective_date"),
        lit(None).cast("date").alias("end_date"),
        lit(True).alias("is_current"),
    )

    # If existing dimension exists, update end dates for changed records
    if existing_dim_drivers is not None:
        # This is a simplified version - in production, you'd do a proper merge
        # For now, we'll just insert new/changed records
        pass

    print(f"✓ Created dim_drivers with {drivers_scd2.count()} records")
    return drivers_scd2


def create_dim_passengers_scd2(spark, passengers_df, existing_dim_passengers=None):
    """Create dim_passengers with SCD Type 2"""
    print("Creating dim_passengers (SCD Type 2)...")

    current_date = datetime.now().date()
    passengers_scd2 = passengers_df.select(
        col("passenger_id"),
        col("name"),
        col("email"),
        col("phone"),
        col("registration_date"),
        col("city"),
        col("preferred_payment_method"),
        col("rating"),
        lit(current_date).alias("effective_date"),
        lit(None).cast("date").alias("end_date"),
        lit(True).alias("is_current"),
    )

    print(f"✓ Created dim_passengers with {passengers_scd2.count()} records")
    return passengers_scd2


def create_agg_daily_rides(spark, fact_rides):
    """Create daily ride aggregations"""
    print("Creating agg_daily_rides...")

    agg_daily = fact_rides.groupBy("pickup_date").agg(
        count("*").alias("total_rides"),
        spark_sum("fare_amount").alias("total_revenue"),
        spark_sum("tip_amount").alias("total_tips"),
        avg("ride_duration_minutes").alias("avg_duration_minutes"),
        avg("ride_distance_km").alias("avg_distance_km"),
        avg("fare_amount").alias("avg_fare"),
        spark_sum("total_fare").alias("total_fare_with_tips"),
    )

    print(f"✓ Created agg_daily_rides with {agg_daily.count()} records")
    return agg_daily


def create_agg_driver_performance(spark, fact_rides, dim_drivers):
    """Create driver performance aggregations"""
    print("Creating agg_driver_performance...")

    # Join fact_rides with dim_drivers
    rides_with_drivers = fact_rides.join(
        dim_drivers.filter(col("is_current") == True), "driver_id", "inner"
    )

    agg_driver = (
        rides_with_drivers.groupBy("driver_id")
        .agg(
            count("*").alias("total_trips"),
            spark_sum("fare_amount").alias("total_earnings"),
            spark_sum("tip_amount").alias("total_tips"),
            avg("rating").alias("avg_rating"),
            avg("ride_duration_minutes").alias("avg_trip_duration"),
            avg("ride_distance_km").alias("avg_trip_distance"),
            count(when(col("ride_status") == "completed", 1)).alias("completed_trips"),
        )
        .withColumn(
            "completion_rate",
            (col("completed_trips") / col("total_trips") * 100).cast("double"),
        )
    )

    print(f"✓ Created agg_driver_performance with {agg_driver.count()} records")
    return agg_driver


def create_agg_demand_patterns(spark, fact_rides):
    """Create demand patterns by hour and location"""
    print("Creating agg_demand_patterns...")

    # Simple aggregation by hour (can be extended with location zones)
    agg_demand = fact_rides.groupBy("pickup_date", "pickup_hour").agg(
        count("*").alias("ride_count"),
        avg("surge_multiplier").alias("avg_surge_multiplier"),
        spark_sum("fare_amount").alias("total_revenue"),
    )

    print(f"✓ Created agg_demand_patterns with {agg_demand.count()} records")
    return agg_demand


def write_to_bigquery(
    df, table_name, project_id, dataset, temp_bucket, write_mode="overwrite"
):
    """Write DataFrame to BigQuery"""
    print(f"Writing {table_name} to BigQuery...")

    try:
        (
            df.write.format("bigquery")
            .option("table", f"{project_id}.{dataset}.{table_name}")
            .option("temporaryGcsBucket", temp_bucket)
            .option("writeDisposition", write_mode.upper())
            .mode(write_mode)
            .save()
        )

        print(f"✓ Successfully wrote {table_name} to BigQuery")
    except Exception as e:
        print(f"Error writing {table_name} to BigQuery: {str(e)}")
        raise


def main():
    """Main transformation job"""
    if len(sys.argv) < 6:
        print(
            "Usage: silver_to_gold.py <silver_path> <project_id> <dataset> <temp_bucket> <processing_date>"
        )
        sys.exit(1)

    silver_path = sys.argv[1]
    project_id = sys.argv[2]
    dataset = sys.argv[3]
    temp_bucket = sys.argv[4]  # Bucket for BigQuery temp files
    processing_date = sys.argv[5] if len(sys.argv) > 5 else None

    print(f"Starting Silver to Gold transformation...")
    print(f"Silver path: {silver_path}")
    print(f"Project ID: {project_id}")
    print(f"Dataset: {dataset}")
    print(f"Processing date: {processing_date}")

    # Initialize Spark session with BigQuery connector
    spark = (
        SparkSession.builder.appName("SilverToGoldTransformation")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        # Read silver data
        rides_df = read_silver_data(spark, silver_path, "rides_processed")
        drivers_df = read_silver_data(spark, silver_path, "drivers_processed")
        passengers_df = read_silver_data(spark, silver_path, "passengers_processed")

        # Create fact table
        fact_rides = create_fact_rides(spark, rides_df)

        # Create dimension tables (SCD Type 2)
        dim_drivers = create_dim_drivers_scd2(spark, drivers_df)
        dim_passengers = create_dim_passengers_scd2(spark, passengers_df)

        # Create aggregated tables
        agg_daily_rides = create_agg_daily_rides(spark, fact_rides)
        agg_driver_performance = create_agg_driver_performance(
            spark, fact_rides, dim_drivers
        )
        agg_demand_patterns = create_agg_demand_patterns(spark, fact_rides)

        # Write to BigQuery
        write_to_bigquery(
            fact_rides, "fact_rides", project_id, dataset, temp_bucket, "overwrite"
        )
        write_to_bigquery(
            dim_drivers, "dim_drivers", project_id, dataset, temp_bucket, "append"
        )  # Append for SCD2
        write_to_bigquery(
            dim_passengers,
            "dim_passengers",
            project_id,
            dataset,
            temp_bucket,
            "append",
        )  # Append for SCD2
        write_to_bigquery(
            agg_daily_rides,
            "agg_daily_rides",
            project_id,
            dataset,
            temp_bucket,
            "overwrite",
        )
        write_to_bigquery(
            agg_driver_performance,
            "agg_driver_performance",
            project_id,
            dataset,
            temp_bucket,
            "overwrite",
        )
        write_to_bigquery(
            agg_demand_patterns,
            "agg_demand_patterns",
            project_id,
            dataset,
            temp_bucket,
            "overwrite",
        )

        print("\n✅ Silver to Gold transformation completed successfully!")

    except Exception as e:
        print(f"\n❌ Error in transformation: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
