from pyspark.sql import SparkSession, functions as F, types as T  # type: ignore
import argparse
import socket


def main():
    # Argument parser for command line options
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming Crime Aggregator"
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--input-topic",
        default="crimes-in-chicago-topic",
        help="Kafka topic with raw crime CSV lines",
    )
    parser.add_argument(
        "--static-file",
        required=True,
        help="Path to IUCR codes CSV",
    )
    parser.add_argument(
        "--checkpoint-location",
        required=True,
        help="Directory for Spark checkpointing",
    )
    parser.add_argument(
        "--delay",
        choices=["A", "C"],
        required=True,
        help="Delay mode: A/C",
    )
    parser.add_argument(
        "-D", "--window-days", type=int, default=7, help="Anomaly parameter: D"
    )
    parser.add_argument(
        "-P", "--threshold", type=float, default=40.0, help="Anomaly parameter: P"
    )
    args = parser.parse_args()

    # Get hostname
    master_host = socket.gethostname()

    # JDBC connection options for Docker Postgres
    jdbc_url = f"jdbc:postgresql://{master_host}:5432/crimes"
    jdbc_props = {
        "user": "postgres",
        "password": "changeme",
        "driver": "org.postgresql.Driver",
    }

    # Initialize Spark session
    spark = SparkSession.builder.appName("CrimesStructuredStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Schema for raw CSV from Kafka
    crime_schema_csv = "ID STRING, Date STRING, IUCR STRING, Arrest STRING, Domestic STRING, District DOUBLE, ComArea DOUBLE, Latitude DOUBLE, Longitude DOUBLE"

    # Load static IUCR codes
    iucr_df = (
        spark.read.option("header", True)
        .csv(args.static_file)
        .select(
            F.col("IUCR"),
            F.col("PRIMARY DESCRIPTION").alias("category"),
            F.col("INDEX CODE").alias("index_code"),
        )
    )

    # Read streaming data from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse CSV lines from Kafka 'value'
    crimes = raw_stream.selectExpr("CAST(value AS STRING) as csv_line").select(
        F.from_csv(
            F.col("csv_line"),
            crime_schema_csv,
            {"header": "false"},
        ).alias("crime")
    )

    # Flatten the struct, parse timestamp and drop redundant columns
    crimes = crimes.select("crime.*")
    crimes = crimes.withColumn("event_time", F.to_timestamp(F.col("Date")))
    crimes = crimes.withColumn(
        "year_month", F.date_format(F.col("event_time"), "yyyy-MM")
    )
    crimes = crimes.drop("Date", "ComArea", "Latitude", "Longitude")

    # Remove leading zeros from IUCR in both streaming and static dataframes
    crimes = crimes.withColumn("IUCR", F.regexp_replace(F.col("IUCR"), "^0+", ""))
    iucr_df = iucr_df.withColumn("IUCR", F.regexp_replace(F.col("IUCR"), "^0+", ""))

    # Enrich with IUCR static data
    enriched = crimes.join(iucr_df, on="IUCR", how="left")

    # Fill missing values
    enriched = enriched.na.fill(
        {"category": "UNKNOWN", "year_month": "UNKNOWN_MONTH", "District": -1}
    )

    # Find anomalies
    anomalies = (
        enriched.withWatermark("event_time", f"{args.window_days} days")
        .groupBy(
            F.window("event_time", f"{args.window_days} days", "1 day").alias("w"),
            F.col("District"),
        )
        .agg(
            F.sum(F.when(F.col("index_code") == "I", 1).otherwise(0)).alias(
                "fbi_indexed"
            ),
            F.count("*").alias("total_crimes"),
        )
        .withColumn("pct_fbi", F.col("fbi_indexed") / F.col("total_crimes") * 100.0)
        .filter(F.col("pct_fbi") >= args.threshold)
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("District"),
            "fbi_indexed",
            "total_crimes",
            F.round(F.col("pct_fbi"), 2).alias("pct_fbi"),
        )
    )

    # Create stream based on delay mode
    if args.delay == "A":
        agg = enriched.groupBy("year_month", "category", "District").agg(
            F.count("*").alias("total_crimes"),
            F.sum(F.when(F.col("Arrest") == "True", 1).otherwise(0)).alias("arrests"),
            F.sum(F.when(F.col("Domestic") == "True", 1).otherwise(0)).alias(
                "domestics"
            ),
            F.sum(F.when(F.col("index_code") == "I", 1).otherwise(0)).alias(
                "fbi_indexed"
            ),
        )
        stream = agg.writeStream.outputMode("complete")

    elif args.delay == "C":
        windowed = (
            enriched.withWatermark("event_time", "31 days")
            .groupBy(
                F.window("event_time", "30 days").alias("w"),
                F.col("category"),
                F.col("District"),
            )
            .agg(
                F.count("*").alias("total_crimes"),
                F.sum(F.when(F.col("Arrest") == "True", 1).otherwise(0)).alias(
                    "arrests"
                ),
                F.sum(F.when(F.col("Domestic") == "True", 1).otherwise(0)).alias(
                    "domestics"
                ),
                F.sum(F.when(F.col("index_code") == "I", 1).otherwise(0)).alias(
                    "fbi_indexed"
                ),
            )
            .select(
                F.date_format(F.col("w.start"), "yyyy-MM").alias("year_month"),
                "category",
                "District",
                "total_crimes",
                "arrests",
                "domestics",
                "fbi_indexed",
            )
        )
        stream = windowed.writeStream.outputMode("append")

    # Writes each micro-batch into the crime_aggregates table
    def write_to_postgres(batch_df, batch_id):
        (
            batch_df.write.mode("overwrite")
            .option("truncate", "true")
            .jdbc(url=jdbc_url, table="crime_aggregates", properties=jdbc_props)
        )

    def write_anomalies(batch_df, batch_id):
        (
            batch_df.write.mode("append").jdbc(
                url=jdbc_url, table="crime_anomalies", properties=jdbc_props
            )
        )

    # Write results
    query = (
        stream.foreachBatch(write_to_postgres)
        .option("checkpointLocation", args.checkpoint_location)
        .start()
    )

    anomaly_query = (
        anomalies.writeStream.foreachBatch(write_anomalies)
        .option("checkpointLocation", f"{args.checkpoint_location}/anomalies")
        .start()
    )

    query.awaitTermination()
    anomaly_query.awaitTermination()


if __name__ == "__main__":
    main()
