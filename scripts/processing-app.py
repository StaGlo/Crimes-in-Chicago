from pyspark.sql import SparkSession, functions as F, types as T
import argparse


def main():
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

    args = parser.parse_args()

    spark = SparkSession.builder.appName("CrimesStructuredStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Schema for raw CSV from Kafka
    crime_schema_csv = "ID STRING, Date STRING, IUCR STRING, Arrest STRING, Domestic STRING, District INT, ComArea INT, Latitude DOUBLE, Longitude DOUBLE"

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

    # Flatten the struct and parse timestamp
    crimes_flat = crimes.select(
        "crime.*",
        F.to_timestamp(F.col("crime.Date"), "MM/dd/yyyy hh:mm:ss a").alias(
            "event_time"
        ),
    )

    print("crimes_flat schema:")
    crimes_flat.printSchema()

    # Enrich with IUCR static data
    enriched = crimes_flat.join(iucr_df, on="IUCR", how="left")

    # Compute monthly aggregations
    agg = (
        enriched.withColumn("month", F.date_trunc("month", F.col("event_time")))
        .groupBy("month", "category", "District")
        .agg(
            F.count("*").alias("total_crimes"),
            F.sum(F.when(F.col("Arrest") == "true", 1).otherwise(0)).alias("arrests"),
            F.sum(F.when(F.col("Domestic") == "true", 1).otherwise(0)).alias(
                "domestics"
            ),
            F.sum(F.when(F.col("index_code") == "I", 1).otherwise(0)).alias(
                "fbi_indexed"
            ),
        )
    )

    # Write results in update mode (delay=A) to console for now
    query = (
        agg.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 50)
        .option("checkpointLocation", args.checkpoint_location)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
