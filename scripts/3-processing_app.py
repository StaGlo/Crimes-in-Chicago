from pyspark.sql import SparkSession, functions as F, types as T  # type: ignore
import argparse


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

    args = parser.parse_args()

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

    print("crimes schema:")
    crimes.printSchema()

    # Enrich with IUCR static data
    enriched = crimes.join(iucr_df, on="IUCR", how="left")

    print("enriched schema:")
    enriched.printSchema()

    # Compute monthly aggregations
    agg = enriched.groupBy("year_month", "category", "District").agg(
        F.count("*").alias("total_crimes"),
        F.sum(F.when(F.col("Arrest") == "True", 1).otherwise(0)).alias("arrests"),
        F.sum(F.when(F.col("Domestic") == "True", 1).otherwise(0)).alias("domestics"),
        F.sum(F.when(F.col("index_code") == "I", 1).otherwise(0)).alias("fbi_indexed"),
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
