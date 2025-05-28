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
    # parser.add_argument(
    #     "--checkpoint-location",
    #     required=True,
    #     help="Directory for Spark checkpointing (e.g. gs://bucket/checkpoints)",
    # )

    args = parser.parse_args()

    spark = SparkSession.builder.appName("CrimesStructuredStreaming").getOrCreate()

    crime_schema = T.StructType(
        [
            T.StructField("ID", T.StringType()),
            T.StructField("Date", T.StringType()),
            T.StructField("IUCR", T.StringType()),
            T.StructField("Arrest", T.StringType()),
            T.StructField("Domestic", T.StringType()),
            T.StructField("District", T.IntegerType()),
            T.StructField("ComArea", T.IntegerType()),
            T.StructField("Latitude", T.DoubleType()),
            T.StructField("Longitude", T.DoubleType()),
        ]
    )

    iucr_df = (
        spark.read.option("header", True)
        .csv(args.static_file)
        .select(
            F.col("IUCR"),
            F.col("PRIMARY DESCRIPTION").alias("category"),
            F.col("INDEX CODE").alias("index_code"),
        )
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .load()
    )

    csv_lines = raw_stream.select(F.expr("CAST(value AS STRING)").alias("csv_line"))

    crimes = csv_lines.select(
        F.from_json(F.col("csv_line").cast(T.StringType()), crime_schema).alias("crime")
    )

    crimes = crimes.withColumn(
        "event_time", F.to_timestamp(F.col("crime.Date"), "MM/dd/yyyy hh:mm:ss a")
    )

    enriched = crimes.join(iucr_df, crimes.crime.IUCR == iucr_df.IUCR, "left")

    aggregated = (
        enriched.withColumn("month", F.date_trunc("month", "event_time"))
        .groupBy("month", "category", "District")
        .agg(
            F.count("*").alias("total_crimes"),
            F.sum(F.when(F.col("crime.Arrest") == "true", 1).otherwise(0)).alias(
                "arrests"
            ),
            F.sum(F.when(F.col("Domestic") == "true", 1).otherwise(0)).alias(
                "domestics"
            ),
            F.sum(F.when(F.col("index_code") == "I", 1).otherwise(0)).alias(
                "fbi_indexed"
            ),
        )
    )

    query = (
        aggregated.writeStream.format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 30)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
