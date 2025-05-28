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
            F.col("INDEX CODE").alias("index_code"),
        )
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .load()
    )

    csv_lines = raw_stream.select(F.expr("CAST(value AS STRING)").alias("line"))

    query = (
        csv_lines.writeStream.format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 30)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
