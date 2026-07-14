"""
stream_pisey_shop_products_cdc_bronze.py
Stream Debezium CDC events for Pisey's shop products table into its own
Bronze table, lakehouse.bronze.pisey_shop_products_cdc, committed to the
same Nessie catalog on MinIO that Trino and the batch job
(load_wfp_bronze.py) both use.

This is one half of a pair with stream_pisey_shop_orders_cdc_bronze.py.
Each source table now gets its own stream, its own Bronze table, and its
own checkpoint, refactored from an earlier version that subscribed to both
topics with one job and one shared Bronze table (tagged by source_table).
Splitting them means a failure or backlog on one table's stream never
blocks the other's, and each can be restarted independently without
touching the other's checkpoint.

Schema matches the earlier shared-table version, minus one column and plus
three. An earlier version of this job derived event_id from Kafka's own
offset (offset + 1), reasoning that a single-partition topic keeps offsets
gapless. True, but offset is a transport-layer artifact: it describes where
a message sits in a Kafka topic's log, not anything about the source
database's own change ordering, and it resets to zero if the topic is ever
recreated or the connector rebuilt from scratch. Debezium already embeds
the right answer in every event and it was going unused: payload.source.lsn
(the Postgres WAL log sequence number, a property of the source database
itself, not the topic), payload.source.txId (the transaction that produced
the change, letting you group events from the same commit), and
payload.source.ts_ms (when the source database committed the change,
distinct from the Kafka publish time and the Spark ingestion time already
captured). All three are pulled out as raw values, same as op and payload:
extracting envelope structure isn't a schema decision, so it doesn't cross
into Silver's job of interpreting it.

Unlike the batch job, this one also writes a Structured Streaming
checkpoint (offsets, commit log) to MinIO. That checkpoint goes through
Hadoop's generic FileSystem API, not Iceberg's S3FileIO, so it needs its
own s3a:// endpoint config below, on top of the catalog's own s3:// config.
No credentials are set for either: both S3FileIO (via the AWS SDK's
default credential chain) and Hadoop's S3A connector (via its own fallback
chain) pick up the AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars
already set in spark.yml, the same scoped LAKE_USER / LAKE_PASSWORD
credentials the batch job relies on. Nothing in this file reads them
directly.

Run from inside the spark-iceberg container (spark.jars.ivy points the Ivy
download cache at a writable dir, since the spark user's home is not
writable):

    /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
org.apache.iceberg:iceberg-aws-bundle:1.5.2,\
org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3,\
org.apache.hadoop:hadoop-aws:3.3.4,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        /opt/spark/jobs/stream_pisey_shop_products_cdc_bronze.py

Coordinates confirmed against a real run: Iceberg and Nessie copied from
load_wfp_bronze.py, hadoop-aws 3.3.4 matched to the transitively-resolved
hadoop-client-api/hadoop-client-runtime versions, spark-sql-kafka-0-10
pinned to 3.5.3 confirmed against the real Spark image
(apache/spark:3.5.3-scala2.12-java17-python3-ubuntu, per
docker-compose.spark.yml). Kafka broker is kafka-cdc, not kafka, per
docker-compose.cdc.yml.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE_TABLE = "products"
KAFKA_TOPIC = "pisey_shop.public.products"
TARGET_TABLE = "lakehouse.bronze.pisey_shop_products_cdc"
CHECKPOINT_LOCATION = "s3a://warehouse/checkpoints/pisey_shop_products_cdc/"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    kafka_key        string,
    source_table     string,
    event            string,
    op               string,
    payload          string,
    lsn              bigint,
    tx_id            bigint,
    source_ts_ms     bigint,
    topic            string,
    partition        int,
    offset           bigint,
    event_timestamp  timestamp,
    ingested_at      timestamp
) USING iceberg
"""


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("pisey_shop_products_cdc_stream")
        # Iceberg + Nessie SQL extensions, identical to the batch job.
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        # Catalog named "lakehouse", identical to load_wfp_bronze.py so table
        # paths match Trino: lakehouse.bronze.*
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.lakehouse.ref", "main")
        .config("spark.sql.catalog.lakehouse.authentication.type", "NONE")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse")
        # Object store I/O against MinIO (S3-compatible), identical to the batch job.
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        # Streaming-only addition: the checkpoint writer uses Hadoop's FileSystem
        # API against an s3a:// path, a different client than S3FileIO above, so
        # it needs its own endpoint config. No access/secret key set here either;
        # same env-var fallback as the catalog's S3FileIO.
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Structured Streaming's checkpoint manager here uses Hadoop's newer
        # FileContext API, not the classic FileSystem API above; FileContext
        # resolves s3a:// through this separate property. Default value is the
        # same class either way, but it must come from hadoop-aws being on the
        # classpath, which the FileSystem-only config above does not provide.
        .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Create once, append forever. This table is never dropped: a streaming
    # job resumes from checkpoint, and dropping the table out from under a
    # running checkpoint would strand it.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(CREATE_TABLE_SQL)

    events = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka-cdc:9092")
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .withColumn("event", F.col("value").cast("string"))
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.lit(SOURCE_TABLE).alias("source_table"),
            F.col("event"),
            F.get_json_object(F.col("event"), "$.payload.op").alias("op"),
            F.get_json_object(F.col("event"), "$.payload").alias("payload"),
            # Source-of-truth ordering fields, pulled from Debezium's own
            # source block rather than derived from Kafka transport metadata.
            # lsn survives topic recreation and connector rebuilds; offset
            # below does not.
            F.get_json_object(F.col("event"), "$.payload.source.lsn").cast("long").alias("lsn"),
            F.get_json_object(F.col("event"), "$.payload.source.txId").cast("long").alias("tx_id"),
            F.get_json_object(F.col("event"), "$.payload.source.ts_ms").cast("long").alias("source_ts_ms"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("event_timestamp"),
            F.current_timestamp().alias("ingested_at"),
        )
    )

    query = (
        events.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .toTable(TARGET_TABLE)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()