"""
load_wfp_bronze.py
Batch-load the WFP Cambodia food price CSV into the Bronze table
lakehouse.bronze.food_prices, committed to the same Nessie catalog on MinIO
that Trino reads.

This job OWNS the table: it drops and recreates it, then loads it. The schema
includes two bookkeeping columns baked in from the start:
    source       -> constant 'wfp'   (which dataset a row came from)
    ingested_at  -> current_timestamp (when this load ran)

Run from inside the spark-iceberg container (spark.jars.ivy points the Ivy
download cache at a writable dir, since the spark user's home is not writable):

    /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3 /opt/spark/jobs/load_wfp_bronze.py

MinIO credentials (the scoped LAKE_USER / LAKE_PASSWORD) are read from the
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars set in spark.yml.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit

# --- Config: change these two lines only if your paths/names differ ----------
CSV_PATH = "/opt/spark/data/wfp_food_prices_khm.csv"
TARGET_TABLE = "lakehouse.bronze.food_prices"
# -----------------------------------------------------------------------------

# Full Bronze schema, source columns plus two bookkeeping columns.
# Spark's `timestamp` maps to Iceberg `timestamptz`, matching current_timestamp().
CREATE_TABLE_SQL = f"""
CREATE TABLE {TARGET_TABLE} (
    price_date    date,
    admin1        string,
    admin2        string,
    market        string,
    market_id     int,
    latitude      double,
    longitude     double,
    category      string,
    commodity     string,
    commodity_id  int,
    unit          string,
    priceflag     string,
    pricetype     string,
    currency      string,
    price         double,
    usd_price     double,
    source        string,
    ingested_at   timestamp
) USING iceberg
"""


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("wfp_bronze_batch_load")
        # Iceberg + Nessie SQL extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        # Catalog named "lakehouse" so table paths match Trino: lakehouse.bronze.*
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.lakehouse.ref", "main")
        .config("spark.sql.catalog.lakehouse.authentication.type", "NONE")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse")
        # Object store I/O against MinIO (S3-compatible)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Read the CSV as raw strings first (no inference). Quoting is handled by the
    # reader, so commas inside "Rice (mixed, low quality)" do not break parsing.
    raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(CSV_PATH)
    )

    print(f"Raw rows read from CSV: {raw.count()}")
    print("Source columns:", raw.columns)

    # Map source columns to the table schema, in table column order, then append
    # the two bookkeeping columns. Types follow the table: prices and coordinates
    # are DOUBLE, ids INTEGER.
    df = raw.select(
        to_date(col("date"), "yyyy-MM-dd").alias("price_date"),
        col("admin1").alias("admin1"),
        col("admin2").alias("admin2"),
        col("market").alias("market"),
        col("market_id").cast("int").alias("market_id"),
        col("latitude").cast("double").alias("latitude"),
        col("longitude").cast("double").alias("longitude"),
        col("category").alias("category"),
        col("commodity").alias("commodity"),
        col("commodity_id").cast("int").alias("commodity_id"),
        col("unit").alias("unit"),
        col("priceflag").alias("priceflag"),
        col("pricetype").alias("pricetype"),
        col("currency").alias("currency"),
        col("price").cast("double").alias("price"),
        col("usdprice").cast("double").alias("usd_price"),
        lit("wfp").alias("source"),
        current_timestamp().alias("ingested_at"),
    )

    # Lightweight profile before writing.
    print(f"Rows after mapping: {df.count()}")
    unparseable_dates = df.filter(col("price_date").isNull()).count()
    print(f"Rows with unparseable date: {unparseable_dates}")
    print("Date span:")
    df.selectExpr("min(price_date) AS first_date", "max(price_date) AS last_date").show()
    df.show(5, truncate=False)

    # Own the table: drop and recreate with the full schema, then load.
    # Dropping first makes the job fully reproducible: one source file in,
    # one identical table out, no leftover columns or schema drift.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    spark.sql(CREATE_TABLE_SQL)
    df.writeTo(TARGET_TABLE).append()

    # Read back to confirm the commit landed.
    written = spark.table(TARGET_TABLE).count()
    print(f"Wrote {TARGET_TABLE}: {written} rows")

    spark.stop()


if __name__ == "__main__":
    main()