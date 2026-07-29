"""
build_pisey_silver_vault.py
Build the Silver Data Vault for Pisey's shop from the two Bronze CDC tables
written by the Pair 3 streaming jobs:

    lakehouse.bronze.pisey_shop_products_cdc   (Debezium events for products)
    lakehouse.bronze.pisey_shop_orders_cdc     (Debezium events for orders)

Output, all in lakehouse.silver:

    hub_product     one row per product_id business key
    hub_customer    one row per customer_name business key
    hub_order       one row per order_id business key
    link_order      one row per order, tying an order to its customer and product
    sat_product     product attributes over time (name, category, price, stock)
    sat_order       order attributes over time (quantity, status)

hub_customer has no satellite. The orders table carries a free-text
customer_name and nothing else about the customer, so the business key is the
only thing known. That is a weakness of the source, not the model: a name is
not a reliable identifier. The Vault records it honestly and leaves room for a
proper customer_id satellite the day a customer table exists.

Why the order is a hub, not a pure transactional link
-----------------------------------------------------
The P5.T diagram drew the sale as a link with measures on a satellite, which is
right for an immutable event. But public.orders has order_status, which changes
over an order's life, and those changes arrive as Debezium updates. An entity
with a mutable lifecycle is a hub with a status satellite, not an immutable
link. So: hub_order + sat_order for the lifecycle, and link_order for the
relationship to customer and product.

CDC drives the history
----------------------
Each Bronze row is one Debezium change event. The satellite loaders order every
event for a key by lsn (the Postgres WAL sequence number the streaming job went
out of its way to capture, because it reflects real source commit order and
survives topic rebuilds), then keep a new satellite row only when the tracked
attributes actually changed. A value that goes A then B then back to A produces
three rows, because change detection compares each event to the one before it,
not to a set of seen values. load_dts is taken from the source commit time
(source.ts_ms), not the Spark run time, so the Vault's history lines up with the
database's real history and point-in-time queries are correct.

Tombstones
----------
A delete in Postgres produces two Kafka messages: the 'd' change event, which
this job keeps, and a null-value tombstone for log compaction, which it drops.
The tombstone has no payload and no op, so it carries no business key; ingesting
it would seed a null-key row into every hub, link and satellite. The parse
functions filter it with op IS NOT NULL. The op profile still shows it as a NULL
op, on purpose, so the tombstone is visible as a diagnostic before it is removed.

Full load
---------
This job replays the entire Bronze CDC log each run and rebuilds every table
with atomic createOrReplace() writes: one immutable log in, one identical Vault
out. That is reproducible and fine at shop scale. The incremental, Airflow-
triggered version (watermark on ingested_at, seam handled against the current
satellite version) is a separate job for a later pair.

Run from inside the spark-iceberg container. Same three packages as the batch
job (this job only reads and writes Iceberg tables, no Kafka and no s3a
checkpoint, so it needs neither spark-sql-kafka nor hadoop-aws):

    /opt/spark/bin/spark-submit /opt/spark/jobs/batch/build_pisey_silver_vault.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.window import Window

PRODUCTS_CDC = "lakehouse.bronze.pisey_shop_products_cdc"
ORDERS_CDC = "lakehouse.bronze.pisey_shop_orders_cdc"
SILVER_NS = "lakehouse.silver"


# Deterministic event ordering for change detection. lsn first, because it is
# the true source commit order; source_ts_ms and offset only break ties.
# Built lazily inside a function: F.col() needs an active SparkContext, which
# does not exist yet at module import time.
def event_order():
    return [
        F.col("lsn").asc_nulls_first(),
        F.col("source_ts_ms").asc_nulls_first(),
        F.col("offset").asc_nulls_first(),
    ]


def sha_key(*cols: Column) -> Column:
    """sha2-256 over trimmed, upper-cased business key parts, '||' joined.
    Nulls map to a fixed token so a missing part never collides with empty."""
    parts = [F.coalesce(F.upper(F.trim(c.cast("string"))), F.lit("^^NULL^^")) for c in cols]
    return F.sha2(F.concat_ws("||", *parts), 256)


def hashdiff(*cols: Column) -> Column:
    """sha2-256 over descriptive attributes, order matters, case preserved."""
    parts = [F.coalesce(c.cast("string"), F.lit("^^NULL^^")) for c in cols]
    return F.sha2(F.concat_ws("||", *parts), 256)


def after_or_before(path: str) -> Column:
    """Row field from payload.after, or payload.before when after is absent
    (delete events carry the last known state in before)."""
    return F.coalesce(
        F.get_json_object(F.col("payload"), f"$.after.{path}"),
        F.get_json_object(F.col("payload"), f"$.before.{path}"),
    )


def decode_pg_numeric(path: str, scale: int = 2) -> Column:
    """Decode a Kafka Connect Decimal (org.apache.kafka.connect.data.Decimal).

    The connector runs on the default decimal.handling.mode = precise, so a
    Postgres numeric(10,2) arrives base64-encoded: the big-endian, two's-
    complement bytes of the unscaled integer, with the scale in the field schema.
    Confirmed against a real event: unit_price "BOI=" -> bytes 0x04E2 -> unscaled
    1250 -> 12.50 at scale 2.

    unbase64 -> hex -> conv(16, 10) reads the bytes as an unsigned integer, which
    is exact for non-negative values (prices never go below zero here). A value
    that could be negative would need signed two's-complement handling, which a
    small Python UDF would give. Null in (a delete's after) stays null out.
    """
    b64 = F.get_json_object(F.col("payload"), f"$.after.{path}")
    unscaled = F.conv(F.hex(F.unbase64(b64)), 16, 10).cast("decimal(38,0)")
    return (unscaled / F.lit(10 ** scale)).cast("decimal(10,2)")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("build_pisey_silver_vault")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri", "http://nessie:19120/api/v2")
        .config("spark.sql.catalog.lakehouse.ref", "main")
        .config("spark.sql.catalog.lakehouse.authentication.type", "NONE")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .getOrCreate()
    )


def profile_ops(raw, label: str) -> None:
    """Print the op distribution. The point-in-time proof depends on there being
    real update ('u') events in the log, so this is the first thing to check."""
    print(f"--- {label}: op distribution ---")
    raw.groupBy("op").count().orderBy("op").show()


def parse_products(spark: SparkSession):
    """Normalize product CDC events into typed rows with a hub hash key."""
    # Drop Debezium tombstones: null-value messages emitted after a delete for
    # log compaction. They have no payload and no op, so they carry no business
    # key. Left in, they seed a bogus null-key row into every hub, link and sat.
    raw = spark.table(PRODUCTS_CDC).where(F.col("op").isNotNull())
    return raw.select(
        sha_key(after_or_before("product_id")).alias("hk_product"),
        after_or_before("product_id").cast("int").alias("product_id"),
        # after.* only: on a delete these are null, which is what we want the
        # satellite to record (the row no longer exists).
        F.get_json_object(F.col("payload"), "$.after.name").alias("name"),
        F.get_json_object(F.col("payload"), "$.after.category").alias("category"),
        # numeric(10,2) arrives as a base64 Kafka Connect Decimal; decode it.
        decode_pg_numeric("unit_price", scale=2).alias("unit_price"),
        F.get_json_object(F.col("payload"), "$.after.stock_quantity").cast("int").alias("stock_quantity"),
        (F.col("op") == F.lit("d")).alias("dv_is_deleted"),
        F.col("lsn"),
        F.col("source_ts_ms"),
        F.col("offset"),
        F.expr("timestamp_millis(source_ts_ms)").alias("load_dts"),
        F.lit("pisey_shop.products").alias("record_source"),
    )


def parse_orders(spark: SparkSession):
    """Normalize order CDC events into typed rows with hub and link hash keys."""
    # Drop Debezium tombstones (see parse_products). The delete of an order
    # emits both a 'd' event, which we keep, and a null-value tombstone, which
    # we do not: it has no order_id and would create a spurious null-key row.
    raw = spark.table(ORDERS_CDC).where(F.col("op").isNotNull())
    return raw.select(
        sha_key(after_or_before("order_id")).alias("hk_order"),
        after_or_before("order_id").cast("int").alias("order_id"),
        sha_key(after_or_before("customer_name")).alias("hk_customer"),
        after_or_before("customer_name").alias("customer_name"),
        sha_key(after_or_before("product_id")).alias("hk_product"),
        after_or_before("product_id").cast("int").alias("product_id"),
        F.get_json_object(F.col("payload"), "$.after.quantity").cast("int").alias("quantity"),
        F.get_json_object(F.col("payload"), "$.after.order_status").alias("order_status"),
        # ZonedTimestamp: ISO-8601 with a Z. cast to timestamp parses it directly.
        F.get_json_object(F.col("payload"), "$.after.ordered_at").cast("timestamp").alias("ordered_at"),
        (F.col("op") == F.lit("d")).alias("dv_is_deleted"),
        F.col("lsn"),
        F.col("source_ts_ms"),
        F.col("offset"),
        F.expr("timestamp_millis(source_ts_ms)").alias("load_dts"),
        F.lit("pisey_shop.orders").alias("record_source"),
    )


def first_seen(df, key_col: str):
    """One row per business key: the earliest event that carried it."""
    w = Window.partitionBy(key_col).orderBy(*event_order())
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def satellite(df, hk_col: str, attr_cols, table: str) -> None:
    """Emit a satellite row only when the tracked attributes change, in lsn
    order. Includes dv_is_deleted so a delete produces a closing row."""
    hd = hashdiff(*[F.col(c) for c in attr_cols], F.col("dv_is_deleted"))
    w = Window.partitionBy(hk_col).orderBy(*event_order())
    changed = (
        df.withColumn("hashdiff", hd)
        .withColumn("_prev", F.lag("hashdiff").over(w))
        .where(F.col("_prev").isNull() | (F.col("hashdiff") != F.col("_prev")))
    )
    out_cols = [F.col(hk_col), F.col("load_dts"), F.col("hashdiff")] \
               + [F.col(c) for c in attr_cols] \
               + [F.col("dv_is_deleted"), F.col("record_source")]
    sat = changed.select(*out_cols)
    sat.writeTo(f"{SILVER_NS}.{table}").using("iceberg").createOrReplace()
    print(f"{table} rows: {sat.count()}")


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NS}")

    # Confirm the log actually holds updates before building anything. If every
    # op is 'c'/'r' and there are no 'u' events, there is no history to model.
    profile_ops(spark.table(PRODUCTS_CDC), "products_cdc")
    profile_ops(spark.table(ORDERS_CDC), "orders_cdc")

    products = parse_products(spark)
    orders = parse_orders(spark)
    products.cache()
    orders.cache()
    print(f"product CDC events: {products.count()}")
    print(f"order CDC events:   {orders.count()}")

    # --- hubs -----------------------------------------------------------------
    # hub_product collects product keys from BOTH sources: the products stream
    # and the product_id references inside orders. A business key seen from any
    # source belongs in the hub.
    #
    # The orders table is on Postgres REPLICA IDENTITY DEFAULT, so a delete
    # event's `before` image carries only the primary key (order_id). Its
    # customer_name and product_id come through null. Those nulls must not become
    # business keys, or each delete seeds a spurious null-key row. We filter them
    # out per hub below. Setting REPLICA IDENTITY FULL on the source table would
    # carry the whole deleted row instead and remove the need for the filter.
    product_keys = (
        products.select("hk_product", "product_id", "lsn", "source_ts_ms", "offset", "load_dts", "record_source")
        .unionByName(
            orders.where(F.col("product_id").isNotNull())
            .select("hk_product", "product_id", "lsn", "source_ts_ms", "offset", "load_dts", "record_source")
        )
    )
    hub_product = first_seen(product_keys, "hk_product").select(
        "hk_product", "product_id", "load_dts", "record_source"
    )
    hub_product.writeTo(f"{SILVER_NS}.hub_product").using("iceberg").createOrReplace()
    print(f"hub_product rows: {hub_product.count()}")

    hub_customer = first_seen(
        orders.where(F.col("customer_name").isNotNull()), "hk_customer"
    ).select("hk_customer", "customer_name", "load_dts", "record_source")
    hub_customer.writeTo(f"{SILVER_NS}.hub_customer").using("iceberg").createOrReplace()
    print(f"hub_customer rows: {hub_customer.count()}")

    hub_order = first_seen(orders, "hk_order").select(
        "hk_order", "order_id", "load_dts", "record_source"
    )
    hub_order.writeTo(f"{SILVER_NS}.hub_order").using("iceberg").createOrReplace()
    print(f"hub_order rows: {hub_order.count()}")

    # --- link -----------------------------------------------------------------
    # One row per order. The relationship (order, customer, product) is fixed at
    # creation, so build from events that carry all three keys, then first_seen.
    # This drops the delete's partial image, whose null customer/product would
    # otherwise produce a link row with null legs.
    link_order = (
        first_seen(
            orders.where(F.col("customer_name").isNotNull() & F.col("product_id").isNotNull()),
            "hk_order",
        )
        .withColumn(
            "hk_link_order",
            sha_key(F.col("order_id"), F.col("customer_name"), F.col("product_id")),
        )
        .select("hk_link_order", "hk_order", "hk_customer", "hk_product", "load_dts", "record_source")
    )
    link_order.writeTo(f"{SILVER_NS}.link_order").using("iceberg").createOrReplace()
    print(f"link_order rows: {link_order.count()}")

    # --- satellites -----------------------------------------------------------
    satellite(products, "hk_product", ["name", "category", "unit_price", "stock_quantity"], "sat_product")
    satellite(orders, "hk_order", ["quantity", "order_status", "ordered_at"], "sat_order")

    print("Silver Data Vault build complete.")
    spark.stop()


if __name__ == "__main__":
    main()