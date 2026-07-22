# silver_food_prices.py
# Bronze -> Silver transformation for the WFP Cambodia food price dataset.
#
# What this job does:
#   1. Reads the Bronze Iceberg table written in Pair 3.
#   2. Enforces the schema with try_cast and quarantines rows that fail.
#   3. Normalizes the multi-value priceflag column into boolean flags.
#   4. Decomposes the unit column into quantity + base unit and derives
#      a comparable price per base unit.
#   5. Deduplicates on the natural key.
#   6. Stamps a processed_at build marker, orders columns for readability,
#      and rewrites the Silver table from scratch (the job owns the table
#      lifecycle, same policy as the Bronze ingestion job: a full refresh
#      with an atomic snapshot swap, not an incremental merge).
#
# Run: reuse the exact spark-submit invocation from the Pair 3 batch job,
# minus the --packages flag if you are running the custom image that bakes
# the jars in. On PowerShell, keep the whole command on a single line.

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# NOTE: the SparkSession builder config below is confirmed working against
# the running stack (Iceberg + Nessie extensions, "lakehouse" catalog via
# Nessie at /api/v2, S3FileIO against MinIO). If you fork this for a
# different stack, the catalog block is what needs to change.

CATALOG = "lakehouse"   # named to match Trino, so paths read the same in both engines
BRONZE_TABLE = f"{CATALOG}.bronze.food_prices"
SILVER_TABLE = f"{CATALOG}.silver.food_prices"
REJECTS_TABLE = f"{CATALOG}.silver.food_prices_rejects"

NATURAL_KEY = ["price_date", "market_id", "commodity_id", "pricetype", "unit", "priceflag"]

# Target types for schema enforcement. Everything not listed here stays a
# trimmed string.
# Column names follow the Bronze table written by the Pair 3 job, which
# renamed date -> price_date and usdprice -> usd_price and added the
# lineage columns source and ingested_at (carried through untouched).
# Bronze is already typed, so on today's data every try_cast below is a
# no-op verification. The enforcement stays: it is the contract that
# holds when Bronze arrives as strings or the upstream schema drifts.
TYPED_COLUMNS = {
    "price_date": "date",
    "market_id": "int",
    "commodity_id": "int",
    "latitude": "double",
    "longitude": "double",
    "price": "double",
    "usd_price": "double",
}

# Note: admin1/admin2 are renamed to province/district immediately after
# the Bronze read, so every list below uses the Silver names.
STRING_COLUMNS = [
    "province", "district", "market", "category",
    "commodity", "unit", "priceflag", "pricetype", "currency",
]

# Every unit that exists in the source, decomposed by hand. The job fails
# loudly if a unit shows up that is not in this map. That is deliberate:
# an unknown unit is a schema change, and schema changes should stop the
# pipeline, not slide through it.
UNIT_MAP = {
    # source unit: (quantity, base unit)
    "KG":     (1.0,  "KG"),
    "L":      (1.0,  "L"),
    "5 L":    (5.0,  "L"),
    "730 ML": (0.73, "L"),
    "140 ML": (0.14, "L"),
    "10 pcs": (10.0, "pcs"),
    "Day":    (1.0,  "Day"),
}


def build_spark() -> SparkSession:
    # The master is supplied by spark-submit (local[*] on this stack), not
    # hard-coded here. S3FileIO takes credentials from the AWS SDK default
    # chain, i.e. the AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY environment
    # variables set on the container (the scoped LAKE_USER identity).
    return (
        SparkSession.builder
        .appName("silver-food-prices")
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

    # -----------------------------------------------------------------------
    # 1. Read Bronze and trim every string column defensively.
    # -----------------------------------------------------------------------
    df = spark.table(BRONZE_TABLE)
    bronze_count = df.count()

    # Rename WFP's generic administrative levels to what they mean in
    # Cambodia: admin1 is the province, admin2 is the district (srok, or
    # khan within Phnom Penh). Bronze keeps the source names; Silver is
    # where columns earn business names.
    df = (
        df
        .withColumnRenamed("admin1", "province")
        .withColumnRenamed("admin2", "district")
    )

    for c in STRING_COLUMNS:
        df = df.withColumn(c, F.trim(F.col(c)))

    # -----------------------------------------------------------------------
    # 2. Schema enforcement with try_cast + quarantine.
    #    try_cast returns null instead of throwing. Note: in PySpark 3.5 it
    #    exists only as a SQL function (via expr), not as a Column method;
    #    Column.try_cast arrives in PySpark 4.0. A row is rejected when a
    #    source value was present but did not survive the cast.
    # -----------------------------------------------------------------------
    reasons = []
    for c, t in TYPED_COLUMNS.items():
        casted = F.expr(f"try_cast({c} AS {t})")
        df = df.withColumn(f"__cast_{c}", casted)
        reasons.append(
            F.when(F.col(c).isNotNull() & casted.isNull(),
                   F.lit(f"{c}: failed cast to {t}"))
        )

    df = df.withColumn(
        "reject_reasons",
        F.filter(F.array(*reasons), lambda x: x.isNotNull()),
    )

    rejects = df.filter(F.size("reject_reasons") > 0)
    good = df.filter(F.size("reject_reasons") == 0)

    # Replace the raw columns with their cast versions and drop the scaffolding.
    for c, t in TYPED_COLUMNS.items():
        good = good.withColumn(c, F.col(f"__cast_{c}"))
    good = good.drop(*[f"__cast_{c}" for c in TYPED_COLUMNS], "reject_reasons")

    # -----------------------------------------------------------------------
    # 3. Normalize priceflag. The source contains "actual", "aggregate", and
    #    a handful of "actual,aggregate" rows. Keep the original string for
    #    lineage, add boolean flags for querying.
    # -----------------------------------------------------------------------
    flags = F.split(F.col("priceflag"), ",")
    good = (
        good
        .withColumn("is_actual", F.array_contains(flags, "actual"))
        .withColumn("is_aggregate", F.array_contains(flags, "aggregate"))
    )

    # -----------------------------------------------------------------------
    # 4. Unit standardization. Fail loudly on unknown units, then derive a
    #    price per base unit so commodities priced under different pack
    #    sizes become comparable.
    # -----------------------------------------------------------------------
    known_units = list(UNIT_MAP.keys())
    unknown = (
        good.filter(~F.col("unit").isin(known_units))
        .select("unit").distinct().collect()
    )
    if unknown:
        units = ", ".join(sorted(r["unit"] for r in unknown))
        print(f"FATAL: unmapped units found in source: {units}", file=sys.stderr)
        sys.exit(1)

    qty_expr = F.create_map(
        *[x for u, (q, b) in UNIT_MAP.items() for x in (F.lit(u), F.lit(q))]
    )
    base_expr = F.create_map(
        *[x for u, (q, b) in UNIT_MAP.items() for x in (F.lit(u), F.lit(b))]
    )
    good = (
        good
        .withColumn("unit_qty", qty_expr[F.col("unit")])
        .withColumn("unit_base", base_expr[F.col("unit")])
        .withColumn("price_per_base_unit", F.round(F.col("price") / F.col("unit_qty"), 2))
        .withColumn("usd_price_per_base_unit", F.round(F.col("usd_price") / F.col("unit_qty"), 4))
        # WFP includes a non-food wage indicator (category="non-food",
        # commodity="Wage (non-qualified labour)", unit="Day") because a
        # daily wage is a food-security signal, not because it is a food
        # price. It shares price_per_base_unit's column but not its
        # meaning: KHR per day of labor is not KHR per kg of food. Flag it
        # rather than silently averaging it in with everything else.
        .withColumn("is_food", F.col("category") != F.lit("non-food"))
    )

    # -----------------------------------------------------------------------
    # 5. Deduplicate on the natural key. Silver guarantees uniqueness
    #    regardless of what upstream does.
    # -----------------------------------------------------------------------
    before = good.count()
    good = good.dropDuplicates(NATURAL_KEY)
    after = good.count()

    # -----------------------------------------------------------------------
    # 6. Stamp the build, order the columns, write Silver.
    #    processed_at marks which Silver build a reader is looking at,
    #    distinct from ingested_at, which marks when the row entered Bronze.
    #    The final select fixes a human-readable column order: time,
    #    location, commodity, unit, price, flags, lineage.
    # -----------------------------------------------------------------------
    good = good.withColumn("processed_at", F.current_timestamp())

    good = good.select(
        # time
        "price_date",
        # location
        "province", "district", "market", "market_id", "latitude", "longitude",
        # commodity
        "category", "commodity", "commodity_id",
        # unit
        "unit", "unit_qty", "unit_base",
        # price
        "currency", "price", "price_per_base_unit",
        "usd_price", "usd_price_per_base_unit",
        # observation flags
        "pricetype", "priceflag", "is_actual", "is_aggregate", "is_food",
        # lineage
        "source", "ingested_at", "processed_at",
    )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")

    (
        good.writeTo(SILVER_TABLE)
        .partitionedBy(F.years("price_date"))
        .createOrReplace()
    )

    (
        rejects
        .withColumn("reject_reasons", F.concat_ws("; ", "reject_reasons"))
        .drop(*[f"__cast_{c}" for c in TYPED_COLUMNS])
        .writeTo(REJECTS_TABLE)
        .createOrReplace()
    )

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"bronze rows read:        {bronze_count}")
    print(f"rows quarantined:        {rejects.count()}")
    print(f"duplicates removed:      {before - after}")
    print(f"silver rows written:     {after}")


if __name__ == "__main__":
    main()