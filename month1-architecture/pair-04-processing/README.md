# Pair 4: Processing with PySpark

The processing pair. It takes the raw Bronze tables landed in Pair 3 and turns them into a Silver layer that makes real promises: every column has a type, every unit means the same thing, every row is unique on a defined key, and anything that fails those promises is quarantined instead of silently dropped. The theory post explains why distributed processing exists at all and what Spark contributed. The project post builds the Bronze-to-Silver transformation for the WFP dataset on a custom Spark image that bakes in every jar the batch and streaming jobs need, so `spark-submit --packages` disappears from the workflow.

## Posts

- **Theory.** Why Spark: processing data at a scale one machine can't handle. [Read on Medium →](https://medium.com/@withrathanak/why-spark-processing-data-at-a-scale-one-machine-cant-handle-d701f227b303)
- **Project.** Processing Bronze into Silver with PySpark: a real transformation pipeline. [Read on Medium →]()

## Architecture

![Bronze-to-Silver transformation with a quarantine path: the Pair 3 Bronze Iceberg table feeds a PySpark job that enforces types with try_cast and routes cast failures to a rejects table, normalizes the priceflag column into boolean flags, decomposes units into quantity plus base unit to derive a comparable price_per_base_unit, deduplicates on the natural key, and rewrites the Silver Iceberg table atomically through Nessie](Post4-P.png)
*Figure 1. Bronze to Silver, with a quarantine path for anything that fails the contract.*

## Tools by layer

| Layer | Tool | Role |
|---|---|---|
| Input | Iceberg Bronze table (`lakehouse.bronze.food_prices`) | Typed, unopinionated raw layer from Pair 3 |
| Processing | Apache Spark / PySpark on Spark 3.5.3 | Reads Bronze, transforms, writes Silver |
| Runtime | Custom Docker image (`lakehouse-spark:3.5.3-iceberg1.5.2`) | Bakes Iceberg, Nessie, hadoop-aws, AWS SDK, and Kafka connector jars into `/opt/spark/jars`, replacing `--packages` at submit time |
| Table format | Apache Iceberg on Parquet | Silver and quarantine tables with schema evolution and snapshot history |
| Catalog | Project Nessie | Registers `lakehouse.silver.food_prices` and `lakehouse.silver.food_prices_rejects` |
| Object storage | MinIO AIStor | Underlying store for the Iceberg data and metadata files |
| Read verification | Trino | Queries the Silver table through the same Nessie catalog |

## How to run it

The Pair 2 storage stack and the Pair 3 Bronze table must both already exist. This project rebuilds the Spark container from a custom image and runs a single PySpark job.

### 1. Prerequisites

Docker Desktop is running, `lake-net` exists, the Pair 2 stack is healthy, and the Pair 3 batch job has populated `lakehouse.bronze.food_prices`.

```bash
docker exec -it trino trino
```

```sql
SELECT count(*) FROM lakehouse.bronze.food_prices;
-- expect 86,625
```

### 2. Build the custom Spark image

The Dockerfile pins the exact jar versions the Ivy cache resolved during Pair 3, plus the Kafka set the CDC streaming job used. One image serves both batch and streaming, so neither pipeline downloads a jar at submit time again.

```bash
cd project/
docker compose -f docker-compose.spark.yml build
docker compose -f docker-compose.spark.yml up -d
```

First build downloads the jars once (the AWS SDK bundle alone is a few hundred megabytes). Every build after that hits the Docker layer cache and is instant.

### 3. Verify the jars are on the classpath

Before trusting any job, confirm the jars actually landed where Spark loads them.

```bash
docker exec spark-iceberg sh -c "ls /opt/spark/jars | grep -i -e iceberg -e nessie -e hadoop-aws -e aws-java-sdk -e kafka -e pool2"
```

Nine lines back means Iceberg 1.5.2, iceberg-aws-bundle 1.5.2, Nessie 0.91.3, hadoop-aws 3.3.4, aws-java-sdk-bundle 1.12.262, spark-sql-kafka-0-10 3.5.3, spark-token-provider 3.5.3, kafka-clients 3.4.1, and commons-pool2 2.11.1 are all in place.

### 4. Run the transformation job

`--packages` is gone: the jars are baked in.

```bash
docker exec spark-iceberg /opt/spark/bin/spark-submit /opt/spark/jobs/batch/silver_food_prices.py
```

On PowerShell, keep the whole command on a single line: backslash continuation corrupts argument parsing.

The job prints a summary at the end:

```
bronze rows read:        86625
rows quarantined:        0
duplicates removed:      0
silver rows written:     86625
```

Zero rejects and zero dedupe hits are the correct outcome on today's WFP source, not a wasted step. The enforcement is the contract that holds when the source changes or the schema drifts.

### 5. Verify from Trino

```sql
SELECT count(*) FROM lakehouse.silver.food_prices;
-- expect 86,625

-- priceflag normalization
SELECT priceflag, is_actual, is_aggregate, count(*)
FROM lakehouse.silver.food_prices
GROUP BY 1, 2, 3;
-- expect actual 6,691 / aggregate 79,918 / actual,aggregate 16
-- with the hybrid rows true on both flags

-- quarantine table exists and is queryable even when empty
SELECT count(*) FROM lakehouse.silver.food_prices_rejects;

-- the query the Silver layer was built to answer:
-- vegetable oil, now comparable across pack sizes
SELECT price_date, unit, price, price_per_base_unit
FROM lakehouse.silver.food_prices
WHERE commodity_id = 96 AND pricetype = 'Retail'
ORDER BY price_date DESC
LIMIT 10;

-- and the non-food guardrail
SELECT is_food, count(*) FROM lakehouse.silver.food_prices GROUP BY 1;
-- expect is_food=false -> 1,380 (Wage, non-qualified labour)
--        is_food=true  -> 85,245
```

## Dataset

World Food Programme, *Cambodia Food Prices*, via the Humanitarian Data Exchange: [data.humdata.org/dataset/wfp-food-prices-for-cambodia](https://data.humdata.org/dataset/wfp-food-prices-for-cambodia). Sourced from AMO-MAFF (Cambodia Agricultural Market Information System) and FAO GIEWS. Licensed CC BY-IGO.

## References

- Zaharia et al., *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing*, NSDI 2012.
- Armbrust et al., *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*, CIDR 2021.