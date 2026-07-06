# Pair 3 — Batch Ingestion and Change Data Capture

The ingestion pair. It covers the two ways raw data first lands in the Bronze layer: periodic batch loads, and continuous change data capture. The theory post frames these as two engineering choices around how change is detected and how fresh the copy must be. The first project post lands the WFP Cambodia food price dataset into Bronze with a PySpark batch job on top of the Pair 2 storage stack. A second project post covering the CDC counterpart (PostgreSQL to Bronze via Debezium and Kafka) will follow.

## Posts

- **Theory:** Batch ingestion vs. change data capture: how change is detected, how fresh the copy must be. [Read on Medium →](https://medium.com/@withrathanak/two-ways-to-get-data-in-batch-ingestion-and-change-data-capture-e018cf56200c)
- **Project (batch):** Loading the WFP Cambodia food price dataset into Bronze with PySpark and Iceberg. [Read on Medium →](https://medium.com/@withrathanak/project-i-loaded-86k-rows-of-cambodia-food-price-data-into-a-lakehouse-heres-the-actual-process-3790c08488af)
- **Project (CDC):** *Coming soon:* PostgreSQL to Bronze via Debezium and Kafka.

## Architecture

![Batch ingestion and change data capture side by side, both landing in the same Bronze namespace: on the batch side, a scheduled PySpark job reads the WFP CSV and writes an Iceberg table; on the CDC side, a change stream flows from PostgreSQL through Debezium into Kafka and then into Bronze](Post3-P.png)
*Figure 1. Batch ingestion and change data capture as two paths into Bronze.*

## Tools by layer (batch project)

| Layer | Tool | Role |
|---|---|---|
| Source | WFP Cambodia food price CSV | Raw dataset for the batch load |
| Processing | Apache Spark / PySpark | Reads the CSV and writes the Iceberg table |
| Table format | Apache Iceberg on Parquet | Target table with ACID guarantees and snapshot history |
| Catalog | Project Nessie | Registers the Bronze table in the `lakehouse` catalog |
| Object storage | MinIO AIStor | Underlying store for the Iceberg data and metadata files |
| Runtime | Docker (`apache/spark` image) | Isolated PySpark environment on the `lake-net` network |

## How to run it (batch)

The Pair 2 storage stack must already be up. This project adds a PySpark job that reads the WFP CSV and writes it to `lakehouse.bronze.food_prices` with two extra columns: `source = 'wfp'` and `ingested_at = current_timestamp`.

### 1. Prerequisites

Docker Desktop is running, the `lake-net` network exists, and the Pair 2 stack is healthy.

```commandline
docker compose -f docker-compose.data-lakehouse.yml ps
```

Confirm MinIO, Nessie, and Trino are healthy.

### 2. Stage the dataset

Place `wfp_food_prices_khm.csv` where the Spark container can read it.

```commandline
cd project/batch-ingestion/
mkdir data, jobs
```
Copy `wfp_food_prices_khm.csv`into _.\data_

Copy `load_wfp_bronze.py` into _.\jobs_

### 3. Run the PySpark job

Run the ingestion job inside the `apache/spark` image, joined to `lake-net` so it can reach Nessie and MinIO.

````commandline
docker compose --env-file .env -f docker-compose.spark.yml up -d
````

````commandline
docker exec -it spark-iceberg bash
````

````commandline
/opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3 /opt/spark/jobs/load_wfp_bronze.py
````

Three flags worth naming, all learned from real runtime errors:

- `/opt/spark/bin/spark-submit`: the `apache/spark` image does not put `spark-submit` on `PATH`.
- `--conf spark.jars.ivy=/tmp/.ivy2`: the default Ivy cache path (`/home/spark/.ivy2`) is not writable in the image; without this override, package resolution fails.
- `--packages`: this pulls Iceberg, Nessie, and Hadoop AWS jars at runtime. Acceptable for a first run; a custom Docker image with the jars baked in is on the roadmap for Pair 4 so runs are not gated on network resolution.

### 4. Verify from Trino

```bash
docker exec -it trino trino
```

```sql
-- row count and the two ingestion metadata columns
SELECT count(*) FROM lakehouse.bronze.food_prices;
SELECT source, min(ingested_at), max(ingested_at) FROM lakehouse.bronze.food_prices GROUP BY source;

-- snapshot history: proof the write landed as an Iceberg commit
SELECT committed_at, snapshot_id
FROM lakehouse.bronze."food_prices$snapshots"
ORDER BY committed_at DESC;
```

## Dataset

World Food Programme, *Cambodia Food Prices*, via the Humanitarian Data Exchange: [data.humdata.org/dataset/wfp-food-prices-for-cambodia](https://data.humdata.org/dataset/wfp-food-prices-for-cambodia). Sourced from AMO-MAFF (Cambodia Agricultural Market Information System) and FAO GIEWS. Licensed CC BY-IGO.