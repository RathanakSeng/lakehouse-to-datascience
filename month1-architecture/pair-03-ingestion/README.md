# Pair 3 — Batch Ingestion and Change Data Capture

The ingestion pair. It covers the two ways raw data first lands in the Bronze layer: periodic batch loads, and continuous change data capture. The theory post frames these as two engineering choices around how change is detected and how fresh the copy must be. The batch project lands the WFP Cambodia food price dataset into Bronze with a PySpark job on top of the Pair 2 storage stack. The CDC project streams every insert, update, and delete from a live PostgreSQL source into Bronze through Debezium, Kafka, and Spark Structured Streaming.

## Posts

- **Theory:** Batch ingestion vs. change data capture: how change is detected, how fresh the copy must be. [Read on Medium →](https://medium.com/@withrathanak/two-ways-to-get-data-in-batch-ingestion-and-change-data-capture-e018cf56200c)
- **Project (batch):** Loading the WFP Cambodia food price dataset into Bronze with PySpark and Iceberg. [Read on Medium →](https://medium.com/@withrathanak/project-i-loaded-86k-rows-of-cambodia-food-price-data-into-a-lakehouse-heres-the-actual-process-3790c08488af)
- **Project (CDC):** Building a real-time ingestion pipeline with Kafka and Debezium: every step. [Read on Medium →]()

## Architecture

![Batch ingestion and change data capture side by side, both landing in the same Bronze namespace: on the batch side, a scheduled PySpark job reads the WFP CSV and writes an Iceberg table; on the CDC side, a change stream flows from PostgreSQL through Debezium into Kafka and then into Bronze](Post3-P.png)
*Figure 1. Batch ingestion and change data capture as two paths into Bronze.*

## Tools by layer 

### Batch project

| Layer | Tool | Role |
|---|---|---|
| Source | WFP Cambodia food price CSV | Raw dataset for the batch load |
| Processing | Apache Spark / PySpark | Reads the CSV and writes the Iceberg table |
| Table format | Apache Iceberg on Parquet | Target table with ACID guarantees and snapshot history |
| Catalog | Project Nessie | Registers the Bronze table in the `lakehouse` catalog |
| Object storage | MinIO AIStor | Underlying store for the Iceberg data and metadata files |
| Runtime | Docker (`apache/spark` image) | Isolated PySpark environment on the `lake-net` network |

### CDC project

| Layer | Tool | Role |
|---|---|---|
| Source database | PostgreSQL 16 with `wal_level=logical` | Operational database that CDC reads changes from |
| Change capture | Debezium 2.6 on Kafka Connect | Reads the Postgres WAL and emits change events |
| Event transport | Apache Kafka 4.3.1 (KRaft mode) | One topic per source table |
| Secrets injection | Kafka Connect `FileConfigProvider` (KIP-297) | Keeps DB credentials out of connector JSON |
| Stream processing | Spark Structured Streaming | Two independent jobs, one per source table, each with its own checkpoint |
| Table format | Apache Iceberg on Parquet | Append-only Bronze tables holding raw envelopes |
| Catalog | Project Nessie | Registers the two CDC tables in the `lakehouse` catalog |
| Object storage | MinIO AIStor | Shared with the batch project; same `warehouse` bucket |
| Read verification | Trino | Queries the Bronze CDC tables while the streams run |

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

## How to run it (CDC)
 
The Pair 2 storage stack must already be up. This project adds a Postgres source (`pisey_db`), Kafka in KRaft mode (`kafka-cdc`), Kafka Connect with Debezium (`connect-cdc`), plus two independent Spark Structured Streaming jobs. Each job writes to its own append-only Iceberg table under `lakehouse.bronze`, with its own checkpoint.
 
### 1. Prerequisites
 
Docker Desktop is running, `lake-net` exists, and the Pair 2 stack is healthy.
 
```bash
docker compose -f docker-compose.data-lakehouse.yml ps
```
 
### 2. Bring up the CDC services
 
```bash
cd project/cdc-ingestion/
cp .env.example .env
# edit .env: PISEY_DB_USER, PISEY_DB_PASSWORD, and set the same values
# in ./secrets/connect/pisey_db.properties so the connector can resolve them
 
docker compose -f docker-compose.cdc.yml up -d
docker compose -f docker-compose.cdc.yml ps
```
 
Confirm `pisey_db`, `kafka-cdc`, `connect-cdc`, and `kafka-cdc-ui` are healthy.
 
### 3. Seed the source database
 
Create the `products` and `orders` tables and seed products. `orders` starts empty on purpose so the snapshot phase captures a populated table and an empty table in the same connector run.
 
```bash
docker exec -i pisey_db psql -U "$PISEY_DB_USER" -d pisey_shop < sql/schema.sql
docker exec -i pisey_db psql -U "$PISEY_DB_USER" -d pisey_shop < sql/seed_products.sql
```
 
### 4. Register the Debezium connector
 
The connector JSON references credentials with `${file:/opt/kafka/secrets/pisey_db.properties:key}`, resolved by Kafka Connect's `FileConfigProvider` at registration time. No live credentials appear in the JSON posted to the REST API.
 
```bash
curl -X POST -H "Content-Type: application/json" \
  --data @connectors/pisey-shop-source-connector.json \
  http://localhost:8083/connectors
 
# check it's healthy
curl http://localhost:8083/connectors/pisey-shop-source/status
```
 
`snapshot.mode: initial` reads both tables as they exist right now, tagged `op: r`, then switches to tailing the WAL for live changes tagged `c`, `u`, `d`.
 
Two settings worth naming, both learned from real failures:
 
- `wal_level=logical` on the Postgres source. Without it, connector registration fails at validation: the default WAL does not carry enough to reconstruct row-level changes.
- `table.include.list` uses `schema.table` (`public.products`), not `database.table`. Getting this wrong does not error; the connector reports healthy and silently captures zero rows.
### 5. Run the two streaming jobs
 
Each source table gets its own streaming job, its own Bronze table, and its own checkpoint. Both jobs run inside the `spark-iceberg` container, joined to `lake-net`. The `--packages` list is pinned to versions that match the container's Spark 3.5.3.
 
```bash
# products stream
docker exec -it spark-iceberg /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark/jobs/stream/stream_pisey_shop_products_cdc_bronze.py
 
# orders stream (in a separate terminal)
docker exec -it spark-iceberg /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.91.3,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark/jobs/stream/stream_pisey_shop_orders_cdc_bronze.py  
```

### 6. Exercise the source database
 
Run a few statements against `pisey_db` to generate a mix of inserts, updates, and deletes across both tables:
 
```sql
-- customer buys two woks
INSERT INTO public.orders (customer_name, product_id, quantity, order_status)
VALUES ('Ms. Chanthy', 1, 2, 'placed');
 
-- payment confirmed
UPDATE public.orders SET order_status = 'paid'
WHERE customer_name = 'Ms. Chanthy' AND product_id = 1;
 
-- stock decrements
UPDATE public.products SET stock_quantity = stock_quantity - 2
WHERE product_id = 1;
 
-- a different customer cancels
INSERT INTO public.orders (customer_name, product_id, quantity, order_status)
VALUES ('Mr. Sovann', 3, 1, 'placed');
DELETE FROM public.orders
WHERE customer_name = 'Mr. Sovann' AND product_id = 3;
```
 
### 7. Verify from Trino
 
```bash
docker exec -it trino trino
```
 
```sql
-- events per operation type, with distinct source transactions
SELECT op, COUNT(*) AS events, COUNT(DISTINCT tx_id) AS transactions
FROM lakehouse.bronze.pisey_shop_products_cdc
GROUP BY 1 ORDER BY 2 DESC;
 
-- both CDC tables side by side
SELECT source_table, op, COUNT(*) AS events
FROM (
    SELECT source_table, op FROM lakehouse.bronze.pisey_shop_products_cdc
    UNION ALL
    SELECT source_table, op FROM lakehouse.bronze.pisey_shop_orders_cdc
)
GROUP BY 1, 2 ORDER BY 1, 3 DESC;
```
 
`op = r` marks snapshot rows; `c`, `u`, `d` are live inserts, updates, and deletes. `tx_id` reflects the actual Postgres transaction, so two updates committed together share a value, which a Kafka-offset-based counter cannot express.

## Dataset


**Batch project.** World Food Programme, *Cambodia Food Prices*, via the Humanitarian Data Exchange: [data.humdata.org/dataset/wfp-food-prices-for-cambodia](https://data.humdata.org/dataset/wfp-food-prices-for-cambodia). Sourced from AMO-MAFF (Cambodia Agricultural Market Information System) and FAO GIEWS. Licensed CC BY-IGO.
 
**CDC project.** An original mock operational database: Pisey's kitchenware shop, with `products` and `orders` tables seeded in PostgreSQL. Seed scripts are in `project/cdc-ingestion/sql/`.