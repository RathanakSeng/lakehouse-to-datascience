# Pair 5: Star Schema and Data Vault

The modeling pair. The theory post compares the two dominant analytical modeling patterns as opposite responses to the same pressure: arrange the data for the questions you have today (star schema), or arrange it to absorb change without rewriting what already exists (Data Vault). The project post builds the Data Vault at the Silver layer, fed by the Pair 3 CDC Bronze tables, so that Pisey's price changes and order lifecycle land as history instead of overwriting each other. A star schema mart projected on top of the Vault belongs in Gold and comes later in the series.

## Posts

- **Theory.** Star schema vs Data Vault: two ways to model the same business. [Read on Medium →](https://medium.com/@withrathanak/star-schema-vs-data-vault-two-ways-to-model-the-same-business-d0cf9af68ef4?sharedUserId=withrathanak)
- **Project.** When the shop keeps changing: building a CDC-fed Silver Data Vault. [Read on Medium →]()

## Architecture

![CDC-fed Data Vault at Silver: the Pair 3 Bronze CDC tables for products and orders feed a PySpark job that decodes each Debezium envelope, hashes business keys, and writes three hubs (hub_product, hub_customer, hub_order), one link (link_order), and two satellites (sat_product, sat_order); load_dts on every satellite row is stamped from the source commit time so history is preserved by default](Post5-P.png)
*Figure 1. CDC events from Pair 3 modeled into a Silver Data Vault: hubs, one link, and satellites.*

## Tools by layer

| Layer | Tool | Role |
|---|---|---|
| Input | Bronze CDC tables (`lakehouse.bronze.pisey_shop_products_cdc`, `lakehouse.bronze.pisey_shop_orders_cdc`) | Append-only change event log from Pair 3 |
| Processing | Apache Spark / PySpark on Spark 3.5.3 | Reads Bronze, decodes CDC envelopes, builds hubs, link, and satellites |
| Runtime | Custom Docker image `lakehouse-spark:3.5.3-iceberg1.5.2` from Pair 4 | Iceberg, Nessie, hadoop-aws, and AWS SDK jars baked in |
| Table format | Apache Iceberg on Parquet | Six Silver tables, each rebuilt via atomic `createOrReplace` |
| Catalog | Project Nessie | Registers the Data Vault tables under `lakehouse.silver` |
| Object storage | MinIO AIStor | Underlying store for Iceberg data and metadata |
| Read verification | Trino | Point-in-time joins and vault navigation |

## How to run it

The Pair 2 storage stack, the Pair 3 CDC pipeline, and the Pair 4 custom Spark image all have to exist first. This job reads only from Bronze; nothing new needs to run continuously.

### 1. Prerequisites

Confirm Bronze holds real change history. Point-in-time modeling only works if the log carries at least one update event.

```bash
docker exec -it trino trino
```

```sql
-- op distribution per source table
SELECT op, COUNT(*) FROM lakehouse.bronze.pisey_shop_products_cdc GROUP BY 1;
SELECT op, COUNT(*) FROM lakehouse.bronze.pisey_shop_orders_cdc   GROUP BY 1;
```

At minimum: some `c` events, and at least one `u` event on `products` so the price history is not flat. If not, generate a few updates against `pisey_db` from Pair 3 before running the vault build.

### 2. Run the vault build

`--packages` stays absent: the Pair 4 image bakes every jar the job needs.

```bash
docker exec spark-iceberg /opt/spark/bin/spark-submit /opt/spark/jobs/batch/build_pisey_silver_vault.py
```

The job prints the op distribution for both Bronze tables first as a diagnostic gate, then builds `hub_product`, `hub_customer`, `hub_order`, `link_order`, `sat_product`, and `sat_order` in turn. It replays the whole CDC log every run, so reruns are safe: a failed run is fixed by running again, not by cleaning up.

Three CDC-shaped realities are handled inside the job and worth naming, because a tutorial-shaped stream would not show them:

- **Tombstones.** After a delete, Debezium emits a null-value compaction marker with a null `op` and no payload. The job filters these with `op IS NOT NULL` before hashing keys. The op profile still prints the null count as a diagnostic.
- **Partial delete images.** The `orders` table runs on Postgres's default `REPLICA IDENTITY`, so a delete's `before` image carries only the primary key. `customer_name` and `product_id` arrive null. Hubs and links for those keys are built only from events where the key is present.
- **Encoded decimals.** On Debezium's default precise mode, `numeric(10,2)` arrives as a base64 Kafka Connect Decimal, not a number. `unit_price` is decoded from bytes and scale; a plain cast would write null into every price.

### 3. Verify from Trino

```sql
-- vault inventory
SELECT COUNT(*) FROM lakehouse.silver.hub_product;
SELECT COUNT(*) FROM lakehouse.silver.hub_customer;
SELECT COUNT(*) FROM lakehouse.silver.hub_order;
SELECT COUNT(*) FROM lakehouse.silver.link_order;
SELECT COUNT(*) FROM lakehouse.silver.sat_product;
SELECT COUNT(*) FROM lakehouse.silver.sat_order;

-- sat_product should hold more than one version per product if a price ever changed
SELECT hk_product, COUNT(*) AS versions
FROM lakehouse.silver.sat_product
GROUP BY 1 ORDER BY 2 DESC;

-- the query the Vault was built to answer:
-- what did each live order cost at the price current when it was placed
SELECT h.order_id,
       o.quantity,
       sp.unit_price,
       o.quantity * sp.unit_price * 4000 AS order_value_khr
FROM lakehouse.silver.sat_order   o
JOIN lakehouse.silver.hub_order   h  ON o.hk_order   = h.hk_order
JOIN lakehouse.silver.link_order  l  ON o.hk_order   = l.hk_order
JOIN lakehouse.silver.sat_product sp ON l.hk_product = sp.hk_product
                          AND sp.load_dts = (
       SELECT MAX(sp2.load_dts)
       FROM lakehouse.silver.sat_product sp2
       WHERE sp2.hk_product = l.hk_product
         AND sp2.load_dts <= o.ordered_at
     )
WHERE NOT o.dv_is_deleted;
```

## Dataset

An original mock operational database: Pisey's kitchenware shop, with `products` and `orders` tables seeded in PostgreSQL as part of Pair 3. This pair does not seed data of its own; it consumes the CDC events already in Bronze.