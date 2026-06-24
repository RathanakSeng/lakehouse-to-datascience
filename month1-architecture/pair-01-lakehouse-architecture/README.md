# Pair 1: The Big Picture

The opening pair of the series. It sets the vocabulary and the map for everything that follows: what a warehouse, a lake, and a lakehouse actually are, and what a complete lakehouse looks like end to end before any single layer is built.

There is no code or configuration in this pair. The build work begins in Pair 2. This folder holds the architecture diagram and links to the two posts.

## Posts

- **Theory:** Data Warehouse vs Data Lake vs Lakehouse: what is actually different. [Read on Medium →](https://medium.com/@withrathanak/data-warehouse-vs-data-lake-vs-lakehouse-whats-actually-different-c9bd08c05ac7)
- **Project:** I designed a lakehouse architecture from scratch: the full blueprint. [Read on Medium →](https://medium.com/@withrathanak/the-lakehouse-architecture-i-designed-every-layer-and-the-tool-i-chose-for-it-e0b9b8285382)

## Architecture

The full system, from ingestion through to consumption. Every layer named here is built and operated across the rest of the series.

![Lakehouse architecture overview](Post1-P.png)
*Figure 1. The full lakehouse, from ingestion to the query layer.*

## Tools by layer

| Layer | Tool | Role |
|---|---|---|
| Operational source | PostgreSQL | System of record that changes are captured from |
| Ingestion (streaming + CDC) | Apache Kafka, Debezium | Capture row-level changes and move them as a stream |
| Object storage | MinIO AIStor | Stores all table data and files |
| Table format | Apache Iceberg on Parquet | ACID tables, schema evolution, and time travel on object storage |
| Catalog | Project Nessie | Iceberg REST catalog with commits and branches |
| Processing | Apache Spark / PySpark | Distributed transformations across the medallion layers |
| Query engine | Trino | Fast SQL over the lakehouse tables |
| Orchestration | Apache Airflow | Schedules and runs the pipelines, with retries and alerts |
| Query federation / BI | Dremio | One SQL interface across the lakehouse and external sources |

The storage layout follows the Bronze, Silver, Gold medallion pattern: raw landing, cleaned and conformed, then business-ready.

## Reference

Armbrust et al., *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*, CIDR 2021.