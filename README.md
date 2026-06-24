# From Raw Data to Real Insight
### A 12-week public learning series on Data Engineering, Architecture, and Data Science

---

**Rathanak Seng** · Data Scientist · MSc Data Science & Engineering

This series is a public teaching resource: 24 posts, 12 weeks, one continuous argument from raw data to deployed model.

**Contact:** [LinkedIn →](https://www.linkedin.com/in/rathanak-seng-2a8901295) · [Medium →](https://medium.com/@withrathanak) · [Facebook →](https://www.facebook.com/seng.rathanak.3)

---

## What this is

Over 12 weeks, I build a complete, production-informed lakehouse from scratch, layer by layer: object storage and table format, real-time CDC ingestion, distributed processing, a Bronze/Silver/Gold medallion architecture, dimensional and Data Vault modeling, orchestration, query federation, and governance. Then I move into the data science that runs on top of it all. Every post is written to teach. Every project is reproducible. The code in this repository is the working proof behind each article.

The central thesis: **good data makes good models.** This series is the proof.

---

## Who this is for

- **Developers and data practitioners** looking for honest, production-grounded tutorials on MinIO, Apache Iceberg, Project Nessie, Trino, Kafka, Debezium CDC, Spark, Airflow, and Dremio
- **Students and early-career data engineers** looking to start their data journey
- **Academic readers** evaluating a sustained body of applied technical work across the full data stack

---

## Structure

The series runs in three phases, each built on the last. Every pair is self-contained: a Theory post explains the *why*, and a Project post proves it with real working code. Read any pair on its own, or read in order to follow one continuous architecture from storage up to data science.

```
lakehouse-to-datascience/
├── month1-architecture/
│   ├── pair-01-lakehouse-architecture/      # Warehouse vs lake vs lakehouse + full blueprint
└── └── pair-02-medallion-storage/           # Medallion Architecture: Bronze/Silver/Gold + Iceberg, MinIO, Nessie, Trino
```

Each pair folder contains a `project/` directory for runnable code and configuration, plus a pair-level `README.md` with concept diagrams and links to the published posts.

---

## Technology covered

1. **Storage & formats:** MinIO AIStor, Apache Iceberg on Parquet, Project Nessie, PostgreSQL
2. **Ingestion & streaming:** Apache Kafka, Debezium CDC
3. **Processing:** Apache Spark / PySpark
4. **Orchestration:** Apache Airflow
5. **Query & federation:** Trino, Dremio
6. **Architecture & modeling:** Bronze/Silver/Gold medallion, star schema, Data Vault (Hubs, Links, Satellites)
7. **Data Science:** exploratory data analysis, feature engineering, classification, regression.

---

## Published articles

| Phase | Pair | Post | Link |
|-------|------|------|------|
| Architecture | Pair 1 | Theory: Data Warehouse vs Data Lake vs Lakehouse | [Medium →](https://medium.com/@withrathanak/data-warehouse-vs-data-lake-vs-lakehouse-whats-actually-different-c9bd08c05ac7) |
| Architecture | Pair 1 | Project: Lakehouse architecture blueprint | [Medium →](https://medium.com/@withrathanak/the-lakehouse-architecture-i-designed-every-layer-and-the-tool-i-chose-for-it-e0b9b8285382) |
| Architecture | Pair 2 | Theory: Bronze, Silver, Gold | [Medium →](https://medium.com/@withrathanak/bronze-silver-gold-why-your-data-needs-three-zones-6b971b0b2a76) |
| Architecture | Pair 2 | Project: Lakehouse storage layer with Iceberg, MinIO, Nessie, Trino | [Medium →](https://medium.com/@withrathanak/project-build-a-lakehouse-storage-layer-with-iceberg-parquet-and-minio-aistor-c6b86d1641af) |

*Table updated as posts publish. Full series index on [Medium →](https://medium.com/@withrathanak)*
