# Project showcase

## One-line summary

Built a real-time Azure lakehouse that streams synthetic ride-booking events from FastAPI through Event Hubs into Databricks, unifies them with historical data, and publishes SCD-managed facts and dimensions.

## Portfolio description

This project simulates an Uber-style booking platform and implements its analytical data path. A FastAPI application generates realistic ride events and publishes them to Azure Event Hubs. Databricks consumes the events through Spark's Kafka connector and combines them with an initial historical load through Lakeflow append flows. A watermarked streaming OBT enriches rides with location, vehicle, payment, status, and cancellation reference data before Gold CDC flows produce a ride fact table and six dimensions.

## Resume bullets

- Engineered a real-time Azure pipeline that sends FastAPI-generated ride events through Event Hubs into Spark Structured Streaming on Databricks.
- Unified historical and live ride data with Lakeflow append flows, then enriched the stream through watermarked joins against six reference datasets.
- Designed a Gold model with a ride fact table, five SCD Type 1 dimensions, and an SCD Type 2 location dimension.
- Implemented explicit JSON parsing, Kafka SASL/SSL connectivity, Delta tables, and Unity Catalog-aligned organization.

## Interview talking points

### Why combine batch and streaming data?

The initial load establishes history while the event stream keeps the model current. Append flows route both through one downstream contract.

### Why use an OBT before the star schema?

The OBT centralizes enrichment into a reusable ride-level contract. Gold projections can focus on analytical grain and change behavior without repeating joins.

### Why use a watermark?

It defines tolerated event-time lateness and helps keep streaming state bounded. This OBT uses a three-minute delay on booking time.

### Why is location SCD Type 2?

City reference attributes can change. Type 2 retains their effective history, while the other dimensions currently preserve only the latest state.

## Suggested GitHub topics

```text
azure
azure-event-hubs
azure-databricks
adls-gen2
pyspark
spark-structured-streaming
delta-lake
lakeflow
unity-catalog
fastapi
real-time-data
data-engineering
star-schema
scd-type-2
```

## Suggested repository description

Real-time Uber-style lakehouse using FastAPI, Azure Event Hubs, ADLS, Databricks Lakeflow, Spark Structured Streaming, Delta Lake, and SCD dimensional modeling.
