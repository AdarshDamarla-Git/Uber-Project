# Architecture

## Component architecture

```mermaid
flowchart TB
    subgraph LOCAL["Python application"]
        UI["Jinja2 pages"] --> API["FastAPI routes"] --> GEN["Faker ride generator"] --> PRODUCER["Event Hubs producer"]
    end
    subgraph AZURE["Azure services"]
        EH["Event Hubs<br/>Kafka-compatible endpoint"]
        ADLS["ADLS raw container<br/>bulk rides + mappings"]
    end
    subgraph DBX["Azure Databricks"]
        KAFKA["Spark Kafka source"] --> BR["Bronze Delta tables"]
        BR --> UNION["stg_rides<br/>two append flows"] --> OBT["silver_obt<br/>watermarked stream"] --> GOLD["Gold CDC flows<br/>fact + dimensions"]
    end
    PRODUCER --> EH --> KAFKA
    ADLS --> BR
```

## Batch and streaming convergence

```mermaid
flowchart LR
    subgraph BATCH["Initial batch path"]
        JSON["bulk_rides.json"] --> BT["bulk_rides Delta table"]
    end
    subgraph LIVE["Continuous path"]
        BOOK["Ride booking"] --> EVENT["Event Hub event"] --> RR["rides_raw JSON"] --> PARSE["Explicit schema parsing"]
    end
    BT --> APPEND["Lakeflow append flows"]
    PARSE --> APPEND
    APPEND --> STG["stg_rides"] --> DOWNSTREAM["Shared enrichment path"]
```

The bulk table establishes history, while live events continuously enter the same streaming target.

## Real-time sequence

```mermaid
sequenceDiagram
    autonumber
    actor U as Demo user
    participant F as FastAPI app
    participant G as Faker generator
    participant E as Azure Event Hubs
    participant K as Spark Kafka source
    participant S as stg_rides
    participant O as silver_obt
    participant M as Gold model

    U->>F: Open /book
    F->>G: Generate ride confirmation
    G-->>F: Ride dictionary
    F->>E: Send JSON event
    F-->>U: Render confirmation
    K->>E: Consume Kafka offsets
    K->>S: Parse and append ride
    S->>O: Apply booking-time watermark
    O->>O: Left join lookup tables
    O->>M: Stream enriched ride
    M->>M: Apply SCD1/SCD2 flows
```

## Lakehouse lineage

```mermaid
flowchart TB
    EH["Event Hubs"] --> RR["rides_raw"] --> STG["stg_rides"]
    BULK["bulk_rides"] --> STG
    STG --> OBT["silver_obt"]
    VM["map_vehicle_makes"] --> OBT
    VT["map_vehicle_types"] --> OBT
    RS["map_ride_statuses"] --> OBT
    PM["map_payment_methods"] --> OBT
    CITY["map_cities"] --> OBT
    CR["map_cancellation_reasons"] --> OBT
    OBT --> PASS["dim_passenger"]
    OBT --> DRIVER["dim_driver"]
    OBT --> VEHICLE["dim_vehicle"]
    OBT --> PAYMENT["dim_payment"]
    OBT --> BOOKING["dim_booking"]
    OBT --> LOCATION["dim_location"]
    OBT --> FACT["fact"]
```

## Processing semantics

| Stage | Behavior |
|---|---|
| Event ingestion | Reads from earliest offsets and fails on detected data loss |
| Throughput | Limits each trigger to 10,000 offsets |
| Live parsing | Uses a declared Spark `StructType` |
| Batch/live merge | Appends both streams into `stg_rides` |
| OBT enrichment | Uses left joins to preserve rides without lookup matches |
| Event time | Watermarks `booking_timestamp` by three minutes |
| Gold | Deduplicates business keys before CDC application |
| Location | Maintains Type 2 history using `city_updated_at` |

## Security boundaries

- The local producer needs an Event Hubs connection string.
- The Databricks pipeline needs the Kafka-compatible connection string through Spark configuration.
- The batch loader needs ADLS read access.
- Production deployments should use workload identity, Unity Catalog storage credentials, and secret scopes instead of embedded connection strings or SAS tokens.
