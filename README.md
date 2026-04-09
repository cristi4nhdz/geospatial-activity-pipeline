# Geospatial Activity Pipeline

A real-time geospatial intelligence pipeline that ingests live vessel and aircraft positions from AISStream and OpenSky Network across 2 Kafka topics, normalizes and upserts spatial tracks into PostgreSQL/PostGIS with GIST-indexed geometry columns, and stores areas of interest as polygons for downstream spatial querying.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Environment Setup](#environment-setup)
  - [Running the Pipeline](#running-the-pipeline)
- [Project Structure](#project-structure)

---

## Overview

Pulls live AIS vessel positions via AISStream WebSocket and ADS-B aircraft transponder data via OpenSky Network REST API. Normalizes raw Class A, B, and Extended position reports and aircraft state vectors, publishes them to Kafka topics, and upserts tracks into a PostGIS spatial database with GIST-indexed geometry columns for fast spatial queries.

---

## Architecture

```mermaid
flowchart TD
    subgraph Sources["Ingestion Sources"]
        AIS["AISStream\nWebSocket"]
        OpenSky["OpenSky Network\nREST API"]
    end

    subgraph Kafka["Kafka Event Bus"]
        K1["ais.vessels"]
        K2["adsb.aircraft"]
    end

    subgraph Consumers["Kafka Consumers"]
        C1["vessel_consumer.py"]
        C2["aircraft_consumer.py"]
    end

    subgraph Storage["Storage"]
        PG["PostgreSQL/PostGIS\nvessel_tracks\naircraft_tracks\naoi"]
    end

    AIS -->|vessel pings| K1
    OpenSky -->|aircraft states| K2
    K1 -->|consume| C1
    K2 -->|consume| C2
    C1 -->|upsert| PG
    C2 -->|insert| PG
```

---

## Tech Stack

| Layer | Technology |
| ------- | ------------ |
| Language | Python 3.13 |
| Messaging | Apache Kafka |
| Geospatial DB | PostgreSQL + PostGIS |
| Orchestration | Docker Compose |
| Environment | Conda |

---

## Features

- **2-Source Ingestion** — AISStream WebSocket and OpenSky Network REST API publishing live vessel and aircraft positions to Kafka
- **AIS Normalization** — Handles Class A, Class B Standard, and Class B Extended position reports, normalizing MMSI, vessel name, coordinates, speed, heading, course, and navigational status
- **ADS-B Normalization** — Filters airborne-only records and normalizes ICAO24, callsign, origin country, altitude, velocity, heading, and vertical rate
- **Configurable AOI** — Bounding boxes per data source defined in YAML config, no hardcoded coordinates
- **WebSocket Reconnection** — AIS producer automatically reconnects on connection drop
- **PostGIS Spatial Schema** — Three tables with `GEOMETRY(Point/Polygon, 4326)` columns and GIST spatial indexes: `vessel_tracks`, `aircraft_tracks`, and `aoi`
- **Consumer Lag Monitor** — Reports committed vs end offsets per consumer group and partition on a configurable interval
- **Config-Driven** — YAML-based configuration for Kafka topics, bounding boxes, PostGIS connection, and API credentials

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/)
- [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or Anaconda
- Python 3.13

### Accounts Required

| Service | Purpose | Link |
| --------- | --------- | ------ |
| AISStream | Live vessel WebSocket feed | aisstream.io |
| OpenSky Network | Live aircraft REST API | opensky-network.org |

### Environment Setup

**1. Clone the repository:**

```bash
# Clone the repo
git clone https://github.com/cristi4nhdz/geospatial-activity-pipeline.git
cd geospatial-activity-pipeline
```

**2. Configure your environment:**

```bash
cp config/settings_example.yaml config/settings.yaml
# edit settings.yaml with your API keys and credentials
```

**3. Create a local Conda environment:**

```bash
conda env create -f environment.yaml
conda activate geo-pipeline
```

**4. Start the Docker stack:**

```bash
docker compose -f docker/docker-compose.yaml up -d
```

### Running the Pipeline

```bash
# AIS vessel producer
python -m ingestion.ais_producer

# ADS-B aircraft producer
python -m ingestion.adsb_producer

# Vessel consumer
python -m ingestion.consumers.vessel_consumer

# Aircraft consumer
python -m ingestion.consumers.aircraft_consumer

# Lag monitor
python -m ingestion.consumers.lag_monitor
```

---

## Project Structure

```text
geospatial-activity-pipeline/
|-- docker/
│   |-- docker-compose.yaml
│   |-- postgres/
│       |-- init.sql
|-- ingestion/
│   |-- __init__.py
│   |-- ais_producer.py
│   |-- adsb_producer.py
|   |-- consumers/
|       |-- __init__.py
|       |-- vessel_consumer.py
|       |-- aircraft_consumer.py
|       |-- lag_monitor.py
|-- db/
│   |-- schema.sql
│   |-- queries/
|-- config/
│   |-- __init__.py
│   |-- settings_example.yaml
│   |-- config_loader.py
│   |-- logging_config.py
|-- logs/
|-- environment.yaml
|-- README.md
```
