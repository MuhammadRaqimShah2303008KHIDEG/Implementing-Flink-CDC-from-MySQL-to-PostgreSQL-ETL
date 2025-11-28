# 🚀 Implementing Flink CDC pipeline from MySQL-to-PostgreSQL ETL
<img width="1386" height="1024" alt="ChatGPT Image Nov 28, 2025, 03_52_49 PM" src="https://github.com/user-attachments/assets/da69cdc2-3720-418b-8222-94758748434d" />



Real-Time CDC Pipeline – MySQL to PostgreSQL (Apache Flink + Docker)
## Overview

This project implements a real-time, fault-tolerant Change Data Capture (CDC) pipeline using Apache Flink, deployed inside Docker. It continuously streams changes from MySQL to PostgreSQL, keeping the data warehouse synchronized with production with sub-second latency.

The pipeline captures every INSERT, UPDATE, and DELETE at the row level using the Flink MySQL CDC connector, processes the events in parallel workers, and writes them efficiently into PostgreSQL.

## Key Features
✔ Fully Dockerized Architecture

Each table has its own CDC worker container

Independent execution → one worker failure does not affect others

Easy to restart, update, monitor, or scale

✔ Parallel Processing

Each worker:

Initializes its own TableEnvironment

Creates the CDC source table definition

Processes the event stream independently

Writes output into PostgreSQL using batched UPSERTs

✔ CDC with Flink MySQL Connector

Supports:

Real-time binlog reading

Schema changes

Low-latency streaming

Exactly-once semantics (Flink-managed)

✔ Optimized PostgreSQL Sink

Batched writes using psycopg2.execute_values()

Handles updates with ON CONFLICT DO UPDATE

Adds DB identifier (dbId) for multi-source tracking

## Architecture

The system consists of:

1) MySQL (Source System)

Operational database where changes occur.

2) Flink CDC Workers (4 Containers)
Each worker is running independent Flink task, capturing changes for it's respective table and pushing those changes to the pg_process.py to transform for postgres. 

3) PostgreSQL (Data Warehouse)

All changes are UPSERTed into the warehouse schema for analytics, reporting, and reconciliation.

## How It Works

Each worker starts a Flink TableEnvironment

CDC table is registered using CREATE TABLE ... WITH ('connector'='mysql-cdc')

Flink continuously reads changes from MySQL binlog

Each event row is converted to Python dict

Rows are batched and written to PostgreSQL

System runs indefinitely as a streaming job

## Benefits

Real-time replication with very low latency

Strong fault isolation (separate worker per table)

Easy local/production deployment using Docker

Scalable — add more workers as needed

Reliable synchronization between MySQL and PostgreSQL
