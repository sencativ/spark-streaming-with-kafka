## Real-time Game Voucher Aggregation

This repository contains a real-time data processing pipeline using Kafka and Spark. It includes:

- A Kafka Producer that generates random game voucher purchase events and publishes them to a Kafka topic.
- A Spark Streaming job that consumes messages from the Kafka topic, aggregates them by a 10-minute window, and outputs the results to the console.

# Usage

1. Clone This Repo.
2. Run `make docker-build` for x86 user, or `make docker-build-arm` for arm chip user.

---

```
## docker-build                 - Build Docker Images (amd64) including its inter-container network.
## docker-build-arm             - Build Docker Images (arm64) including its inter-container network.
## postgres                     - Run a Postgres container
## spark                        - Run a Spark cluster, rebuild the postgres container, then create the destination tables
## jupyter                      - Spinup jupyter notebook for testing and validation purposes.
## airflow                      - Spinup airflow scheduler and webserver.
## kafka                        - Spinup kafka cluster (Kafka+Zookeeper).
## datahub                      - Spinup datahub instances.
## metabase                     - Spinup metabase instance.
## clean                        - Cleanup all running containers related to the challenge.
```

---
