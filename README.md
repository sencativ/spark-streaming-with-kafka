# Real-time Game Voucher Aggregation

This repository contains a real-time data processing pipeline using Kafka and Spark. It includes:

- A Kafka Producer that generates random game voucher purchase events and publishes them to a Kafka topic.
- A Spark Streaming job that consumes messages from the Kafka topic, aggregates them by a 10-minute window, and outputs the results to the console.

## Screenshots Result

- Producer
  Kafka Producer: Produces random game voucher purchase events and publishes them to a Kafka topic.

![Producer](https://cdn.discordapp.com/attachments/716655315613122670/1305188137949073408/image.png?ex=67321ead&is=6730cd2d&hm=4769843c3b5c5201f6b32e1d1642c7a87ae4c4dff6453ecf39c7a177ba02b93e&)

- Consumer
  Spark Streaming Job: Consumes messages from the Kafka topic, aggregates them by a 10-minute window

![Consumer](https://cdn.discordapp.com/attachments/716655315613122670/1305187663367770253/image.png?ex=67321e3c&is=6730ccbc&hm=5e4b1371fccc36cfb6bc4b18ab3d1d7013aeb51c053949e008196bedd45cf90d&)

## Usage

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
