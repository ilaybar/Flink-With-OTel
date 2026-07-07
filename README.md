# Flink OpenTelemetry Demo

A minimal Apache Flink streaming job with OpenTelemetry tracing, using Grafana Tempo for trace storage, Loki for log aggregation, and Grafana for visualization.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Java 11+](https://adoptium.net/)
- [Maven](https://maven.apache.org/download.cgi)

## Quick Start

### 1. Start the observability stack

```powershell
docker compose up -d
```

This starts:
| Service | Port | Purpose |
|---------|------|---------|
| Tempo | `4317` (gRPC), `4318` (HTTP) | Trace ingestion |
| Loki | `3100` | Log aggregation |
| Grafana | `3000` | Dashboards |
| Promtail | - | Ships logs to Loki |

Verify all containers are running:

```powershell
docker compose ps
```

### 2. Build and run the Flink job

```powershell
mvn clean package -q
java -jar target\flink-otel-demo-1.0.0-SNAPSHOT.jar
```

The job generates 100 records, processes them, and exports traces to Tempo via OTLP.

### 3. Explore in Grafana

Open http://localhost:3000 (anonymous login enabled)

**Traces:**
- Go to **Explore** → select **Tempo** datasource
- Click **Search** → select service `flink-otel-demo`
- Click **Run query** to find traces
- Click any trace ID to see the waterfall view

**Logs:**
- Go to **Explore** → select **Loki** datasource
- Query: `{job="flink-otel-demo"}`
- Click **Run query**

## Project Structure

```
├── pom.xml                          # Maven build (Flink 1.19.1, OTel 1.40.0)
├── docker-compose.yml               # Tempo + Loki + Grafana + Promtail
├── tempo.yaml                       # Tempo config (OTLP receiver on :4317)
├── loki.yaml                        # Loki config
├── promtail.yaml                    # Scrapes app.log to Loki
├── grafana/
│   └── datasources/
│       └── datasources.yaml         # Pre-configured Tempo & Loki datasources
└── src/main/java/com/example/flinkotel/
    └── FlinkOtelDemo.java           # Flink job with manual OpenTelemetry spans
```

## Configuration

- **OTLP endpoint**: defaults to `http://localhost:4317`, override with `$env:OTEL_EXPORTER_OTLP_ENDPOINT`
- **Flink parallelism**: controlled by `env.getExecutionEnvironment()` (28 parallel subtasks with default MiniCluster config)

## Air-Gap / Internal Artifactory Setup

If your on-prem environment has no internet access, pull and host these images in your internal Docker registry (e.g., Artifactory, Nexus):

| Image | Purpose |
|-------|---------|
| `grafana/tempo:latest` | Trace storage (OTLP gRPC receiver) |
| `grafana/loki:latest` | Log aggregation |
| `grafana/grafana:latest` | Dashboards (pre-configured with Tempo & Loki) |
| `grafana/promtail:latest` | Scrapes `app.log` and ships to Loki |

Pull from Docker Hub, tag, and push to your internal registry:

```powershell
docker pull grafana/tempo:latest
docker tag grafana/tempo:latest <your-registry>/grafana/tempo:latest
docker push <your-registry>/grafana/tempo:latest
```

Repeat for `loki`, `grafana`, and `promtail`. Then update the `image:` values in `docker-compose.yml` to point to your registry.

The **Flink job** is self-contained in the fat JAR (`target/flink-otel-demo-1.0.0-SNAPSHOT.jar`) — no internet needed. Only Java 11+ is required on the target machine.

## Cleanup

Stop the observability stack:

```powershell
docker compose down -v
```

The `-v` flag removes volumes (trace/log data).
