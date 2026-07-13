# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A minimal Apache Flink streaming job instrumented with OpenTelemetry tracing, demonstrating how to get Flink pipeline spans into Grafana Tempo (traces) and app logs into Loki, viewed through Grafana. It's a single-class demo (`FlinkOtelDemo.java`), not a production application — the whole pipeline is source → map → sink, each stage manually creating OTel spans.

## Commands

Start the observability stack (Tempo, Loki, Promtail, Grafana) via Docker Compose:

```powershell
docker compose up -d
docker compose ps      # verify all containers are running
docker compose down -v # stop and remove volumes (trace/log data)
```

Build and run the Flink job (requires Java 11+, Maven):

```powershell
mvn clean package -q
java -jar target\flink-otel-demo-1.0.0-SNAPSHOT.jar
```

There is no test suite, linter, or CI config in this repo — `mvn clean package` (which runs the shade plugin) is the only build step.

## Architecture

- **`src/main/java/com/example/flinkotel/FlinkOtelDemo.java`** — the entire application. `main()` calls `initOpenTelemetry()` once at process startup to configure a global `OpenTelemetrySdk` (OTLP gRPC exporter, batch span processor, W3C trace-context propagator), then builds a local Flink `StreamExecutionEnvironment` with a 3-stage pipeline:
  - `NumberSource` (`RichParallelSourceFunction`) emits 100 longs, one `numbersource.emit` PRODUCER span per record.
  - `TracedTransform` (`RichMapFunction`) maps each long to a string, one `transform.process` INTERNAL span per record.
  - `TracingSink` (`SinkFunction`) prints each result, one `sink.write` CONSUMER span per record.
  Each operator fetches its own `Tracer` lazily via `getTracer()` (`GlobalOpenTelemetry.getTracer(...)`) inside `open()`/on first use, since `Tracer` isn't serializable and each parallel subtask runs in its own JVM/classloader.
  - `flinkConfig.setString("classloader.resolve-order", "parent-first")` is required so the OTel SDK classes resolve consistently across Flink's child-first classloader and the shaded JAR.
  - The Flink `MiniCluster` local environment runs with the default parallelism (one source subtask per available core), so multiple concurrent traces are produced per run — useful for exercising trace/span concurrency in Tempo.

- **Observability stack** (`docker-compose.yml`): four services on a shared `observability` Docker network.
  - `tempo` — OTLP receiver on `4317` (gRPC, used by the Flink job) and `4318` (HTTP); config in `tempo.yaml`, local filesystem trace storage.
  - `loki` — log aggregation on `3100`; config in `loki.yaml`.
  - `promtail` — tails `app.log` (mounted read-only from the repo root) and ships lines to Loki; config in `promtail.yaml`. The app writes `app.log` via the Log4j2 `FileAppender` (`src/main/resources/log4j2.properties`), so promtail only picks up logs after the Flink job has run at least once locally.
  - `grafana` — port `3000`, anonymous admin login enabled, pre-provisioned with Tempo and Loki datasources (`grafana/datasources/datasources.yaml`).
  - The Flink job runs *outside* Docker (`java -jar ...` on the host) and talks to Tempo's OTLP endpoint over `localhost:4317`; only the observability stack itself is containerized.

- **Configuration**: OTLP endpoint is read from `$env:OTEL_EXPORTER_OTLP_ENDPOINT` (defaults to `http://localhost:4317`) — override this if Tempo is reachable at a different host/port (e.g., when running the jar from inside a container on the same Docker network).

- **Air-gapped environments**: see the "Air-Gap / Internal Artifactory Setup" section in README.md for pulling/re-pushing the four Grafana images to an internal registry. The Flink job itself is self-contained in the fat JAR and needs no internet access beyond a JDK.
