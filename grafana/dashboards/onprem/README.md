# On-Prem Dashboards (OpenTelemetry)

Four import-ready Grafana dashboards for a fleet instrumented with OpenTelemetry, replacing the
per-framework (JMX / Micrometer) dashboard copies with one dashboard per concern:

| File | Dashboard | Covers | Metric source |
|------|-----------|--------|---------------|
| `jvm-runtime.json` | JVM Runtime (OpenTelemetry) | all Java + Spring apps + Flink JM/TMs | OTel Java agent (`jvm_*`) |
| `kafka-clients.json` | Kafka Clients (OpenTelemetry) | every JVM app that produces/consumes | OTel Java agent (`kafka_producer_*`, `kafka_consumer_*`) |
| `flink-jobs.json` | Flink Jobs | checkpoints, throughput, backpressure | Flink `PrometheusReporter` (`flink_*`) |
| `python-runtime.json` | Python Runtime (OpenTelemetry) | Python components | `opentelemetry-instrumentation-system-metrics` |

## Import

Grafana → Dashboards → New → Import → upload the JSON (or paste it). Repeat per file.
Each dashboard has a **Datasource** variable (type `datasource`), so one import works across all
of your namespaces — pick the namespace's Prometheus in the dropdown instead of importing a copy
per namespace.

## What the apps must expose

All JVM apps (plain Java, Spring, Flink JM/TM) use the same OTel Java agent — the mechanism to
*attach* it is identical everywhere (`-javaagent:` flag). What differs per app type is what you do
with the metrics pipeline that's already there.

- **Plain Java** — you're already attaching the JMX exporter agent this way, so this is a direct
  swap. During the validation window (see `ONPREM_ROLLOUT.md`), attach both agents at once — the
  JVM allows multiple `-javaagent` flags — on different ports, then drop the JMX one once the new
  dashboards check out:

  ```dockerfile
  # before
  ENTRYPOINT ["java","-javaagent:/opt/jmx_prometheus_javaagent.jar=9404:/opt/jmx-config.yaml","-jar","app.jar"]

  # during validation (both agents active)
  ENTRYPOINT ["java", \
    "-javaagent:/opt/jmx_prometheus_javaagent.jar=9404:/opt/jmx-config.yaml", \
    "-javaagent:/otel/opentelemetry-javaagent.jar", \
    "-jar","app.jar"]
  ENV OTEL_SERVICE_NAME=my-plain-java-app
  ENV OTEL_METRICS_EXPORTER=prometheus
  ENV OTEL_EXPORTER_PROMETHEUS_PORT=9464
  ENV OTEL_TRACES_EXPORTER=none
  ENV OTEL_LOGS_EXPORTER=none

  # after cutover — drop the JMX agent and its config entirely
  ENTRYPOINT ["java","-javaagent:/otel/opentelemetry-javaagent.jar","-jar","app.jar"]
  ```

- **Spring** — Micrometer isn't a javaagent, it's a library your app already calls, so there's
  nothing to "remove" from the JVM launch line — you're *adding* the OTel agent next to it, not
  replacing anything at the process level:

  ```dockerfile
  ENTRYPOINT ["java","-javaagent:/otel/opentelemetry-javaagent.jar","-jar","app.jar"]
  ENV OTEL_SERVICE_NAME=my-spring-app
  ENV OTEL_METRICS_EXPORTER=prometheus
  ENV OTEL_EXPORTER_PROMETHEUS_PORT=9464
  ENV OTEL_TRACES_EXPORTER=none
  ENV OTEL_LOGS_EXPORTER=none
  # default is already true — call it out explicitly so it doesn't get "cleaned up" later:
  ENV OTEL_INSTRUMENTATION_MICROMETER_ENABLED=true
  ```

  The Micrometer bridge (`OTEL_INSTRUMENTATION_MICROMETER_ENABLED`, on by default) re-exports
  meters your app already registers with Micrometer — including any custom business counters/gauges
  you wrote, not just JVM/Kafka ones — through the OTel agent's `:9464` endpoint. That's what lets
  you point the ServiceMonitor at `:9464` and stop scraping Spring Boot Actuator's
  `/actuator/prometheus` without losing custom metrics. Only disable the bridge if you deliberately
  want the agent's auto-instrumentation only (no custom Micrometer meters).

  One thing to actually remove once validated: stop scraping the Actuator `/prometheus` endpoint —
  leaving both active double-scrapes the same JVM/Kafka metrics under two different names again,
  which is the exact problem this rollout is meant to fix.

- **Flink JM/TM** — mechanically the same as plain Java (it's a JVM process), but you rarely
  control Flink's own launch script directly. Set `JAVA_TOOL_OPTIONS` in the container/environment
  instead of editing `ENTRYPOINT`:

  ```
  JAVA_TOOL_OPTIONS=-javaagent:/otel/opentelemetry-javaagent.jar
  OTEL_SERVICE_NAME=my-flink-job
  OTEL_METRICS_EXPORTER=prometheus
  OTEL_EXPORTER_PROMETHEUS_PORT=9464
  OTEL_TRACES_EXPORTER=none
  OTEL_LOGS_EXPORTER=none
  ```

  Point the ServiceMonitor / scrape config at port `9464` for all of the above.

- **Flink framework metrics** — add to Flink config:

  ```yaml
  metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
  metrics.reporter.prom.port: 9249-9260
  ```

  (needs `flink-metrics-prometheus` on the classpath; scrape JM and TM pods on 9249+).

- **Python** — `pip install opentelemetry-distro opentelemetry-instrumentation-system-metrics opentelemetry-exporter-prometheus`
  and run under `opentelemetry-instrument`, or call `SystemMetricsInstrumentor().instrument()`.

## Variables & chaining

- `job` = the Prometheus `job` label, i.e. your ServiceMonitor name. Since each deployment has its
  own ServiceMonitor, `job` identifies the deployment and `pod` identifies the pod by name — no
  custom `deployment` label needed. (The `pod` label is added by the Prometheus Operator's default
  target relabeling; the raw `instance` label is just `<pod-ip>:<port>`, so the dashboards don't
  use it.)
- Kafka dashboard chains **job → topic → client → partition**: picking a topic narrows the client
  and partition dropdowns to series that actually carry that topic. Chaining is one-directional
  (Grafana limitation): picking a client does **not** narrow the topic list.
- Kafka client metrics have **no consumer-group label**; the "Client" variable is the Kafka
  `client.id`. Set a meaningful `client.id` per app if you haven't.

## If a panel is empty

Metric names were taken from the OTel Java instrumentation and Python contrib docs, but exporter
versions differ in unit-suffix handling (`_bytes`, `_seconds`, `_ratio`, `_total`). Check the real
names your app exposes:

```
curl http://<pod>:9464/metrics | grep -i kafka
```

and adjust the panel query — the shape of the query stays the same.
