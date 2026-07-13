# On-Prem OpenTelemetry Rollout

Where this demo's patterns go once they leave this repo, and in what order. Current on-prem state
(baseline this plan starts from): JMX exporter agent on plain Java apps, Micrometer/Actuator on
Spring apps, 4 hand-maintained dashboards split by framework, no traces, no OTel anywhere, Spark
job about to be rewritten in Flink.

## Phase 1 — Metrics unification (JVM + Kafka)

**Goal:** collapse the JMX-vs-Micrometer split into one metric naming scheme, one JVM dashboard,
one Kafka dashboard. This is what `grafana/dashboards/onprem/jvm-runtime.json` and
`kafka-clients.json` are built for.

1. Pick one plain-Java app and one Spring app as pilots.
2. Attach the OTel Java agent *alongside* the existing exporter (JVM supports multiple
   `-javaagent` flags) on a different port — see `grafana/dashboards/onprem/README.md` for the
   exact Dockerfile diff per app type. Nothing old is removed yet.
3. Point a temporary Prometheus scrape at the new `:9464` endpoint and confirm the pilots show up
   correctly in the imported dashboards.
4. Run old and new side by side for about a week; sanity-check the new dashboard against the old
   ones (heap trends, GC frequency, Kafka throughput should roughly match).
5. Cut the pilots over: remove the JMX exporter agent / stop scraping `/actuator/prometheus`,
   keep only the OTel agent's `:9464`.
6. Repeat the Dockerfile change across the fleet. Because every dashboard here is templated on the
   `job` (ServiceMonitor) variable, no dashboard edits are needed as apps are added — they just
   appear in the dropdown once scraped.
7. Retire the 4 old dashboards once the fleet has cut over.

**Exit criteria:** every JVM app (plain Java + Spring) exports `jvm_*` / `kafka_*` metrics on
`:9464`; the old JMX exporter and Actuator `/prometheus` scrapes are gone.

## Phase 2 — Python runtime

Same agent-attach pattern, different mechanism (no bytecode-weaving javaagent for Python):

```
pip install opentelemetry-distro opentelemetry-instrumentation-system-metrics opentelemetry-exporter-prometheus
opentelemetry-instrument python app.py
```

Wire the pilot Python service's ServiceMonitor to scrape it, confirm against
`grafana/dashboards/onprem/python-runtime.json`. Python metrics are intentionally **not** merged
into the JVM dashboard (different runtime, different metric names) — see the dashboard-merge
rationale already captured in this repo's chat history if you need to re-justify that to someone.

## Phase 3 — Flink (the Spark rewrite)

Do this as part of the Spark→Flink rewrite itself, not after:

- Attach the OTel Java agent to JobManager and TaskManager processes the same way as any other
  plain Java process (`JAVA_TOOL_OPTIONS`, since editing Flink's own launch script is more
  fragile) — this is what makes the Flink JVMs show up in the *same* JVM dashboard as everything
  else, not a separate one.
- Add Flink's own `flink-metrics-prometheus` reporter for framework metrics (checkpoints,
  backpressure, throughput) — this is `grafana/dashboards/onprem/flink-jobs.json`. Config snippet
  is in that dashboard's README section.
- **Don't port the demo's per-record span pattern as-is.** `FlinkOtelDemo.java` in this repo
  creates one span per record, which is fine for the 100-record demo but will drown a real backend
  at production throughput. Either sample (trace 1-in-N records) or move to per-batch/per-checkpoint
  spans, and reserve manual spans for business logic — let the Flink metrics reporter (and, once
  traces are on-prem, Flink 1.19+'s built-in `OpenTelemetryTraceReporter`) cover the runtime
  behavior instead of hand-rolling it.

**Exit criteria:** rewritten Flink job's JM/TMs appear in the JVM dashboard; checkpoint/throughput/
backpressure visible in the Flink dashboard; span volume doesn't overwhelm the trace backend.

## Phase 4 — Traces on-prem

Not started yet; this repo's Tempo/Loki/Grafana compose stack is the reference architecture, not
something to lift as-is (it assumes the Flink job runs outside Docker on localhost, which won't
hold on-prem). When you get here:

- Decide the trace backend (Tempo is the one already proven in this repo).
- Turn on `OTEL_TRACES_EXPORTER` (currently `none` in every agent config in this rollout) once
  that backend exists.
- Correlate logs and traces: inject `trace_id` into log lines via the OTel Log4j2 MDC bridge, then
  add a Grafana **derived field** on the Loki datasource that turns any log line's trace ID into a
  link to the Tempo waterfall (and enable Tempo → Loki "logs for this span" for the reverse
  direction). This is the single feature that most clearly justifies OTel over JMX/Micrometer to
  a skeptical stakeholder, since JMX/Micrometer can't do it at all.

## Phase 5 — Collector consolidation (optional, later)

Not needed for Phases 1–2 (Prometheus-mode agents scrape directly, no Collector in the path). Revisit
once there's an actual reason: many apps needing central routing/filtering, or traces + logs + metrics
all needing one ingestion point. If/when you do this:

- Consider **Grafana Alloy** over a vanilla OTel Collector — Alloy is itself an OTel Collector
  distribution, so it can absorb the Collector role *and* replace Promtail in one component.
  Promtail is EOL (March 2026) and needs replacing regardless of the Collector decision.

## Housekeeping (do anytime, low risk)

- Pin image versions in `docker-compose.yml` (currently `:latest`) — matters more once images are
  mirrored into an internal registry per the air-gap section of the main README; `:latest` makes
  "which version did we actually push" unanswerable later.
- Mirror the OTel Java agent jar (and any Flink connector jars) into the internal
  artifactory/Nexus the same way the four Grafana images are mirrored — it's a single jar fetched
  from GitHub releases / Maven Central, easy to miss when air-gap-proofing since it isn't a Docker
  image.
- Once Phase 1 dashboards are live, check the on-prem Grafana's dashboard JSON into version control
  with a provisioning config (`grafana/dashboards/onprem/*.json` here is the starting point) instead
  of hand-editing them in the UI, so the fleet's dashboards stay reproducible the way this repo's do.

## Open decisions (yours to make, not blocking Phase 1/2)

- Collector vs. no Collector (Phase 5) — default to "no" until a concrete need shows up.
- Alloy vs. Promtail replacement timing — driven by Promtail's EOL date, independent of the OTel
  rollout.
- Span sampling strategy for the rewritten Flink job (Phase 3) — depends on real production volume,
  not knowable from this demo's 100-record run.
