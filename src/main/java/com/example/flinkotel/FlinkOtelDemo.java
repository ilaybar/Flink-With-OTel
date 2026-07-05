package com.example.flinkotel;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ServiceAttributes;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Random;

public class FlinkOtelDemo {

    private static final String OTLP_ENDPOINT = System.getenv()
            .getOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317");

    public static void main(String[] args) throws Exception {
        initOpenTelemetry();

        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("classloader.resolve-order", "parent-first");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(flinkConfig);

        DataStream<Long> numbers = env
                .addSource(new NumberSource())
                .name("number-source")
                .uid("number-source");

        DataStream<String> processed = numbers
                .map(new TracedTransform())
                .name("traced-transform")
                .uid("traced-transform");

        processed
                .addSink(new TracingSink())
                .name("tracing-sink")
                .uid("tracing-sink");

        env.execute("Flink OpenTelemetry Demo");
    }

    static void initOpenTelemetry() {
        Resource resource = Resource.getDefault().merge(
                Resource.create(Attributes.of(
                        ServiceAttributes.SERVICE_NAME, "flink-otel-demo",
                        ServiceAttributes.SERVICE_VERSION, "1.0.0"
                ))
        );

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .addSpanProcessor(BatchSpanProcessor.builder(
                        OtlpGrpcSpanExporter.builder()
                                .setEndpoint(OTLP_ENDPOINT)
                                .build()
                ).build())
                .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(sdk::close));

        GlobalOpenTelemetry.set(sdk);
    }

    static Tracer getTracer() {
        return GlobalOpenTelemetry.getTracer("com.example.flinkotel", "1.0.0");
    }

    public static class NumberSource extends RichParallelSourceFunction<Long> {
        private volatile boolean running = true;
        private transient Tracer tracer;
        private transient Random random;

        @Override
        public void open(Configuration parameters) {
            tracer = getTracer();
            random = new Random();
        }

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long index = 0;
            while (running && index < 100) {
                Span span = tracer.spanBuilder("numbersource.emit")
                        .setSpanKind(SpanKind.PRODUCER)
                        .setAttribute("record.index", index)
                        .setAttribute("parallelism", getRuntimeContext().getNumberOfParallelSubtasks())
                        .startSpan();

                try (Scope ignored = span.makeCurrent()) {
                    ctx.collectWithTimestamp(index, System.currentTimeMillis());
                    Thread.sleep(random.nextInt(500) + 100);
                } finally {
                    span.end();
                }
                index++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class TracedTransform extends RichMapFunction<Long, String> {
        private transient Tracer tracer;

        @Override
        public void open(Configuration parameters) {
            tracer = getTracer();
        }

        @Override
        public String map(Long value) throws Exception {
            Span span = tracer.spanBuilder("transform.process")
                    .setSpanKind(SpanKind.INTERNAL)
                    .setAttribute("input.value", value)
                    .setAttribute("task.index", getRuntimeContext().getIndexOfThisSubtask())
                    .startSpan();

            try (Scope ignored = span.makeCurrent()) {
                String result = "record-" + value;
                span.setAttribute("output.value", result);
                return result;
            } finally {
                span.end();
            }
        }
    }

    public static class TracingSink implements SinkFunction<String> {
        private transient Tracer tracer;

        @Override
        public void invoke(String value, Context context) throws Exception {
            if (tracer == null) {
                tracer = getTracer();
            }

            Span span = tracer.spanBuilder("sink.write")
                    .setSpanKind(SpanKind.CONSUMER)
                    .setAttribute("record.value", value)
                    .startSpan();

            try (Scope ignored = span.makeCurrent()) {
                System.out.println("Sink received: " + value);
            } finally {
                span.end();
            }
        }
    }
}
