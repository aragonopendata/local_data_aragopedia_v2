package com.localidata.util;

import com.localidata.generic.Prop;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.opentelemetry.api.common.Attributes;

import java.time.Duration;

public class OpenTelemetryConfig {
    private static OpenTelemetry openTelemetry;
    private static Tracer tracer;

    public static void initialize() {
        if (openTelemetry == null) {

            if (!Prop.loadConf()) {
                throw new RuntimeException("Failed to load configuration properties");
            }

            String jaegerEndpoint = Prop.jaegerEndpoint;

            if (jaegerEndpoint == null || !jaegerEndpoint.startsWith("http://") && !jaegerEndpoint.startsWith("https://")) {
                throw new IllegalArgumentException("Invalid Jaeger endpoint: " + jaegerEndpoint);
            }

            Resource resource = Resource.create(
                    Attributes.of(ResourceAttributes.SERVICE_NAME, "DatacubeService")
            );

            OtlpGrpcSpanExporter otlpExporter = OtlpGrpcSpanExporter.builder()
                    .setEndpoint(jaegerEndpoint)
                    .setTimeout(java.time.Duration.ofSeconds(30))
                    .build();

            BatchSpanProcessor batchSpanProcessor = BatchSpanProcessor.builder(otlpExporter)
                .setScheduleDelay(Duration.ofSeconds(5))
                .setMaxQueueSize(2048)
                .setMaxExportBatchSize(512)
                .build();

            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .addSpanProcessor(batchSpanProcessor)
                    .setResource(resource)
                    .build();

            openTelemetry = OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .buildAndRegisterGlobal();

            tracer = openTelemetry.getTracer("ProcessTracer");
        }
    }

    public static Tracer getTracer() {
        if (tracer == null) {
            throw new IllegalStateException("OpenTelemetryConfig has not been initialized. Call initialize() first.");
        }
        return tracer;
    }
}