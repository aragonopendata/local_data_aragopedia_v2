package com.localidata.util;

import com.localidata.generic.Prop;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import java.time.Duration;

public class OpenTelemetryConfig {
    private static final OpenTelemetry openTelemetry;

    static {
        String jaegerEndpoint = Prop.jaegerEndpoint;

        JaegerGrpcSpanExporter jaegerExporter = JaegerGrpcSpanExporter.builder()
                .setEndpoint(jaegerEndpoint)
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(jaegerExporter).build())
                .build();

        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();
    }

    public static Tracer getTracer() {
        return GlobalOpenTelemetry.getTracer("ProcessTracer");
    }
}