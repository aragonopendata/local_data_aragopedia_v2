package com.localidata.util;

import com.localidata.generic.Prop;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import java.time.Duration;

public class OpenTelemetryConfig {

    private static String jaegerEndpoint = Prop.jaegerEndpoint;

    public static void initOpenTelemetry() {

        JaegerGrpcSpanExporter jaegerExporter = JaegerGrpcSpanExporter.builder()
                .setEndpoint(jaegerEndpoint)
                .build();

        BatchSpanProcessor spanProcessor = BatchSpanProcessor.builder(jaegerExporter)
                .setScheduleDelay(Duration.ofMillis(100))
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(spanProcessor)
                .build();

        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();

        GlobalOpenTelemetry.set(openTelemetry);
    }
}
