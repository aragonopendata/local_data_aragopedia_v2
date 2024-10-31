package com.localidata.util;

import com.localidata.generic.Prop;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import java.time.Duration;

public class OpenTelemetryConfig {


    public static void initOpenTelemetry() {

        Prop.loadConf();

        JaegerGrpcSpanExporter jaegerExporter = JaegerGrpcSpanExporter.builder()
                .setEndpoint(Prop.jaegerEndpoint)
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

        //return openTelemetry;
    }
}
