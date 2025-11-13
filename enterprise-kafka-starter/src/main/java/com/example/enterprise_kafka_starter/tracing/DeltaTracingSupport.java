package com.example.enterprise_kafka_starter.tracing;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import java.util.concurrent.Callable;

/**
 * Helper for wrapping Delta operations in OpenTelemetry spans.
 * Used inside the starter; apps don't see this.
 */
public final class DeltaTracingSupport {

    private static final Tracer TRACER =
            GlobalOpenTelemetry.getTracer("enterprise-delta");

    private DeltaTracingSupport() {
    }

    public static <T> T traced(
            String spanName,
            String layer,      // "bronze" | "silver"
            String table,      // fully qualified delta table
            String feedName,   // your feed id from YAML
            Callable<T> action
    ) throws Exception {

        Span span = TRACER.spanBuilder(spanName)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("delta.layer", layer)
                .setAttribute("delta.table", table)
                .setAttribute("delta.feed", feedName)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            return action.call();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    // convenience for void actions
    public static void tracedVoid(
            String spanName,
            String layer,
            String table,
            String feedName,
            ThrowingRunnable action
    ) throws Exception {
        traced(spanName, layer, table, feedName, () -> {
            action.run();
            return null;
        });
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
