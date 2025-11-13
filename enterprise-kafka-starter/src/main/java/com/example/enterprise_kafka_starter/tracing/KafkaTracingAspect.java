package com.example.enterprise_kafka_starter.tracing;

import com.example.enterprise_kafka_starter.annotation.EnterpriseKafkaListener;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;

import java.lang.reflect.Method;
import java.util.Optional;

@Slf4j
@Aspect
@RequiredArgsConstructor
public class KafkaTracingAspect {

    private final EnterpriseTracingProperties tracingProperties;
    private final KafkaHeadersGetter headersGetter = new KafkaHeadersGetter();

    @Around("@annotation(com.example.enterprise_kafka_starter.annotation.EnterpriseKafkaListener)")
    public Object traceKafkaListener(ProceedingJoinPoint pjp) throws Throwable {

        if (!tracingProperties.isEnabled()) {
            return pjp.proceed();
        }

        // 1. Find the ConsumerRecord argument (if any)
        ConsumerRecord<?, ?> record = findConsumerRecordArgument(pjp)
                .orElse(null);

        Tracer tracer = GlobalOpenTelemetry
                .getTracer(tracingProperties.getTracerName());

        Context parentCtx = Context.current();

        String topic = "unknown";
        Integer partition = null;
        Long offset = null;

        if (record != null) {
            topic = record.topic();
            partition = record.partition();
            offset = record.offset();

            Headers headers = record.headers();
            parentCtx = GlobalOpenTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .extract(parentCtx, headers, headersGetter);
        }

        MethodSignature sig = (MethodSignature) pjp.getSignature();
        Method method = sig.getMethod();
        EnterpriseKafkaListener ann = method.getAnnotation(EnterpriseKafkaListener.class);

        String spanName = "kafka consume";
        if (ann != null && ann.topics().length == 1) {
            spanName = "consume " + ann.topics()[0];
        } else if (record != null) {
            spanName = "consume " + record.topic();
        }

        Span span = tracer.spanBuilder(spanName)
                .setSpanKind(SpanKind.CONSUMER)
                .setParent(parentCtx)
                .startSpan();

        try (var scope = span.makeCurrent()) {

            if (tracingProperties.isAddMessagingAttributes() && record != null) {
                span.setAttribute("messaging.system", "kafka");
                span.setAttribute("messaging.operation", "process");
                span.setAttribute("messaging.destination", topic);
                span.setAttribute("messaging.kafka.partition", partition);
                span.setAttribute("messaging.kafka.offset", offset);
            }

            // Let the actual listener method run (your EventsConsumer.onMessage)
            return pjp.proceed();

        } catch (Throwable t) {
            span.recordException(t);
            span.setStatus(StatusCode.ERROR, t.getMessage());
            throw t;
        } finally {
            span.end();
        }
    }

    private Optional<ConsumerRecord<?, ?>> findConsumerRecordArgument(ProceedingJoinPoint pjp) {
        for (Object arg : pjp.getArgs()) {
            if (arg instanceof ConsumerRecord<?, ?> rec) {
                return Optional.of(rec);
            }
        }
        return Optional.empty();
    }
}
