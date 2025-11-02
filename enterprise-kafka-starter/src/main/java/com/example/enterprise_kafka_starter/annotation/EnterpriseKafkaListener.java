package com.example.enterprise_kafka_starter.annotation;

import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.annotation.*;

/**
 * Enterprise-standard listener built on top of Spring's @KafkaListener.
 * Teams annotate their methods with this; we wire everything behind the scenes.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@KafkaListener
public @interface EnterpriseKafkaListener {

    @AliasFor(annotation = KafkaListener.class, attribute = "id")
    String id() default "";

    @AliasFor(annotation = KafkaListener.class, attribute = "topics")
    String[] topics() default {};

    @AliasFor(annotation = KafkaListener.class, attribute = "topicPattern")
    String topicPattern() default "";

    @AliasFor(annotation = KafkaListener.class, attribute = "groupId")
    String groupId() default "";

    /** Points to the factory we ship via auto-config (can be overridden by apps if they define same bean name). */
    @AliasFor(annotation = KafkaListener.class, attribute = "containerFactory")
    String containerFactory() default "enterpriseKafkaListenerContainerFactory";

    @AliasFor(annotation = KafkaListener.class, attribute = "concurrency")
    String concurrency() default "1";

    @AliasFor(annotation = KafkaListener.class, attribute = "properties")
    String[] properties() default {};
}
