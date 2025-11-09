package com.example.enterprise_kafka_starter.jdbc;

import lombok.Data;

@Data
public class SchemaAttribute {
  private String name;
  private String type;
  private Boolean trackChanges; // null => true
}
