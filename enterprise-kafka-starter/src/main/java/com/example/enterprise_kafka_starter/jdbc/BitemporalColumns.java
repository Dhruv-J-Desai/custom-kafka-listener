package com.example.enterprise_kafka_starter.jdbc;

import lombok.Data;

@Data
public class BitemporalColumns {
  private String validFromColumn;
  private String validToColumn;
  private String isCurrentColumn;
}
