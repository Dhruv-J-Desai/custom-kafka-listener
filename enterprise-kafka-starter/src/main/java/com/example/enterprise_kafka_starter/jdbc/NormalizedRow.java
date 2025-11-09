package com.example.enterprise_kafka_starter.jdbc;

import lombok.Builder;
import lombok.Data;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder
class NormalizedRow {
  private Map<String,Object> values;
  private List<String> keyColumns;
  private String eventTimeColumn;

  Object get(String col){ return values.get(col); }
  LocalDateTime eventTs(){ return (LocalDateTime) values.get(eventTimeColumn); }
}
