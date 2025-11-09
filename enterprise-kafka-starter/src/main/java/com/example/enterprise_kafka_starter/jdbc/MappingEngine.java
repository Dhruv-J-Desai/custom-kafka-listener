package com.example.enterprise_kafka_starter.jdbc;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

class MappingEngine {
  private final Configuration cfg = Configuration.builder()
      .jsonProvider(new JacksonJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .options(Option.SUPPRESS_EXCEPTIONS)
      .build();

  Object parse(String json) { return cfg.jsonProvider().parse(json); }

  Map<String,Object> apply(Object doc, Map<String,String> mapping) {
    Map<String,Object> out = new LinkedHashMap<>();
    for (var e : mapping.entrySet()) {
      out.put(e.getKey(), eval(doc, e.getValue()));
    }
    return out;
  }

  private Object eval(Object doc, String expr) {
    String[] parts = expr.split("\\|");
    Object cur = null;

    // First token (JSONPath or literal)
    if (parts.length > 0) {
      String first = parts[0].trim();
      if (first.startsWith("$")) cur = JsonPath.using(cfg).parse(doc).read(first);
      else if (!first.isBlank()) cur = first;
    }

    // Transforms
    for (int i=1;i<parts.length;i++) {
      String t = parts[i].trim();
      if (t.equals("trim")) cur = (cur==null? null : cur.toString().trim());
      else if (t.equals("upper")) cur = (cur==null? null : cur.toString().toUpperCase(Locale.ROOT));
      else if (t.equals("lower")) cur = (cur==null? null : cur.toString().toLowerCase(Locale.ROOT));
      else if (t.equals("digits_only")) cur = (cur==null? null : cur.toString().replaceAll("\\D+",""));
      else if (t.startsWith("to_decimal(")) {
        int scale = Integer.parseInt(t.substring("to_decimal(".length(), t.length()-1));
        if (cur==null || cur.toString().isBlank()) cur = null;
        else cur = new BigDecimal(cur.toString().replaceAll("[^\\d.\\-]","")).setScale(scale);
      } else if (t.equals("currency_to_decimal")) {
        if (cur==null) cur = null;
        else cur = new BigDecimal(cur.toString().replaceAll("[^\\d.\\-\\.]",""));
      } else if (t.startsWith("parse_ts(")) {
        String fmt = t.substring("parse_ts(".length(), t.length()-1).replaceAll("^\"|\"$","");
        if (cur==null || cur.toString().isBlank()) cur = null;
        else cur = LocalDateTime.parse(cur.toString(), DateTimeFormatter.ofPattern(fmt));
      }
    }
    return cur;
  }
}
