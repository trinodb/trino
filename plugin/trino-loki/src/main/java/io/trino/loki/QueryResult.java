package io.trino.loki;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class QueryResult {

  static ObjectMapper mapper = new ObjectMapper();

  public static QueryResult fromJSON(InputStream input) throws IOException {
    return mapper.readValue(input, QueryResult.class);
  }

  public String getStatus() {
    return status;
  }

  public Data getData() {
    return data;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public void setData(Data data) {
    this.data = data;
  }

  private String status;
  private Data data;

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class Data {

    public String getResultType() {
      return resultType;
    }

    public void setResultType(String resultType) {
      this.resultType = resultType;
    }

    private String resultType;

    public List<Stream> getStreams() {
      return streams;
    }

    public void setStreams(List<Stream> streams) {
      this.streams = streams;
    }

    @JsonProperty("result")
    private List<Stream> streams;
  }

  static class Stream {
    public Map<String, String> getLabels() {
      return labels;
    }

    public void setLabels(Map<String, String> labels) {
      this.labels = labels;
    }

    public List<LogEntry> getValues() {
      return values;
    }

    public void setValues(List<LogEntry> values) {
      this.values = values;
    }

    @JsonProperty("stream")
    private Map<String, String> labels;

    private List<LogEntry> values;
  }

  @JsonDeserialize(using = LogEntryDeserializer.class)
  static class LogEntry {
    public Long getTs() {
      return ts;
    }

    public void setTs(Long ts) {
      this.ts = ts;
    }

    public String getLine() {
      return line;
    }

    public void setLine(String line) {
      this.line = line;
    }

    private Long ts;
    private String line;
  }

  static class LogEntryDeserializer extends StdDeserializer<LogEntry> {
    LogEntryDeserializer() {
      super(LogEntry.class);
    }

    @Override
    public LogEntry deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      final JsonNode node = p.getCodec().readTree(p);
      LogEntry entry = new LogEntry();
      entry.setTs(node.get(0).asLong());
      entry.setLine(node.get(1).asText());
      return entry;
    }
  }
}
