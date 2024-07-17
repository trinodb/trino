package io.trino.loki;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

public class TestQueryResult {
  @Test
  void testDeserialize() throws IOException {
    final InputStream input =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("result.json");
    QueryResult result = QueryResult.fromJSON(input);

    var values = result.getData().getStreams().getFirst().getValues();
    assertThat(values).hasSize(100);
  }
}
