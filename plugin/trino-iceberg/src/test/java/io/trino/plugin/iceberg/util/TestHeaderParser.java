package io.trino.plugin.iceberg.util;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
class TestHeaderParser {
    @Test
    void testparseHeaders() {
        String customHeader = "header1=value1,header2=value2";
        Map<String, String> expectedCustomHeadersMap = new HashMap<>();
        expectedCustomHeadersMap.put("header.header1", "value1");
        expectedCustomHeadersMap.put("header.header2", "value2");
        Map<String, String> actualCustomHeadersMap = HeaderParser.parseHeaders(customHeader);
        assertThat(expectedCustomHeadersMap)
                .isEqualTo(actualCustomHeadersMap);
    }
}
