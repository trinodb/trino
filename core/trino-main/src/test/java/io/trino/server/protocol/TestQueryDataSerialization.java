/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import io.trino.client.JsonCodec;
import io.trino.client.QueryData;
import io.trino.client.QueryResults;
import io.trino.client.RawQueryData;
import io.trino.client.StatementStats;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.InlineSegment;
import io.trino.client.spooling.Segment;
import io.trino.client.spooling.encoding.JsonQueryDataDecoder;
import io.trino.server.protocol.spooling.QueryDataJacksonModule;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;

import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.FixJsonDataUtils.fixData;
import static io.trino.client.JsonCodec.jsonCodec;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.ROW_OFFSET;
import static io.trino.client.spooling.DataAttribute.SCHEMA;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.client.spooling.DataAttributes.empty;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestQueryDataSerialization
{
    private static final List<Column> COLUMNS_LIST = ImmutableList.of(new Column("_col0", "bigint", new ClientTypeSignature("bigint")));
    private static final JsonCodec<QueryResults> CLIENT_CODEC = jsonCodec(QueryResults.class);
    private static final io.airlift.json.JsonCodec<QueryResults> SERVER_CODEC = new JsonCodecFactory(new ObjectMapperProvider()
            .withModules(Set.of(new QueryDataJacksonModule())))
            .jsonCodec(QueryResults.class);

    @Test
    public void testNullDataSerialization()
    {
        assertThat(serialize(null)).doesNotContain("data");
        assertThat(serialize(RawQueryData.of(null))).doesNotContain("data");
    }

    @Test
    public void testEmptyArraySerialization()
    {
        testRoundTrip(COLUMNS_LIST, RawQueryData.of(ImmutableList.of()), "[]");

        assertThatThrownBy(() -> testRoundTrip(COLUMNS_LIST, RawQueryData.of(ImmutableList.of(ImmutableList.of())), "[[]]"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("row/column size mismatch");
    }

    @Test
    public void testQueryDataSerialization()
    {
        Iterable<List<Object>> values = ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(5L));
        testRoundTrip(COLUMNS_LIST, RawQueryData.of(values), "[[1],[5]]");
    }

    @Test
    public void testEncodedQueryDataSerialization()
    {
        EncodedQueryData queryData = new EncodedQueryData("json", ImmutableMap.of(), ImmutableList.of(Segment.inlined("[[10], [20]]".getBytes(UTF_8), dataAttributes(10, 2, 12))));
        testRoundTrip(COLUMNS_LIST, queryData, """
                {
                  "encoding": "json",
                  "segments": [
                    {
                      "type": "inline",
                      "data": "W1sxMF0sIFsyMF1d",
                      "metadata": {
                        "rowOffset": 10,
                        "rowsCount": 2,
                        "segmentSize": 12
                      }
                    }
                  ]
                }""");
    }

    @Test
    public void testEncodedQueryDataSerializationWithExtraMetadata()
    {
        EncodedQueryData queryData = new EncodedQueryData("json", ImmutableMap.of("decryptionKey", "secret"), ImmutableList.of(Segment.inlined("[[10], [20]]".getBytes(UTF_8), dataAttributes(10, 2, 12))));
        testRoundTrip(COLUMNS_LIST, queryData, """
                {
                  "encoding": "json",
                  "metadata": {
                    "decryptionKey": "secret"
                  },
                  "segments": [
                    {
                      "type": "inline",
                      "data": "W1sxMF0sIFsyMF1d",
                      "metadata": {
                        "rowOffset": 10,
                        "rowsCount": 2,
                        "segmentSize": 12
                      }
                    }
                  ]
                }""");
    }

    @Test
    public void testSpooledQueryDataSerialization()
    {
        EncodedQueryData queryData = EncodedQueryData.builder("json")
                .withSegment(Segment.inlined("super".getBytes(UTF_8), dataAttributes(0, 100, 5)))
                .withSegment(Segment.spooled(URI.create("http://localhost:8080/v1/download/20160128_214710_00012_rk68b/segments/1"), dataAttributes(100, 100, 1024), Map.of("x-amz-server-side-encryption", List.of("AES256"))))
                .withSegment(Segment.spooled(URI.create("http://localhost:8080/v1/download/20160128_214710_00012_rk68b/segments/2"), dataAttributes(200, 100, 1024), Map.of("x-amz-server-side-encryption", List.of("AES256"))))
                .withAttributes(DataAttributes.builder()
                        .set(SCHEMA, "serializedSchema")
                        .build())
                .build();
        testSerializationRoundTrip(queryData, """
                {
                  "encoding": "json",
                  "metadata": {
                    "schema": "serializedSchema"
                  },
                  "segments": [
                    {
                      "type": "inline",
                      "data": "c3VwZXI=",
                      "metadata": {
                        "rowOffset": 0,
                        "rowsCount": 100,
                        "segmentSize": 5
                      }
                    },
                    {
                      "type": "spooled",
                      "uri": "http://localhost:8080/v1/download/20160128_214710_00012_rk68b/segments/1",
                      "metadata": {
                        "rowOffset": 100,
                        "rowsCount": 100,
                        "segmentSize": 1024
                      },
                      "headers": {"x-amz-server-side-encryption":["AES256"]}
                    },
                    {
                      "type": "spooled",
                      "uri": "http://localhost:8080/v1/download/20160128_214710_00012_rk68b/segments/2",
                      "metadata": {
                        "rowOffset": 200,
                        "rowsCount": 100,
                        "segmentSize": 1024
                      },
                      "headers": {"x-amz-server-side-encryption":["AES256"]}
                    }
                  ]
                }""");
    }

    @Test
    public void testEncodedQueryDataToString()
    {
        EncodedQueryData inlineQueryData = new EncodedQueryData("json", ImmutableMap.of("decryption_key", "secret"), ImmutableList.of(Segment.inlined("[[10], [20]]".getBytes(UTF_8), dataAttributes(10, 2, 12))));
        assertThat(inlineQueryData.toString()).isEqualTo("EncodedQueryData{encoding=json, segments=[InlineSegment{offset=10, rows=2, size=12}], metadata=[decryption_key]}");

        EncodedQueryData spooledQueryData = new EncodedQueryData("json+zstd", ImmutableMap.of("decryption_key", "secret"), ImmutableList.of(Segment.spooled(URI.create("http://coordinator:8080/v1/segments/uuid"), dataAttributes(10, 2, 1256), headers())));
        assertThat(spooledQueryData.toString()).isEqualTo("EncodedQueryData{encoding=json+zstd, segments=[SpooledSegment{offset=10, rows=2, size=1256, headers=[x-amz-server-side-encryption]}], metadata=[decryption_key]}");
    }

    private void testRoundTrip(List<Column> columns, QueryData queryData, String expectedDataRepresentation)
    {
        testSerializationRoundTrip(queryData, expectedDataRepresentation);
        assertEquals(columns, deserialize(serialize(queryData)), queryData);
    }

    private void testSerializationRoundTrip(QueryData queryData, String expectedDataRepresentation)
    {
        assertThat(serialize(queryData))
                .isEqualToIgnoringWhitespace(queryResultsJson(expectedDataRepresentation));
    }

    private String queryResultsJson(String expectedDataField)
    {
        return format("""
                {
                  "id": "20160128_214710_00012_rk68b",
                  "infoUri": "http://coordinator/query.html?20160128_214710_00012_rk68b",
                  "columns": [
                    {
                      "name": "_col0",
                      "type": "bigint",
                      "typeSignature": {
                        "rawType": "bigint",
                        "arguments": []
                      }
                    }
                  ],
                  "data": %s,
                  "stats": {
                    "state": "FINISHED",
                    "queued": false,
                    "scheduled": false,
                    "nodes": 0,
                    "totalSplits": 0,
                    "queuedSplits": 0,
                    "runningSplits": 0,
                    "completedSplits": 0,
                    "cpuTimeMillis": 0,
                    "wallTimeMillis": 0,
                    "queuedTimeMillis": 0,
                    "elapsedTimeMillis": 0,
                    "processedRows": 0,
                    "processedBytes": 0,
                    "physicalInputBytes": 0,
                    "physicalWrittenBytes": 0,
                    "peakMemoryBytes": 0,
                    "spilledBytes": 0
                  },
                  "warnings": []
                }""", expectedDataField);
    }

    private static void assertEquals(List<Column> columns, QueryData left, QueryData right)
    {
        Iterable<List<Object>> leftValues = decodeData(left, columns);
        Iterable<List<Object>> rightValues = decodeData(right, columns);

        if (leftValues == null) {
            assertThat(rightValues).isNull();
            return;
        }

        assertThat(leftValues).hasSameElementsAs(rightValues);
    }

    private static Iterable<List<Object>> decodeData(QueryData data, List<Column> columns)
    {
        if (data instanceof RawQueryData) {
            return fixData(columns, data.getData());
        }

        if (data instanceof EncodedQueryData queryDataV2 && queryDataV2.getSegments().getFirst() instanceof InlineSegment inlineSegment) {
            try {
                return new JsonQueryDataDecoder.Factory().create(columns, empty())
                        .decode(new ByteArrayInputStream(inlineSegment.getData()), inlineSegment.getMetadata());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        throw new AssertionError("Unexpected data type: " + data.getClass().getSimpleName());
    }

    private static QueryData deserialize(String serialized)
    {
        try {
            return CLIENT_CODEC.fromJson(serialized).getData();
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static String serialize(QueryData data)
    {
        return SERVER_CODEC.toJson(new QueryResults(
                "20160128_214710_00012_rk68b",
                URI.create("http://coordinator/query.html?20160128_214710_00012_rk68b"),
                null,
                null,
                ImmutableList.of(new Column("_col0", BIGINT, new ClientTypeSignature(BIGINT))),
                data,
                StatementStats.builder()
                        .setState("FINISHED")
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                null,
                ImmutableList.of(),
                null,
                null));
    }

    private DataAttributes dataAttributes(long currentOffset, long rowCount, int byteSize)
    {
        return DataAttributes.builder()
                .set(ROW_OFFSET, currentOffset)
                .set(ROWS_COUNT, rowCount)
                .set(SEGMENT_SIZE, byteSize)
                .build();
    }

    private Map<String, List<String>> headers()
    {
        return Map.of("x-amz-server-side-encryption", List.of("AES256"));
    }
}
