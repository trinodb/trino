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
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.client.QueryResults;
import io.trino.client.ResultRowsDecoder;
import io.trino.client.StatementStats;
import io.trino.client.TrinoJsonCodec;
import io.trino.client.TypedQueryData;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.server.protocol.spooling.ServerQueryDataJacksonModule;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.ROW_OFFSET;
import static io.trino.client.spooling.DataAttribute.SCHEMA;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.client.spooling.Segment.inlined;
import static io.trino.client.spooling.Segment.spooled;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

public class TestQueryDataSerialization
{
    private static final List<Column> COLUMNS_LIST = ImmutableList.of(new Column("_col0", "bigint", new ClientTypeSignature("bigint")));
    private static final TrinoJsonCodec<QueryResults> CLIENT_CODEC = jsonCodec(QueryResults.class);
    private static final JsonCodec<QueryResults> SERVER_CODEC = new JsonCodecFactory(new ObjectMapperProvider()
            .withModules(Set.of(new ServerQueryDataJacksonModule())))
            .jsonCodec(QueryResults.class);

    @Test
    public void testNullDataSerialization()
    {
        assertThat(serialize(null)).doesNotContain("data");
        assertThat(serialize(QueryData.NULL)).doesNotContain("data");
    }

    @Test
    public void testEmptyArraySerialization()
    {
        testRoundTrip(TypedQueryData.of(ImmutableList.of()), "[]");

        assertThatThrownBy(() -> testRoundTrip(TypedQueryData.of(ImmutableList.of(ImmutableList.of())), "[[]]"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unexpected token END_ARRAY");
    }

    @Test
    public void testQueryDataSerialization()
    {
        List<List<Object>> values = ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(5L));
        testRoundTrip(TypedQueryData.of(values), "[[1],[5]]");
    }

    @Test
    public void testEncodedQueryDataSerialization()
    {
        EncodedQueryData queryData = new EncodedQueryData("json", ImmutableMap.of(), ImmutableList.of(inlined("[[10], [20]]".getBytes(UTF_8), dataAttributes(10, 2, 12))));
        testRoundTrip(queryData,
                """
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
        EncodedQueryData queryData = new EncodedQueryData("json", ImmutableMap.of("decryptionKey", "secret"), ImmutableList.of(inlined("[[10], [20]]".getBytes(UTF_8), dataAttributes(10, 2, 12))));
        testRoundTrip(queryData,
                """
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
                .withSegments(List.of(
                        inlined("super".getBytes(UTF_8), dataAttributes(0, 100, 5)),
                        spooled(
                                URI.create("http://localhost:8080/v1/download/20160128_214710_00012_rk68b/segments/1"),
                                URI.create("http://localhost:8080/v1/ack/20160128_214710_00012_rk68b/segments/1"),
                                dataAttributes(100, 100, 1024), Map.of("x-amz-server-side-encryption", List.of("AES256"))),
                        spooled(
                                URI.create("http://localhost:8080/v1/download/20160128_214710_00012_rk68b/segments/2"),
                                URI.create("http://localhost:8080/v1/ack/20160128_214710_00012_rk68b/segments/2"),
                                dataAttributes(200, 100, 1024), Map.of("x-amz-server-side-encryption", List.of("AES256")))))
                .withAttributes(DataAttributes.builder()
                        .set(SCHEMA, "serializedSchema")
                        .build())
                .build();
        testSerializationRoundTrip(queryData,
                """
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
                      "ackUri": "http://localhost:8080/v1/ack/20160128_214710_00012_rk68b/segments/1",
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
                      "ackUri": "http://localhost:8080/v1/ack/20160128_214710_00012_rk68b/segments/2",
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
        EncodedQueryData inlineQueryData = new EncodedQueryData("json", ImmutableMap.of("decryption_key", "secret"), ImmutableList.of(inlined("[[10], [20]]".getBytes(UTF_8), dataAttributes(10, 2, 12))));
        assertThat(inlineQueryData.toString()).isEqualTo("EncodedQueryData{encoding=json, segments=[InlineSegment{offset=10, rows=2, size=12}], metadata=[decryption_key]}");

        EncodedQueryData spooledQueryData = new EncodedQueryData("json+zstd", ImmutableMap.of("decryption_key", "secret"), ImmutableList.of(spooled(
                URI.create("http://coordinator:8080/v1/segments/uuid"),
                URI.create("http://coordinator:8080/v1/segments/uuid"),
                dataAttributes(10, 2, 1256), headers())));
        assertThat(spooledQueryData.toString()).isEqualTo("EncodedQueryData{encoding=json+zstd, segments=[SpooledSegment{offset=10, rows=2, size=1256, headers=[x-amz-server-side-encryption]}], metadata=[decryption_key]}");
    }

    private void testRoundTrip(QueryData queryData, String expectedDataRepresentation)
    {
        testSerializationRoundTrip(queryData, expectedDataRepresentation);
        assertEquals(deserialize(serialize(queryData)), queryData);

        assertThat(serialize(deserialize(serialize(queryData)))).isEqualToIgnoringWhitespace(queryResultsJson(expectedDataRepresentation));
    }

    private void testSerializationRoundTrip(QueryData queryData, String expectedDataRepresentation)
    {
        assertThat(serialize(queryData))
                .isEqualToIgnoringWhitespace(queryResultsJson(expectedDataRepresentation));
    }

    private String queryResultsJson(String expectedDataField)
    {
        return format(
                """
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
                    "planningTimeMillis": 0,
                    "analysisTimeMillis": 0,
                    "cpuTimeMillis": 0,
                    "wallTimeMillis": 0,
                    "queuedTimeMillis": 0,
                    "elapsedTimeMillis": 0,
                    "finishingTimeMillis": 0,
                    "physicalInputTimeMillis": 0,
                    "processedRows": 0,
                    "processedBytes": 0,
                    "physicalInputBytes": 0,
                    "physicalWrittenBytes": 0,
                    "internalNetworkInputBytes": 0,
                    "peakMemoryBytes": 0,
                    "spilledBytes": 0
                  },
                  "warnings": []
                }""", expectedDataField);
    }

    private static void assertEquals(QueryData left, QueryData right)
    {
        Iterable<List<Object>> leftValues = decodeData(left);
        Iterable<List<Object>> rightValues = decodeData(right);

        if (leftValues == null) {
            assertThat(rightValues).isNull();
            return;
        }

        assertThat(leftValues).hasSameElementsAs(rightValues);
    }

    private static Iterable<List<Object>> decodeData(QueryData data)
    {
        try (ResultRowsDecoder decoder = new ResultRowsDecoder()) {
            return decoder.toRows(COLUMNS_LIST, data);
        }
        catch (Exception e) {
            return fail(e);
        }
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
                OptionalLong.empty()));
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
