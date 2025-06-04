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
import io.trino.server.protocol.spooling.ServerQueryDataJacksonModule;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestQueryResultsSerialization
{
    private static final List<Column> COLUMNS = ImmutableList.of(new Column("_col0", BIGINT, new ClientTypeSignature("bigint")));

    // As close as possible to the server mapper (client mapper differs)
    private static final JsonCodec<QueryResults> SERVER_CODEC = new JsonCodecFactory(new ObjectMapperProvider()
            .withModules(Set.of(new ServerQueryDataJacksonModule())))
            .jsonCodec(QueryResults.class);

    private static final TrinoJsonCodec<QueryResults> CLIENT_CODEC = jsonCodec(QueryResults.class);

    @Test
    public void testNullDataSerialization()
    {
        // data field should not be serialized
        assertThat(serialize(null)).isEqualToIgnoringWhitespace(
                """
                  {
                  "id" : "20160128_214710_00012_rk68b",
                  "infoUri" : "http://coordinator/query.html?20160128_214710_00012_rk68b",
                  "columns" : [ {
                    "name" : "_col0",
                    "type" : "bigint",
                    "typeSignature" : {
                      "rawType" : "bigint",
                      "arguments" : [ ]
                    }
                  } ],
                  "stats" : {
                    "state" : "FINISHED",
                    "queued" : false,
                    "scheduled" : false,
                    "nodes" : 0,
                    "totalSplits" : 0,
                    "queuedSplits" : 0,
                    "runningSplits" : 0,
                    "completedSplits" : 0,
                    "planningTimeMillis": 0,
                    "analysisTimeMillis": 0,
                    "cpuTimeMillis" : 0,
                    "wallTimeMillis" : 0,
                    "queuedTimeMillis" : 0,
                    "elapsedTimeMillis" : 0,
                    "finishingTimeMillis": 0,
                    "physicalInputTimeMillis": 0,
                    "processedRows" : 0,
                    "processedBytes" : 0,
                    "physicalInputBytes" : 0,
                    "physicalWrittenBytes" : 0,
                    "internalNetworkInputBytes": 0,
                    "peakMemoryBytes" : 0,
                    "spilledBytes" : 0
                  },
                  "warnings" : [ ]
                }
                """);
    }

    @Test
    public void testEmptyArraySerialization()
            throws Exception
    {
        testRoundTrip(TypedQueryData.of(ImmutableList.of()), "[]");

        assertThatThrownBy(() -> testRoundTrip(TypedQueryData.of(ImmutableList.of(ImmutableList.of())), "[[]]"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unexpected token END_ARRAY");
    }

    @Test
    public void testSerialization()
            throws Exception
    {
        QueryData values = TypedQueryData.of(ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(5L)));
        testRoundTrip(values, "[[1],[5]]");
    }

    private void testRoundTrip(QueryData results, String expectedDataRepresentation)
            throws Exception
    {
        assertThat(serialize(results))
                .isEqualToIgnoringWhitespace(queryResultsJson(expectedDataRepresentation));

        String serialized = serialize(results);
        try (ResultRowsDecoder decoder = new ResultRowsDecoder()) {
            assertThat(decoder.toRows(COLUMNS, CLIENT_CODEC.fromJson(serialized).getData()))
                    .containsAll(decoder.toRows(COLUMNS, results));
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String queryResultsJson(String expectedDataField)
    {
        return format(
                """
                {
                    "id" : "20160128_214710_00012_rk68b",
                    "infoUri" : "http://coordinator/query.html?20160128_214710_00012_rk68b",
                    "columns" : [ {
                      "name" : "_col0",
                      "type" : "bigint",
                      "typeSignature" : {
                        "rawType" : "bigint",
                        "arguments" : [ ]
                      }
                    } ],
                    "data" : %s,
                    "stats" : {
                      "state" : "FINISHED",
                      "queued" : false,
                      "scheduled" : false,
                      "nodes" : 0,
                      "totalSplits" : 0,
                      "queuedSplits" : 0,
                      "runningSplits" : 0,
                      "completedSplits" : 0,
                      "planningTimeMillis": 0,
                      "analysisTimeMillis": 0,
                      "cpuTimeMillis" : 0,
                      "wallTimeMillis" : 0,
                      "queuedTimeMillis" : 0,
                      "elapsedTimeMillis" : 0,
                      "finishingTimeMillis": 0,
                      "physicalInputTimeMillis": 0,
                      "processedRows" : 0,
                      "processedBytes" : 0,
                      "physicalInputBytes" : 0,
                      "physicalWrittenBytes" : 0,
                      "internalNetworkInputBytes": 0,
                      "peakMemoryBytes" : 0,
                      "spilledBytes" : 0
                    },
                    "warnings" : [ ]
                  }""", expectedDataField);
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
}
