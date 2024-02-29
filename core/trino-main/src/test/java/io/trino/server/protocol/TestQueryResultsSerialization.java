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

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.OptionalDouble;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestQueryResultsSerialization
{
    private static final JsonCodec<QueryResults> CODEC = jsonCodec(QueryResults.class);

    @Test
    public void testNullDataSerialization()
    {
        // data field should not be serialized
        assertThat(serialize(null)).isEqualToIgnoringWhitespace("""
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
              "cpuTimeMillis" : 0,
              "wallTimeMillis" : 0,
              "queuedTimeMillis" : 0,
              "elapsedTimeMillis" : 0,
              "processedRows" : 0,
              "processedBytes" : 0,
              "physicalInputBytes" : 0,
              "physicalWrittenBytes" : 0,
              "peakMemoryBytes" : 0,
              "spilledBytes" : 0
            },
            "warnings" : [ ]
          }
          """);
    }

    @Test
    public void testEmptyArraySerialization()
    {
        testRoundTrip(ImmutableList.of(), "[]");

        assertThatThrownBy(() -> testRoundTrip(ImmutableList.of(ImmutableList.of()), "[[]]"))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("row/column size mismatch");
    }

    @Test
    public void testSerialization()
    {
        Iterable<List<Object>> values = ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(5L));
        testRoundTrip(values, "[[1],[5]]");
    }

    private void testRoundTrip(Iterable<List<Object>> results, String expectedDataRepresentation)
    {
        assertThat(serialize(results))
                .isEqualToIgnoringWhitespace(queryResultsJson(expectedDataRepresentation));

        String serialized = serialize(results);
        assertThat(CODEC.fromJson(serialized).getData()).isEqualTo(results);
    }

    private String queryResultsJson(String expectedDataField)
    {
        return format("""
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
                      "cpuTimeMillis" : 0,
                      "wallTimeMillis" : 0,
                      "queuedTimeMillis" : 0,
                      "elapsedTimeMillis" : 0,
                      "processedRows" : 0,
                      "processedBytes" : 0,
                      "physicalInputBytes" : 0,
                      "physicalWrittenBytes" : 0,
                      "peakMemoryBytes" : 0,
                      "spilledBytes" : 0
                    },
                    "warnings" : [ ]
                  }""", expectedDataField);
    }

    private static String serialize(Iterable<List<Object>> data)
    {
        return CODEC.toJson(new QueryResults(
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
}
