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
package io.trino.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestQueryDataSerialization
{
    private static final JsonCodec<QueryResults> CODEC = jsonCodec(QueryResults.class);

    @Test
    public void testNullDataSerialization()
    {
        testRoundTrip(null, null);
    }

    @Test
    public void testEmptyArraySerialization()
    {
        testRoundTrip(ImmutableList.of(), "[]");

        assertThatThrownBy(() -> testRoundTrip(ImmutableList.of(ImmutableList.of()), "[[]]"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("row/column size mismatch");
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
                .isEqualTo(queryResultsJson(expectedDataRepresentation));

        assertThat(deserialize(serialize(results)).getData()).isEqualTo(results);
    }

    private String queryResultsJson(String expectedDataField)
    {
        String fieldRepresentation = Optional.ofNullable(expectedDataField)
                .map(data -> "\"data\":" + data + ",")
                .orElse("");

        return format("{\"id\":\"20160128_214710_00012_rk68b\",\"infoUri\":\"http://coordinator/query.html?20160128_214710_00012_rk68b\",\"partialCancelUri\":null," +
                "\"nextUri\":null,\"columns\":[{\"name\":\"_col0\",\"type\":\"bigint\",\"typeSignature\":{\"rawType\":\"bigint\",\"arguments\":[]}}],%s" +
                "\"stats\":{\"state\":\"FINISHED\",\"queued\":false,\"scheduled\":false,\"progressPercentage\":null,\"runningPercentage\":null,\"nodes\":0," +
                "\"totalSplits\":0,\"queuedSplits\":0,\"runningSplits\":0,\"completedSplits\":0,\"cpuTimeMillis\":0,\"wallTimeMillis\":0,\"queuedTimeMillis\":0," +
                "\"elapsedTimeMillis\":0,\"processedRows\":0,\"processedBytes\":0,\"physicalInputBytes\":0,\"physicalWrittenBytes\":0,\"peakMemoryBytes\":0," +
                "\"spilledBytes\":0,\"rootStage\":null},\"error\":null,\"warnings\":[],\"updateType\":null,\"updateCount\":null}", fieldRepresentation);
    }

    private static QueryResults deserialize(String serialized)
    {
        try {
            return CODEC.fromJson(serialized);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
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
