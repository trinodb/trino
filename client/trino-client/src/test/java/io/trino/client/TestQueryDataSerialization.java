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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalDouble;

import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.JsonCodec.jsonCodec;
import static io.trino.client.QueryDataPart.inlineQueryDataPart;
import static io.trino.client.QueryDataReference.INLINE;
import static io.trino.client.QueryDataSerialization.JSON;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestQueryDataSerialization
{
    private static final List<Column> COLUMNS_LIST = ImmutableList.of(new Column("_col0", "bigint", new ClientTypeSignature("bigint")));
    private static final JsonCodec<QueryResults> CODEC = jsonCodec(QueryResults.class, new QueryDataJsonModule());

    @Test
    public void testNullDataSerialization()
    {
        testRoundTrip(COLUMNS_LIST, null, "null");
        testRoundTrip(COLUMNS_LIST, LegacyQueryData.create(null), "null");
    }

    @Test
    public void testEmptyArraySerialization()
    {
        testRoundTrip(COLUMNS_LIST, LegacyQueryData.create(ImmutableList.of()), "[]");

        assertThatThrownBy(() -> testRoundTrip(COLUMNS_LIST, LegacyQueryData.create(ImmutableList.of(ImmutableList.of())), "[[]]"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("row/column size mismatch");
    }

    @Test
    public void testLegacySerialization()
    {
        Iterable<List<Object>> values = ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(5L));
        testRoundTrip(COLUMNS_LIST, LegacyQueryData.create(values), "[[1],[5]]");
    }

    @Test
    public void testEncodedSerialization()
    {
        EncodedQueryData queryData = new EncodedQueryData(ImmutableList.of(inlineQueryDataPart("[[10], [20]]".getBytes(StandardCharsets.UTF_8), 2)), QueryDataEncodings.singleEncoding(INLINE, JSON));
        testRoundTrip(COLUMNS_LIST, queryData, "{\"parts\":[{\"value\":\"W1sxMF0sIFsyMF1d\",\"row_count\":2,\"size\":\"12B\"}],\"encoding\":\"inline-json\"}");
    }

    private void testRoundTrip(List<Column> columns, QueryData queryData, String expectedDataRepresentation)
    {
        assertThat(serialize(queryData))
                .isEqualTo(queryResultsJson(expectedDataRepresentation));

        assertEquals(columns, deserialize(serialize(queryData)), queryData);
    }

    private String queryResultsJson(String expectedDataField)
    {
        return format("{\"id\":\"20160128_214710_00012_rk68b\",\"infoUri\":\"http://coordinator/query.html?20160128_214710_00012_rk68b\",\"partialCancelUri\":null," +
                "\"nextUri\":null,\"columns\":[{\"name\":\"_col0\",\"type\":\"bigint\",\"typeSignature\":{\"rawType\":\"bigint\",\"arguments\":[]}}],\"data\":%s," +
                "\"stats\":{\"state\":\"FINISHED\",\"queued\":false,\"scheduled\":false,\"progressPercentage\":null,\"runningPercentage\":null,\"nodes\":0," +
                "\"totalSplits\":0,\"queuedSplits\":0,\"runningSplits\":0,\"completedSplits\":0,\"cpuTimeMillis\":0,\"wallTimeMillis\":0,\"queuedTimeMillis\":0," +
                "\"elapsedTimeMillis\":0,\"processedRows\":0,\"processedBytes\":0,\"physicalInputBytes\":0,\"physicalWrittenBytes\":0,\"peakMemoryBytes\":0," +
                "\"spilledBytes\":0,\"rootStage\":null},\"error\":null,\"warnings\":[],\"updateType\":null,\"updateCount\":null}", expectedDataField);
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
        if (data instanceof LegacyQueryData) {
            return new LegacyQueryDataDecoder().decode(data, columns);
        }
        else {
            return new InlineQueryDataDecoder(new JsonQueryDataDeserializer()).decode(data, columns);
        }
    }

    private static QueryData deserialize(String serialized)
    {
        try {
            return CODEC.fromJson(serialized).getData();
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static String serialize(QueryData data)
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
