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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;

import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.JsonCodec.jsonCodec;
import static io.trino.client.NoQueryData.NO_DATA;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestQueryDataSerialization
{
    @Test
    public void testNullDataSerialization()
    {
        testRoundTrip(NO_DATA, "null");
        testRoundTrip(JsonInlineQueryData.create(null, false), "null");
    }

    @Test
    public void testEmptyArraySerialization()
    {
        testRoundTrip(JsonInlineQueryData.create(ImmutableList.of(), false), "[]");
    }

    @Test
    public void testValuesSerialization()
    {
        Iterable<List<Object>> values = ImmutableList.of(ImmutableList.of(1L), ImmutableList.of(5L));
        testRoundTrip(JsonInlineQueryData.create(values, true), "[[1],[5]]");
    }

    @QueryDataFormat(formatName = "json-download-uris")
    public static class CustomQueryData
            implements QueryData
    {
        private List<String> dataUris;

        @JsonCreator
        public CustomQueryData(@JsonProperty("data-uris") List<String> dataUris)
        {
            this.dataUris = ImmutableList.copyOf(dataUris);
        }

        @JsonProperty("data-uris")
        public List<String> getDataUris()
        {
            return dataUris;
        }

        @Override
        public Iterable<List<Object>> getData()
        {
            return ImmutableList.of(ImmutableList.of("some", "fake", "data"));
        }

        @Override
        public boolean isPresent()
        {
            return false;
        }
    }

    @Test
    public void testCustomFormatSerialization()
    {
        testRoundTrip(new CustomQueryData(ImmutableList.of("link1", "link2", "link3")), "{\"format\":\"json-download-uris\",\"data-uris\":[\"link1\",\"link2\",\"link3\"]}", CustomQueryData.class);

        assertThatThrownBy(() -> testRoundTrip(new CustomQueryData(ImmutableList.of("link1", "link2", "link3")), "{\"format\":\"json-download-uris\",\"data-uris\":[\"link1\",\"link2\",\"link3\"]}"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown type ID: json-download-uris");

        assertThatThrownBy(() -> deserialize(queryResultsJson("{\"format\":\"unknown-download-uris\",\"data-uris\":[\"link1\",\"link2\",\"link3\"]}")))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown type ID: unknown-download-uris");

        assertThatThrownBy(() -> deserialize(queryResultsJson("{\"data-uris\":[\"link1\",\"link2\",\"link3\"]}")))
                .hasRootCauseInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("Cannot construct instance of `io.trino.client.JsonInlineQueryData`");
    }

    private void testRoundTrip(QueryData queryData, String expectedDataRepresentation, Class<? extends QueryData>... formats)
    {
        assertThat(serialize(queryData, formats))
                .isEqualTo(queryResultsJson(expectedDataRepresentation));

        assertEquals(deserialize(serialize(queryData, formats), formats), queryData);
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

    private static void assertEquals(QueryData left, QueryData right)
    {
        if (!left.getClass().equals(right.getClass())) {
            throw new AssertionError(format("%s class is different than %s class", left.getClass(), right.getClass()));
        }

        if (left.isPresent() != right.isPresent()) {
            throw new AssertionError("mismatch with the isPresent check");
        }

        if (left.isEmpty() != right.isEmpty()) {
            throw new AssertionError("mismatch with the isEmpty check");
        }

        if (left instanceof NoQueryData && right instanceof NoQueryData) {
            assertThat(left.isPresent()).isFalse();
            assertThat(right.isPresent()).isFalse();
            assertThat(left).isEqualTo(right);
            return;
        }

        assertThat(left.getData()).hasSameElementsAs(right.getData());
    }

    private static QueryData deserialize(String serialized, Class<? extends QueryData>... formats)
    {
        JsonCodec<QueryResults> codec = codec(formats);
        try {
            return codec.fromJson(serialized).getData();
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static String serialize(QueryData data, Class<? extends QueryData>... formats)
    {
        JsonCodec<QueryResults> codec = codec(formats);
        return codec.toJson(new QueryResults(
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

    private static JsonCodec<QueryResults> codec(Class<? extends QueryData>... formats)
    {
        return jsonCodec(QueryResults.class, new QueryDataJsonSerializationModule(new QueryDataFormatResolver(Arrays.asList(formats))));
    }
}
