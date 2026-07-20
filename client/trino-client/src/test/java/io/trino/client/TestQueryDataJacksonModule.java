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

import com.google.common.collect.ImmutableList;
import io.trino.client.spooling.EncodedQueryData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

import static io.trino.client.TrinoJsonCodec.jsonCodec;
import static io.trino.client.TrinoJsonCodec.singlePassQueryResultsCodec;
import static org.assertj.core.api.Assertions.assertThat;

class TestQueryDataJacksonModule
{
    private static final TrinoJsonCodec<QueryResults> DEFAULT_CODEC = jsonCodec(QueryResults.class);
    private static final TrinoJsonCodec<QueryResults> EAGER_CODEC = singlePassQueryResultsCodec(false);

    private static final String COLUMNS = "[ " +
            "{\"name\": \"c1\", \"type\": \"bigint\", \"typeSignature\": {\"rawType\": \"bigint\", \"arguments\": []}}, " +
            "{\"name\": \"c2\", \"type\": \"varchar\", \"typeSignature\": {\"rawType\": \"varchar\", \"arguments\": []}} ]";

    private static final String DATA = "[[1, \"a\"], [2, null], [null, \"b\"]]";

    private static final String STATS = "{" +
            "\"state\": \"FINISHED\", \"queued\": false, \"scheduled\": false, \"nodes\": 0, " +
            "\"totalSplits\": 0, \"queuedSplits\": 0, \"runningSplits\": 0, \"completedSplits\": 0, " +
            "\"cpuTimeMillis\": 0, \"wallTimeMillis\": 0, \"queuedTimeMillis\": 0, \"elapsedTimeMillis\": 0, " +
            "\"processedRows\": 0, \"processedBytes\": 0, \"peakMemoryBytes\": 0}";

    private static final String RESPONSE = "{" +
            "\"id\": \"20160128_214710_00012_rk68b\", " +
            "\"infoUri\": \"http://localhost:54855/query.html?20160128_214710_00012_rk68b\", " +
            "\"nextUri\": \"http://localhost:54855/v1/statement/next\", " +
            "\"columns\": " + COLUMNS + ", " +
            "\"data\": " + DATA + ", " +
            "\"stats\": " + STATS + ", " +
            "\"updateType\": \"INSERT\", " +
            "\"updateCount\": 42}";

    @Test
    public void testEagerDecodingOfInlineData()
            throws Exception
    {
        QueryResults results = EAGER_CODEC.fromJson(RESPONSE);
        assertThat(results.getData()).isInstanceOf(TypedQueryData.class);
        assertThat(results.getData().getRowsCount()).isEqualTo(3);
        assertThat(materialize(results)).containsExactly(
                Arrays.asList(1L, "a"),
                Arrays.asList(2L, null),
                Arrays.asList((Object) null, "b"));
    }

    @Test
    public void testParityWithDefaultCodec()
            throws Exception
    {
        QueryResults eager = EAGER_CODEC.fromJson(RESPONSE);
        QueryResults buffered = DEFAULT_CODEC.fromJson(RESPONSE);

        assertThat(buffered.getData()).isInstanceOf(JsonQueryData.class);
        assertThat(eager.getId()).isEqualTo(buffered.getId());
        assertThat(eager.getInfoUri()).isEqualTo(buffered.getInfoUri());
        assertThat(eager.getNextUri()).isEqualTo(buffered.getNextUri());
        assertThat(eager.getColumns()).isEqualTo(buffered.getColumns());
        assertThat(eager.getStats().getState()).isEqualTo(buffered.getStats().getState());
        assertThat(eager.getUpdateType()).isEqualTo(buffered.getUpdateType());
        assertThat(eager.getUpdateCount()).isEqualTo(OptionalLong.of(42));
        assertThat(materialize(eager)).isEqualTo(materialize(buffered));
    }

    @Test
    public void testDataBeforeColumnsFallsBackToBuffering()
            throws Exception
    {
        String response = "{" +
                "\"id\": \"20160128_214710_00012_rk68b\", " +
                "\"infoUri\": \"http://localhost:54855/query.html\", " +
                "\"data\": " + DATA + ", " +
                "\"columns\": " + COLUMNS + ", " +
                "\"stats\": " + STATS + "}";

        QueryResults results = EAGER_CODEC.fromJson(response);
        assertThat(results.getData()).isInstanceOf(JsonQueryData.class);
        assertThat(results.getData().getRowsCount()).isEqualTo(3);
        assertThat(materialize(results)).containsExactly(
                Arrays.asList(1L, "a"),
                Arrays.asList(2L, null),
                Arrays.asList((Object) null, "b"));
    }

    @Test
    public void testSpooledDataPassesThrough()
            throws Exception
    {
        String response = "{" +
                "\"id\": \"20160128_214710_00012_rk68b\", " +
                "\"infoUri\": \"http://localhost:54855/query.html\", " +
                "\"columns\": " + COLUMNS + ", " +
                "\"data\": {" +
                "  \"encoding\": \"json\"," +
                "  \"segments\": [{" +
                "    \"type\": \"inline\"," +
                "    \"data\": \"W1sxMF0sIFsyMF1d\"," +
                "    \"metadata\": {\"rowOffset\": 0, \"rowsCount\": 2, \"segmentSize\": 12}}]}, " +
                "\"stats\": " + STATS + "}";

        QueryResults results = EAGER_CODEC.fromJson(response);
        assertThat(results.getData()).isInstanceOf(EncodedQueryData.class);
        EncodedQueryData data = (EncodedQueryData) results.getData();
        assertThat(data.getEncoding()).isEqualTo("json");
        assertThat(data.getSegments()).hasSize(1);
    }

    @Test
    public void testMissingAndNullData()
            throws Exception
    {
        String withoutData = "{" +
                "\"id\": \"20160128_214710_00012_rk68b\", " +
                "\"infoUri\": \"http://localhost:54855/query.html\", " +
                "\"stats\": " + STATS + "}";
        assertThat(EAGER_CODEC.fromJson(withoutData).getData()).isNull();

        String withNullData = "{" +
                "\"id\": \"20160128_214710_00012_rk68b\", " +
                "\"infoUri\": \"http://localhost:54855/query.html\", " +
                "\"data\": null, " +
                "\"stats\": " + STATS + "}";
        assertThat(EAGER_CODEC.fromJson(withNullData).getData()).isNull();
    }

    @Test
    public void testUnknownFieldsAreIgnored()
            throws Exception
    {
        String response = "{" +
                "\"id\": \"20160128_214710_00012_rk68b\", " +
                "\"unknownScalar\": 1, " +
                "\"unknownObject\": {\"a\": [1, 2, {\"b\": 3}]}, " +
                "\"infoUri\": \"http://localhost:54855/query.html\", " +
                "\"columns\": " + COLUMNS + ", " +
                "\"data\": " + DATA + ", " +
                "\"unknownArray\": [[1], [2]], " +
                "\"stats\": " + STATS + "}";

        QueryResults results = EAGER_CODEC.fromJson(response);
        assertThat(results.getData().getRowsCount()).isEqualTo(3);
        assertThat(materialize(results)).hasSize(3);
    }

    @Test
    public void testErrorResponse()
            throws Exception
    {
        String response = "{" +
                "\"id\": \"20160128_214710_00012_rk68b\", " +
                "\"infoUri\": \"http://localhost:54855/query.html\", " +
                "\"stats\": " + STATS + ", " +
                "\"error\": {\"message\": \"line 1:1: oops\", \"errorCode\": 1, \"errorName\": \"SYNTAX_ERROR\", \"errorType\": \"USER_ERROR\"}}";

        QueryResults results = EAGER_CODEC.fromJson(response);
        assertThat(results.getError()).isNotNull();
        assertThat(results.getError().getMessage()).isEqualTo("line 1:1: oops");
        assertThat(results.getError().getErrorName()).isEqualTo("SYNTAX_ERROR");
    }

    private static List<List<Object>> materialize(QueryResults results)
            throws Exception
    {
        try (ResultRowsDecoder decoder = new ResultRowsDecoder()) {
            return ImmutableList.copyOf(decoder.toRows(results));
        }
    }
}
