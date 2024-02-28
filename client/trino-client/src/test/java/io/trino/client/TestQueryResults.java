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
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.google.common.base.Strings;
import org.junit.jupiter.api.Test;

import static io.trino.client.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryResults
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private static final String GOLDEN_VALUE = "{\n" +
            "  \"id\" : \"20160128_214710_00012_rk68b\",\n" +
            "  \"infoUri\" : \"http://localhost:54855/query.html?20160128_214710_00012_rk68b\",\n" +
            "  \"columns\" : [ {\n" +
            "    \"name\" : \"_col0\",\n" +
            "    \"type\" : \"bigint\",\n" +
            "    \"typeSignature\" : {\n" +
            "      \"rawType\" : \"varchar\",\n" +
            "      \"typeArguments\" : [ ],\n" +
            "      \"literalArguments\" : [ ],\n" +
            "      \"arguments\" : [ ]\n" +
            "    }\n" +
            "  } ],\n" +
            "  \"data\" : [ [ %s ] ],\n" +
            "  \"stats\" : {\n" +
            "    \"state\" : \"FINISHED\",\n" +
            "    \"queued\" : false,\n" +
            "    \"scheduled\" : false,\n" +
            "    \"nodes\" : 0,\n" +
            "    \"totalSplits\" : 0,\n" +
            "    \"queuedSplits\" : 0,\n" +
            "    \"runningSplits\" : 0,\n" +
            "    \"completedSplits\" : 0,\n" +
            "    \"cpuTimeMillis\" : 0,\n" +
            "    \"wallTimeMillis\" : 0,\n" +
            "    \"queuedTimeMillis\" : 0,\n" +
            "    \"elapsedTimeMillis\" : 0,\n" +
            "    \"processedRows\" : 0,\n" +
            "    \"processedBytes\" : 0,\n" +
            "    \"peakMemoryBytes\" : 0\n" +
            "  }\n" +
            "}";

    @Test
    public void testCompatibility()
            throws JsonProcessingException
    {
        QueryResults results = QUERY_RESULTS_CODEC.fromJson(format(GOLDEN_VALUE, "\"123\""));
        assertThat(results.getId()).isEqualTo("20160128_214710_00012_rk68b");
    }

    @Test
    public void testReadLongColumn()
            throws JsonProcessingException
    {
        String longString = Strings.repeat("a", StreamReadConstraints.DEFAULT_MAX_STRING_LEN + 1);
        QueryResults results = QUERY_RESULTS_CODEC.fromJson(format(GOLDEN_VALUE, '"' + longString + '"'));
        assertThat(results.getId()).isEqualTo("20160128_214710_00012_rk68b");
    }
}
