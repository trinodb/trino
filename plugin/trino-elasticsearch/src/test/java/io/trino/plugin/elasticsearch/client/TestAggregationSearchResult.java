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
package io.trino.plugin.elasticsearch.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_INVALID_RESPONSE;
import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_QUERY_FAILURE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAggregationSearchResult
{
    @Test
    public void testCompleteResponse()
            throws IOException
    {
        byte[] response =
                """
                {"timed_out": false, "_shards": {"failed": 0}, "hits": {"total": {"value": 3, "relation": "eq"}}, "aggregations": {"k": {"value": 42}}}
                """.getBytes(UTF_8);
        AggregationSearchResult result = ElasticsearchClient.parseAggregationResponse(response);
        assertThat(result.response().path("aggregations").path("k").path("value").asLong()).isEqualTo(42);
        assertThat(result.totalHits()).isEqualTo(3);
        assertThat(result.responseSize()).isEqualTo(response.length);
    }

    @Test
    public void testExactTotalHitsPreservesIntegerPrecision()
            throws IOException
    {
        for (long count : new long[] {0, 9_007_199_254_740_993L, Long.MAX_VALUE}) {
            byte[] response = format(
                    """
                    {"timed_out":false,"_shards":{"failed":0},"hits":{"total":{"value":%s,"relation":"eq"}}}
                    """,
                    count).getBytes(UTF_8);
            assertThat(ElasticsearchClient.parseAggregationResponse(response).totalHits()).isEqualTo(count);
        }
    }

    @Test
    public void testRejectTimedOutResponse()
    {
        assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseAggregationResponse(
                """
                {"timed_out": true, "_shards": {"failed": 0}, "aggregations": {"k": {"value": 42}}}
                """.getBytes(UTF_8)))
                .hasErrorCode(ELASTICSEARCH_QUERY_FAILURE)
                .hasMessage("Elasticsearch aggregation search timed out");
    }

    @Test
    public void testRejectResponseWithoutCompletionInformation()
    {
        for (String response : new String[] {"{}", "null", "{\"timed_out\":false}"}) {
            assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseAggregationResponse(response.getBytes(UTF_8)))
                    .hasErrorCode(ELASTICSEARCH_INVALID_RESPONSE)
                    .hasMessage("Elasticsearch aggregation response is missing search completion information");
        }
    }

    @Test
    public void testRejectFailedShardResponse()
    {
        assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseAggregationResponse(
                """
                {"timed_out": false, "_shards": {"failed": 1, "failures": [{"reason": "unavailable shard"}]}, "aggregations": {"k": {"value": 42}}}
                """.getBytes(UTF_8)))
                .hasErrorCode(ELASTICSEARCH_QUERY_FAILURE)
                .hasMessageContaining("Elasticsearch aggregation search failed on one or more shards")
                .hasMessageContaining("unavailable shard");
    }

    @Test
    public void testRejectInexactOrMalformedHitCount()
    {
        for (String hits : List.of(
                "{}",
                "{\"total\":{\"value\":1,\"relation\":\"gte\"}}",
                "{\"total\":{\"value\":1.5,\"relation\":\"eq\"}}",
                "{\"total\":{\"value\":-1,\"relation\":\"eq\"}}",
                "{\"total\":{\"value\":9223372036854775808,\"relation\":\"eq\"}}")) {
            byte[] response = format(
                    """
                    {"timed_out":false,"_shards":{"failed":0},"hits":%s,"aggregations":{"k":{"value":42}}}
                    """,
                    hits).getBytes(UTF_8);
            assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseAggregationResponse(response))
                    .hasErrorCode(ELASTICSEARCH_INVALID_RESPONSE)
                    .hasMessage("Elasticsearch count response is missing an exact nonnegative hit count");
        }
    }
}
