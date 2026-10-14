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

public class TestCountSearchResult
{
    @Test
    public void testExactCount()
            throws IOException
    {
        for (long count : new long[] {0, 9_007_199_254_740_993L, Long.MAX_VALUE}) {
            byte[] response = format(
                    """
                    {"timed_out":false,"_shards":{"failed":0},"hits":{"total":{"value":%s,"relation":"eq"}}}
                    """,
                    count).getBytes(UTF_8);
            assertThat(ElasticsearchClient.parseCountResponse(response)).isEqualTo(count);
        }
    }

    @Test
    public void testRejectInexactOrMalformedCount()
    {
        for (String total : List.of(
                "{}",
                "{\"value\":1,\"relation\":\"gte\"}",
                "{\"value\":1.5,\"relation\":\"eq\"}",
                "{\"value\":-1,\"relation\":\"eq\"}",
                "{\"value\":9223372036854775808,\"relation\":\"eq\"}")) {
            byte[] response = format(
                    """
                    {"timed_out":false,"_shards":{"failed":0},"hits":{"total":%s}}
                    """,
                    total).getBytes(UTF_8);
            assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseCountResponse(response))
                    .hasErrorCode(ELASTICSEARCH_INVALID_RESPONSE)
                    .hasMessage("Elasticsearch count response is missing an exact nonnegative hit count");
        }
    }

    @Test
    public void testRejectIncompleteSearch()
    {
        assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseCountResponse(
                """
                {"timed_out":true,"_shards":{"failed":0},"hits":{"total":{"value":1,"relation":"eq"}}}
                """.getBytes(UTF_8)))
                .hasErrorCode(ELASTICSEARCH_QUERY_FAILURE)
                .hasMessage("Elasticsearch count search timed out");
        assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseCountResponse(
                """
                {"timed_out":false,"_shards":{"failed":1,"failures":[{"reason":"unavailable shard"}]},"hits":{"total":{"value":1,"relation":"eq"}}}
                """.getBytes(UTF_8)))
                .hasErrorCode(ELASTICSEARCH_QUERY_FAILURE)
                .hasMessageContaining("Elasticsearch count search failed on one or more shards");
        assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseCountResponse("{}".getBytes(UTF_8)))
                .hasErrorCode(ELASTICSEARCH_INVALID_RESPONSE)
                .hasMessage("Elasticsearch count response is missing search completion information");
    }
}
