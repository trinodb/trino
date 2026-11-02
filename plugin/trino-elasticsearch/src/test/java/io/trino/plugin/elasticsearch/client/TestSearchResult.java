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
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_INVALID_RESPONSE;
import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_QUERY_FAILURE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSearchResult
{
    @Test
    public void testCompleteSearchResponse()
            throws IOException
    {
        SearchResult result = ElasticsearchClient.parseSearchResponse(
                """
                {
                    "_scroll_id":"scroll-1",
                    "timed_out":false,
                    "_shards":{"failed":0},
                    "hits":{"hits":[{
                        "_id":"1",
                        "_score":1.5,
                        "_source":{"value":9007199254740993,"text":"é"},
                        "fields":{"value":[9007199254740993]}
                    }]}
                }
                """.getBytes(UTF_8));
        assertThat(result.scrollId()).contains("scroll-1");
        SearchDocument document = getOnlyElement(result.hits());
        assertThat(document.id()).isEqualTo("1");
        assertThat(document.score()).isEqualTo(1.5f);
        assertThat(document.sourceAsMap()).isEqualTo(Map.of("value", 9_007_199_254_740_993L, "text", "é"));
        String source = "{\"value\":9007199254740993,\"text\":\"é\"}";
        assertThat(document.sourceAsString()).isEqualTo(source);
        assertThat(document.sourceLength()).isEqualTo(source.getBytes(UTF_8).length);
        assertThat(document.fields()).isEqualTo(Map.of("value", List.of(9_007_199_254_740_993L)));
    }

    @Test
    public void testResponseWithoutSourceOrScore()
            throws IOException
    {
        SearchResult result = ElasticsearchClient.parseSearchResponse(
                """
                {"timed_out":false,"_shards":{"failed":0},"hits":{"hits":[{"_id":"1","_score":null}]}}
                """.getBytes(UTF_8));
        assertThat(result.scrollId()).isEmpty();
        SearchDocument document = getOnlyElement(result.hits());
        assertThat(document.score()).isNaN();
        assertThat(document.sourceAsMap()).isEmpty();
        assertThat(document.sourceAsString()).isEqualTo("{}");
        assertThat(document.sourceLength()).isEqualTo(2);
        assertThat(document.fields()).isEmpty();
    }

    @Test
    public void testRejectIncompleteSearch()
    {
        assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseSearchResponse(
                """
                {"timed_out":true,"_shards":{"failed":0},"hits":{"hits":[{"_id":"1"}]}}
                """.getBytes(UTF_8)))
                .hasErrorCode(ELASTICSEARCH_QUERY_FAILURE)
                .hasMessage("Elasticsearch scan search timed out");
        assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseSearchResponse(
                """
                {"timed_out":false,"_shards":{"failed":1,"failures":[{"reason":"unavailable shard"}]},"hits":{"hits":[{"_id":"1"}]}}
                """.getBytes(UTF_8)))
                .hasErrorCode(ELASTICSEARCH_QUERY_FAILURE)
                .hasMessageContaining("Elasticsearch scan search failed on one or more shards");
        assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseSearchResponse("{\"hits\":{\"hits\":[]}}".getBytes(UTF_8)))
                .hasErrorCode(ELASTICSEARCH_INVALID_RESPONSE)
                .hasMessage("Elasticsearch scan response is missing search completion information");
    }

    @Test
    public void testRejectResponseWithoutHits()
    {
        assertTrinoExceptionThrownBy(() -> ElasticsearchClient.parseSearchResponse("{\"timed_out\":false,\"_shards\":{\"failed\":0}}".getBytes(UTF_8)))
                .hasErrorCode(ELASTICSEARCH_INVALID_RESPONSE)
                .hasMessage("Elasticsearch scan response is missing search hits");
    }
}
