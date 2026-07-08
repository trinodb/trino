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
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSort;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBuildSort
{
    @Test
    public void testDefaultDocOrder()
    {
        List<JsonNode> sort = buildSort(ImmutableList.of(), false);
        assertThat(sort).hasSize(1);
        assertThat(sort.get(0).asText()).isEqualTo("_doc");
    }

    @Test
    public void testRelevanceWhenQueryPresent()
    {
        assertThat(buildSort(ImmutableList.of(), true)).isEmpty();
    }

    @Test
    public void testAscendingNullsLast()
    {
        List<JsonNode> sort = buildSort(ImmutableList.of(new ElasticsearchColumnSort("age", true, false)), false);
        assertThat(sort).hasSize(1);
        assertThat(sort.get(0).toString()).isEqualTo("{\"age\":{\"order\":\"asc\",\"missing\":\"_last\"}}");
    }

    @Test
    public void testDescendingNullsFirstOnKeywordSubfield()
    {
        List<JsonNode> sort = buildSort(ImmutableList.of(new ElasticsearchColumnSort("name.keyword", false, true)), false);
        assertThat(sort.get(0).toString()).isEqualTo("{\"name.keyword\":{\"order\":\"desc\",\"missing\":\"_first\"}}");
    }

    @Test
    public void testMultipleKeysPreserveOrder()
    {
        List<JsonNode> sort = buildSort(
                ImmutableList.of(
                        new ElasticsearchColumnSort("a", true, false),
                        new ElasticsearchColumnSort("b", false, true)),
                false);
        assertThat(sort).hasSize(2);
        assertThat(sort.get(0).toString()).isEqualTo("{\"a\":{\"order\":\"asc\",\"missing\":\"_last\"}}");
        assertThat(sort.get(1).toString()).isEqualTo("{\"b\":{\"order\":\"desc\",\"missing\":\"_first\"}}");
    }
}
