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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseAggregationPushDownTest
        extends BaseElasticsearchConnectorTest
{
    public BaseAggregationPushDownTest(String image)
    {
        super(image);
    }

    @Test
    public void testAggregationPushdown()
    {
        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();

        // count()
        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(1) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count() FROM nation")).isFullyPushedDown();

        // GROUP BY
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on bigint column
        // GROUP BY and WHERE on aggregation key
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on varchar column
        // GROUP BY and WHERE on "other" (not aggregation key, not aggregation input)
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey")).isNotFullyPushedDown(FilterNode.class);

        // The groups of custKey is more than 50(page size), so the case must trigger AggregationQueryPageSource#getNextPage multiple times
        assertThat(query("SELECT custkey, sum(totalprice) FROM orders GROUP BY custkey")).isFullyPushedDown();
    }

    @Test
    public void testKeywordAggregationWithEmptyBucketPushdown() throws Exception
    {
        String mappings = "{\n" +
                "  \"properties\": {\n" +
                "    \"kw_column\": {\n" +
                "      \"type\": \"keyword\"\n" +
                "    },\n" +
                "    \"kw_column1\": {\n" +
                "      \"type\": \"keyword\"\n" +
                "    },\n" +
                "    \"text_column\": {\n" +
                "      \"type\": \"text\"\n" +
                "    },\n" +
                "    \"value_column\": {\n" +
                "      \"type\": \"float\"\n" +
                "    },\n" +
                "    \"value_column2\": {\n" +
                "      \"type\": \"double\"\n" +
                "    },\n" +
                "    \"int_column\": {\n" +
                "      \"type\": \"long\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        String indexName = "keyword_agg_pushdown";
        createIndex(indexName, mappings);
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("kw_column", "A")
                .put("kw_column1", "a")
                .put("text_column", "A")
                .put("value_column", 2.0)
                .put("int_column", 2)
                .build());
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("kw_column", "B")
                .put("text_column", "B")
                .build());
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("text_column", "B")
                .put("value_column", 4.0)
                .build());
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("text_column", "B")
                .put("value_column", 3.0)
                .build());

        assertThat(query("SELECT kw_column, kw_column1, count(*), avg(value_column), sum(int_column) FROM keyword_agg_pushdown GROUP BY kw_column, kw_column1")).isFullyPushedDown();
        // For all null column aggregation
        assertThat(query("SELECT sum(value_column2), max(value_column2), min(value_column2), avg(value_column2), count(value_column2) FROM keyword_agg_pushdown GROUP BY kw_column")).isFullyPushedDown();
        // ES only supports aggregates min/max for numeric types
        assertThat(query("SELECT kw_column, max(kw_column1) FROM keyword_agg_pushdown GROUP BY kw_column")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
    }
}
