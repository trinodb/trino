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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * P0 predicate-pushdown acceptance contract executed against both Elasticsearch 7 and Elasticsearch 8.
 */
public abstract class BaseElasticsearchP0PredicatePushdownTest
        extends BaseElasticsearchFullTextPushdownTest
{
    private final RestClient client;

    protected BaseElasticsearchP0PredicatePushdownTest(ElasticsearchServer server)
    {
        super(server);
        this.client = server.getClient();
    }

    @Override
    protected List<Integer> largeInValuesCountData()
    {
        // P0 native terms pushdown removes the old bool-clause ceiling. Exercise both sides of Elasticsearch's
        // traditional 1024-clause default in the generic connector test suite.
        return ImmutableList.of(1, 10, 1_000, 2_000);
    }

    @Override
    @Test
    public void testRegexpLikeIsNotPushedDown()
    {
        // BaseElasticsearchConnectorTest predates the explicit full-text safety modes and asserts that regexp_like is
        // always residual. P0 changes that contract: UNSAFE explicitly accepts Lucene/Joni semantic differences and
        // makes a successfully translated regexp authoritative.
        String catalogName = getSession().getCatalog().orElseThrow();
        Session unsafe = Session.builder(getSession())
                .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "UNSAFE")
                .build();
        String sql = "SELECT count(*) FROM orders WHERE regexp_like(orderstatus, '^[oO]$')";

        // The fixture analyzes orderstatus to lowercase; this pattern intentionally matches both the source value and
        // the analyzed term so the assertion can validate pushdown and results independently of that known difference.
        assertQuery(unsafe, sql);
        assertThat(query(unsafe, sql)).isFullyPushedDown();
    }

    @Test
    public void testNativeTermsBeyondDefaultBoolClauseLimit()
    {
        String values = IntStream.range(0, 2_000)
                .mapToObj(Integer::toString)
                .collect(joining(","));

        // This used to fail once the generated bool.should exceeded Elasticsearch's default max clause count.
        // QueryBuilder now emits a single native terms query, while assertQuery verifies connector/reference equality.
        assertQuery("SELECT count(*) FROM orders WHERE orderkey IN (" + values + ")");
    }

    @Test
    public void testDynamicFilterReducesElasticsearchProbeInput()
    {
        Session dynamicFilteringSession = Session.builder(getSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .build();
        String sql = "SELECT count(*) FROM orders o JOIN customer c ON o.custkey = c.custkey WHERE c.name = 'Customer#000000001'";

        // The right side is intentionally the small build side. Its value is not statically known from the name
        // predicate, so reducing the orders scan requires the runtime join dynamic filter.
        assertQuery(dynamicFilteringSession, sql);

        QueryRunner runner = getQueryRunner();
        MaterializedResultWithPlan filtered = runner.executeWithPlan(dynamicFilteringSession, sql);
        QueryStats filteredStats = queryStats(runner, filtered);

        Session dynamicFilteringDisabled = Session.builder(dynamicFilteringSession)
                .setSystemProperty("enable_dynamic_filtering", "false")
                .build();
        MaterializedResultWithPlan unfiltered = runner.executeWithPlan(dynamicFilteringDisabled, sql);
        QueryStats unfilteredStats = queryStats(runner, unfiltered);

        assertThat(filteredStats.getDynamicFiltersStats().getTotalDynamicFilters()).isGreaterThan(0);
        assertThat(filteredStats.getDynamicFiltersStats().getDynamicFiltersCompleted()).isGreaterThan(0);
        assertThat(filteredStats.getPhysicalInputPositions())
                .as("Elasticsearch physical input positions with dynamic filtering")
                .isLessThan(unfilteredStats.getPhysicalInputPositions());
    }

    @Test
    public void testPrimitiveArrayExactMembershipPushdown()
            throws IOException
    {
        String indexName = "p0_array_membership_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                  "_meta": {
                    "trino": {
                      "tags": { "isArray": true },
                      "numbers": { "isArray": true },
                      "text_tags": { "isArray": true },
                      "text_with_keyword": { "isArray": true }
                    }
                  },
                  "properties": {
                    "id": { "type": "keyword" },
                    "tags": { "type": "keyword" },
                    "numbers": { "type": "integer" },
                    "text_tags": { "type": "text" },
                    "text_with_keyword": {
                      "type": "text",
                      "fields": { "keyword": { "type": "keyword" } }
                    }
                  }
                }
                """;
        createIndex(indexName, properties);
        try {
            index(indexName, ImmutableMap.<String, Object>builder()
                    .put("id", "1")
                    .put("tags", ImmutableList.of("telegram", "facebook"))
                    .put("numbers", ImmutableList.of(1, 2, 3))
                    .put("text_tags", ImmutableList.of("telegram", "social network"))
                    .put("text_with_keyword", ImmutableList.of("CaseSensitive", "other"))
                    .buildOrThrow());
            index(indexName, ImmutableMap.<String, Object>builder()
                    .put("id", "2")
                    .put("tags", ImmutableList.of("twitter"))
                    .put("numbers", ImmutableList.of(4, 5))
                    .put("text_tags", ImmutableList.of("facebook"))
                    .put("text_with_keyword", ImmutableList.of("casesensitive"))
                    .buildOrThrow());
            index(indexName, ImmutableMap.<String, Object>builder()
                    .put("id", "3")
                    .put("tags", ImmutableList.of())
                    .put("numbers", ImmutableList.of())
                    .put("text_tags", ImmutableList.of())
                    .put("text_with_keyword", ImmutableList.of())
                    .buildOrThrow());
            index(indexName, ImmutableMap.of("id", "4"));

            List<String> tagsWithNull = new ArrayList<>();
            tagsWithNull.add(null);
            tagsWithNull.add("facebook");
            index(indexName, ImmutableMap.of("id", "5", "tags", tagsWithNull));

            List<String> onlyNullTag = new ArrayList<>();
            onlyNullTag.add(null);
            index(indexName, ImmutableMap.of("id", "6", "tags", onlyNullTag));

            assertThat(query("SELECT id FROM " + indexName + " WHERE contains(tags, 'telegram')"))
                    .matches("VALUES VARCHAR '1'")
                    .isFullyPushedDown();
            assertThat(query("SELECT id FROM " + indexName + " WHERE contains(tags, 'facebook')"))
                    .matches("VALUES VARCHAR '1', VARCHAR '5'")
                    .isFullyPushedDown();
            assertThat(query("SELECT id FROM " + indexName + " WHERE arrays_overlap(tags, ARRAY['facebook', 'missing'])"))
                    .matches("VALUES VARCHAR '1', VARCHAR '5'")
                    .isFullyPushedDown();
            assertThat(query("SELECT id FROM " + indexName + " WHERE contains(numbers, 2)"))
                    .matches("VALUES VARCHAR '1'")
                    .isFullyPushedDown();
            assertThat(query("SELECT id FROM " + indexName + " WHERE arrays_overlap(numbers, ARRAY[3, 9])"))
                    .matches("VALUES VARCHAR '1'")
                    .isFullyPushedDown();

            // text.keyword is exact and case-sensitive. The lowercase document must not match the mixed-case value.
            assertThat(query("SELECT id FROM " + indexName + " WHERE contains(text_with_keyword, 'CaseSensitive')"))
                    .matches("VALUES VARCHAR '1'")
                    .isFullyPushedDown();

            // Analyzed-text-only membership stays in Trino; a term query would not have SQL string equality semantics.
            assertThat(query("SELECT id FROM " + indexName + " WHERE contains(text_tags, 'telegram')"))
                    .matches("VALUES VARCHAR '1'")
                    .isNotFullyPushedDown(FilterNode.class);

            // NULL in the constant array can make arrays_overlap indeterminate when no non-null value matches.
            assertThat(query("SELECT id FROM " + indexName + " WHERE arrays_overlap(tags, ARRAY['telegram', CAST(NULL AS varchar)])"))
                    .matches("VALUES VARCHAR '1'")
                    .isNotFullyPushedDown(FilterNode.class);

            // Duplicate constants are harmless, but the existential predicate still maps to native terms semantics.
            assertThat(query("SELECT id FROM " + indexName + " WHERE arrays_overlap(tags, ARRAY['facebook', 'facebook'])"))
                    .matches("VALUES VARCHAR '1', VARCHAR '5'")
                    .isFullyPushedDown();

            // Whole-array equality and whole-array NULL checks are intentionally not part of P0 exact membership.
            assertThat(query("SELECT id FROM " + indexName + " WHERE tags = ARRAY['telegram', 'facebook']"))
                    .matches("VALUES VARCHAR '1'")
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT id FROM " + indexName + " WHERE tags IS NULL"))
                    .matches("VALUES VARCHAR '4'")
                    .isNotFullyPushedDown(FilterNode.class);
        }
        finally {
            deleteIndex(indexName);
        }
    }

    @Test
    public void testMultipleUnsafeRegexpPredicatesOnSameField()
            throws IOException
    {
        String indexName = "p0_same_field_regexp_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                  "properties": {
                    "value": { "type": "keyword" },
                    "id": { "type": "keyword" }
                  }
                }
                """;
        createIndex(indexName, properties);
        try {
            index(indexName, ImmutableMap.of("value", "alpha-beta", "id", "1"));
            index(indexName, ImmutableMap.of("value", "alpha", "id", "2"));

            String catalogName = getSession().getCatalog().orElseThrow();
            Session unsafe = Session.builder(getSession())
                    .setCatalogSessionProperty(catalogName, "full_text_pushdown_mode", "UNSAFE")
                    .build();

            assertThat(query(unsafe, "SELECT id FROM " + indexName + " WHERE regexp_like(value, '^alpha') AND regexp_like(value, 'beta$')"))
                    .matches("VALUES VARCHAR '1'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(indexName);
        }
    }

    private static QueryStats queryStats(QueryRunner runner, MaterializedResultWithPlan result)
    {
        return runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats();
    }

    private void createIndex(String indexName, @Language("JSON") String properties)
            throws IOException
    {
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity("{\"mappings\": " + properties + "}");
        client.performRequest(request);
    }

    private void index(String index, Map<String, Object> document)
            throws IOException
    {
        String json = new JsonMapper().writeValueAsString(document);
        Request request = new Request("PUT", format("/%s/_doc/%s?refresh", index, System.nanoTime()));
        request.setJsonEntity(json);
        client.performRequest(request);
    }

    private void deleteIndex(String indexName)
            throws IOException
    {
        client.performRequest(new Request("DELETE", "/" + indexName));
    }
}
