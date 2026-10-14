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
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.plugin.elasticsearch.client.Shard;
import io.trino.plugin.elasticsearch.decoders.BigintDecoder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.AbstractTestQueries;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_QUERY_FAILURE;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.PASSWORD;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryRunner.USER;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.SCAN;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseElasticsearchConnectorTest
        extends BaseConnectorTest
{
    private ElasticsearchServer server;
    private RestClient client;
    protected final String jmxBaseName = randomNameSuffix();

    BaseElasticsearchConnectorTest(ElasticsearchServer server)
    {
        this.server = requireNonNull(server, "server is null");
        this.client = server.getClient();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return ElasticsearchQueryRunner.builder(server)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .addConnectorProperties(Map.of("jmx.base-name", jmxBaseName))
                .build();
    }

    @AfterAll
    public final void destroy()
            throws IOException
    {
        server.close();
        server = null;
        client.close();
        client = null;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_UPDATE -> false;
            case SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    /**
     * This method overrides the default values used for the data provider
     * of the test {@link AbstractTestQueries#testLargeIn()} by taking
     * into account that by default Elasticsearch supports only up to `1024`
     * clauses in query.
     * <p>
     * Consult `index.query.bool.max_clause_count` elasticsearch.yml setting
     * for more details.
     *
     * @return the amount of clauses to be used in large queries
     */
    @Override
    protected List<Integer> largeInValuesCountData()
    {
        return ImmutableList.of(200, 500, 1000);
    }

    @Test
    public void testWithoutBackpressure()
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        assertQuerySucceeds("SELECT * FROM orders");
        // Check that JMX stats show no sign of backpressure
        assertQueryReturnsEmptyResult(format("SELECT 1 FROM jmx.current.\"%s.client:*name=%s*\" WHERE \"backpressurestats.alltime.count\" > 0", jmxBaseName, catalogName));
        assertQueryReturnsEmptyResult(format("SELECT 1 FROM jmx.current.\"%s.client:*name=%s*\" WHERE \"backpressurestats.alltime.max\" > 0", jmxBaseName, catalogName));
    }

    @Test
    @Override
    public void testSelectAll()
    {
        // List columns explicitly, as there's no defined order in Elasticsearch
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment FROM orders");
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        // The column metadata for the Elasticsearch connector tables are provided
        // based on the column name in alphabetical order.
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("clerk", "varchar", "", "")
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderdate", "timestamp(3)", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("totalprice", "real", "", "")
                .build();
    }

    @Test
    @Override
    public void testPredicateReflectedInExplain()
    {
        // The format of the string representation of what gets shown in the table scan is connector-specific
        // and there's no requirement that the conform to a specific shape or contain certain keywords.

        assertExplain(
                "EXPLAIN SELECT name FROM nation WHERE nationkey = 42",
                "nationkey::bigint",
                "::\\s\\[\\[42\\]\\]");
    }

    @Test
    @Override
    public void testSortItemsReflectedInExplain()
    {
        // The format of the string representation of what gets shown in the table scan is connector-specific
        // and there's no requirement that the conform to a specific shape or contain certain keywords.
        assertExplain(
                "EXPLAIN SELECT name FROM nation ORDER BY nationkey DESC NULLS LAST LIMIT 5",
                "TopNPartial\\[count = 5, orderBy = \\[nationkey DESC");
    }

    @Test
    @Override
    public void testTopNPushdown()
    {
        assertThat(query("SELECT nationkey FROM nation ORDER BY nationkey DESC LIMIT 5"))
                .ordered()
                .matches("VALUES BIGINT '24', BIGINT '23', BIGINT '22', BIGINT '21', BIGINT '20'")
                .isNotFullyPushedDown(TopNNode.class);
        assertExplain(
                "EXPLAIN SELECT nationkey FROM nation ORDER BY nationkey DESC LIMIT 5",
                "TopNPartial\\[count = 5, orderBy = \\[nationkey DESC");
    }

    @Test
    public void testTopNPushdownWithMultipleSortColumnsAndMissingValues()
            throws IOException
    {
        String tableName = "test_topn_sorting_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "sort_key": {
                            "type": "long"
                        },
                        "tie_key": {
                            "type": "long"
                        }
                    }
                }
                """;

        createIndex(tableName, properties);
        try {
            index(tableName, ImmutableMap.<String, Object>builder()
                    .put("id", "1")
                    .put("sort_key", 2)
                    .put("tie_key", 10)
                    .buildOrThrow());
            index(tableName, ImmutableMap.<String, Object>builder()
                    .put("id", "2")
                    .put("tie_key", 90)
                    .buildOrThrow());
            index(tableName, ImmutableMap.<String, Object>builder()
                    .put("id", "3")
                    .put("sort_key", 1)
                    .put("tie_key", 10)
                    .buildOrThrow());
            index(tableName, ImmutableMap.<String, Object>builder()
                    .put("id", "4")
                    .put("sort_key", 1)
                    .put("tie_key", 20)
                    .buildOrThrow());

            String nullsFirstQuery = format("SELECT id, sort_key, tie_key FROM %s ORDER BY sort_key ASC NULLS FIRST, tie_key DESC LIMIT 3", tableName);
            assertThat(query(nullsFirstQuery))
                    .ordered()
                    .matches("VALUES (CAST('2' AS VARCHAR), CAST(NULL AS BIGINT), BIGINT '90'), (CAST('4' AS VARCHAR), BIGINT '1', BIGINT '20'), (CAST('3' AS VARCHAR), BIGINT '1', BIGINT '10')")
                    .isNotFullyPushedDown(TopNNode.class);
            assertExplain(
                    "EXPLAIN " + nullsFirstQuery,
                    "TopNPartial\\[count = 3, orderBy = \\[sort_key ASC NULLS FIRST, tie_key DESC");

            assertThat(query(format("SELECT id, sort_key, tie_key FROM %s ORDER BY sort_key DESC NULLS LAST, tie_key ASC LIMIT 3", tableName)))
                    .ordered()
                    .matches("VALUES (CAST('1' AS VARCHAR), BIGINT '2', BIGINT '10'), (CAST('3' AS VARCHAR), BIGINT '1', BIGINT '10'), (CAST('4' AS VARCHAR), BIGINT '1', BIGINT '20')")
                    .isNotFullyPushedDown(TopNNode.class);
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    @Override
    public void testLimitPushdown()
    {
        assertQuery("SELECT count(*) FROM (SELECT nationkey FROM nation LIMIT 5)", "VALUES 5");
        assertThat(query("SELECT nationkey FROM nation LIMIT 5"))
                .isNotFullyPushedDown(LimitNode.class);
        assertExplain(
                "EXPLAIN SELECT nationkey FROM nation LIMIT 5",
                "LimitPartial\\[count = 5");
    }

    @Test
    public void testCountStarPushdown()
    {
        assertThat(query("SELECT COUNT(*) FROM nation"))
                .matches("VALUES BIGINT '25'")
                .isFullyPushedDown();
        assertThat(query("SELECT COUNT(*), SUM(nationkey) FROM nation"))
                .matches("VALUES (BIGINT '25', BIGINT '300')")
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT regionkey, COUNT(*) FROM nation GROUP BY regionkey"))
                .isFullyPushedDown();

        // Simple COUNT(*)
        assertQuery("SELECT COUNT(*) FROM nation", "SELECT 25");

        // COUNT(*) with WHERE clause
        assertQuery("SELECT COUNT(*) FROM nation WHERE regionkey = 1", "SELECT 5");

        // COUNT(*) with other aggregations (global) - ensure COUNT(*) isn't lost
        assertQuery("SELECT COUNT(*), MIN(nationkey), MAX(nationkey) FROM nation", "VALUES (25, 0, 24)");
        assertQuery("SELECT COUNT(*), SUM(nationkey) FROM nation", "VALUES (25, 300)");

        // COUNT(*) with GROUP BY - this should be pushed down
        assertQuery(
                "SELECT regionkey, COUNT(*) FROM nation GROUP BY regionkey ORDER BY regionkey",
                "VALUES (0, 5), (1, 5), (2, 5), (3, 5), (4, 5)");

        // COUNT(*) with GROUP BY and LIMIT
        assertQuery(
                "SELECT regionkey, COUNT(*) FROM nation GROUP BY regionkey ORDER BY regionkey LIMIT 2",
                "VALUES (0, 5), (1, 5)");

        // COUNT(*) vs COUNT(column) - both should be pushed down
        assertQuery(
                "SELECT regionkey, COUNT(*), COUNT(nationkey) FROM nation GROUP BY regionkey ORDER BY regionkey",
                "VALUES (0, 5, 5), (1, 5, 5), (2, 5, 5), (3, 5, 5), (4, 5, 5)");

        // Multiple aggregations with COUNT(*)
        assertQuery(
                "SELECT regionkey, COUNT(*), MIN(nationkey), MAX(nationkey) FROM nation GROUP BY regionkey ORDER BY regionkey",
                "VALUES (0, 5, 0, 16), (1, 5, 1, 24), (2, 5, 8, 21), (3, 5, 6, 23), (4, 5, 4, 20)");
    }

    @Test
    public void testCountStarWithDoubleSumPushdown()
            throws IOException
    {
        String tableName = "count_star_double_sum_" + randomNameSuffix();
        createIndex(tableName,
                """
                {"properties": {"group_key": {"type": "keyword"}, "value": {"type": "double"}}}
                """);
        try {
            index(tableName, ImmutableMap.of("group_key", "a", "value", 1.25));
            index(tableName, ImmutableMap.of("group_key", "a", "value", 2.5));
            index(tableName, ImmutableMap.of("group_key", "b", "value", 3.0));

            assertThat(query("SELECT COUNT(*), SUM(value) FROM " + tableName))
                    .matches("VALUES (BIGINT '3', DOUBLE '6.75')")
                    .isFullyPushedDown();
            assertThat(query("SELECT group_key, COUNT(*), SUM(value) FROM " + tableName + " GROUP BY group_key"))
                    .matches("VALUES (VARCHAR 'a', BIGINT '2', DOUBLE '3.75'), (VARCHAR 'b', BIGINT '1', DOUBLE '3.0')")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testClosedIndexMetadataWithoutFieldDefaults()
            throws Exception
    {
        String suffix = randomNameSuffix();
        String openIndex = "field_defaults_open_" + suffix;
        String closedIndex = "field_defaults_closed_" + suffix;
        String alias = "field_defaults_alias_" + suffix;
        String mappings =
                """
                {"properties": {"value": {"type": "long"}, "other_keyword": {"type": "keyword"}}}
                """;
        createIndex(openIndex, mappings);
        createIndexWithBody(closedIndex, format(
                """
                {"settings": {"index.mapping.ignore_malformed": true}, "mappings": %s}
                """,
                mappings));
        ElasticsearchClient metadataClient = new ElasticsearchClient(
                new ElasticsearchConfig()
                        .setHosts(List.of(server.getAddress().getHost()))
                        .setPort(server.getAddress().getPort())
                        .setIgnorePublishAddress(true)
                        .setTlsEnabled(true)
                        .setTrustStorePath(new File(getResource("truststore.jks").toURI()))
                        .setTruststorePassword("123456")
                        .setVerifyHostnames(false),
                Optional.empty(),
                Optional.of(new PasswordConfig().setUser(USER).setPassword(PASSWORD)));
        try {
            addAlias(openIndex, alias);
            addAlias(closedIndex, alias);
            assertThat(metadataClient.getIndexMetadata(openIndex).schema().fields())
                    .allMatch(field -> field.supportsPredicates() && field.supportsTopNAndAggregations());
            client.performRequest(new Request("POST", "/" + closedIndex + "/_close"));
            for (String target : List.of(closedIndex, alias)) {
                List<IndexMetadata.Field> fields = metadataClient.getIndexMetadata(target).schema().fields();
                assertThat(fields)
                        .extracting(IndexMetadata.Field::name)
                        .containsExactlyInAnyOrder("value", "other_keyword");
                assertThat(fields)
                        .allMatch(field -> !field.supportsPredicates() && !field.supportsTopNAndAggregations());
            }
        }
        finally {
            metadataClient.close();
            deleteIndex(openIndex);
            deleteIndex(closedIndex);
        }
    }

    @Test
    public void testFieldIgnoreMalformedPreservesSourceErrors()
            throws IOException
    {
        String tableName = "field_ignore_malformed_" + randomNameSuffix();
        createIndex(tableName,
                """
                {"properties": {"value": {"type": "integer", "ignore_malformed": true}}}
                """);
        try {
            index(tableName, ImmutableMap.of("value", 1));
            index(tableName, ImmutableMap.of("value", "n/a"));
            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();
            String expectedError = "Cannot parse value for field 'value' as INTEGER: n/a";
            for (String sql : List.of(
                    "SELECT count(*) FROM " + tableName + " WHERE value IS NOT NULL",
                    "SELECT count(value) FROM " + tableName,
                    "SELECT max(value) FROM " + tableName,
                    "SELECT value FROM " + tableName + " ORDER BY value DESC LIMIT 1")) {
                assertQueryFails(withoutPushdown, sql, expectedError);
                assertQueryFails(sql, expectedError);
            }
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testIndexIgnoreMalformedPreservesSourceErrors()
            throws IOException
    {
        String tableName = "index_ignore_malformed_" + randomNameSuffix();
        createIndexWithBody(tableName,
                """
                {
                    "settings": {"index.mapping.ignore_malformed": true},
                    "mappings": {
                        "properties": {
                            "id": {"type": "long", "ignore_malformed": false},
                            "value": {"type": "long"},
                            "safe_value": {"type": "long", "ignore_malformed": false},
                            "other_keyword": {"type": "keyword"},
                            "row": {"properties": {"value": {"type": "long"}}}
                        }
                    }
                }
                """);
        try {
            index(tableName, ImmutableMap.of("id", 1, "value", 10, "safe_value", 1, "other_keyword", "first", "row", ImmutableMap.of("value", 10)));
            index(tableName, ImmutableMap.of("id", 2, "value", "n/a", "safe_value", 2, "other_keyword", "second", "row", ImmutableMap.of("value", "n/a")));
            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();
            for (String field : List.of("value", "row.value")) {
                String expectedError = "Cannot parse value for field '" + field + "' as BIGINT: n/a";
                for (String sql : List.of(
                        format("SELECT count(*) FROM %s WHERE %s IS NOT NULL", tableName, field),
                        format("SELECT count(%s) FROM %s", field, tableName),
                        format("SELECT id FROM %s WHERE %s IS NOT NULL", tableName, field),
                        format("SELECT id FROM %s ORDER BY %s DESC NULLS LAST LIMIT 1", tableName, field))) {
                    assertQueryFails(withoutPushdown, sql, expectedError);
                    assertQueryFails(sql, expectedError);
                }
            }
            assertThat(query("SELECT count(*) FROM " + tableName))
                    .matches("VALUES BIGINT '2'")
                    .isFullyPushedDown();
            assertThat(query("SELECT safe_value FROM " + tableName + " WHERE safe_value = 1"))
                    .matches("VALUES BIGINT '1'")
                    .isFullyPushedDown();
            assertThat(query("SELECT other_keyword FROM " + tableName + " WHERE other_keyword = 'first'"))
                    .matches("VALUES VARCHAR 'first'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testIndexIgnoreMalformedAcrossAlias()
            throws IOException
    {
        String suffix = randomNameSuffix();
        String firstIndex = "ignore_malformed_alias_a_" + suffix;
        String secondIndex = "ignore_malformed_alias_b_" + suffix;
        String alias = "ignore_malformed_alias_" + suffix;
        String mappings =
                """
                {"properties": {"value": {"type": "long"}, "other_keyword": {"type": "keyword"}}}
                """;
        createIndex(firstIndex, mappings);
        createIndexWithBody(secondIndex, format(
                """
                {"settings": {"index.mapping.ignore_malformed": true}, "mappings": %s}
                """,
                mappings));
        try {
            index(firstIndex, ImmutableMap.of("value", 10, "other_keyword", "first"));
            index(secondIndex, ImmutableMap.of("value", "n/a", "other_keyword", "second"));
            addAlias(firstIndex, alias);
            addAlias(secondIndex, alias);
            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();
            String expectedError = "Cannot parse value for field 'value' as BIGINT: n/a";
            for (String sql : List.of(
                    "SELECT count(*) FROM " + alias + " WHERE value IS NOT NULL",
                    "SELECT count(value) FROM " + alias)) {
                assertQueryFails(withoutPushdown, sql, expectedError);
                assertQueryFails(sql, expectedError);
            }
            assertThat(query("SELECT count(*) FROM " + alias))
                    .matches("VALUES BIGINT '2'")
                    .isFullyPushedDown();
            assertThat(query("SELECT other_keyword FROM " + alias + " WHERE other_keyword = 'first'"))
                    .matches("VALUES VARCHAR 'first'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(firstIndex);
            deleteIndex(secondIndex);
        }
    }

    protected void assertIndexIgnoreMalformedBooleanSemantics(boolean supportsIgnoreMalformed)
            throws IOException
    {
        String tableName = "boolean_index_ignore_malformed_" + randomNameSuffix();
        createIndexWithBody(tableName, format(
                """
                {
                    "settings": {"index.mapping.ignore_malformed": true},
                    "mappings": {
                        "properties": {
                            "value": {"type": "boolean"},
                            "safe_value": {"type": "boolean"%s},
                            "other_keyword": {"type": "keyword"}
                        }
                    }
                }
                """,
                supportsIgnoreMalformed ? ", \"ignore_malformed\": false" : ""));
        try {
            index(tableName, ImmutableMap.of("value", true, "safe_value", true, "other_keyword", "first"));
            if (supportsIgnoreMalformed) {
                index(tableName, ImmutableMap.of("value", "n/a", "safe_value", false, "other_keyword", "second"));
                Session withoutPushdown = Session.builder(getSession())
                        .setSystemProperty("allow_pushdown_into_connectors", "false")
                        .build();
                String expectedError = "Cannot parse value for field 'value' as BOOLEAN: n/a";
                for (String sql : List.of(
                        "SELECT count(*) FROM " + tableName + " WHERE value IS NOT NULL",
                        "SELECT count(value) FROM " + tableName)) {
                    assertQueryFails(withoutPushdown, sql, expectedError);
                    assertQueryFails(sql, expectedError);
                }
            }
            else {
                assertThatThrownBy(() -> index(tableName, ImmutableMap.of("value", "n/a")))
                        .hasMessageContaining("\"status\":400");
                index(tableName, ImmutableMap.of("value", false, "safe_value", false, "other_keyword", "second"));
                assertThat(query("SELECT value FROM " + tableName + " WHERE value"))
                        .matches("VALUES true")
                        .isFullyPushedDown();
            }
            assertThat(query("SELECT safe_value FROM " + tableName + " WHERE safe_value"))
                    .matches("VALUES true")
                    .isFullyPushedDown();
            assertThat(query("SELECT other_keyword FROM " + tableName + " WHERE other_keyword = 'first'"))
                    .matches("VALUES VARCHAR 'first'")
                    .isFullyPushedDown();
            assertThat(query("SELECT count(*) FROM " + tableName))
                    .matches("VALUES BIGINT '2'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testTopNAndAggregationSkipFieldsWithDocValuesDifferentFromSource()
            throws IOException
    {
        String tableName = "doc_values_source_mismatch_" + randomNameSuffix();
        createIndexWithBody(tableName,
                """
                {
                    "settings": {
                        "analysis": {
                            "normalizer": {
                                "lowercase_normalizer": {
                                    "type": "custom",
                                    "filter": ["lowercase"]
                                }
                            }
                        }
                    },
                    "mappings": {
                        "_meta": {
                            "trino": {
                                "raw_keyword": {"asRawJson": true}
                            }
                        },
                        "properties": {
                            "d": {"type": "double", "doc_values": false},
                            "no_doc_values_keyword": {"type": "keyword", "doc_values": false},
                            "limited_keyword": {"type": "keyword", "ignore_above": 3},
                            "normalized_keyword": {"type": "keyword", "normalizer": "lowercase_normalizer"},
                            "null_keyword": {"type": "keyword", "null_value": "unknown"},
                            "raw_keyword": {"type": "keyword"}
                        }
                    }
                }
                """);
        try {
            index(tableName, ImmutableMap.of(
                    "d", 2.5,
                    "no_doc_values_keyword", "a",
                    "limited_keyword", "ab",
                    "normalized_keyword", "Abc",
                    "null_keyword", "present",
                    "raw_keyword", "Abc"));
            index(tableName, ImmutableMap.of(
                    "d", 1.5,
                    "no_doc_values_keyword", "b",
                    "limited_keyword", "abcdef",
                    "normalized_keyword", "abc",
                    "raw_keyword", "abc"));
            Map<String, Object> documentWithNull = new HashMap<>();
            documentWithNull.put("d", 3.0);
            documentWithNull.put("no_doc_values_keyword", "c");
            documentWithNull.put("limited_keyword", "zzzzzz");
            documentWithNull.put("normalized_keyword", "Zed");
            documentWithNull.put("null_keyword", null);
            documentWithNull.put("raw_keyword", "Zed");
            index(tableName, documentWithNull);

            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();

            String doubleTopN = "SELECT d FROM " + tableName + " ORDER BY d DESC LIMIT 1";
            assertThat(query(doubleTopN))
                    .matches("VALUES DOUBLE '3.0'")
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query(withoutPushdown, doubleTopN)).matches("VALUES DOUBLE '3.0'");

            String keywordGrouping = "SELECT no_doc_values_keyword, count(*) FROM " + tableName + " GROUP BY no_doc_values_keyword";
            assertThat(query(keywordGrouping))
                    .matches("VALUES (VARCHAR 'a', BIGINT '1'), (VARCHAR 'b', BIGINT '1'), (VARCHAR 'c', BIGINT '1')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, keywordGrouping))
                    .matches("VALUES (VARCHAR 'a', BIGINT '1'), (VARCHAR 'b', BIGINT '1'), (VARCHAR 'c', BIGINT '1')");

            String keywordCount = "SELECT count(no_doc_values_keyword) FROM " + tableName;
            assertThat(query(keywordCount))
                    .matches("VALUES BIGINT '3'")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, keywordCount)).matches("VALUES BIGINT '3'");

            String limitedCount = "SELECT count(limited_keyword) FROM " + tableName;
            assertThat(query(limitedCount))
                    .matches("VALUES BIGINT '3'")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, limitedCount)).matches("VALUES BIGINT '3'");

            String limitedTopN = "SELECT limited_keyword FROM " + tableName + " ORDER BY limited_keyword DESC LIMIT 1";
            assertThat(query(limitedTopN))
                    .matches("VALUES VARCHAR 'zzzzzz'")
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query(withoutPushdown, limitedTopN)).matches("VALUES VARCHAR 'zzzzzz'");

            String normalizedGrouping = "SELECT normalized_keyword, count(*) FROM " + tableName + " GROUP BY normalized_keyword";
            assertThat(query(normalizedGrouping))
                    .matches("VALUES (VARCHAR 'Abc', BIGINT '1'), (VARCHAR 'abc', BIGINT '1'), (VARCHAR 'Zed', BIGINT '1')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, normalizedGrouping))
                    .matches("VALUES (VARCHAR 'Abc', BIGINT '1'), (VARCHAR 'abc', BIGINT '1'), (VARCHAR 'Zed', BIGINT '1')");

            String normalizedTopN = "SELECT normalized_keyword FROM " + tableName + " ORDER BY normalized_keyword DESC LIMIT 1";
            assertThat(query(normalizedTopN))
                    .matches("VALUES VARCHAR 'abc'")
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query(withoutPushdown, normalizedTopN)).matches("VALUES VARCHAR 'abc'");

            String nullValueCount = "SELECT count(null_keyword) FROM " + tableName;
            assertThat(query(nullValueCount))
                    .matches("VALUES BIGINT '1'")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, nullValueCount)).matches("VALUES BIGINT '1'");

            String rawJsonGrouping = "SELECT raw_keyword, count(*) FROM " + tableName + " GROUP BY raw_keyword";
            assertThat(query(rawJsonGrouping))
                    .matches("VALUES (VARCHAR '\"Abc\"', BIGINT '1'), (VARCHAR '\"abc\"', BIGINT '1'), (VARCHAR '\"Zed\"', BIGINT '1')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, rawJsonGrouping))
                    .matches("VALUES (VARCHAR '\"Abc\"', BIGINT '1'), (VARCHAR '\"abc\"', BIGINT '1'), (VARCHAR '\"Zed\"', BIGINT '1')");

            Map<String, String> predicateQueries = ImmutableMap.<String, String>builder()
                    .put("SELECT normalized_keyword FROM %s WHERE normalized_keyword = 'abc'", "VALUES VARCHAR 'abc'")
                    .put("SELECT normalized_keyword FROM %s WHERE normalized_keyword LIKE 'Ab%%'", "VALUES VARCHAR 'Abc'")
                    .put("SELECT limited_keyword FROM %s WHERE limited_keyword = 'abcdef'", "VALUES VARCHAR 'abcdef'")
                    .put("SELECT limited_keyword FROM %s WHERE limited_keyword IS NULL", "SELECT CAST(NULL AS VARCHAR) WHERE false")
                    .put("SELECT null_keyword FROM %s WHERE null_keyword IS NULL", "VALUES CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)")
                    .put("SELECT null_keyword FROM %s WHERE null_keyword = 'unknown'", "SELECT CAST(NULL AS VARCHAR) WHERE false")
                    .put("SELECT raw_keyword FROM %s WHERE raw_keyword = '\"Abc\"'", "VALUES VARCHAR '\"Abc\"'")
                    .buildOrThrow();
            for (Map.Entry<String, String> predicateQuery : predicateQueries.entrySet()) {
                String sql = format(predicateQuery.getKey(), tableName);
                assertThat(query(sql))
                        .matches(predicateQuery.getValue())
                        .isNotFullyPushedDown(FilterNode.class);
                assertThat(query(withoutPushdown, sql)).matches(predicateQuery.getValue());
            }

            assertThat(query("SELECT no_doc_values_keyword FROM " + tableName + " WHERE no_doc_values_keyword = 'a'"))
                    .matches("VALUES VARCHAR 'a'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testTopNAndAggregationSkipScriptedAndSourceMismatchedFields()
            throws IOException
    {
        Session withoutPushdown = Session.builder(getSession())
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();

        String scriptedTable = "scripted_mapping_safety_" + randomNameSuffix();
        createIndexWithBody(scriptedTable,
                """
                {
                    "mappings": {
                        "properties": {
                            "base": {"type": "long"},
                            "scripted_long": {"type": "long", "script": {"source": "emit(doc['base'].value)"}},
                            "scripted_double": {"type": "double", "script": {"source": "emit(doc['base'].value * 1.5)"}}
                        }
                    }
                }
                """);
        try {
            index(scriptedTable, ImmutableMap.of("base", 10));
            index(scriptedTable, ImmutableMap.of("base", 20));

            String scriptedLongCount = "SELECT count(scripted_long) FROM " + scriptedTable;
            assertThat(query(scriptedLongCount))
                    .matches("VALUES BIGINT '0'")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, scriptedLongCount)).matches("VALUES BIGINT '0'");

            String scriptedLongGrouping = "SELECT scripted_long, count(*) FROM " + scriptedTable + " GROUP BY scripted_long";
            assertThat(query(scriptedLongGrouping))
                    .matches("VALUES (CAST(NULL AS BIGINT), BIGINT '2')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, scriptedLongGrouping)).matches("VALUES (CAST(NULL AS BIGINT), BIGINT '2')");

            String scriptedDoubleCount = "SELECT count(scripted_double) FROM " + scriptedTable;
            assertThat(query(scriptedDoubleCount))
                    .matches("VALUES BIGINT '0'")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, scriptedDoubleCount)).matches("VALUES BIGINT '0'");

            String scriptedDoubleGrouping = "SELECT scripted_double, count(*) FROM " + scriptedTable + " GROUP BY scripted_double";
            assertThat(query(scriptedDoubleGrouping))
                    .matches("VALUES (CAST(NULL AS DOUBLE), BIGINT '2')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, scriptedDoubleGrouping)).matches("VALUES (CAST(NULL AS DOUBLE), BIGINT '2')");

            String scriptedPredicate = "SELECT base FROM " + scriptedTable + " WHERE scripted_long IS NULL AND scripted_double IS NULL";
            assertThat(query(scriptedPredicate))
                    .matches("VALUES BIGINT '10', BIGINT '20'")
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(withoutPushdown, scriptedPredicate)).matches("VALUES BIGINT '10', BIGINT '20'");

            assertThat(query("SELECT count(*) FROM " + scriptedTable))
                    .matches("VALUES BIGINT '2'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(scriptedTable);
        }

        String copyToTable = "copy_to_mapping_safety_" + randomNameSuffix();
        createIndexWithBody(copyToTable,
                """
                {
                    "mappings": {
                        "properties": {
                            "source_keyword": {"type": "keyword", "copy_to": ["target_keyword", "row.target_keyword"]},
                            "target_keyword": {"type": "keyword"},
                            "other_keyword": {"type": "keyword"},
                            "row": {
                                "properties": {
                                    "target_keyword": {"type": "keyword"},
                                    "other_keyword": {"type": "keyword"}
                                }
                            }
                        }
                    }
                }
                """);
        try {
            index(copyToTable, ImmutableMap.of("source_keyword", "first", "other_keyword", "first", "row", ImmutableMap.of("other_keyword", "first")));
            index(copyToTable, ImmutableMap.of("source_keyword", "second", "other_keyword", "second", "row", ImmutableMap.of("other_keyword", "second")));

            for (String field : List.of("source_keyword", "other_keyword", "row.other_keyword")) {
                assertKeywordPushdown(copyToTable, field);
            }

            String targetCount = "SELECT count(target_keyword) FROM " + copyToTable;
            assertThat(query(targetCount))
                    .matches("VALUES BIGINT '0'")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, targetCount)).matches("VALUES BIGINT '0'");

            String targetGrouping = "SELECT target_keyword, count(*) FROM " + copyToTable + " GROUP BY target_keyword";
            assertThat(query(targetGrouping))
                    .matches("VALUES (CAST(NULL AS VARCHAR), BIGINT '2')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, targetGrouping)).matches("VALUES (CAST(NULL AS VARCHAR), BIGINT '2')");

            assertThat(query("SELECT row.target_keyword, count(*) FROM " + copyToTable + " GROUP BY row.target_keyword"))
                    .matches("VALUES (CAST(NULL AS VARCHAR), BIGINT '2')")
                    .isNotFullyPushedDown(AggregationNode.class);

            String copiedPredicate = "SELECT source_keyword FROM " + copyToTable + " WHERE target_keyword IS NULL";
            assertThat(query(copiedPredicate))
                    .matches("VALUES VARCHAR 'first', VARCHAR 'second'")
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(withoutPushdown, copiedPredicate)).matches("VALUES VARCHAR 'first', VARCHAR 'second'");

            String copiedLike = "SELECT source_keyword FROM " + copyToTable + " WHERE target_keyword LIKE 'fi%'";
            assertThat(query(copiedLike))
                    .matches("SELECT CAST(NULL AS VARCHAR) WHERE false")
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(withoutPushdown, copiedLike)).matches("SELECT CAST(NULL AS VARCHAR) WHERE false");

            assertThat(query("SELECT count(*) FROM " + copyToTable))
                    .matches("VALUES BIGINT '2'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(copyToTable);
        }

        String sourceExcludesTable = "source_excludes_mapping_safety_" + randomNameSuffix();
        createIndexWithBody(sourceExcludesTable,
                """
                {
                    "mappings": {
                        "_source": {"excludes": ["excluded_keyword"]},
                        "properties": {
                            "excluded_keyword": {"type": "keyword"},
                            "other_keyword": {"type": "keyword"}
                        }
                    }
                }
                """);
        try {
            index(sourceExcludesTable, ImmutableMap.of("excluded_keyword", "first", "other_keyword", "first"));
            index(sourceExcludesTable, ImmutableMap.of("excluded_keyword", "second", "other_keyword", "second"));

            assertKeywordPushdown(sourceExcludesTable, "other_keyword");

            String excludedCount = "SELECT count(excluded_keyword) FROM " + sourceExcludesTable;
            assertThat(query(excludedCount))
                    .matches("VALUES BIGINT '0'")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, excludedCount)).matches("VALUES BIGINT '0'");

            String excludedGrouping = "SELECT excluded_keyword, count(*) FROM " + sourceExcludesTable + " GROUP BY excluded_keyword";
            assertThat(query(excludedGrouping))
                    .matches("VALUES (CAST(NULL AS VARCHAR), BIGINT '2')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, excludedGrouping)).matches("VALUES (CAST(NULL AS VARCHAR), BIGINT '2')");

            String excludedPredicate = "SELECT excluded_keyword FROM " + sourceExcludesTable + " WHERE excluded_keyword IS NULL";
            assertThat(query(excludedPredicate))
                    .matches("VALUES CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)")
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(withoutPushdown, excludedPredicate)).matches("VALUES CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)");

            assertThat(query("SELECT count(*) FROM " + sourceExcludesTable))
                    .matches("VALUES BIGINT '2'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(sourceExcludesTable);
        }
    }

    @Test
    public void testSourceFiltersOnlyDisableAffectedFields()
            throws IOException
    {
        String tableName = "source_filter_mapping_safety_" + randomNameSuffix();
        createIndexWithBody(tableName,
                """
                {
                    "mappings": {
                        "_source": {
                            "includes": ["kept_*", "included_object", "*.kept", "selected", "excluded_object"],
                            "excludes": ["kept_excluded", "included_object.excluded", "selected.excluded*", "excluded_object"]
                        },
                        "properties": {
                            "kept_keyword": {"type": "keyword"},
                            "kept_excluded": {"type": "keyword"},
                            "dropped_keyword": {"type": "keyword"},
                            "included_object": {
                                "properties": {
                                    "kept": {"type": "keyword"},
                                    "excluded": {"type": "keyword"}
                                }
                            },
                            "wildcard_object": {
                                "properties": {
                                    "kept": {"type": "keyword"},
                                    "filtered": {"type": "keyword"}
                                }
                            },
                            "selected": {
                                "properties": {
                                    "kept": {"type": "keyword"},
                                    "excluded_keyword": {"type": "keyword"}
                                }
                            },
                            "excluded_object": {
                                "properties": {"kept": {"type": "keyword"}}
                            }
                        }
                    }
                }
                """);
        try {
            for (String value : List.of("first", "second")) {
                index(tableName, ImmutableMap.<String, Object>builder()
                        .put("kept_keyword", value)
                        .put("kept_excluded", value)
                        .put("dropped_keyword", value)
                        .put("included_object", ImmutableMap.of("kept", value, "excluded", value))
                        .put("wildcard_object", ImmutableMap.of("kept", value, "filtered", value))
                        .put("selected", ImmutableMap.of("kept", value, "excluded_keyword", value))
                        .put("excluded_object", ImmutableMap.of("kept", value))
                        .buildOrThrow());
            }

            for (String field : List.of("kept_keyword", "included_object.kept", "wildcard_object.kept", "selected.kept")) {
                assertKeywordPushdown(tableName, field);
            }
            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();
            for (String field : List.of("kept_excluded", "dropped_keyword", "included_object.excluded", "wildcard_object.filtered", "selected.excluded_keyword", "excluded_object.kept")) {
                String predicate = format("SELECT %s FROM %s WHERE %s IS NULL", field, tableName, field);
                assertThat(query(predicate))
                        .matches("VALUES CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)")
                        .isNotFullyPushedDown(FilterNode.class);
                assertThat(query(withoutPushdown, predicate)).matches("VALUES CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)");

                String grouping = format("SELECT %s, count(*) FROM %s GROUP BY %s", field, tableName, field);
                assertThat(query(grouping))
                        .matches("VALUES (CAST(NULL AS VARCHAR), BIGINT '2')")
                        .isNotFullyPushedDown(AggregationNode.class);
                assertThat(query(withoutPushdown, grouping)).matches("VALUES (CAST(NULL AS VARCHAR), BIGINT '2')");
            }
        }
        finally {
            deleteIndex(tableName);
        }
    }

    private void assertKeywordPushdown(String tableName, String field)
    {
        Session withoutPushdown = Session.builder(getSession())
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();
        String predicate = format("SELECT %s FROM %s WHERE %s = 'first'", field, tableName, field);
        assertThat(query(predicate))
                .matches("VALUES VARCHAR 'first'")
                .isFullyPushedDown();
        assertThat(query(withoutPushdown, predicate)).matches("VALUES VARCHAR 'first'");

        String grouping = format("SELECT %s, count(*) FROM %s GROUP BY %s", field, tableName, field);
        assertThat(query(grouping))
                .matches("VALUES (VARCHAR 'first', BIGINT '1'), (VARCHAR 'second', BIGINT '1')")
                .isFullyPushedDown();
        assertThat(query(withoutPushdown, grouping)).matches("VALUES (VARCHAR 'first', BIGINT '1'), (VARCHAR 'second', BIGINT '1')");

        String topN = format("SELECT %s FROM %s ORDER BY %s DESC NULLS LAST LIMIT 1", field, tableName, field);
        assertThat(query(topN))
                .matches("VALUES VARCHAR 'second'")
                .isNotFullyPushedDown(TopNNode.class);
        assertThat(query(withoutPushdown, topN)).matches("VALUES VARCHAR 'second'");
        assertExplain("EXPLAIN " + topN, "topNSortItems=\\[TopNSortItem\\[field=" + field.replace(".", "\\.") + ", order=DESC NULLS LAST");
    }

    @Test
    public void testDisabledSourceDoesNotBypassSourceReadFailure()
            throws IOException
    {
        String tableName = "disabled_source_mapping_safety_" + randomNameSuffix();
        createIndexWithBody(tableName,
                """
                {
                    "mappings": {
                        "_source": {"enabled": false},
                        "properties": {
                            "key": {"type": "keyword"},
                            "row": {"properties": {"key": {"type": "keyword"}}}
                        }
                    }
                }
                """);
        try {
            index(tableName, ImmutableMap.of("key", "first", "row", ImmutableMap.of("key", "first")));
            index(tableName, ImmutableMap.of("key", "second", "row", ImmutableMap.of("key", "second")));
            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();
            String expectedError = "unable to fetch fields from _source field: _source is disabled in the mappings for index \\[" + tableName + "\\]";
            for (String field : List.of("key", "row.key")) {
                String predicate = format("SELECT %s FROM %s WHERE %s IS NULL", field, tableName, field);
                assertQueryFails(predicate, expectedError);
                assertQueryFails(withoutPushdown, predicate, expectedError);
                String count = format("SELECT count(%s) FROM %s", field, tableName);
                assertQueryFails(count, expectedError);
                assertQueryFails(withoutPushdown, count, expectedError);
            }
            assertThat(query("SELECT count(*) FROM " + tableName))
                    .matches("VALUES BIGINT '2'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testRuntimeFieldsPreserveSourceSemantics()
            throws IOException
    {
        String tableName = "runtime_mapping_safety_" + randomNameSuffix();
        createIndexWithBody(tableName,
                """
                {
                    "mappings": {
                        "properties": {
                            "id": {"type": "integer"},
                            "source_key": {"type": "keyword"},
                            "quantity": {"type": "integer"},
                            "metric_value": {"type": "double"}
                        },
                        "runtime": {
                            "source_key": {"type": "keyword", "script": {"source": "emit('runtime')"}},
                            "quantity": {
                                "type": "long",
                                "script": {"source": "emit(params._source.containsKey('quantity') ? -((Number) params._source['quantity']).longValue() : 100L)"}
                            },
                            "metric_value": {
                                "type": "double",
                                "script": {"source": "emit(params._source.containsKey('metric_value') ? 100.0 - ((Number) params._source['metric_value']).doubleValue() : 100.0)"}
                            }
                        }
                    }
                }
                """);
        try {
            index(tableName, ImmutableMap.of("id", 1, "source_key", "alpha", "quantity", 10, "metric_value", 1.5));
            index(tableName, ImmutableMap.of("id", 2, "source_key", "zulu", "quantity", 20, "metric_value", 2.5));
            index(tableName, ImmutableMap.of("id", 3));

            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();

            Map<String, String> predicates = ImmutableMap.<String, String>builder()
                    .put("source_key = 'alpha'", "VALUES INTEGER '1'")
                    .put("source_key LIKE 'z%'", "VALUES INTEGER '2'")
                    .put("quantity > 15", "VALUES INTEGER '2'")
                    .put("metric_value IS NULL", "VALUES INTEGER '3'")
                    .buildOrThrow();
            for (Map.Entry<String, String> predicate : predicates.entrySet()) {
                String sql = "SELECT id FROM " + tableName + " WHERE " + predicate.getKey();
                assertThat(query(sql))
                        .matches(predicate.getValue())
                        .isNotFullyPushedDown(FilterNode.class);
                assertThat(query(withoutPushdown, sql)).matches(predicate.getValue());
            }

            Map<String, String> topNQueries = ImmutableMap.of(
                    "source_key", "VALUES VARCHAR 'zulu'",
                    "quantity", "VALUES INTEGER '20'");
            for (Map.Entry<String, String> topN : topNQueries.entrySet()) {
                String sql = format("SELECT %s FROM %s ORDER BY %s DESC NULLS LAST LIMIT 1", topN.getKey(), tableName, topN.getKey());
                assertThat(query(sql))
                        .matches(topN.getValue())
                        .isNotFullyPushedDown(TopNNode.class);
                assertThat(query(withoutPushdown, sql)).matches(topN.getValue());
            }

            String grouping = "SELECT source_key, count(*) FROM " + tableName + " GROUP BY source_key";
            String groupedRows = "VALUES (VARCHAR 'alpha', BIGINT '1'), (VARCHAR 'zulu', BIGINT '1'), (CAST(NULL AS VARCHAR), BIGINT '1')";
            assertThat(query(grouping))
                    .matches(groupedRows)
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, grouping)).matches(groupedRows);

            String metrics = "SELECT count(metric_value), sum(metric_value), avg(metric_value), min(metric_value), max(metric_value) FROM " + tableName;
            String metricValues = "VALUES (BIGINT '2', DOUBLE '4.0', DOUBLE '2.0', DOUBLE '1.5', DOUBLE '2.5')";
            assertThat(query(metrics))
                    .matches(metricValues)
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, metrics)).matches(metricValues);

            assertThat(query("SELECT count(*) FROM " + tableName))
                    .matches("VALUES BIGINT '3'")
                    .isFullyPushedDown();

            assertThat(query("SELECT id FROM " + tableName + " WHERE id = 1"))
                    .matches("VALUES INTEGER '1'")
                    .isFullyPushedDown();
            assertThat(query("SELECT id, count(*) FROM " + tableName + " GROUP BY id"))
                    .matches("VALUES (INTEGER '1', BIGINT '1'), (INTEGER '2', BIGINT '1'), (INTEGER '3', BIGINT '1')")
                    .isFullyPushedDown();
            String safeTopN = "SELECT id FROM " + tableName + " ORDER BY id DESC LIMIT 1";
            assertThat(query(safeTopN)).matches("VALUES INTEGER '3'");
            assertExplain("EXPLAIN " + safeTopN, "topNSortItems=\\[TopNSortItem\\[field=id, order=DESC NULLS LAST");
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testTopNAndAggregationSkipNestedOrDisabledFieldsAndPreserveProjectedMapping()
            throws IOException
    {
        String tableName = "nested_aggregation_mapping_safety_" + randomNameSuffix();
        createIndexWithBody(tableName,
                """
                {
                    "settings": {
                        "analysis": {
                            "normalizer": {
                                "lowercase_normalizer": {
                                    "type": "custom",
                                    "filter": ["lowercase"]
                                }
                            }
                        }
                    },
                    "mappings": {
                        "properties": {
                            "row": {
                                "properties": {
                                    "a_unsupported": {"type": "half_float"},
                                    "safe_keyword": {"type": "keyword"},
                                    "unsafe_keyword": {"type": "keyword", "normalizer": "lowercase_normalizer"}
                                }
                            },
                            "nested_object": {
                                "type": "nested",
                                "properties": {"key": {"type": "keyword"}}
                            },
                            "disabled_object": {
                                "type": "object",
                                "enabled": false,
                                "properties": {"key": {"type": "keyword"}}
                            }
                        }
                    }
                }
                """);
        try {
            index(tableName, ImmutableMap.of(
                    "row", ImmutableMap.of("a_unsupported", 1.0, "safe_keyword", "safe", "unsafe_keyword", "Abc"),
                    "nested_object", ImmutableMap.of("key", "a"),
                    "disabled_object", ImmutableMap.of("key", "a")));
            index(tableName, ImmutableMap.of(
                    "row", ImmutableMap.of("a_unsupported", 2.0, "safe_keyword", "safe", "unsafe_keyword", "abc"),
                    "nested_object", ImmutableMap.of("key", "b"),
                    "disabled_object", ImmutableMap.of("key", "b")));
            index(tableName, ImmutableMap.of(
                    "row", ImmutableMap.of("a_unsupported", 3.0, "safe_keyword", "safe", "unsafe_keyword", "Zed"),
                    "nested_object", ImmutableMap.of("key", "a"),
                    "disabled_object", ImmutableMap.of("key", "a")));

            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();

            String projectedGrouping = "SELECT row.unsafe_keyword, count(*) FROM " + tableName + " GROUP BY row.unsafe_keyword";
            assertThat(query(projectedGrouping))
                    .matches("VALUES (VARCHAR 'Abc', BIGINT '1'), (VARCHAR 'abc', BIGINT '1'), (VARCHAR 'Zed', BIGINT '1')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, projectedGrouping))
                    .matches("VALUES (VARCHAR 'Abc', BIGINT '1'), (VARCHAR 'abc', BIGINT '1'), (VARCHAR 'Zed', BIGINT '1')");

            String projectedTopN = "SELECT row.unsafe_keyword FROM " + tableName + " ORDER BY row.unsafe_keyword DESC LIMIT 1";
            assertThat(query(projectedTopN))
                    .matches("VALUES VARCHAR 'abc'")
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query(withoutPushdown, projectedTopN)).matches("VALUES VARCHAR 'abc'");

            String nestedGrouping = "SELECT nested_object.key, count(*) FROM " + tableName + " GROUP BY nested_object.key";
            assertThat(query(nestedGrouping))
                    .matches("VALUES (VARCHAR 'a', BIGINT '2'), (VARCHAR 'b', BIGINT '1')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, nestedGrouping))
                    .matches("VALUES (VARCHAR 'a', BIGINT '2'), (VARCHAR 'b', BIGINT '1')");

            String nestedTopN = "SELECT nested_object.key FROM " + tableName + " ORDER BY nested_object.key DESC LIMIT 1";
            assertThat(query(nestedTopN))
                    .matches("VALUES VARCHAR 'b'")
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query(withoutPushdown, nestedTopN)).matches("VALUES VARCHAR 'b'");

            String disabledGrouping = "SELECT disabled_object.key, count(*) FROM " + tableName + " GROUP BY disabled_object.key";
            assertThat(query(disabledGrouping))
                    .matches("VALUES (VARCHAR 'a', BIGINT '2'), (VARCHAR 'b', BIGINT '1')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, disabledGrouping))
                    .matches("VALUES (VARCHAR 'a', BIGINT '2'), (VARCHAR 'b', BIGINT '1')");

            String disabledTopN = "SELECT disabled_object.key FROM " + tableName + " ORDER BY disabled_object.key DESC LIMIT 1";
            assertThat(query(disabledTopN))
                    .matches("VALUES VARCHAR 'b'")
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query(withoutPushdown, disabledTopN)).matches("VALUES VARCHAR 'b'");

            for (String predicate : List.of("row.unsafe_keyword = 'abc'", "row.unsafe_keyword LIKE 'ab%'", "nested_object.key = 'b'", "disabled_object.key = 'b'")) {
                String predicateQuery = "SELECT row.unsafe_keyword FROM " + tableName + " WHERE " + predicate;
                assertThat(query(predicateQuery))
                        .matches("VALUES VARCHAR 'abc'")
                        .isNotFullyPushedDown(FilterNode.class);
                assertThat(query(withoutPushdown, predicateQuery)).matches("VALUES VARCHAR 'abc'");
            }

            assertThat(query("SELECT row.safe_keyword FROM " + tableName + " WHERE row.safe_keyword = 'safe'"))
                    .matches("VALUES VARCHAR 'safe', VARCHAR 'safe', VARCHAR 'safe'")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testTopNAndAggregationRequireCompatibleMappingsAcrossAlias()
            throws IOException
    {
        String suffix = randomNameSuffix();
        String firstIndex = "mapping_alias_a_" + suffix;
        String secondIndex = "mapping_alias_b_" + suffix;
        String alias = "mapping_alias_" + suffix;
        createIndex(firstIndex,
                """
                {
                    "dynamic": false,
                    "properties": {
                        "key": {"type": "keyword"},
                        "first_only": {"type": "keyword"},
                        "copy_source": {"type": "keyword", "copy_to": "key"},
                        "other_keyword": {"type": "keyword"}
                    }
                }
                """);
        createIndexWithBody(secondIndex,
                """
                {
                    "settings": {
                        "analysis": {
                            "normalizer": {
                                "lowercase_normalizer": {
                                    "type": "custom",
                                    "filter": ["lowercase"]
                                }
                            }
                        }
                    },
                    "mappings": {
                        "dynamic": false,
                        "properties": {
                            "key": {"type": "keyword", "normalizer": "lowercase_normalizer"},
                            "second_only": {"type": "keyword"},
                            "other_keyword": {"type": "keyword"}
                        }
                    }
                }
                """);
        try {
            index(firstIndex, ImmutableMap.of("key", "abc", "first_only", "x", "second_only", "x", "copy_source", "copied", "other_keyword", "first"));
            index(secondIndex, ImmutableMap.of("key", "Zed", "first_only", "zzz", "second_only", "zzz", "other_keyword", "second"));
            addAlias(firstIndex, alias);
            addAlias(secondIndex, alias);

            assertKeywordPushdown(alias, "other_keyword");

            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();

            assertThat(query("SELECT count(*) FROM " + alias))
                    .matches("VALUES BIGINT '2'")
                    .isFullyPushedDown();

            String grouping = "SELECT key, count(*) FROM " + alias + " GROUP BY key";
            assertThat(query(grouping))
                    .matches("VALUES (VARCHAR 'abc', BIGINT '1'), (VARCHAR 'Zed', BIGINT '1')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(withoutPushdown, grouping))
                    .matches("VALUES (VARCHAR 'abc', BIGINT '1'), (VARCHAR 'Zed', BIGINT '1')");

            String exclusiveColumn = computeActual("SHOW COLUMNS FROM " + alias).getMaterializedRows().stream()
                    .map(row -> (String) row.getField(0))
                    .filter(name -> name.equals("first_only") || name.equals("second_only"))
                    .findFirst()
                    .orElseThrow();
            String topN = format("SELECT %s FROM %s ORDER BY %s DESC NULLS LAST LIMIT 1", exclusiveColumn, alias, exclusiveColumn);
            assertThat(query(topN))
                    .matches("VALUES VARCHAR 'zzz'")
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query(withoutPushdown, topN)).matches("VALUES VARCHAR 'zzz'");

            String aliasPredicate = "SELECT key FROM " + alias + " WHERE key = 'zed'";
            assertThat(query(aliasPredicate))
                    .matches("SELECT CAST(NULL AS VARCHAR) WHERE false")
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query(withoutPushdown, aliasPredicate)).matches("SELECT CAST(NULL AS VARCHAR) WHERE false");
        }
        finally {
            deleteIndex(firstIndex);
            deleteIndex(secondIndex);
        }
    }

    @Test
    public void testAggregationPaginationSearchCount()
            throws Exception
    {
        String tableName = "aggregation_page_search_count_" + randomNameSuffix();
        createIndexWithBody(tableName,
                """
                {
                    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                    "mappings": {"properties": {"value": {"type": "integer"}}}
                }
                """);
        try {
            for (int key = 0; key < 3; key++) {
                index(tableName, ImmutableMap.of("value", key));
            }
            try (QueryAssertions assertions = new QueryAssertions(createAdHocQueryRunner(Map.of("elasticsearch.aggregation-page-size", "4")))) {
                for (int keyCount = 3; keyCount <= 5; keyCount++) {
                    if (keyCount > 3) {
                        index(tableName, ImmutableMap.of("value", keyCount - 1));
                    }
                    long queriesBefore = shardQueryCount(List.of(tableName));
                    assertThat(assertions.query("SELECT value, count(*) FROM " + tableName + " GROUP BY value"))
                            .matches("SELECT CAST(x AS integer), BIGINT '1' FROM UNNEST(sequence(0, %s)) t(x)".formatted(keyCount - 1))
                            .skipResultsCorrectnessCheckForPushdown()
                            .isFullyPushedDown();
                    long expectedSearches = keyCount == 3 ? 1 : 2;
                    assertThat(shardQueryCount(List.of(tableName)) - queriesBefore)
                            .as("searches for %s keys with aggregation page size 4", keyCount)
                            .isEqualTo(expectedSearches);
                }
            }
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testAggregationPaginationWithSmallPageSize()
            throws Exception
    {
        try (QueryAssertions assertions = new QueryAssertions(createAdHocQueryRunner(Map.of("elasticsearch.aggregation-page-size", "2")))) {
            assertThat(assertions.query("SELECT regionkey, nationkey, COUNT(*) FROM nation GROUP BY regionkey, nationkey"))
                    .matches("SELECT regionkey, nationkey, BIGINT '1' FROM nation")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testCountStarWithMultipleGroupingColumns()
    {
        // COUNT(*) with multiple GROUP BY columns
        assertQuery(
                "SELECT regionkey, nationkey % 2 as parity, COUNT(*) FROM nation GROUP BY regionkey, nationkey % 2 ORDER BY regionkey, parity",
                "VALUES " +
                        "(0, 0, 3), (0, 1, 2), " +
                        "(1, 0, 2), (1, 1, 3), " +
                        "(2, 0, 3), (2, 1, 2), " +
                        "(3, 0, 2), (3, 1, 3), " +
                        "(4, 0, 3), (4, 1, 2)");
    }

    @Test
    public void testCountStarWithHaving()
    {
        // COUNT(*) with GROUP BY and HAVING
        assertQuery(
                "SELECT regionkey, COUNT(*) as cnt FROM nation GROUP BY regionkey HAVING COUNT(*) > 4 ORDER BY regionkey",
                "VALUES (0, 5), (1, 5), (2, 5), (3, 5), (4, 5)");
    }

    @Test
    public void testAggregationOnTextFieldsFallsBack()
    {
        assertThat(query("SELECT MIN(name), MAX(name) FROM nation"))
                .skippingTypesCheck()
                .matches("VALUES ('ALGERIA', 'VIETNAM')")
                .isNotFullyPushedDown(AggregationNode.class);

        // COUNT on text field
        assertQuery("SELECT COUNT(name) FROM nation", "VALUES (25)");

        // MIN/MAX on text field
        assertQuery(
                "SELECT regionkey, MIN(name), MAX(name) FROM nation GROUP BY regionkey ORDER BY regionkey LIMIT 2",
                "VALUES (0, 'ALGERIA', 'MOZAMBIQUE'), (1, 'ARGENTINA', 'UNITED STATES')");

        // COUNT(*) and COUNT(text) together
        assertQuery(
                "SELECT regionkey, COUNT(*), COUNT(name) FROM nation GROUP BY regionkey ORDER BY regionkey LIMIT 2",
                "VALUES (0, 5, 5), (1, 5, 5)");
    }

    @Test
    public void testCountStarOnDistinctPushdown()
            throws IOException
    {
        String tableName = "count_star_distinct_" + randomNameSuffix();
        createIndex(tableName,
                """
                {"properties": {"k": {"type": "keyword"}}}
                """);
        try {
            index(tableName, ImmutableMap.of("k", "a"));
            index(tableName, ImmutableMap.of("k", "a"));
            index(tableName, ImmutableMap.of("k", "b"));

            assertThat(query("SELECT count(*) FROM (SELECT DISTINCT k FROM " + tableName + ")"))
                    .matches("VALUES BIGINT '2'");
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testMinMaxOnNonNumericColumns()
            throws IOException
    {
        String tableName = "min_max_non_numeric_" + randomNameSuffix();
        createIndex(tableName,
                """
                {"properties": {"k": {"type": "keyword"}, "b": {"type": "boolean"}}}
                """);
        try {
            index(tableName, ImmutableMap.of("k", "a", "b", true));
            index(tableName, ImmutableMap.of("k", "b", "b", false));

            assertThat(query("SELECT min(k), max(k), min(b), max(b) FROM " + tableName))
                    .matches("VALUES (VARCHAR 'a', VARCHAR 'b', false, true)")
                    .isNotFullyPushedDown(AggregationNode.class);
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testBigintMinMaxAndSumAreNotPushedDown()
            throws IOException
    {
        String tableName = "bigint_aggregation_precision_" + randomNameSuffix();
        createIndex(tableName,
                """
                {"properties": {"kind": {"type": "keyword"}, "value": {"type": "long"}}}
                """);
        try {
            index(tableName, ImmutableMap.of("kind", "precision", "value", 9_007_199_254_740_993L));
            index(tableName, ImmutableMap.of("kind", "precision", "value", 9_007_199_254_740_994L));
            index(tableName, ImmutableMap.of("kind", "overflow", "value", Long.MAX_VALUE));
            index(tableName, ImmutableMap.of("kind", "overflow", "value", 1L));

            assertThat(query("SELECT MIN(value), MAX(value), SUM(value) FROM " + tableName + " WHERE kind = 'precision'"))
                    .matches("VALUES (BIGINT '9007199254740993', BIGINT '9007199254740994', BIGINT '18014398509481987')")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertQueryFails("SELECT SUM(value) FROM " + tableName + " WHERE kind = 'overflow'", ".*bigint addition overflow.*");
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testAggregationWithMissingGroupingKeys()
            throws IOException
    {
        String tableName = "test_aggregation_missing_grouping_keys_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                    "properties": {
                        "group_key": {
                            "type": "keyword"
                        },
                        "value": {
                            "type": "double"
                        }
                    }
                }
                """;

        createIndex(tableName, properties);
        try {
            index(tableName, ImmutableMap.<String, Object>builder()
                    .put("group_key", "a")
                    .put("value", 10.0)
                    .buildOrThrow());
            index(tableName, ImmutableMap.<String, Object>builder()
                    .put("group_key", "a")
                    .put("value", 20.0)
                    .buildOrThrow());
            index(tableName, ImmutableMap.<String, Object>builder()
                    .put("group_key", "b")
                    .put("value", 5.0)
                    .buildOrThrow());
            index(tableName, ImmutableMap.of("value", 7));
            index(tableName, ImmutableMap.of("value", 8));

            assertThat(query("SELECT group_key, COUNT(*), SUM(value) FROM " + tableName + " GROUP BY group_key"))
                    .matches("VALUES (CAST(NULL AS VARCHAR), BIGINT '2', DOUBLE '15.0'), (CAST('a' AS VARCHAR), BIGINT '2', DOUBLE '30.0'), (CAST('b' AS VARCHAR), BIGINT '1', DOUBLE '5.0')")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testMixedAggregationsWithEmptyInput()
            throws IOException
    {
        String tableName = "mixed_aggregations_empty_input_" + randomNameSuffix();
        createIndex(tableName,
                """
                {"properties": {"value": {"type": "double"}}}
                """);
        try {
            index(tableName, ImmutableMap.of("value", 1.0));

            assertThat(query("SELECT COUNT(*), COUNT(value), SUM(value), AVG(value), MIN(value), MAX(value) FROM " + tableName + " WHERE value < -1"))
                    .matches("VALUES (BIGINT '0', BIGINT '0', CAST(NULL AS double), CAST(NULL AS double), CAST(NULL AS double), CAST(NULL AS double))")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testMixedAggregationsWithMissingValues()
            throws IOException
    {
        String tableName = "test_aggregation_missing_values_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                        {
                            "properties": {
                                "kind": {
                                    "type": "keyword"
                                },
                                "value": {
                                    "type": "double"
                                }
                            }
                        }
                        """;

        createIndex(tableName, properties);
        try {
            index(tableName, ImmutableMap.<String, Object>builder()
                    .put("kind", "with_value")
                    .put("value", 10.0)
                    .buildOrThrow());
            index(tableName, ImmutableMap.<String, Object>builder()
                    .put("kind", "with_value")
                    .put("value", 20.0)
                    .buildOrThrow());
            index(tableName, ImmutableMap.<String, Object>builder()
                    .put("kind", "without_value")
                    .buildOrThrow());
            index(tableName, ImmutableMap.<String, Object>builder()
                    .put("kind", "without_value")
                    .buildOrThrow());

            assertThat(query("SELECT COUNT(*), COUNT(value), SUM(value), AVG(value), MIN(value), MAX(value) FROM " + tableName + " WHERE kind = 'without_value'"))
                    .matches("VALUES (BIGINT '2', BIGINT '0', CAST(NULL AS double), CAST(NULL AS double), CAST(NULL AS double), CAST(NULL AS double))")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo(format("CREATE TABLE %s.tpch.orders (\n", catalogName) +
                        "   clerk varchar,\n" +
                        "   comment varchar,\n" +
                        "   custkey bigint,\n" +
                        "   orderdate timestamp(3),\n" +
                        "   orderkey bigint,\n" +
                        "   orderpriority varchar,\n" +
                        "   orderstatus varchar,\n" +
                        "   shippriority bigint,\n" +
                        "   totalprice real\n" +
                        ")");
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).result().matches(getDescribeOrdersResult());
    }

    private QueryRunner createAdHocQueryRunner(Map<String, String> connectorProperties)
            throws Exception
    {
        return ElasticsearchQueryRunner.builder(server)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("jmx.base-name", randomNameSuffix())
                        .putAll(connectorProperties)
                        .buildOrThrow())
                .build();
    }

    @Test
    public void testNullPredicate()
            throws IOException
    {
        String indexName = "null_predicate1";
        @Language("JSON")
        String properties =
                """
                {
                  "properties": {
                    "null_keyword": { "type": "keyword" },
                    "custkey": { "type": "keyword" }
                  }
                }
                """;
        createIndex(indexName, properties);
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("null_keyword", 32)
                .put("custkey", 1301)
                .buildOrThrow());

        assertQueryReturnsEmptyResult("SELECT * FROM null_predicate1 WHERE null_keyword IS NULL");
        assertQueryReturnsEmptyResult("SELECT * FROM null_predicate1 WHERE null_keyword = '10' OR null_keyword IS NULL");

        assertThat(query("SELECT custkey, null_keyword FROM null_predicate1 WHERE null_keyword = '32' OR null_keyword IS NULL"))
                .matches("VALUES (VARCHAR '1301', VARCHAR '32')");
        assertThat(query("SELECT custkey FROM null_predicate1 WHERE null_keyword = '32' OR null_keyword IS NULL"))
                .matches("VALUES (VARCHAR '1301')");

        // not null filter
        // filtered column is selected
        assertThat(query("SELECT custkey, null_keyword FROM null_predicate1 WHERE null_keyword IS NOT NULL"))
                .matches("VALUES (VARCHAR '1301', VARCHAR '32')");
        assertThat(query("SELECT custkey, null_keyword FROM null_predicate1 WHERE null_keyword = '32' OR null_keyword IS NOT NULL"))
                .matches("VALUES (VARCHAR '1301', VARCHAR '32')");

        // filtered column is not selected
        assertThat(query("SELECT custkey FROM null_predicate1 WHERE null_keyword = '32' OR null_keyword IS NOT NULL"))
                .matches("VALUES (VARCHAR '1301')");

        indexName = "null_predicate2";
        properties =
                """
                {
                  "properties": {
                    "null_keyword": { "type": "keyword" },
                    "custkey": { "type": "keyword" }
                  }
                }
                """;
        createIndex(indexName, properties);
        index(indexName, ImmutableMap.of("custkey", 1301));

        // not null filter
        assertQueryReturnsEmptyResult("SELECT * FROM null_predicate2 WHERE null_keyword IS NOT NULL");
        assertQueryReturnsEmptyResult("SELECT * FROM null_predicate2 WHERE null_keyword = '10' OR null_keyword IS NOT NULL");

        // filtered column is selected
        assertThat(query("SELECT custkey, null_keyword FROM null_predicate2 WHERE null_keyword IS NULL"))
                .matches("VALUES (VARCHAR '1301', CAST(NULL AS VARCHAR))");
        assertThat(query("SELECT custkey, null_keyword FROM null_predicate2 WHERE null_keyword = '32' OR null_keyword IS NULL"))
                .matches("VALUES (VARCHAR '1301', CAST(NULL AS VARCHAR))");

        // filtered column is not selected
        assertThat(query("SELECT custkey FROM null_predicate2 WHERE null_keyword = '32' OR null_keyword IS NULL"))
                .matches("VALUES (VARCHAR '1301')");

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("null_keyword", 32)
                .put("custkey", 1302)
                .buildOrThrow());

        assertThat(query("SELECT custkey, null_keyword FROM null_predicate2 WHERE null_keyword = '32' OR null_keyword IS NULL"))
                .matches("VALUES (VARCHAR '1301', CAST(NULL AS VARCHAR)), (VARCHAR '1302', VARCHAR '32')");
        assertThat(query("SELECT custkey FROM null_predicate2 WHERE null_keyword = '32' OR null_keyword IS NULL"))
                .matches("VALUES (VARCHAR '1301'), (VARCHAR '1302')");
    }

    @Test
    public void testNestedFields()
            throws IOException
    {
        String indexName = "data";
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("name", "nestfield")
                .put("fields.fielda", 32)
                .put("fields.fieldb", "valueb")
                .buildOrThrow());

        assertThat(query("SELECT name, fields.fielda, fields.fieldb FROM data"))
                .matches("VALUES (VARCHAR 'nestfield', BIGINT '32', VARCHAR 'valueb')");
    }

    @Test
    public void testFieldNameWithSpecialCharacters()
            throws IOException
    {
        String index = "field_name_with_special_characters_" + randomNameSuffix();
        index(index, ImmutableMap.<String, Object>builder()
                .put("with$sign", 55)
                .put("nested", ImmutableMap.<String, Object>builder()
                        .put("with$sign", "few bucks")
                        .buildOrThrow())
                .buildOrThrow());

        assertThat(query("SELECT \"with$sign\", nested.\"with$sign\" FROM " + index))
                .skippingTypesCheck()
                .matches("VALUES (CAST(55 AS BIGINT), 'few bucks')");
    }

    @Test
    public void testNameConflict()
            throws IOException
    {
        String indexName = "name_conflict";
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("field", "value")
                .put("Conflict", "conflict1")
                .put("conflict", "conflict2")
                .buildOrThrow());

        assertThat(query("SELECT * FROM name_conflict"))
                .matches("VALUES (VARCHAR 'value')");
    }

    @Test
    public void testArrayFields()
            throws IOException
    {
        String indexName = "test_arrays";

        @Language("JSON")
        String mapping =
                """
                {
                  "_meta": {
                    "trino": {
                      "a": {
                        "b": {
                          "y": {
                            "isArray": true
                          }
                        }
                      },
                      "c": {
                        "f": {
                          "g": {
                            "isArray": true
                          },
                          "isArray": true
                        }
                      },
                      "j": {
                        "isArray": true
                      },
                      "k": {
                        "isArray": true
                      }
                    }
                  },
                  "properties":{
                    "a": {
                      "type": "object",
                      "properties": {
                        "b": {
                          "type": "object",
                          "properties": {
                            "x": {
                              "type": "integer"
                            },
                            "y": {
                              "type": "keyword"
                            }
                          }\s
                        }
                      }
                    },
                    "c": {
                      "type": "object",
                      "properties": {
                        "d": {
                          "type": "keyword"
                        },
                        "e": {
                          "type": "keyword"
                        },
                        "f": {
                          "type": "object",
                          "properties": {
                            "g": {
                              "type": "integer"
                            },
                            "h": {
                              "type": "integer"
                            }
                          }\s
                        }
                      }
                    },
                    "i": {
                      "type": "long"
                    },
                    "j": {
                      "type": "long"
                    },
                    "k": {
                      "type": "long"
                    }
                  }
                }
                """;

        createIndex(indexName, mapping);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("a", ImmutableMap.<String, Object>builder()
                        .put("b", ImmutableMap.<String, Object>builder()
                                .put("x", 1)
                                .put("y", ImmutableList.<String>builder()
                                        .add("hello")
                                        .add("world")
                                        .build())
                                .buildOrThrow())
                        .buildOrThrow())
                .put("c", ImmutableMap.<String, Object>builder()
                        .put("d", "foo")
                        .put("e", "bar")
                        .put("f", ImmutableList.<Map<String, Object>>builder()
                                .add(ImmutableMap.<String, Object>builder()
                                        .put("g", ImmutableList.<Integer>builder()
                                                .add(10)
                                                .add(20)
                                                .build())
                                        .put("h", 100)
                                        .buildOrThrow())
                                .add(ImmutableMap.<String, Object>builder()
                                        .put("g", ImmutableList.<Integer>builder()
                                                .add(30)
                                                .add(40)
                                                .build())
                                        .put("h", 200)
                                        .buildOrThrow())
                                .build())
                        .buildOrThrow())
                .put("j", ImmutableList.<Long>builder()
                        .add(50L)
                        .add(60L)
                        .build())
                .buildOrThrow());

        assertThat(query("SELECT a.b.y[1], c.f[1].g[2], c.f[2].g[1], j[2], k[1] FROM test_arrays"))
                .matches("VALUES (VARCHAR 'hello', 20, 30, BIGINT '60', CAST(NULL AS BIGINT))");
    }

    @Test
    public void testAsRawJson()
            throws IOException
    {
        String indexName = "raw_json_" + randomNameSuffix();

        @Language("JSON")
        String mapping =
                """
                {
                  "_meta": {
                    "trino": {
                      "es_object": {
                        "array_of_string_arrays": {
                          "asRawJson": true
                        },
                        "arrayOfIntArrays": {
                          "asRawJson": true
                        }
                      },
                      "es_array_object": {
                        "isArray": true,
                        "array_of_string_arrays": {
                          "asRawJson": true
                        },
                        "arrayOfIntArrays": {
                          "asRawJson": true
                        }
                      },
                      "es_raw_object": {
                        "asRawJson": true,
                        "array_of_string_arrays": {
                          "isArray": true
                        },
                        "arrayOfIntArrays": {
                          "isArray": true
                        }
                      },
                      "array_of_string_arrays": {
                        "asRawJson": true
                      },
                      "array_of_long_arrays": {
                        "asRawJson": true
                      }
                    }
                  },
                  "properties": {
                    "es_object": {
                      "type": "object",
                      "properties": {
                        "array_of_string_arrays": {
                          "type": "keyword"
                        },
                        "arrayOfIntArrays": {
                          "type": "integer"
                        }
                      }
                    },
                    "es_array_object": {
                      "type": "object",
                      "properties": {
                        "array_of_string_arrays": {
                          "type": "keyword"
                        },
                        "arrayOfIntArrays": {
                          "type": "integer"
                        }
                      }
                    },
                    "es_raw_object": {
                      "type": "object",
                      "properties": {
                        "array_of_string_arrays": {
                          "type": "keyword"
                        },
                        "arrayOfIntArrays": {
                          "type": "integer"
                        }
                      }
                    },
                    "array_of_string_arrays": {
                      "type": "text"
                    },
                    "array_of_long_arrays": {
                      "type": "long"
                    },
                    "order_field": {
                      "type": "integer"
                    }
                  }
                }
                """;

        createIndex(indexName, mapping);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("es_object", ImmutableMap.<String, Object>builder()
                        .put("array_of_string_arrays", ImmutableList.<List<String>>builder()
                                .add(ImmutableList.<String>builder()
                                        .add("abc")
                                        .add("def")
                                        .build())
                                .build())
                        .put("arrayOfIntArrays", ImmutableList.<Object>builder()
                                .add(123)
                                .add(ImmutableList.<Integer>builder()
                                        .add(234)
                                        .add(345)
                                        .build())
                                .build())
                        .buildOrThrow())
                .put("es_array_object", ImmutableMap.<String, Object>builder()
                        .put("array_of_string_arrays", ImmutableList.<List<String>>builder()
                                .add(ImmutableList.<String>builder()
                                        .add("abc")
                                        .add("def")
                                        .build())
                                .build())
                        .put("arrayOfIntArrays", ImmutableList.<Object>builder()
                                .add(123)
                                .add(ImmutableList.<Integer>builder()
                                        .add(234)
                                        .add(345)
                                        .build())
                                .build())
                        .buildOrThrow())
                .put("es_raw_object", ImmutableMap.<String, Object>builder()
                        .put("array_of_string_arrays", ImmutableList.<List<String>>builder()
                                .add(ImmutableList.<String>builder()
                                        .add("abc")
                                        .add("def")
                                        .build())
                                .build())
                        .put("arrayOfIntArrays", ImmutableList.<Object>builder()
                                .add(123)
                                .add(ImmutableList.<Integer>builder()
                                        .add(234)
                                        .add(345)
                                        .build())
                                .build())
                        .buildOrThrow())
                .put("array_of_string_arrays", ImmutableList.<List<String>>builder()
                        .add(ImmutableList.<String>builder()
                                .add("abc")
                                .add("def")
                                .build())
                        .build())
                .put("array_of_long_arrays", ImmutableList.<Object>builder()
                        .add(123L)
                        .add(ImmutableList.<Long>builder()
                                .add(234L)
                                .add(345L)
                                .build())
                        .build())
                .put("order_field", 1)
                .buildOrThrow());

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("es_object", ImmutableMap.<String, Object>builder()
                        .put("array_of_string_arrays", "Join the Trino Slack: https://trino.io/slack.html")
                        .put("arrayOfIntArrays", 867)
                        .buildOrThrow())
                .put("es_array_object", ImmutableMap.<String, Object>builder()
                        .put("array_of_string_arrays", "If you like Presto, you'll love Trino: https://trino.io/slack.html")
                        .put("arrayOfIntArrays", 321)
                        .buildOrThrow())
                .put("es_raw_object", ImmutableMap.<String, Object>builder()
                        .put("array_of_string_arrays", "The founders and core contributors of Presto, and are now working on Trino: https://trino.io/blog/2020/12/27/announcing-trino.html")
                        .put("arrayOfIntArrays", 654)
                        .buildOrThrow())
                .put("array_of_string_arrays", "Check out the bi-weekly Trino Community Broadcast https://trino.io/broadcast/")
                .put("array_of_long_arrays", 5309L)
                .put("order_field", 2)
                .buildOrThrow());

        MaterializedResult rows = computeActual(
                """
                SELECT
                    json_extract(array_of_string_arrays, '$[0][0]'),
                    json_extract(array_of_string_arrays, '$[0][1]'),
                    array_of_string_arrays,
                    json_extract(array_of_long_arrays, '$[0]'),
                    try(json_extract(array_of_long_arrays, '$[1][0]')),
                    try(json_extract(array_of_long_arrays, '$[1][1]')),
                    array_of_long_arrays
                FROM %s
                ORDER BY order_field
                """.formatted(indexName));

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row("\"abc\"", "\"def\"", "[[\"abc\",\"def\"]]", "123", "234", "345", "[123,[234,345]]")
                .row(null, null, "\"Check out the bi-weekly Trino Community Broadcast https://trino.io/broadcast/\"", null, null, null, "5309")
                .build();

        assertThat(rows.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());

        MaterializedResult nestedRows = computeActual(
                """
                SELECT
                    json_extract(es_object.array_of_string_arrays, '$[0][0]'),
                    json_extract(es_object.array_of_string_arrays, '$[0][1]'),
                    es_object.array_of_string_arrays,
                    json_extract(es_object.arrayOfIntArrays, '$[0]'),
                    try(json_extract(es_object.arrayOfIntArrays, '$[1][0]')),
                    try(json_extract(es_object.arrayOfIntArrays, '$[1][1]')),
                    es_object.arrayOfIntArrays
                FROM %s
                ORDER BY order_field
                """.formatted(indexName));

        MaterializedResult nestedExpected = resultBuilder(getSession(), nestedRows.getTypes())
                .row("\"abc\"", "\"def\"", "[[\"abc\",\"def\"]]", "123", "234", "345", "[123,[234,345]]")
                .row(null, null, "\"Join the Trino Slack: https://trino.io/slack.html\"", null, null, null, "867")
                .build();

        assertThat(nestedRows.getMaterializedRows()).isEqualTo(nestedExpected.getMaterializedRows());

        MaterializedResult arrayRows = computeActual(
                """
                SELECT
                    json_extract(es_array_object[1].array_of_string_arrays, '$[0][0]'),
                    json_extract(es_array_object[1].array_of_string_arrays, '$[0][1]'),
                    es_array_object[1].array_of_string_arrays,
                    json_extract(es_array_object[1].arrayOfIntArrays, '$[0]'),
                    try(json_extract(es_array_object[1].arrayOfIntArrays, '$[1][0]')),
                    try(json_extract(es_array_object[1].arrayOfIntArrays, '$[1][1]')),
                    es_array_object[1].arrayOfIntArrays
                FROM %s
                ORDER BY order_field
                """.formatted(indexName));

        MaterializedResult arrayExpected = resultBuilder(getSession(), arrayRows.getTypes())
                .row("\"abc\"", "\"def\"", "[[\"abc\",\"def\"]]", "123", "234", "345", "[123,[234,345]]")
                .row(null, null, "\"If you like Presto, you'll love Trino: https://trino.io/slack.html\"", null, null, null, "321")
                .build();

        assertThat(arrayRows.getMaterializedRows()).isEqualTo(arrayExpected.getMaterializedRows());

        MaterializedResult rawRows = computeActual(
                """
                SELECT
                    json_extract(es_raw_object, '$.array_of_string_arrays[0][0]'),
                    json_extract(es_raw_object, '$.array_of_string_arrays[0][1]'),
                    json_extract(es_raw_object, '$.array_of_string_arrays'),
                    json_extract(es_raw_object, '$.arrayOfIntArrays[0]'),
                    try(json_extract(es_raw_object, '$.arrayOfIntArrays[1][0]')),
                    try(json_extract(es_raw_object, '$.arrayOfIntArrays[1][1]')),
                    json_extract(es_raw_object, '$.arrayOfIntArrays')
                FROM %s
                ORDER BY order_field
                """.formatted(indexName));

        MaterializedResult rawRowsExpected = resultBuilder(getSession(), rawRows.getTypes())
                .row("\"abc\"", "\"def\"", "[[\"abc\",\"def\"]]", "123", "234", "345", "[123,[234,345]]")
                .row(null, null, "\"The founders and core contributors of Presto, and are now working on Trino: https://trino.io/blog/2020/12/27/announcing-trino.html\"", null, null, null, "654")
                .build();

        assertThat(rawRows.getMaterializedRows()).isEqualTo(rawRowsExpected.getMaterializedRows());
    }

    @Test
    public void testAsRawJsonForAllPrimitiveTypes()
            throws IOException
    {
        String indexName = "raw_json_primitive_" + randomNameSuffix();

        @Language("JSON")
        String mapping =
                """
                {
                  "_meta": {
                    "trino": {
                      "es_binary": {
                        "asRawJson": true
                      },
                      "es_boolean": {
                        "asRawJson": true
                      },
                      "es_long": {
                        "asRawJson": true
                      },
                      "es_integer": {
                        "asRawJson": true
                      },
                      "es_short": {
                        "asRawJson": true
                      },
                      "es_byte": {
                        "asRawJson": true
                      },
                      "es_double": {
                        "asRawJson": true
                      },
                      "es_float": {
                        "asRawJson": true
                      }
                    }
                  },
                  "properties": {
                    "es_binary": {
                      "type": "binary"
                    },
                    "es_boolean": {
                      "type": "boolean"
                    },
                    "es_long": {
                      "type": "long"
                    },
                    "es_integer": {
                      "type": "integer"
                    },
                    "es_short": {
                      "type": "short"
                    },
                    "es_byte": {
                      "type": "byte"
                    },
                    "es_double": {
                      "type": "double"
                    },
                    "es_float": {
                      "type": "float"
                    },
                    "order_field": {
                      "type": "integer"
                    }
                  }
                }""";

        createIndex(indexName, mapping);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("es_binary", "test".getBytes(UTF_8))
                .put("es_boolean", true)
                .put("es_long", (long) 123)
                .put("es_integer", 123)
                .put("es_short", (short) 123)
                .put("es_byte", (byte) 123)
                .put("es_double", (double) 123)
                .put("es_float", (float) 123)
                .put("order_field", 1)
                .buildOrThrow());

        MaterializedResult rows = computeActual(
                """
                SELECT
                    es_binary,
                    es_boolean,
                    es_long,
                    es_integer,
                    es_short,
                    es_byte,
                    es_double,
                    es_float
                FROM %s
                ORDER BY order_field
                """.formatted(indexName));

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row("\"dGVzdA==\"", "true", "123", "123", "123", "123", "123.0", "123.0")
                .build();
        assertThat(rows.getTypes())
                .hasOnlyElementsOfType(VarcharType.class);

        assertThat(rows.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());

        deleteIndex(indexName);
    }

    @Test
    public void testAsRawJsonCases()
            throws IOException
    {
        String indexName = "raw_json_cases_" + randomNameSuffix();

        @Language("JSON")
        String mapping =
                """
                {
                  "_meta": {
                    "trino": {
                      "es_binary": {
                        "asRawJson": true
                      },
                      "es_boolean": {
                        "asRawJson": true
                      },
                      "es_timestamp": {
                        "asRawJson": true
                      }
                    }
                  },
                  "properties": {
                    "es_binary": {
                      "type": "binary"
                    },
                    "es_boolean": {
                      "type": "boolean"
                    },
                    "es_timestamp": {
                      "type": "date"
                    }
                  }
                }""";

        createIndex(indexName, mapping);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("es_binary", "test".getBytes(UTF_8))
                .put("es_boolean", true)
                .put("es_timestamp", 123)
                .buildOrThrow());

        MaterializedResult rows = computeActual(
                """
                SELECT
                    es_binary,
                    es_boolean,
                    es_timestamp
                FROM %s
                """.formatted(indexName));

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row("\"dGVzdA==\"", "true", "123")
                .build();

        assertThat(rows.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());
        assertThat(rows.getTypes())
                .hasOnlyElementsOfType(VarcharType.class);

        deleteIndex(indexName);
    }

    @Test
    public void testAsRawJsonAndIsArraySameFieldException()
            throws IOException
    {
        String indexName = "raw_json_array_exception" + randomNameSuffix();

        @Language("JSON")
        String mapping =
                """
                {
                  "_meta": {
                    "trino": {
                      "array_raw_field": {
                        "asRawJson": true,
                        "isArray": true
                      }
                    }
                  },
                  "properties": {
                    "array_raw_field": {
                      "type": "text"
                    }
                  }
                }
                """;

        createIndex(indexName, mapping);

        assertQueryReturnsEmptyResult("SHOW TABLES LIKE '" + indexName + "'");

        index(indexName, ImmutableMap.of("array_raw_field", "test"));

        assertThatThrownBy(() -> computeActual("SELECT array_raw_field FROM " + indexName))
                .hasMessage("A column, (array_raw_field) cannot be declared as a Trino array and also be rendered as json.");

        deleteIndex(indexName);
    }

    @Test
    public void testMixedArray()
            throws IOException
    {
        String indexName = "test_mixed_arrays";

        @Language("JSON")
        String mapping =
                """
                {
                  "_meta": {
                    "trino": {
                      "a": {
                        "isArray": true
                      }
                    }
                  },
                  "properties": {
                    "a": {
                      "type": "keyword"
                    }
                  }
                }
                """;

        createIndex(indexName, mapping);

        index(indexName, ImmutableMap.of());

        index(indexName, ImmutableMap.of("a", "hello"));

        index(indexName, ImmutableMap.of("a", ImmutableList.of("foo", "bar")));

        assertThat(query("SELECT a FROM test_mixed_arrays"))
                .matches("VALUES NULL, ARRAY[VARCHAR 'hello'], ARRAY[VARCHAR 'foo', VARCHAR 'bar']");
    }

    @Test
    public void testEmptyNumericFields()
            throws IOException
    {
        String indexName = "emptynumeric";

        @Language("JSON")
        String mapping =
                """
                {
                  "properties": {
                    "byte_column":         {"type": "byte"},
                    "short_column":        {"type": "short"},
                    "integer_column":      {"type": "integer"},
                    "long_column":         {"type": "long"},
                    "float_column":        {"type": "float"},
                    "scaled_float_column": {"type": "scaled_float", "scaling_factor": 100},
                    "double_column":       {"type": "double"}
                  }
                }
                """;

        createIndex(indexName, mapping);
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("byte_column", "")
                .put("short_column", "")
                .put("integer_column", "")
                .put("long_column", "")
                .put("float_column", "")
                .put("scaled_float_column", "")
                .put("double_column", "")
                .buildOrThrow());

        assertThat(query("SELECT byte_column, short_column, integer_column, long_column, float_column, scaled_float_column, double_column FROM emptynumeric"))
                .matches("VALUES (CAST(NULL AS TINYINT), CAST(NULL AS SMALLINT), CAST(NULL AS INTEGER), CAST(NULL AS BIGINT), CAST(NULL AS REAL), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE))");

        deleteIndex(indexName);
    }

    @Test
    public void testEmptyObjectFields()
            throws IOException
    {
        String indexName = "emptyobject";
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("name", "stringfield")
                .put("emptyobject", ImmutableMap.of())
                .put("fields.fielda", 32)
                .put("fields.fieldb", ImmutableMap.of())
                .buildOrThrow());

        assertThat(query("SELECT name, fields.fielda FROM emptyobject"))
                .matches("VALUES (VARCHAR 'stringfield', BIGINT '32')");
    }

    @Test
    public void testNestedVariants()
            throws IOException
    {
        String indexName = "nested_variants";
        @Language("JSON")
        String properties =
                """
                {
                    "properties": {
                        "a": {
                            "properties": {
                                "b.c": {
                                    "type": "text"
                                }
                            }
                        }
                    }
                }
                """;

        createIndex(indexName, properties);
        index(indexName,
                ImmutableMap.of("a",
                        ImmutableMap.of("b",
                                ImmutableMap.of(
                                        "c", "value1"))));

        index(indexName,
                ImmutableMap.of("a.b",
                        ImmutableMap.of(
                                "c", "value2")));

        index(indexName,
                ImmutableMap.of("a",
                        ImmutableMap.of(
                                "b.c", "value3")));

        index(indexName,
                ImmutableMap.of("a.b.c", "value4"));

        assertThat(query("SELECT a.b.c FROM nested_variants"))
                .matches("VALUES VARCHAR 'value1', VARCHAR 'value2', VARCHAR 'value3', VARCHAR 'value4'");

        assertTrinoExceptionThrownBy(() -> computeActual("SELECT a.\"b.c\" FROM nested_variants"))
                .hasErrorCode(INVALID_COLUMN_REFERENCE)
                .hasMessageContaining("Column reference 'a.b.c' is invalid");
    }

    @Test
    public void testLike()
            throws IOException
    {
        String indexName = "like_test";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "keyword_column":   { "type": "keyword" },
                    "text_column":      { "type": "text" }
                  }
                }
                """;

        createIndex(indexName, mappings);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("keyword_column", "so.me tex\\t")
                .put("text_column", "so.me tex\\t")
                .buildOrThrow());

        // Add another document to make sure '.' is escaped and not treated as any character
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("keyword_column", "soome tex\\t")
                .put("text_column", "soome tex\\t")
                .buildOrThrow());

        // Add another document to make sure '%' can be escaped and not treated as any character
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("keyword_column", "soome%text")
                .put("text_column", "soome%text")
                .buildOrThrow());

        // Add another document to make sure utf8 character sequence length is right
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("keyword_column", "中文")
                .put("text_column", "中文")
                .buildOrThrow());

        // Add another document to make sure utf8 character sequence length is right
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("keyword_column", "こんにちは")
                .put("text_column", "こんにちは")
                .buildOrThrow());

        // Add another document to make sure utf8 character sequence length is right
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("keyword_column", "안녕하세요")
                .put("text_column", "안녕하세요")
                .buildOrThrow());

        // Add another document to make sure utf8 character sequence length is right
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("keyword_column", "Привет")
                .put("text_column", "Привет")
                .buildOrThrow());

        assertThat(query(
                """
                SELECT keyword_column
                FROM like_test
                WHERE keyword_column
                LIKE 's_.m%ex\\t'
                """))
                .matches("VALUES VARCHAR 'so.me tex\\t'")
                .isFullyPushedDown();

        assertThat(query(
                """
                SELECT text_column
                FROM like_test
                WHERE text_column
                LIKE 's_.m%ex\\t'
                """))
                .matches("VALUES VARCHAR 'so.me tex\\t'");

        assertThat(query("" +
                "SELECT " +
                "text_column " +
                "FROM " + indexName + " " +
                "WHERE keyword_column LIKE 'soome$%%' ESCAPE '$'"))
                .matches("VALUES VARCHAR 'soome%text'")
                .isFullyPushedDown();

        assertThat(query("" +
                "SELECT " +
                "text_column " +
                "FROM " + indexName + " " +
                "WHERE keyword_column LIKE '中%'"))
                .matches("VALUES VARCHAR '中文'")
                .isFullyPushedDown();

        assertThat(query("" +
                "SELECT " +
                "text_column " +
                "FROM " + indexName + " " +
                "WHERE keyword_column LIKE 'こんに%'"))
                .matches("VALUES VARCHAR 'こんにちは'")
                .isFullyPushedDown();

        assertThat(query("" +
                "SELECT " +
                "text_column " +
                "FROM " + indexName + " " +
                "WHERE keyword_column LIKE '안녕하%'"))
                .matches("VALUES VARCHAR '안녕하세요'")
                .isFullyPushedDown();

        assertThat(query("" +
                "SELECT " +
                "text_column " +
                "FROM " + indexName + " " +
                "WHERE keyword_column LIKE 'При%'"))
                .matches("VALUES VARCHAR 'Привет'")
                .isFullyPushedDown();
    }

    @Test
    public void testDataTypes()
            throws IOException
    {
        String indexName = "types";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "boolean_column":      { "type": "boolean" },
                    "float_column":        { "type": "float" },
                    "double_column":       { "type": "double" },
                    "integer_column":      { "type": "integer" },
                    "long_column":         { "type": "long" },
                    "keyword_column":      { "type": "keyword" },
                    "text_column":         { "type": "text" },
                    "binary_column":       { "type": "binary" },
                    "timestamp_column":    { "type": "date" },
                    "ipv4_column":         { "type": "ip" },
                    "ipv6_column":         { "type": "ip" },
                    "scaled_float_column": { "type": "scaled_float", "scaling_factor": 100 }
                  }
                }
                """;

        createIndex(indexName, mappings);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("boolean_column", true)
                .put("float_column", 1.0f)
                .put("double_column", 1.0d)
                .put("integer_column", 1)
                .put("long_column", 1L)
                .put("keyword_column", "cool")
                .put("text_column", "some text")
                .put("binary_column", new byte[] {(byte) 0xCA, (byte) 0xFE})
                .put("timestamp_column", 0)
                .put("ipv4_column", "1.2.3.4")
                .put("ipv6_column", "2001:db8:0:0:1:0:0:1")
                .put("scaled_float_column", 123456.78d)
                .buildOrThrow());

        MaterializedResult rows = computeActual(
                """
                SELECT
                    boolean_column,
                    float_column,
                    double_column,
                    integer_column,
                    long_column,
                    keyword_column,
                    text_column,
                    binary_column,
                    timestamp_column,
                    ipv4_column,
                    ipv6_column,
                    scaled_float_column
                FROM types
                """);

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(
                        true,
                        1.0f,
                        1.0d,
                        1,
                        1L,
                        "cool",
                        "some text",
                        new byte[] {(byte) 0xCA, (byte) 0xFE},
                        LocalDateTime.of(1970, 1, 1, 0, 0),
                        "1.2.3.4",
                        "2001:db8::1:0:0:1",
                        123456.78d)
                .build();

        assertThat(rows.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testTableWithUnsupportedTypes()
            throws IOException
    {
        String indexName = "unsupported_types";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "long_column":      { "type": "long" },
                    "unsupported_type": { "type": "completion"}
                  }
                }
                """;

        createIndex(indexName, mappings);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("long_column", 1L)
                .put("unsupported_type", ImmutableList.of("foo", "bar"))
                .buildOrThrow());

        MaterializedResult rows = computeActual("SELECT * FROM unsupported_types");
        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(1L)
                .build();

        assertThat(rows.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testBoolean()
            throws IOException
    {
        String indexName = "booleans";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "boolean_column":   { "type": "boolean" }
                  }
                }
                """;

        createIndex(indexName, mappings);

        index(indexName, ImmutableMap.of("boolean_column", true));

        index(indexName, ImmutableMap.of("boolean_column", "true"));

        index(indexName, ImmutableMap.of("boolean_column", false));

        index(indexName, ImmutableMap.of("boolean_column", "false"));

        index(indexName, ImmutableMap.of("boolean_column", ""));

        MaterializedResult rows = computeActual("SELECT boolean_column FROM booleans");

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(true)
                .row(true)
                .row(false)
                .row(false)
                .row(false)
                .build();

        assertEqualsIgnoreOrder(rows.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testTimestamps()
            throws IOException
    {
        String indexName = "timestamps";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "timestamp_column":   { "type": "date" }
                  }
                }
                """;

        createIndex(indexName, mappings);

        index(indexName, ImmutableMap.of("timestamp_column", "2015-01-01"));

        index(indexName, ImmutableMap.of("timestamp_column", "2015-01-01T12:10:30Z"));

        index(indexName, ImmutableMap.of("timestamp_column", 1420070400001L));

        index(indexName, ImmutableMap.of("timestamp_column", "1420070400001"));

        MaterializedResult rows = computeActual("SELECT timestamp_column FROM timestamps");

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(LocalDateTime.parse("2015-01-01T00:00:00"))
                .row(LocalDateTime.parse("2015-01-01T12:10:30"))
                .row(LocalDateTime.parse("2015-01-01T00:00:00.001"))
                .row(LocalDateTime.parse("2015-01-01T00:00:00.001"))
                .build();

        assertEqualsIgnoreOrder(rows.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testNestedTimestamps()
            throws IOException
    {
        String indexName = "nested_timestamps";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties":{
                    "field": {
                      "properties": {
                        "timestamp_column": { "type": "date" }
                      }
                    }
                  }
                }
                """;

        createIndex(indexName, mappings);

        index(indexName, ImmutableMap.of("field", ImmutableMap.of("timestamp_column", 0)));
        index(indexName, ImmutableMap.of("field", ImmutableMap.of("timestamp_column", "1")));
        index(indexName, ImmutableMap.of("field", ImmutableMap.of("timestamp_column", "1970-01-01T01:01:00+0000")));

        assertThat(query("SELECT field.timestamp_column FROM " + indexName))
                .matches("VALUES " +
                        "(TIMESTAMP '1970-01-01 00:00:00.000')," +
                        "(TIMESTAMP '1970-01-01 00:00:00.001')," +
                        "(TIMESTAMP '1970-01-01 01:01:00.000')")
                .isFullyPushedDown();
    }

    @Test
    public void testScaledFloat()
            throws Exception
    {
        String indexName = "scaled_float_type";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "text_column":         { "type": "text" },
                    "scaled_float_column": { "type": "scaled_float", "scaling_factor": 100 }
                  }
                }
                """;

        createIndex(indexName, mappings);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("text_column", "foo")
                .put("scaled_float_column", 123.4567d)
                .buildOrThrow());

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("text_column", "bar")
                .put("scaled_float_column", 123.46d)
                .buildOrThrow());

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("text_column", "random value")
                .put("scaled_float_column", 9.8d)
                .buildOrThrow());

        // Trino query filters in the engine, so the rounding (dependent on scaling factor) does not impact results
        assertThat(query(
                """
                SELECT text_column, scaled_float_column
                FROM scaled_float_type
                WHERE scaled_float_column = 123.46
                """))
                .result().matches(resultBuilder(getSession(), ImmutableList.of(VARCHAR, DOUBLE))
                        .row("bar", 123.46d)
                        .build());
    }

    @Test
    public void testCoercions()
            throws IOException
    {
        String indexName = "coercions";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "float_column":     { "type": "float" },
                    "double_column":    { "type": "double" },
                    "integer_column":   { "type": "integer" },
                    "long_column":      { "type": "long" }
                  }
                }
                """;

        createIndex(indexName, mappings);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("float_column", "1.0")
                .put("double_column", "1.0")
                .put("integer_column", "1")
                .put("long_column", "1")
                .buildOrThrow());

        MaterializedResult rows = computeActual(
                """
                SELECT
                    float_column,
                    double_column,
                    integer_column,
                    long_column
                FROM coercions
                """);

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(1.0f, 1.0d, 1, 1L)
                .build();

        assertThat(rows.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testFilters()
            throws IOException
    {
        String indexName = "filter_pushdown";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "boolean_column":   { "type": "boolean" },
                    "float_column":     { "type": "float" },
                    "double_column":    { "type": "double" },
                    "integer_column":   { "type": "integer" },
                    "long_column":      { "type": "long" },
                    "keyword_column":   { "type": "keyword" },
                    "text_column":      { "type": "text" },
                    "binary_column":    { "type": "binary" },
                    "timestamp_column": { "type": "date" },
                    "ipv4_column":      { "type": "ip" },
                    "ipv6_column":      { "type": "ip" }
                  }
                }
                """;

        createIndex(indexName, mappings);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("boolean_column", true)
                .put("byte_column", 1)
                .put("short_column", 2)
                .put("integer_column", 3)
                .put("long_column", 4L)
                .put("float_column", 1.0f)
                .put("double_column", 1.0d)
                .put("keyword_column", "cool")
                .put("text_column", "some text")
                .put("binary_column", new byte[] {(byte) 0xCA, (byte) 0xFE})
                .put("timestamp_column", 1569888000000L)
                .put("ipv4_column", "1.2.3.4")
                .put("ipv6_column", "2001:db8:0:0:1:0:0:1")
                .buildOrThrow());

        // _score column
        assertThat(query("SELECT count(*) FROM \"filter_pushdown: cool\" WHERE _score > 0"))
                .matches("VALUES BIGINT '1'");

        // boolean
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE boolean_column = true"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE boolean_column = false"))
                .matches("VALUES BIGINT '0'");

        // tinyint
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE byte_column = 1"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE byte_column = 0"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE byte_column > 1"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE byte_column < 1"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE byte_column > 0"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE byte_column < 10"))
                .matches("VALUES BIGINT '1'");

        // smallint
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE short_column = 2"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE short_column > 2"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE short_column < 2"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE short_column = 0"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE short_column > 0"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE short_column < 10"))
                .matches("VALUES BIGINT '1'");

        // integer
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE integer_column = 3"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE integer_column > 3"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE integer_column < 3"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE integer_column = 0"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE integer_column > 0"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE integer_column < 10"))
                .matches("VALUES BIGINT '1'");

        // bigint
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE long_column = 4"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE long_column > 4"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE long_column < 4"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE long_column = 0"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE long_column > 0"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE long_column < 10"))
                .matches("VALUES BIGINT '1'");

        // real
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE float_column = 1.0"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE float_column > 1.0"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE float_column < 1.0"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE float_column = 0.0"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE float_column > 0.0"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE float_column < 10.0"))
                .matches("VALUES BIGINT '1'");

        // double
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE double_column = 1.0"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE double_column > 1.0"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE double_column < 1.0"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE double_column = 0.0"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE double_column > 0.0"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE double_column < 10.0"))
                .matches("VALUES BIGINT '1'");

        // varchar
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE keyword_column = 'cool'"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE keyword_column = 'bar'"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE text_column = 'some text'"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE text_column = 'some'"))
                .matches("VALUES BIGINT '0'");

        // binary
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE binary_column = x'CAFE'"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE binary_column = x'ABCD'"))
                .matches("VALUES BIGINT '0'");

        // timestamp
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE timestamp_column = TIMESTAMP '2019-10-01 00:00:00'"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE timestamp_column > TIMESTAMP '2019-10-01 00:00:00'"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE timestamp_column < TIMESTAMP '2019-10-01 00:00:00'"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE timestamp_column = TIMESTAMP '2019-10-02 00:00:00'"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE timestamp_column > TIMESTAMP '2001-01-01 00:00:00'"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE timestamp_column < TIMESTAMP '2030-01-01 00:00:00'"))
                .matches("VALUES BIGINT '1'");

        // ipaddress
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE ipv4_column = IPADDRESS '1.2.3.4'"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_pushdown WHERE ipv6_column = IPADDRESS '2001:db8::1:0:0:1'"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testFiltersCharset()
            throws IOException
    {
        String indexName = "filter_charset_pushdown";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "keyword_column":   { "type": "keyword" },
                    "text_column":      { "type": "text" }
                  }
                }
                """;

        createIndex(indexName, mappings);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("keyword_column", "Türkiye")
                .put("text_column", "Türkiye")
                .buildOrThrow());

        assertThat(query("SELECT count(*) FROM filter_charset_pushdown WHERE keyword_column = 'Türkiye'"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_charset_pushdown WHERE keyword_column = 'bar'"))
                .matches("VALUES BIGINT '0'");
        assertThat(query("SELECT count(*) FROM filter_charset_pushdown WHERE text_column = 'Türkiye'"))
                .matches("VALUES BIGINT '1'");
        assertThat(query("SELECT count(*) FROM filter_charset_pushdown WHERE text_column = 'some'"))
                .matches("VALUES BIGINT '0'");

        assertThat(query("SELECT keyword_column FROM filter_charset_pushdown WHERE keyword_column = 'Türkiye'"))
                .matches("VALUES (VARCHAR 'Türkiye')");
        assertThat(query("SELECT text_column FROM filter_charset_pushdown WHERE text_column = 'Türkiye'"))
                .matches("VALUES (VARCHAR 'Türkiye')");
    }

    @Test
    public void testDataTypesNested()
            throws IOException
    {
        String indexName = "types_nested";

        @Language("JSON")
        String properties =
                """
                {
                  "properties": {
                    "field": {
                      "properties": {
                        "boolean_column":   { "type": "boolean" },
                        "float_column":     { "type": "float" },
                        "double_column":    { "type": "double" },
                        "integer_column":   { "type": "integer" },
                        "long_column":      { "type": "long" },
                        "keyword_column":   { "type": "keyword" },
                        "text_column":      { "type": "text" },
                        "binary_column":    { "type": "binary" },
                        "timestamp_column": { "type": "date" },
                        "ipv4_column":      { "type": "ip" },
                        "ipv6_column":      { "type": "ip" }
                      }
                    }
                  }
                }
                """;

        createIndex(indexName, properties);

        index(indexName, ImmutableMap.of(
                "field", ImmutableMap.<String, Object>builder()
                        .put("boolean_column", true)
                        .put("float_column", 1.0f)
                        .put("double_column", 1.0d)
                        .put("integer_column", 1)
                        .put("long_column", 1L)
                        .put("keyword_column", "cool")
                        .put("text_column", "some text")
                        .put("binary_column", new byte[] {(byte) 0xCA, (byte) 0xFE})
                        .put("timestamp_column", 0)
                        .put("ipv4_column", "1.2.3.4")
                        .put("ipv6_column", "2001:db8:0:0:1:0:0:1")
                        .buildOrThrow()));

        MaterializedResult rows = computeActual(
                """
                SELECT
                    field.boolean_column,
                    field.float_column,
                    field.double_column,
                    field.integer_column,
                    field.long_column,
                    field.keyword_column,
                    field.text_column,
                    field.binary_column,
                    field.timestamp_column,
                    field.ipv4_column,
                    field.ipv6_column
                FROM types_nested
                """);

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(true,
                        1.0f,
                        1.0d,
                        1,
                        1L,
                        "cool",
                        "some text",
                        new byte[] {(byte) 0xCA, (byte) 0xFE},
                        LocalDateTime.of(1970, 1, 1, 0, 0),
                        "1.2.3.4",
                        "2001:db8::1:0:0:1")
                .build();

        assertThat(rows.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testNestedTypeDataTypesNested()
            throws IOException
    {
        String indexName = "nested_type_nested";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "nested_field": {
                      "type":"nested",
                      "properties": {
                        "boolean_column":   { "type": "boolean" },
                        "float_column":     { "type": "float" },
                        "double_column":    { "type": "double" },
                        "integer_column":   { "type": "integer" },
                        "long_column":      { "type": "long" },
                        "keyword_column":   { "type": "keyword" },
                        "text_column":      { "type": "text" },
                        "binary_column":    { "type": "binary" },
                        "timestamp_column": { "type": "date" },
                        "ipv4_column":      { "type": "ip" },
                        "ipv6_column":      { "type": "ip" }
                      }
                    }
                  }
                }
                """;

        createIndex(indexName, mappings);

        index(indexName, ImmutableMap.of(
                "nested_field", ImmutableMap.<String, Object>builder()
                        .put("boolean_column", true)
                        .put("float_column", 1.0f)
                        .put("double_column", 1.0d)
                        .put("integer_column", 1)
                        .put("long_column", 1L)
                        .put("keyword_column", "cool")
                        .put("text_column", "some text")
                        .put("binary_column", new byte[] {(byte) 0xCA, (byte) 0xFE})
                        .put("timestamp_column", 0)
                        .put("ipv4_column", "1.2.3.4")
                        .put("ipv6_column", "2001:db8:0:0:1:0:0:1")
                        .buildOrThrow()));

        MaterializedResult rows = computeActual(
                """
                SELECT
                    nested_field.boolean_column,
                    nested_field.float_column,
                    nested_field.double_column,
                    nested_field.integer_column,
                    nested_field.long_column,
                    nested_field.keyword_column,
                    nested_field.text_column,
                    nested_field.binary_column,
                    nested_field.timestamp_column,
                    nested_field.ipv4_column,
                    nested_field.ipv6_column
                FROM nested_type_nested
                """);

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(true,
                        1.0f,
                        1.0d,
                        1,
                        1L,
                        "cool",
                        "some text",
                        new byte[] {(byte) 0xCA, (byte) 0xFE},
                        LocalDateTime.of(1970, 1, 1, 0, 0),
                        "1.2.3.4",
                        "2001:db8::1:0:0:1")
                .build();

        assertThat(rows.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testQueryString()
    {
        assertThat(query("SELECT count(*) FROM \"orders: +packages -slyly\""))
                .matches("VALUES BIGINT '1639'");
    }

    @Test
    public void testMixedCase()
            throws IOException
    {
        String indexName = "mixed_case";
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("Name", "john")
                .put("AGE", 32)
                .buildOrThrow());

        assertThat(query("SELECT name, age FROM mixed_case"))
                .matches("VALUES (VARCHAR 'john', BIGINT '32')");

        assertThat(query("SELECT name, age FROM mixed_case WHERE name = 'john'"))
                .matches("VALUES (VARCHAR 'john', BIGINT '32')");
    }

    @Test
    public void testNumericKeyword()
            throws IOException
    {
        String indexName = "numeric_keyword";
        @Language("JSON")
        String properties =
                """
                {
                  "properties":{
                    "numeric_keyword":   { "type": "keyword" }
                  }
                }
                """;
        createIndex(indexName, properties);
        index(indexName, ImmutableMap.of("numeric_keyword", 20));

        assertThat(query("SELECT numeric_keyword FROM numeric_keyword"))
                .matches("VALUES VARCHAR '20'");
        assertThat(query("SELECT numeric_keyword FROM numeric_keyword where numeric_keyword = '20'"))
                .matches("VALUES VARCHAR '20'");
    }

    @Test
    public void testQueryStringError()
    {
        assertQueryFails("SELECT orderkey FROM \"orders: ++foo AND\"", "\\QFailed to parse query [ ++foo and]\\E");
        assertQueryFails("SELECT count(*) FROM \"orders: ++foo AND\"", "\\QFailed to parse query [ ++foo and]\\E");
    }

    @Test
    public void testAlias()
            throws IOException
    {
        String aliasName = format("alias_%s", randomNameSuffix());
        addAlias("orders", aliasName);

        assertThat(query("SELECT count(*) FROM " + aliasName))
                .matches("SELECT count(*) FROM orders");
    }

    @Test
    public void testFilteredAndRoutedMultiIndexAliasPreservesScanSemantics()
            throws IOException
    {
        String suffix = randomNameSuffix();
        String firstIndex = "filtered_routed_alias_a_" + suffix;
        String secondIndex = "filtered_routed_alias_b_" + suffix;
        String alias = "filtered_routed_alias_" + suffix;
        String selectedRouting = "selected-route";
        String indexBody =
                """
                {
                    "settings": {"number_of_shards": 2},
                    "mappings": {
                        "properties": {
                            "tenant": {"type": "keyword"},
                            "payload": {"type": "keyword"}
                        }
                    }
                }
                """;
        createIndexWithBody(firstIndex, indexBody);
        createIndexWithBody(secondIndex, indexBody);
        try {
            int firstSelectedShard = shardForRouting(firstIndex, selectedRouting);
            int secondSelectedShard = shardForRouting(secondIndex, selectedRouting);
            String otherRouting = null;
            for (int value = 0; value < 100; value++) {
                String candidate = "other-route-" + value;
                if (shardForRouting(firstIndex, candidate) != firstSelectedShard
                        && shardForRouting(secondIndex, candidate) != secondSelectedShard) {
                    otherRouting = candidate;
                    break;
                }
            }
            if (otherRouting == null) {
                throw new AssertionError("Could not find a routing value assigned to a different shard");
            }

            index(firstIndex, selectedRouting, ImmutableMap.of("tenant", "visible", "payload", "first-visible"));
            index(firstIndex, selectedRouting, ImmutableMap.of("tenant", "hidden", "payload", "first-filtered"));
            index(firstIndex, otherRouting, ImmutableMap.of("tenant", "visible", "payload", "first-routed-away"));
            index(secondIndex, selectedRouting, ImmutableMap.of("tenant", "visible", "payload", "second-visible"));
            index(secondIndex, selectedRouting, ImmutableMap.of("tenant", "hidden", "payload", "second-filtered"));
            index(secondIndex, otherRouting, ImmutableMap.of("tenant", "visible", "payload", "second-routed-away"));

            String aliasBody =
                    """
                    {
                        "filter": {"term": {"tenant": "visible"}},
                        "search_routing": "selected-route"
                    }
                    """;
            for (String index : List.of(firstIndex, secondIndex)) {
                Request request = new Request("PUT", format("/%s/_alias/%s", index, alias));
                request.setJsonEntity(aliasBody);
                client.performRequest(request);
            }
            refreshIndex(alias);

            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();

            String rows = "SELECT payload FROM " + alias + " ORDER BY payload";
            assertThat(query(rows))
                    .matches("VALUES VARCHAR 'first-visible', VARCHAR 'second-visible'");
            assertThat(query(withoutPushdown, rows))
                    .matches("VALUES VARCHAR 'first-visible', VARCHAR 'second-visible'");

            String topN = "SELECT payload FROM " + alias + " ORDER BY payload DESC LIMIT 1";
            assertThat(query(topN))
                    .matches("VALUES VARCHAR 'second-visible'");
            assertThat(query(withoutPushdown, topN))
                    .matches("VALUES VARCHAR 'second-visible'");

            String grouping = "SELECT payload, count(*) FROM " + alias + " GROUP BY payload";
            assertThat(query(grouping))
                    .matches("VALUES (VARCHAR 'first-visible', BIGINT '1'), (VARCHAR 'second-visible', BIGINT '1')")
                    .isFullyPushedDown();
            assertThat(query(withoutPushdown, grouping))
                    .matches("VALUES (VARCHAR 'first-visible', BIGINT '1'), (VARCHAR 'second-visible', BIGINT '1')");

            String count = "SELECT count(*) FROM " + alias;
            assertThat(query(count))
                    .matches("VALUES BIGINT '2'")
                    .isFullyPushedDown();
            assertThat(query(withoutPushdown, count))
                    .matches("VALUES BIGINT '2'");
        }
        finally {
            deleteIndex(firstIndex);
            deleteIndex(secondIndex);
        }
    }

    @Test
    public void testAliasScansAndCountsDoNotFanOutAcrossIndices()
            throws Exception
    {
        String suffix = randomNameSuffix();
        String alias = "bounded_fanout_alias_" + suffix;
        List<String> indices = List.of(
                "bounded_fanout_index_a_" + suffix,
                "bounded_fanout_index_b_" + suffix,
                "bounded_fanout_index_c_" + suffix,
                "bounded_fanout_index_d_" + suffix);
        try {
            for (int indexNumber = 0; indexNumber < indices.size(); indexNumber++) {
                String index = indices.get(indexNumber);
                createIndexWithBody(index,
                        """
                        {
                            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                            "mappings": {
                                "properties": {
                                    "tenant": {"type": "keyword"},
                                    "value": {"type": "long"}
                                }
                            }
                        }
                        """);
                index(index, Map.of("tenant", "visible-" + indexNumber, "value", indexNumber));
                index(index, Map.of("tenant", "hidden-" + indexNumber, "value", 100 + indexNumber));
                Request request = new Request("PUT", format("/%s/_alias/%s", index, alias));
                request.setJsonEntity(format("{\"filter\":{\"term\":{\"tenant\":\"visible-%s\"}}}", indexNumber));
                client.performRequest(request);
            }
            Request additionalAlias = new Request("PUT", format("/%s/_alias/%s_hidden", indices.getFirst(), alias));
            additionalAlias.setJsonEntity("{\"filter\":{\"term\":{\"tenant\":\"hidden-0\"}}}");
            client.performRequest(additionalAlias);
            refreshIndex(alias);

            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();
            for (String target : List.of(alias, alias + "*", alias + "," + indices.getFirst())) {
                // Wildcards combine alias filters; an explicit concrete index bypasses its alias filter.
                boolean includesHiddenRow = !target.equals(alias);
                long expectedSum = includesHiddenRow ? 106 : 6;
                long expectedCount = includesHiddenRow ? 5 : 4;
                String table = format("\"%s:value:*\"", target);

                long queriesBefore = shardQueryCount(indices);
                assertThat(computeActual("SELECT sum(value), sum(_score) FROM " + table).getMaterializedRows().getFirst().getFields())
                        .isEqualTo(List.of(expectedSum, (float) expectedCount));
                // Each split reads one data page and then fetches an empty final scroll page.
                assertThat(shardQueryCount(indices) - queriesBefore).isEqualTo(2L * indices.size());

                queriesBefore = shardQueryCount(indices);
                assertThat(computeActual(withoutPushdown, "SELECT count(*) FROM " + table).getOnlyValue()).isEqualTo(expectedCount);
                assertThat(shardQueryCount(indices) - queriesBefore).isEqualTo(indices.size());
            }
            assertAliasOnlyPermissions(alias, indices.getFirst());
        }
        finally {
            for (String index : indices) {
                deleteIndex(index);
            }
        }
    }

    private void assertAliasOnlyPermissions(String alias, String concreteIndex)
            throws Exception
    {
        String identity = "alias_reader_" + randomNameSuffix();
        Request role = new Request("PUT", "/_security/role/" + identity);
        role.setJsonEntity(format(
                """
                {"cluster":["monitor"],"indices":[{"names":["%s"],"privileges":["read","view_index_metadata"]}]}
                """,
                alias));
        client.performRequest(role);
        Request user = new Request("PUT", "/_security/user/" + identity);
        user.setJsonEntity(format("{\"password\":\"%s\",\"roles\":[\"%s\"]}", PASSWORD, identity));
        client.performRequest(user);
        ElasticsearchClient restrictedClient = new ElasticsearchClient(
                new ElasticsearchConfig()
                        .setHosts(List.of(server.getAddress().getHost()))
                        .setPort(server.getAddress().getPort())
                        .setIgnorePublishAddress(true)
                        .setTlsEnabled(true)
                        .setTrustStorePath(new File(getResource("truststore.jks").toURI()))
                        .setTruststorePassword("123456")
                        .setVerifyHostnames(false),
                Optional.empty(),
                Optional.of(new PasswordConfig().setUser(identity).setPassword(PASSWORD)));
        try {
            restrictedClient.initialize();
            assertThat(restrictedClient.getIndexMetadata(alias).schema().fields())
                    .extracting(IndexMetadata.Field::name)
                    .contains("value");
            ElasticsearchTableHandle table = new ElasticsearchTableHandle(SCAN, "tpch", alias, Optional.of("value:*"));
            ElasticsearchColumnHandle value = new ElasticsearchColumnHandle(List.of("value"), BIGINT, new IndexMetadata.PrimitiveType("long"), new BigintDecoder.Descriptor("value"), true, true);
            List<Shard> shards = restrictedClient.getSearchShards(alias);
            assertThat(shards).hasSize(4);
            for (Shard shard : shards) {
                ElasticsearchSplit split = new ElasticsearchSplit(shard.index(), shard.id(), shard.address());
                try (ScanQueryPageSource scan = new ScanQueryPageSource(restrictedClient, getQueryRunner().getPlannerContext().getTypeManager(), table, split, List.of(value))) {
                    SourcePage page = scan.getNextSourcePage();
                    assertThat(page.getPositionCount()).isEqualTo(1);
                    assertThat(BIGINT.getLong(page.getBlock(0), 0)).isBetween(0L, 3L);
                    assertThat(scan.isFinished()).isTrue();
                }
                try (CountQueryPageSource count = new CountQueryPageSource(restrictedClient, table, split)) {
                    assertThat(count.getNextSourcePage().getPositionCount()).isEqualTo(1);
                    assertThat(count.isFinished()).isTrue();
                }
            }
            JsonNode query = new JsonMapper().readTree("{\"match_all\":{}}");
            assertTrinoExceptionThrownBy(() -> restrictedClient.beginSearch(concreteIndex, 0, query, Optional.empty(), List.of(), List.of(), OptionalLong.empty()))
                    .hasErrorCode(ELASTICSEARCH_QUERY_FAILURE)
                    .hasMessageContaining("action [indices:data/read/search] is unauthorized");
            assertTrinoExceptionThrownBy(() -> restrictedClient.count(concreteIndex, 0, query))
                    .hasErrorCode(ELASTICSEARCH_QUERY_FAILURE)
                    .hasMessageContaining("action [indices:data/read/search] is unauthorized");
        }
        finally {
            restrictedClient.close();
            client.performRequest(new Request("DELETE", "/_security/user/" + identity));
            client.performRequest(new Request("DELETE", "/_security/role/" + identity));
        }
    }

    @Test
    public void testSelectInformationSchemaForMultiIndexAlias()
            throws IOException
    {
        addAlias("nation", "multi_alias");
        addAlias("region", "multi_alias");

        // No duplicate entries should be found in information_schema.tables or information_schema.columns.
        testSelectInformationSchemaTables();
        testSelectInformationSchemaColumns();
    }

    @Test // TODO (https://github.com/trinodb/trino/issues/2428)
    @Disabled
    public void testMultiIndexAlias()
            throws IOException
    {
        addAlias("nation", "multi_alias");
        addAlias("region", "multi_alias");

        assertThat(query("SELECT count(*) FROM multi_alias"))
                .matches("SELECT (SELECT count(*) FROM region) + (SELECT count(*) FROM nation)");
    }

    @Test
    public void testEmptyIndexWithMappings()
            throws IOException
    {
        String indexName = "test_empty_index_with_mappings";

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "dummy_column":     { "type": "long" }
                  }
                }
                """;

        createIndex(indexName, mappings);

        assertThat(query(format("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", indexName)))
                .matches("VALUES (VARCHAR 'dummy_column')");
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains(indexName);
        assertQueryReturnsEmptyResult("SELECT * FROM " + indexName);
    }

    @Test
    public void testEmptyIndexNoMappings()
            throws IOException
    {
        String indexName = "test_empty_index";

        createIndex(indexName);
        assertTableDoesNotExist(indexName);
    }

    @Test
    public void testEmptyAliasNoMappings()
            throws IOException
    {
        String indexName = "test_empty_index_for_alias";
        String aliasName = "test_empty_alias";

        createIndex(indexName);
        addAlias(indexName, aliasName);
        assertTableDoesNotExist(aliasName);
    }

    @Test
    public void testMissingIndex()
    {
        assertTableDoesNotExist("nonexistent_table");
    }

    @Test
    public void testQueryTableFunction()
    {
        String catalogName = getSession().getCatalog().orElseThrow();

        // select single record
        assertThat(query("SELECT json_query(result, 'lax $[0][0].hits.hits._source') " +
                format("FROM TABLE(%s.system.raw_query(", catalogName) +
                "schema => 'tpch', " +
                "index => 'nation', " +
                "query => '{\"query\": {\"match\": {\"name\": \"ALGERIA\"}}}')) t(result)"))
                .matches("VALUES VARCHAR '{\"nationkey\":0,\"name\":\"ALGERIA\",\"regionkey\":0,\"comment\":\" haggle. carefully final deposits detect slyly agai\"}'");

        // parameters
        Session session = Session.builder(getSession())
                .addPreparedStatement(
                        "my_query",
                        format("SELECT json_query(result, 'lax $[0][0].hits.hits._source') FROM TABLE(%s.system.raw_query(schema => ?, index => ?, query => ?))", catalogName))
                .build();
        assertThat(query(session, "EXECUTE my_query USING 'tpch', 'nation', '{\"query\": {\"match\": {\"name\": \"ALGERIA\"}}}'"))
                .matches("VALUES VARCHAR '{\"nationkey\":0,\"name\":\"ALGERIA\",\"regionkey\":0,\"comment\":\" haggle. carefully final deposits detect slyly agai\"}'");

        // select multiple records by range. Use array wrapper to wrap multiple results
        assertThat(query("SELECT array_sort(CAST(json_parse(json_query(result, 'lax $[0][0].hits.hits._source.name' WITH ARRAY WRAPPER)) AS array(varchar))) " +
                format("FROM TABLE(%s.system.raw_query(", catalogName) +
                "schema => 'tpch', " +
                "index => 'nation', " +
                "query => '{\"query\": {\"range\": {\"nationkey\": {\"gte\": 0,\"lte\": 3}}}}')) t(result)"))
                .matches("VALUES CAST(ARRAY['ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA'] AS ARRAY(VARCHAR))");

        // use aggregations
        @Language("JSON")
        String query =
                """
                {
                    "size": 0,
                    "aggs" : {
                        "max_orderkey" : { "max" : { "field" : "orderkey" } },
                        "sum_orderkey" : { "sum" : { "field" : "orderkey" } }
                    }
                }
                """;

        assertThat(query(
                """
                WITH data(r) AS (
                   SELECT CAST(json_parse(result) AS ROW(aggregations ROW(max_orderkey ROW(value BIGINT), sum_orderkey ROW(value BIGINT))))
                   FROM TABLE(%s.system.raw_query(
                                        schema => 'tpch',
                                        index => 'orders',
                                        query => '%s')))
                SELECT r.aggregations.max_orderkey.value, r.aggregations.sum_orderkey.value
                FROM data
                """.formatted(catalogName, query)))
                .matches("VALUES (BIGINT '60000', BIGINT '449872500')");

        // no matches
        assertThat(query("SELECT json_query(result, 'lax $[0][0].hits.hits') " +
                format("FROM TABLE(%s.system.raw_query(", catalogName) +
                "schema => 'tpch', " +
                "index => 'nation', " +
                "query => '{\"query\": {\"match\": {\"name\": \"UTOPIA\"}}}')) t(result)"))
                .matches("VALUES VARCHAR '[]'");

        // syntax error
        assertThat(query("SELECT * " +
                format("FROM TABLE(%s.system.raw_query(", catalogName) +
                "schema => 'tpch', " +
                "index => 'nation', " +
                "query => 'wrong syntax')) t(result)"))
                .failure().hasMessageContaining("json_parse_exception");
    }

    @Test
    void testSimpleProjectionPushdown()
            throws IOException
    {
        String tableName = "test_projection_pushdown_" + randomNameSuffix();

        createIndex(tableName);
        index(tableName, ImmutableMap.<String, Object>builder()
                .put("id", 1L)
                .put("root", ImmutableMap.<String, Object>builder()
                        .put("f1", 1L)
                        .put("f2", 2L)
                        .buildOrThrow())
                .buildOrThrow());

        Map<String, Object> record2 = new HashMap<>();
        record2.put("id", 2L);
        record2.put("root", null);
        index(tableName, record2);

        Map<String, Object> record32 = new HashMap<>();
        record32.put("f1", null);
        record32.put("f2", 4L);

        Map<String, Object> record3 = new HashMap<>();
        record3.put("id", 3L);
        record3.put("row", record32);
        index(tableName, record3);

        String selectQuery = "SELECT id, root.f1 FROM " + tableName;
        String expectedResult = "VALUES (BIGINT '1', BIGINT '1'), (BIGINT '2', NULL), (BIGINT '3', NULL)";

        // With Projection Pushdown enabled
        assertThat(query(selectQuery))
                .matches(expectedResult)
                .isFullyPushedDown();

        deleteIndex(tableName);
    }

    @Test
    void testProjectionPushdownWithCaseSensitiveField()
            throws IOException
    {
        String tableName = "test_projection_with_case_sensitive_field_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                    "properties": {
                        "id": {
                            "type": "integer"
                        },
                        "a": {
                            "properties": {
                                "UPPER_CASE": {
                                    "type": "integer"
                                },
                                "lower_case": {
                                    "type": "integer"
                                },
                                "MiXeD_cAsE": {
                                    "type": "integer"
                                }
                            }
                        }
                    }
                }
                """;

        createIndex(tableName, properties);
        index(tableName, ImmutableMap.<String, Object>builder()
                .put("id", 1L)
                .put("a", ImmutableMap.<String, Object>builder()
                        .put("UPPER_CASE", 2)
                        .put("lower_case", 3)
                        .put("MiXeD_cAsE", 4)
                        .buildOrThrow())
                .buildOrThrow());
        index(tableName, ImmutableMap.<String, Object>builder()
                .put("id", 5L)
                .put("a", ImmutableMap.<String, Object>builder()
                        .put("UPPER_CASE", 6)
                        .put("lower_case", 7)
                        .put("MiXeD_cAsE", 8)
                        .buildOrThrow())
                .buildOrThrow());

        String expected = "VALUES (2, 3, 4), (6, 7, 8)";
        assertThat(query("SELECT a.UPPER_CASE, a.lower_case, a.MiXeD_cAsE FROM " + tableName))
                .matches(expected)
                .isFullyPushedDown();
        assertThat(query("SELECT a.upper_case, a.lower_case, a.mixed_case FROM " + tableName))
                .matches(expected)
                .isFullyPushedDown();
        assertThat(query("SELECT a.UPPER_CASE, a.LOWER_CASE, a.MIXED_CASE FROM " + tableName))
                .matches(expected)
                .isFullyPushedDown();

        deleteIndex(tableName);
    }

    @Test
    void testProjectionPushdownWithMultipleRows()
            throws IOException
    {
        String tableName = "test_projection_pushdown_multiple_rows_" + randomNameSuffix();
        @Language("JSON")
        String properties =
                """
                {
                    "properties": {
                        "id": {
                            "type": "integer"
                        },
                        "nested1": {
                            "properties": {
                                "child1": {
                                    "type": "integer"
                                },
                                "child2": {
                                    "type": "text"
                                },
                                "child3": {
                                    "type": "integer"
                                }
                            }
                        },
                        "nested2": {
                            "properties": {
                                "child1": {
                                    "type": "double"
                                },
                                "child2": {
                                    "type": "boolean"
                                },
                                "child3": {
                                    "type": "date"
                                }
                            }
                        }
                    }
                }
                """;

        createIndex(tableName, properties);
        index(tableName, ImmutableMap.<String, Object>builder()
                .put("id", 1)
                .put("nested1", ImmutableMap.<String, Object>builder()
                        .put("child1", 10)
                        .put("child2", "a")
                        .put("child3", 100)
                        .buildOrThrow())
                .put("nested2", ImmutableMap.<String, Object>builder()
                        .put("child1", 10.10d)
                        .put("child2", true)
                        .put("child3", "2023-04-19")
                        .buildOrThrow())
                .buildOrThrow());
        index(tableName, ImmutableMap.<String, Object>builder()
                .put("id", 2)
                .put("nested1", ImmutableMap.<String, Object>builder()
                        .put("child1", 20)
                        .put("child2", "b")
                        .put("child3", 200)
                        .buildOrThrow())
                .put("nested2", ImmutableMap.<String, Object>builder()
                        .put("child1", 20.20d)
                        .put("child2", false)
                        .put("child3", "1990-04-20")
                        .buildOrThrow())
                .buildOrThrow());

        Map<String, Object> record3 = new HashMap<>();
        Map<String, Object> record3Nested1 = new HashMap<>();
        record3Nested1.put("child1", 40);
        record3Nested1.put("child2", null);
        record3Nested1.put("child3", 400);
        record3.put("id", 4);
        record3.put("nested1", record3Nested1);
        record3.put("nested2", null);
        index(tableName, record3);

        Map<String, Object> record4 = new HashMap<>();
        Map<String, Object> record4Nested2 = new HashMap<>();
        record4Nested2.put("child1", null);
        record4Nested2.put("child2", true);
        record4Nested2.put("child3", null);
        record4.put("id", 5);
        record4.put("nested1", null);
        record4.put("nested2", record4Nested2);
        index(tableName, record4);

        // Select one field from one row field
        assertThat(query("SELECT id, nested1.child1 FROM " + tableName))
                .matches("VALUES (1, 10), (2, 20), (4, 40), (5, NULL)")
                .isFullyPushedDown();
        assertThat(query("SELECT nested2.child3, id FROM " + tableName))
                // Use timestamp instead of date as connector converts source date to timestamp
                .matches("VALUES (TIMESTAMP '2023-04-19 00:00:00.000', 1), (TIMESTAMP '1990-04-20 00:00:00.000', 2), (NULL, 4), (NULL, 5)")
                .isFullyPushedDown();

        // Select one field each from multiple row fields
        assertThat(query("SELECT nested2.child1, id, nested1.child2 FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES (DOUBLE '10.10', 1, 'a'), (DOUBLE '20.20', 2, 'b'), (NULL, 4, NULL), (NULL, 5, NULL)")
                .isFullyPushedDown();

        // Select multiple fields from one row field
        assertThat(query("SELECT nested1.child3, id, nested1.child2 FROM " + tableName))
                .skippingTypesCheck()
                .matches("VALUES (100, 1, 'a'), (200, 2, 'b'), (400, 4, NULL), (NULL, 5, NULL)")
                .isFullyPushedDown();
        assertThat(query("SELECT nested2.child2, nested2.child3, id FROM " + tableName))
                // Use timestamp instead of date as connector converts source date to timestamp
                .matches("VALUES (true, TIMESTAMP '2023-04-19 00:00:00.000' , 1), (false, TIMESTAMP '1990-04-20 00:00:00.000', 2), (NULL, NULL, 4), (true, NULL, 5)")
                .isFullyPushedDown();

        // Select multiple fields from multiple row fields
        assertThat(query("SELECT id, nested2.child1, nested1.child3, nested2.child2, nested1.child1 FROM " + tableName))
                .matches("VALUES (1, DOUBLE '10.10', 100, true, 10), (2, DOUBLE '20.20', 200, false, 20), (4, NULL, 400, NULL, 40), (5, NULL, NULL, true, NULL)")
                .isFullyPushedDown();

        // Select only nested fields
        assertThat(query("SELECT nested2.child2, nested1.child3 FROM " + tableName))
                .matches("VALUES (true, 100), (false, 200), (NULL, 400), (true, NULL)")
                .isFullyPushedDown();

        deleteIndex(tableName);
    }

    @Test
    void testProjectionPushdownWithNestedData()
            throws IOException
    {
        String tableName = "test_highly_nested_data_" + randomNameSuffix();
        index(tableName, ImmutableMap.<String, Object>builder()
                .put("id", 1)
                .put("row1_t", ImmutableMap.<String, Object>builder()
                        .put("f1", 2)
                        .put("f2", 3)
                        .put("row2_t", ImmutableMap.<String, Object>builder()
                                .put("f1", 4)
                                .put("f2", 5)
                                .put("row3_t", ImmutableMap.<String, Object>builder()
                                        .put("f1", 6)
                                        .put("f2", 7)
                                        .buildOrThrow())
                                .buildOrThrow())
                        .buildOrThrow())
                .buildOrThrow());
        index(tableName, ImmutableMap.<String, Object>builder()
                .put("id", 11)
                .put("row1_t", ImmutableMap.<String, Object>builder()
                        .put("f1", 12)
                        .put("f2", 13)
                        .put("row2_t", ImmutableMap.<String, Object>builder()
                                .put("f1", 14)
                                .put("f2", 15)
                                .put("row3_t", ImmutableMap.<String, Object>builder()
                                        .put("f1", 16)
                                        .put("f2", 17)
                                        .buildOrThrow())
                                .buildOrThrow())
                        .buildOrThrow())
                .buildOrThrow());
        index(tableName, ImmutableMap.<String, Object>builder()
                .put("id", 21)
                .put("row1_t", ImmutableMap.<String, Object>builder()
                        .put("f1", 22)
                        .put("f2", 23)
                        .put("row2_t", ImmutableMap.<String, Object>builder()
                                .put("f1", 24)
                                .put("f2", 25)
                                .put("row3_t", ImmutableMap.<String, Object>builder()
                                        .put("f1", 26)
                                        .put("f2", 27)
                                        .buildOrThrow())
                                .buildOrThrow())
                        .buildOrThrow())
                .buildOrThrow());

        // Test select projected columns, with and without their parent column
        assertThat(query("SELECT id, row1_t.row2_t.row3_t.f2 FROM " + tableName)).matches("VALUES (BIGINT '1', BIGINT '7'), (BIGINT '11', BIGINT '17'), (BIGINT '21', BIGINT '27')");
        assertThat(query("SELECT id, row1_t.row2_t.row3_t.f2, CAST(row1_t AS JSON) FROM " + tableName))
                .matches(
                        "VALUES (BIGINT '1', BIGINT '7', JSON '%s'), "
                                .formatted(
                                        """
                                        {
                                            "f1": 2,
                                            "f2": 3,
                                            "row2_t": {
                                                "f1": 4,
                                                "f2": 5,
                                                "row3_t": {
                                                    "f1": 6,
                                                    "f2": 7
                                                }
                                            }
                                        }
                                        """) +
                                "(BIGINT '11', BIGINT '17', JSON '%s'), "
                                        .formatted(
                                                """
                                                {
                                                    "f1": 12,
                                                    "f2": 13,
                                                    "row2_t": {
                                                        "f1": 14,
                                                        "f2": 15,
                                                        "row3_t": {
                                                            "f1": 16,
                                                            "f2": 17
                                                        }
                                                    }
                                                }
                                                """) +
                                "(BIGINT '21', BIGINT '27', JSON '%s')"
                                        .formatted(
                                                """
                                                {
                                                    "f1": 22,
                                                    "f2": 23,
                                                    "row2_t": {
                                                        "f1": 24,
                                                        "f2": 25,
                                                        "row3_t": {
                                                            "f1": 26,
                                                            "f2": 27
                                                        }
                                                    }
                                                }
                                                """));

        // Test predicates on immediate child column and deeper nested column
        assertThat(query("SELECT id, CAST(row1_t.row2_t.row3_t AS JSON) FROM " + tableName + " WHERE row1_t.row2_t.row3_t.f2 = 27"))
                .matches("VALUES (BIGINT '21', JSON '%s')"
                        .formatted(
                                """
                                {
                                    "f1": 26,
                                    "f2": 27
                                }
                                """));
        assertThat(query("SELECT id, CAST(row1_t.row2_t.row3_t AS JSON) FROM " + tableName + " WHERE row1_t.row2_t.row3_t.f2 > 20"))
                .matches("VALUES (BIGINT '21', JSON '%s')"
                        .formatted(
                                """
                                {
                                    "f1": 26,
                                    "f2": 27
                                }
                                """));
        assertThat(query("SELECT id, CAST(row1_t AS JSON) FROM " + tableName + " WHERE row1_t.row2_t.row3_t.f2 = 27"))
                .matches("VALUES (BIGINT '21', JSON '%s')"
                        .formatted(
                                """
                                {
                                    "f1": 22,
                                    "f2": 23,
                                    "row2_t": {
                                        "f1": 24,
                                        "f2": 25,
                                        "row3_t": {
                                            "f1": 26,
                                            "f2": 27
                                        }
                                    }
                                }
                                """));
        assertThat(query("SELECT id, CAST(row1_t AS JSON) FROM " + tableName + " WHERE row1_t.row2_t.row3_t.f2 > 20"))
                .matches("VALUES (BIGINT '21', JSON '%s')"
                        .formatted(
                                """
                                {
                                    "f1": 22,
                                    "f2": 23,
                                    "row2_t": {
                                        "f1": 24,
                                        "f2": 25,
                                        "row3_t": {
                                            "f1": 26,
                                            "f2": 27
                                        }
                                    }
                                }
                                """));

        // Test predicates on parent columns
        assertThat(query("SELECT id, row1_t.row2_t.row3_t.f1 FROM " + tableName + " WHERE row1_t.row2_t.row3_t = ROW(16, 17)"))
                .matches("VALUES (BIGINT '11', BIGINT '16')");
        assertThat(query("SELECT id, row1_t.row2_t.row3_t.f1 FROM " + tableName + " WHERE row1_t = ROW(22, 23, ROW(24, 25, ROW(26, 27)))"))
                .matches("VALUES (BIGINT '21', BIGINT '26')");

        deleteIndex(tableName);
    }

    @Test
    void testDereferencePushdownWithNestedFieldsIncludingArrays()
            throws IOException
    {
        String tableName = "test_dereference_pushdown_" + randomNameSuffix();
        index(tableName, ImmutableMap.<String, Object>builder()
                .put("array_string_field", ImmutableList.<String>builder()
                        .addAll(Stream.of("trino", "the", "lean", "machine-ohs")::iterator)
                        .build())
                .put("object_field_outer", ImmutableMap.<String, Object>builder()
                        .put("array_string_field", ImmutableList.<String>builder()
                                .addAll(Stream.of("trino", "the", "lean", "machine-ohs")::iterator)
                                .build())
                        .put("string_field_outer", "sample")
                        .put("int_field_outer", 44)
                        .put("object_field_inner", ImmutableMap.<String, Object>builder()
                                .put("int_field_inner", 432)
                                .buildOrThrow())
                        .buildOrThrow())
                .put("long_field", 314159265359L)
                .put("id_field", "564e6982-88ee-4498-aa98-df9e3f6b6109")
                .put("timestamp_field", "1987-09-17T06:22:48.000Z")
                .put("object_field", ImmutableMap.<String, Object>builder()
                        .put("array_string_field", ImmutableList.<String>builder()
                                .addAll(Stream.of("trino", "the", "lean", "machine-ohs")::iterator)
                                .build())
                        .put("string_field", "sample")
                        .put("int_field", 2)
                        .put("object_field_2", ImmutableMap.<String, Object>builder()
                                .put("array_string_field", ImmutableList.<String>builder()
                                        .addAll(Stream.of("trino", "the", "lean", "machine-ohs")::iterator)
                                        .build())
                                .put("string_field2", "sample")
                                .put("int_field", 33)
                                .put("object_field_3", ImmutableMap.<String, Object>builder()
                                        .put("array_string_field", ImmutableList.<String>builder()
                                                .addAll(Stream.of("trino", "the", "lean", "machine-ohs")::iterator)
                                                .build())
                                        .put("string_field3", "some value")
                                        .put("int_field3", 55)
                                        .buildOrThrow())
                                .buildOrThrow())
                        .buildOrThrow())
                .buildOrThrow());

        HashMap<String, Object> innerRecord = new HashMap<>();
        innerRecord.put("object_field_inner", null);
        index(tableName, ImmutableMap.<String, Object>builder()
                .put("long_field", 11122233L)
                .put("object_field_outer", innerRecord)
                .buildOrThrow());

        assertThat(query("select id_field, object_field.object_field_2.object_field_3.string_field3 from " + tableName + " where  object_field_outer.int_field_outer=44"))
                .skippingTypesCheck()
                .matches("VALUES ('564e6982-88ee-4498-aa98-df9e3f6b6109', 'some value')");
        assertThat(query("select object_field_outer.int_field_outer from " + tableName + " where  object_field_outer.int_field_outer=44"))
                .matches("VALUES CAST(44 as BIGINT)");
        assertThat(query("select long_field, id_field, object_field.object_field_2.object_field_3.string_field3, object_field_outer.object_field_inner.int_field_inner from " + tableName + " where  long_field=11122233"))
                .skippingTypesCheck()
                .matches("VALUES (CAST(11122233 AS BIGINT), NULL, NULL, NULL)");
        deleteIndex(tableName);
    }

    @Test
    public void testWildcardTableSameSchema()
            throws IOException
    {
        String suffix = randomNameSuffix();
        String firstIndex = format("test_wildcard_%s_1", suffix);
        String secondIndex = format("test_wildcard_%s_2", suffix);
        String wildcardTable = format("test_wildcard_%s_*", suffix);

        String mappings =
                """
                {
                    "properties": {
                      "long_column":      { "type": "long" },
                      "text_column":      { "type": "text" }
                    }
                }
                """;

        createIndex(firstIndex, mappings);
        index(firstIndex, ImmutableMap.<String, Object>builder()
                .put("long_column", 1L)
                .put("text_column", "Trino")
                .buildOrThrow());
        createIndex(secondIndex, mappings);
        index(secondIndex, ImmutableMap.<String, Object>builder()
                .put("long_column", 2L)
                .put("text_column", "rocks")
                .buildOrThrow());

        try {
            assertThat(query("DESCRIBE \"" + wildcardTable + "\""))
                    .skippingTypesCheck()
                    .matches("VALUES ('long_column', 'bigint', '', ''), ('text_column', 'varchar', '', '')");
            assertThat(query("SELECT * FROM \"" + wildcardTable + "\""))
                    .matches("VALUES (BIGINT '1', VARCHAR 'Trino'), (BIGINT '2', VARCHAR 'rocks')");

            // Unsupported operations
            assertQueryFails("DROP TABLE \"" + wildcardTable + "\"", "This connector does not support dropping tables");
            assertQueryFails("INSERT INTO \"" + wildcardTable + "\" VALUES (3, 'SQL')", "This connector does not support inserts");
            assertQueryFails("ALTER TABLE \"" + wildcardTable + "\" ADD COLUMN new_column INT", "This connector does not support adding columns");
            assertQueryFails("ALTER TABLE \"" + wildcardTable + "\" RENAME TO new_wildcard_table", "This connector does not support renaming tables");
        }
        finally {
            deleteIndex(firstIndex);
            deleteIndex(secondIndex);
        }
    }

    protected void assertTableDoesNotExist(String name)
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        assertQueryReturnsEmptyResult(format("SELECT * FROM information_schema.columns WHERE table_name = '%s'", name));
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).doesNotContain(name);
        assertQueryFails("SELECT * FROM " + name, ".*Table '" + catalogName + ".tpch." + name + "' does not exist");
    }

    @Test
    public void testMultiShardTopNAndTypedAggregationPagination()
            throws Exception
    {
        String tableName = "multishard_pushdown_" + randomNameSuffix();
        Request create = new Request("PUT", "/" + tableName);
        create.setJsonEntity(
                """
                {
                    "settings": {"number_of_shards": 3, "number_of_replicas": 0},
                    "mappings": {"properties": {
                        "id": {"type": "long"},
                        "sort_key": {"type": "integer"},
                        "boolean_key": {"type": "boolean"},
                        "real_key": {"type": "float"},
                        "bigint_key": {"type": "long"},
                        "date_key": {"type": "date"}
                    }}
                }
                """);
        client.performRequest(create);
        try {
            StringBuilder documents = new StringBuilder();
            for (int id = 0; id < 5_004; id++) {
                Map<String, Object> document = new HashMap<>();
                document.put("id", id);
                document.put("real_key", (id % 4) * 0.25);
                document.put("bigint_key", 9_007_199_254_740_993L + id % 3);
                document.put("date_key", 1_600_000_000_000L + (id % 3) * 1_000L);
                if (id % 7 != 0) {
                    document.put("sort_key", id % 5);
                    document.put("boolean_key", id % 2 == 0);
                }
                documents.append("{\"index\":{\"_id\":\"").append(id).append("\"}}\n")
                        .append(new JsonMapper().writeValueAsString(document)).append('\n');
            }
            Request bulk = new Request("POST", "/" + tableName + "/_bulk?refresh=true");
            bulk.setJsonEntity(documents.toString());
            client.performRequest(bulk);
            assertThat(computeActual("SELECT count(*) FROM " + tableName).getOnlyValue()).isEqualTo(5_004L);

            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();
            for (String order : List.of("ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST")) {
                String sql = "SELECT id, sort_key FROM " + tableName + " ORDER BY sort_key " + order + ", id DESC LIMIT 1005";
                assertThat(query(sql))
                        .ordered()
                        .matches(withoutPushdown, sql)
                        .isNotFullyPushedDown(TopNNode.class);
                assertExplain("EXPLAIN " + sql, "TopNPartial\\[count = 1005");
            }

            try (QueryAssertions assertions = new QueryAssertions(createAdHocQueryRunner(Map.of("elasticsearch.aggregation-page-size", "2")))) {
                for (String key : List.of("boolean_key", "bigint_key", "date_key")) {
                    String sql = "SELECT " + key + ", count(*) FROM " + tableName + " GROUP BY " + key;
                    assertThat(assertions.query(sql))
                            .matches(withoutPushdown, sql)
                            .isFullyPushedDown();
                }
                String realGrouping = "SELECT real_key, count(*) FROM " + tableName + " GROUP BY real_key";
                assertThat(assertions.query(realGrouping))
                        .matches(withoutPushdown, realGrouping)
                        .isNotFullyPushedDown(AggregationNode.class);
                String sql = "SELECT boolean_key, date_key, count(*) FROM " + tableName + " GROUP BY boolean_key, date_key";
                assertThat(assertions.query(sql))
                        .matches(withoutPushdown, sql)
                        .isFullyPushedDown();
            }
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testAggregationAliasesDoNotReplaceGroupingColumns()
            throws IOException
    {
        String tableName = "aggregation_alias_collision_" + randomNameSuffix();
        createIndex(tableName,
                """
                {"properties": {"_efgnrtd_0": {"type": "keyword"}, "__efgnrtd_0": {"type": "keyword"}}}
                """);
        try {
            index(tableName, Map.of("_efgnrtd_0", "a", "__efgnrtd_0", "x"));
            index(tableName, Map.of("_efgnrtd_0", "a", "__efgnrtd_0", "x"));
            index(tableName, Map.of("_efgnrtd_0", "b", "__efgnrtd_0", "y"));
            assertThat(query("SELECT _efgnrtd_0, __efgnrtd_0, count(*) FROM " + tableName + " GROUP BY _efgnrtd_0, __efgnrtd_0"))
                    .matches("VALUES (VARCHAR 'a', VARCHAR 'x', BIGINT '2'), (VARCHAR 'b', VARCHAR 'y', BIGINT '1')")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testFloatingPointTopNAndGroupingPreserveSignedZero()
            throws IOException
    {
        String tableName = "floating_point_signed_zero_" + randomNameSuffix();
        createIndex(tableName,
                """
                {"properties": {"id": {"type": "long"}, "d": {"type": "double"}, "r": {"type": "float"}}}
                """);
        try {
            index(tableName, Map.of("id", 1, "d", -0.0, "r", -0.0));
            index(tableName, Map.of("id", 2, "d", 0.0, "r", 0.0));
            Session withoutPushdown = Session.builder(getSession())
                    .setSystemProperty("allow_pushdown_into_connectors", "false")
                    .build();
            for (String column : List.of("d", "r")) {
                String grouping = "SELECT count(*) FROM " + tableName + " GROUP BY " + column;
                assertThat(query(grouping))
                        .matches("VALUES BIGINT '2'")
                        .isNotFullyPushedDown(AggregationNode.class);
                assertThat(query(withoutPushdown, grouping)).matches("VALUES BIGINT '2'");

                String ascending = "SELECT id FROM " + tableName + " ORDER BY " + column + " ASC, id DESC LIMIT 1";
                assertThat(query(ascending))
                        .matches("VALUES BIGINT '2'")
                        .isNotFullyPushedDown(TopNNode.class);
                assertThat(query(withoutPushdown, ascending)).matches("VALUES BIGINT '2'");

                String descending = "SELECT id FROM " + tableName + " ORDER BY " + column + " DESC, id ASC LIMIT 1";
                assertThat(query(descending))
                        .matches("VALUES BIGINT '1'")
                        .isNotFullyPushedDown(TopNNode.class);
                assertThat(query(withoutPushdown, descending)).matches("VALUES BIGINT '1'");

                for (String predicate : List.of("= 0.0", ">= 0.0", "<= 0.0", ">= -0.0", "<= -0.0")) {
                    String predicateQuery = "SELECT id FROM " + tableName + " WHERE " + column + " " + predicate;
                    assertThat(query(predicateQuery))
                            .matches("VALUES BIGINT '1', BIGINT '2'")
                            .isNotFullyPushedDown(FilterNode.class);
                    assertThat(query(withoutPushdown, predicateQuery)).matches("VALUES BIGINT '1', BIGINT '2'");
                }
                for (String predicate : List.of("> 0.0", "< 0.0", "> -0.0", "< -0.0")) {
                    String predicateQuery = "SELECT id FROM " + tableName + " WHERE " + column + " " + predicate;
                    assertThat(query(predicateQuery))
                            .matches("SELECT CAST(NULL AS BIGINT) WHERE false")
                            .isNotFullyPushedDown(FilterNode.class);
                    assertThat(query(withoutPushdown, predicateQuery)).matches("SELECT CAST(NULL AS BIGINT) WHERE false");
                }

                assertThat(query("SELECT id FROM " + tableName + " WHERE " + column + " < 1.0"))
                        .matches("VALUES BIGINT '1', BIGINT '2'")
                        .isFullyPushedDown();
            }
        }
        finally {
            deleteIndex(tableName);
        }
    }

    @Test
    public void testFloatingPointAggregationOverflow()
            throws IOException
    {
        String tableName = "floating_point_aggregation_overflow_" + randomNameSuffix();
        createIndex(tableName,
                """
                {"properties": {"group_key": {"type": "keyword"}, "value": {"type": "double"}}}
                """);
        try {
            for (String group : List.of("positive", "negative")) {
                double value = group.equals("positive") ? Double.MAX_VALUE : -Double.MAX_VALUE;
                index(tableName, Map.of("group_key", group, "value", value));
                index(tableName, Map.of("group_key", group, "value", value));
            }
            assertThat(query("SELECT group_key, SUM(value), AVG(value) FROM " + tableName + " GROUP BY group_key"))
                    .matches("VALUES (VARCHAR 'positive', infinity(), infinity()), (VARCHAR 'negative', -infinity(), -infinity())")
                    .isFullyPushedDown();
            assertThat(query("SELECT SUM(value), AVG(value) FROM " + tableName + " WHERE group_key = 'positive'"))
                    .matches("VALUES (infinity(), infinity())")
                    .isFullyPushedDown();
        }
        finally {
            deleteIndex(tableName);
        }
    }

    protected String indexEndpoint(String index, String docId)
    {
        return format("/%s/_doc/%s", index, docId);
    }

    private void index(String index, Map<String, Object> document)
            throws IOException
    {
        String json = new JsonMapper().writeValueAsString(document);
        String endpoint = format("%s?refresh", indexEndpoint(index, String.valueOf(System.nanoTime())));

        Request request = new Request("PUT", endpoint);
        request.setJsonEntity(json);

        client.performRequest(request);
    }

    private void index(String index, String routing, Map<String, Object> document)
            throws IOException
    {
        String json = new JsonMapper().writeValueAsString(document);
        String endpoint = format("%s?routing=%s&refresh", indexEndpoint(index, String.valueOf(System.nanoTime())), routing);

        Request request = new Request("PUT", endpoint);
        request.setJsonEntity(json);
        client.performRequest(request);
    }

    private int shardForRouting(String index, String routing)
            throws IOException
    {
        Request request = new Request("GET", format("/%s/_search_shards?routing=%s", index, routing));
        JsonNode response = new JsonMapper().readTree(client.performRequest(request).getEntity().getContent());
        return response.path("shards").get(0).get(0).path("shard").asInt();
    }

    private long shardQueryCount(List<String> indices)
            throws IOException
    {
        Request request = new Request("GET", format("/%s/_stats/search", String.join(",", indices)));
        JsonNode response = new JsonMapper().readTree(client.performRequest(request).getEntity().getContent());
        return response.path("_all").path("total").path("search").path("query_total").asLong();
    }

    private void addAlias(String index, String alias)
            throws IOException
    {
        client
                .performRequest(new Request("PUT", format("/%s/_alias/%s", index, alias)));

        refreshIndex(alias);
    }

    protected String indexMapping(@Language("JSON") String properties)
    {
        return "{\"mappings\": " + properties + "}";
    }

    private void createIndex(String indexName)
            throws IOException
    {
        client.performRequest(new Request("PUT", "/" + indexName));
    }

    private void createIndexWithBody(String indexName, @Language("JSON") String body)
            throws IOException
    {
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(body);
        client.performRequest(request);
    }

    private void createIndex(String indexName, @Language("JSON") String properties)
            throws IOException
    {
        String mappings = indexMapping(properties);

        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(mappings);

        client.performRequest(request);
    }

    private void refreshIndex(String index)
            throws IOException
    {
        client.performRequest(new Request("GET", format("/%s/_refresh", index)));
    }

    private void deleteIndex(String indexName)
            throws IOException
    {
        client.performRequest(new Request("DELETE", "/" + indexName));
    }
}
