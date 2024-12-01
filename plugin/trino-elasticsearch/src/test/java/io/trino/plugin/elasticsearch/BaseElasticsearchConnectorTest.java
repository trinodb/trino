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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.testing.AbstractTestQueries;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestHighLevelClient;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
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
    private RestHighLevelClient client;

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
                .build();
    }

    @AfterAll
    public final void destroy()
            throws IOException
    {
        server.stop();
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
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
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
        assertQueryReturnsEmptyResult(format("SELECT 1 FROM jmx.current.\"trino.plugin.elasticsearch.client:*name=%s*\" WHERE \"backpressurestats.alltime.count\" > 0", catalogName));
        assertQueryReturnsEmptyResult(format("SELECT 1 FROM jmx.current.\"trino.plugin.elasticsearch.client:*name=%s*\" WHERE \"backpressurestats.alltime.max\" > 0", catalogName));
    }

    @Test
    @Override
    public void testSelectAll()
    {
        // List columns explicitly, as there's no defined order in Elasticsearch
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders");
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
                "nationkey::bigint", "::\\s\\[\\[42\\]\\]");
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

    @Test
    public void testNullPredicate()
            throws IOException
    {
        String indexName = "null_predicate1";
        @Language("JSON")
        String properties = """
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

        assertQuery("SELECT custkey, null_keyword FROM null_predicate1 WHERE null_keyword = '32' OR null_keyword IS NULL", "VALUES (1301, 32)");
        assertQuery("SELECT custkey FROM null_predicate1 WHERE null_keyword = '32' OR null_keyword IS NULL", "VALUES (1301)");

        // not null filter
        // filtered column is selected
        assertQuery("SELECT custkey, null_keyword FROM null_predicate1 WHERE null_keyword IS NOT NULL", "VALUES (1301, 32)");
        assertQuery("SELECT custkey, null_keyword FROM null_predicate1 WHERE null_keyword = '32' OR null_keyword IS NOT NULL", "VALUES (1301, 32)");

        // filtered column is not selected
        assertQuery("SELECT custkey FROM null_predicate1 WHERE null_keyword = '32' OR null_keyword IS NOT NULL", "VALUES (1301)");

        indexName = "null_predicate2";
        properties = """
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
        assertQuery("SELECT custkey, null_keyword FROM null_predicate2 WHERE null_keyword IS NULL", "VALUES (1301, NULL)");
        assertQuery("SELECT custkey, null_keyword FROM null_predicate2 WHERE null_keyword = '32' OR null_keyword IS NULL", "VALUES (1301, NULL)");

        // filtered column is not selected
        assertQuery("SELECT custkey FROM null_predicate2 WHERE null_keyword = '32' OR null_keyword IS NULL", "VALUES (1301)");

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("null_keyword", 32)
                .put("custkey", 1302)
                .buildOrThrow());

        assertQuery("SELECT custkey, null_keyword FROM null_predicate2 WHERE null_keyword = '32' OR null_keyword IS NULL", "VALUES (1301, NULL), (1302, 32)");
        assertQuery("SELECT custkey FROM null_predicate2 WHERE null_keyword = '32' OR null_keyword IS NULL", "VALUES (1301), (1302)");
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

        assertQuery(
                "SELECT name, fields.fielda, fields.fieldb FROM data",
                "VALUES ('nestfield', 32, 'valueb')");
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

        assertQuery(
                "SELECT * FROM name_conflict",
                "VALUES ('value')");
    }

    @Test
    public void testArrayFields()
            throws IOException
    {
        String indexName = "test_arrays";

        @Language("JSON")
        String mapping = """
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

        assertQuery(
                "SELECT a.b.y[1], c.f[1].g[2], c.f[2].g[1], j[2], k[1] FROM test_arrays",
                "VALUES ('hello', 20, 30, 60, NULL)");
    }

    @Test
    public void testAsRawJson()
            throws IOException
    {
        String indexName = "raw_json_" + randomNameSuffix();

        @Language("JSON")
        String mapping = """
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
        String mapping = """
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
        String mapping = """
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
        String mapping = """
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
        String mapping = """
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

        assertQuery(
                "SELECT a FROM test_mixed_arrays",
                "VALUES NULL, ARRAY['hello'], ARRAY['foo', 'bar']");
    }

    @Test
    public void testEmptyNumericFields()
            throws IOException
    {
        String indexName = "emptynumeric";

        @Language("JSON")
        String mapping = """
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

        assertQuery(
                "SELECT byte_column, short_column, integer_column, long_column, float_column, scaled_float_column, double_column FROM emptynumeric",
                "VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL)");

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

        assertQuery(
                "SELECT name, fields.fielda FROM emptyobject",
                "VALUES ('stringfield', 32)");
    }

    @Test
    public void testNestedVariants()
            throws IOException
    {
        String indexName = "nested_variants";

        index(indexName,
                ImmutableMap.of("a",
                        ImmutableMap.of("b",
                                ImmutableMap.of("c",
                                        "value1"))));

        index(indexName,
                ImmutableMap.of("a.b",
                        ImmutableMap.of("c",
                                "value2")));

        index(indexName,
                ImmutableMap.of("a",
                        ImmutableMap.of("b.c",
                                "value3")));

        index(indexName,
                ImmutableMap.of("a.b.c", "value4"));

        assertQuery(
                "SELECT a.b.c FROM nested_variants",
                "VALUES 'value1', 'value2', 'value3', 'value4'");
    }

    @Test
    public void testLike()
            throws IOException
    {
        String indexName = "like_test";

        @Language("JSON")
        String mappings = """
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
        String mappings = """
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
        String mappings = """
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
        String mappings = """
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

        assertThat(rows.getMaterializedRows()).containsExactlyInAnyOrderElementsOf(expected.getMaterializedRows());
    }

    @Test
    public void testTimestamps()
            throws IOException
    {
        String indexName = "timestamps";

        @Language("JSON")
        String mappings = """
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

        assertThat(rows.getMaterializedRows()).containsExactlyInAnyOrderElementsOf(expected.getMaterializedRows());
    }

    @Test
    public void testNestedTimestamps()
            throws IOException
    {
        String indexName = "nested_timestamps";

        @Language("JSON")
        String mappings = """
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
                        "(TIMESTAMP '1970-01-01 01:01:00.000')");
    }

    @Test
    public void testScaledFloat()
            throws Exception
    {
        String indexName = "scaled_float_type";

        @Language("JSON")
        String mappings = """
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
        String mappings = """
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
        String mappings = """
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
        assertQuery("SELECT count(*) FROM \"filter_pushdown: cool\" WHERE _score > 0", "VALUES 1");

        // boolean
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE boolean_column = true", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE boolean_column = false", "VALUES 0");

        // tinyint
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column = 1", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column = 0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column > 1", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column < 1", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column > 0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column < 10", "VALUES 1");

        // smallint
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column = 2", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column > 2", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column < 2", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column = 0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column > 0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column < 10", "VALUES 1");

        // integer
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column = 3", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column > 3", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column < 3", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column = 0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column > 0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column < 10", "VALUES 1");

        // bigint
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column = 4", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column > 4", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column < 4", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column = 0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column > 0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column < 10", "VALUES 1");

        // real
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column = 1.0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column > 1.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column < 1.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column = 0.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column > 0.0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column < 10.0", "VALUES 1");

        // double
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column = 1.0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column > 1.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column < 1.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column = 0.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column > 0.0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column < 10.0", "VALUES 1");

        // varchar
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE keyword_column = 'cool'", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE keyword_column = 'bar'", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE text_column = 'some text'", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE text_column = 'some'", "VALUES 0");

        // binary
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE binary_column = x'CAFE'", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE binary_column = x'ABCD'", "VALUES 0");

        // timestamp
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column = TIMESTAMP '2019-10-01 00:00:00'", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column > TIMESTAMP '2019-10-01 00:00:00'", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column < TIMESTAMP '2019-10-01 00:00:00'", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column = TIMESTAMP '2019-10-02 00:00:00'", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column > TIMESTAMP '2001-01-01 00:00:00'", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column < TIMESTAMP '2030-01-01 00:00:00'", "VALUES 1");

        // ipaddress
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE ipv4_column = IPADDRESS '1.2.3.4'", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE ipv6_column = IPADDRESS '2001:db8::1:0:0:1'", "VALUES 1");
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

        assertQuery("SELECT count(*) FROM filter_charset_pushdown WHERE keyword_column = 'Türkiye'", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_charset_pushdown WHERE keyword_column = 'bar'", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_charset_pushdown WHERE text_column = 'Türkiye'", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_charset_pushdown WHERE text_column = 'some'", "VALUES 0");

        assertQuery("SELECT keyword_column FROM filter_charset_pushdown WHERE keyword_column = 'Türkiye'", "VALUES ('Türkiye')");
        assertQuery("SELECT text_column FROM filter_charset_pushdown WHERE text_column = 'Türkiye'", "VALUES ('Türkiye')");
    }

    @Test
    public void testLimitPushdown()
            throws IOException
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isNotFullyPushedDown(LimitNode.class); // Use high limit for result determinism
    }

    @Test
    public void testDataTypesNested()
            throws IOException
    {
        String indexName = "types_nested";

        @Language("JSON")
        String properties = """
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
                "field",
                ImmutableMap.<String, Object>builder()
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
                .row(true, 1.0f, 1.0d, 1, 1L, "cool", "some text", new byte[] {(byte) 0xCA, (byte) 0xFE},
                        LocalDateTime.of(1970, 1, 1, 0, 0), "1.2.3.4", "2001:db8::1:0:0:1")
                .build();

        assertThat(rows.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testNestedTypeDataTypesNested()
            throws IOException
    {
        String indexName = "nested_type_nested";

        @Language("JSON")
        String mappings = """
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
                "nested_field",
                ImmutableMap.<String, Object>builder()
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
                .row(true, 1.0f, 1.0d, 1, 1L, "cool", "some text", new byte[] {(byte) 0xCA, (byte) 0xFE},
                        LocalDateTime.of(1970, 1, 1, 0, 0), "1.2.3.4", "2001:db8::1:0:0:1")
                .build();

        assertThat(rows.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());
    }

    @Test
    public void testQueryString()
    {
        assertQuery("SELECT count(*) FROM \"orders: +packages -slyly\"", "VALUES 1639");
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

        assertQuery(
                "SELECT name, age FROM mixed_case",
                "VALUES ('john', 32)");

        assertQuery(
                "SELECT name, age FROM mixed_case WHERE name = 'john'",
                "VALUES ('john', 32)");
    }

    @Test
    public void testNumericKeyword()
            throws IOException
    {
        String indexName = "numeric_keyword";
        @Language("JSON")
        String properties = """
                            {
                              "properties":{
                                "numeric_keyword":   { "type": "keyword" }
                              }
                            }
                            """;
        createIndex(indexName, properties);
        index(indexName, ImmutableMap.of("numeric_keyword", 20));

        assertQuery(
                "SELECT numeric_keyword FROM numeric_keyword",
                "VALUES 20");
        assertQuery(
                "SELECT numeric_keyword FROM numeric_keyword where numeric_keyword = '20'",
                "VALUES 20");
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

        assertQuery(
                "SELECT count(*) FROM " + aliasName,
                "SELECT count(*) FROM orders");
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

        assertQuery(
                "SELECT count(*) FROM multi_alias",
                "SELECT (SELECT count(*) FROM region) + (SELECT count(*) FROM nation)");
    }

    @Test
    public void testEmptyIndexWithMappings()
            throws IOException
    {
        String indexName = "test_empty_index_with_mappings";

        @Language("JSON")
        String mappings = """
                          {
                            "properties": {
                              "dummy_column":     { "type": "long" }
                            }
                          }
                          """;

        createIndex(indexName, mappings);

        assertQuery(format("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", indexName), "VALUES ('dummy_column')");
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
        assertQuery("SELECT json_query(result, 'lax $[0][0].hits.hits._source') " +
                        format("FROM TABLE(%s.system.raw_query(", catalogName) +
                        "schema => 'tpch', " +
                        "index => 'nation', " +
                        "query => '{\"query\": {\"match\": {\"name\": \"ALGERIA\"}}}')) t(result)",
                "VALUES '{\"nationkey\":0,\"name\":\"ALGERIA\",\"regionkey\":0,\"comment\":\" haggle. carefully final deposits detect slyly agai\"}'");

        // parameters
        Session session = Session.builder(getSession())
                .addPreparedStatement(
                        "my_query",
                        format("SELECT json_query(result, 'lax $[0][0].hits.hits._source') FROM TABLE(%s.system.raw_query(schema => ?, index => ?, query => ?))", catalogName))
                .build();
        assertQuery(
                session,
                "EXECUTE my_query USING 'tpch', 'nation', '{\"query\": {\"match\": {\"name\": \"ALGERIA\"}}}'",
                "VALUES '{\"nationkey\":0,\"name\":\"ALGERIA\",\"regionkey\":0,\"comment\":\" haggle. carefully final deposits detect slyly agai\"}'");

        // select multiple records by range. Use array wrapper to wrap multiple results
        assertQuery("SELECT array_sort(CAST(json_parse(json_query(result, 'lax $[0][0].hits.hits._source.name' WITH ARRAY WRAPPER)) AS array(varchar))) " +
                        format("FROM TABLE(%s.system.raw_query(", catalogName) +
                        "schema => 'tpch', " +
                        "index => 'nation', " +
                        "query => '{\"query\": {\"range\": {\"nationkey\": {\"gte\": 0,\"lte\": 3}}}}')) t(result)",
                "VALUES ARRAY['ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA']");

        // use aggregations
        @Language("JSON")
        String query = """
                       {
                           "size": 0,
                           "aggs" : {
                               "max_orderkey" : { "max" : { "field" : "orderkey" } },
                               "sum_orderkey" : { "sum" : { "field" : "orderkey" } }
                           }
                       }
                       """;

        assertQuery(
                """
                WITH data(r) AS (
                   SELECT CAST(json_parse(result) AS ROW(aggregations ROW(max_orderkey ROW(value BIGINT), sum_orderkey ROW(value BIGINT))))
                   FROM TABLE(%s.system.raw_query(
                                        schema => 'tpch',
                                        index => 'orders',
                                        query => '%s')))
                SELECT r.aggregations.max_orderkey.value, r.aggregations.sum_orderkey.value
                FROM data
                """.formatted(catalogName, query),
                "VALUES (60000, 449872500)");

        // no matches
        assertQuery("SELECT json_query(result, 'lax $[0][0].hits.hits') " +
                        format("FROM TABLE(%s.system.raw_query(", catalogName) +
                        "schema => 'tpch', " +
                        "index => 'nation', " +
                        "query => '{\"query\": {\"match\": {\"name\": \"UTOPIA\"}}}')) t(result)",
                "VALUES '[]'");

        // syntax error
        assertThat(query("SELECT * " +
                format("FROM TABLE(%s.system.raw_query(", catalogName) +
                "schema => 'tpch', " +
                "index => 'nation', " +
                "query => 'wrong syntax')) t(result)"))
                .failure().hasMessageContaining("json_parse_exception");
    }

    protected void assertTableDoesNotExist(String name)
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        assertQueryReturnsEmptyResult(format("SELECT * FROM information_schema.columns WHERE table_name = '%s'", name));
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).doesNotContain(name);
        assertQueryFails("SELECT * FROM " + name, ".*Table '" + catalogName + ".tpch." + name + "' does not exist");
    }

    protected String indexEndpoint(String index, String docId)
    {
        return format("/%s/_doc/%s", index, docId);
    }

    private void index(String index, Map<String, Object> document)
            throws IOException
    {
        String json = new ObjectMapper().writeValueAsString(document);
        String endpoint = format("%s?refresh", indexEndpoint(index, String.valueOf(System.nanoTime())));

        Request request = new Request("PUT", endpoint);
        request.setJsonEntity(json);

        client.getLowLevelClient().performRequest(request);
    }

    private void addAlias(String index, String alias)
            throws IOException
    {
        client.getLowLevelClient()
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
        client.getLowLevelClient().performRequest(new Request("PUT", "/" + indexName));
    }

    private void createIndex(String indexName, @Language("JSON") String properties)
            throws IOException
    {
        String mappings = indexMapping(properties);

        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(mappings);

        client.getLowLevelClient().performRequest(request);
    }

    private void refreshIndex(String index)
            throws IOException
    {
        client.getLowLevelClient().performRequest(new Request("GET", format("/%s/_refresh", index)));
    }

    private void deleteIndex(String indexName)
            throws IOException
    {
        client.getLowLevelClient().performRequest(new Request("DELETE", "/" + indexName));
    }
}
