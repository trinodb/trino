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
package io.prestosql.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

import static io.prestosql.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestElasticsearchIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private ElasticsearchServer elasticsearch;
    private RestHighLevelClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        elasticsearch = new ElasticsearchServer();

        HostAndPort address = elasticsearch.getAddress();
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(address.getHost(), address.getPort())));

        return createElasticsearchQueryRunner(elasticsearch.getAddress(), TpchTable.getTables(), ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        elasticsearch.stop();
        client.close();
    }

    @Test
    @Override
    public void testSelectAll()
    {
        // List columns explicitly, as there's no defined order in Elasticsearch
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("clerk", "varchar", "", "")
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderdate", "timestamp", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("totalprice", "real", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Override
    public void testShowCreateTable()
    {
        // TODO (https://github.com/prestosql/presto/issues/3385) Fix SHOW CREATE TABLE
        assertThatThrownBy(super::testShowCreateTable)
                .hasMessage("No PropertyMetadata for property: original-name");
        throw new SkipException("Fix SHOW CREATE TABLE");
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
                .build());

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
                .build());

        assertQuery(
                "SELECT * FROM name_conflict",
                "VALUES ('value')");
    }

    @Test
    public void testArrayFields()
            throws IOException
    {
        String indexName = "test_arrays";

        String mapping = "" +
                "{" +
                "  \"mappings\": {" +
                "    \"doc\": {" +
                "      \"_meta\": {" +
                "        \"presto\": {" +
                "          \"a\": {" +
                "            \"b\": {" +
                "              \"y\": {" +
                "                \"isArray\": true" +
                "              }" +
                "            }" +
                "          }," +
                "          \"c\": {" +
                "            \"f\": {" +
                "              \"g\": {" +
                "                \"isArray\": true" +
                "              }," +
                "              \"isArray\": true" +
                "            }" +
                "          }," +
                "          \"j\": {" +
                "            \"isArray\": true" +
                "          }," +
                "          \"k\": {" +
                "            \"isArray\": true" +
                "          }" +
                "        }" +
                "      }," +
                "      \"properties\":{" +
                "        \"a\": {" +
                "          \"type\": \"object\"," +
                "          \"properties\": {" +
                "            \"b\": {" +
                "              \"type\": \"object\"," +
                "              \"properties\": {" +
                "                \"x\": {" +
                "                  \"type\": \"integer\"" +
                "                }," +
                "                \"y\": {" +
                "                  \"type\": \"keyword\"" +
                "                }" +
                "              } " +
                "            }" +
                "          }" +
                "        }," +
                "        \"c\": {" +
                "          \"type\": \"object\"," +
                "          \"properties\": {" +
                "            \"d\": {" +
                "              \"type\": \"keyword\"" +
                "            }," +
                "            \"e\": {" +
                "              \"type\": \"keyword\"" +
                "            }," +
                "            \"f\": {" +
                "              \"type\": \"object\"," +
                "              \"properties\": {" +
                "                \"g\": {" +
                "                  \"type\": \"integer\"" +
                "                }," +
                "                \"h\": {" +
                "                  \"type\": \"integer\"" +
                "                }" +
                "              } " +
                "            }" +
                "          }" +
                "        }," +
                "        \"i\": {" +
                "          \"type\": \"long\"" +
                "        }," +
                "        \"j\": {" +
                "          \"type\": \"long\"" +
                "        }," +
                "        \"k\": {" +
                "          \"type\": \"long\"" +
                "        }" +
                "      }" +
                "    }" +
                "  }" +
                "}";

        createIndex(indexName, mapping);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("a", ImmutableMap.<String, Object>builder()
                        .put("b", ImmutableMap.<String, Object>builder()
                                .put("x", 1)
                                .put("y", ImmutableList.<String>builder()
                                        .add("hello")
                                        .add("world")
                                        .build())
                                .build())
                        .build())
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
                                        .build())
                                .add(ImmutableMap.<String, Object>builder()
                                        .put("g", ImmutableList.<Integer>builder()
                                                .add(30)
                                                .add(40)
                                                .build())
                                        .put("h", 200)
                                        .build())
                                .build())
                        .build())
                .put("j", ImmutableList.<Long>builder()
                        .add(50L)
                        .add(60L)
                        .build())
                .build());

        assertQuery(
                "SELECT a.b.y[1], c.f[1].g[2], c.f[2].g[1], j[2], k[1] FROM test_arrays",
                "VALUES ('hello', 20, 30, 60, NULL)");
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
                .build());

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
    public void testDataTypes()
            throws IOException
    {
        String indexName = "types";

        String mapping = "" +
                "{" +
                "  \"mappings\": {" +
                "    \"doc\": {" +
                "      \"properties\": {" +
                "        \"boolean_column\":   { \"type\": \"boolean\" }," +
                "        \"float_column\":     { \"type\": \"float\" }," +
                "        \"double_column\":    { \"type\": \"double\" }," +
                "        \"integer_column\":   { \"type\": \"integer\" }," +
                "        \"long_column\":      { \"type\": \"long\" }," +
                "        \"keyword_column\":   { \"type\": \"keyword\" }," +
                "        \"text_column\":      { \"type\": \"text\" }," +
                "        \"binary_column\":    { \"type\": \"binary\" }," +
                "        \"timestamp_column\": { \"type\": \"date\" }," +
                "        \"ipv4_column\": { \"type\": \"ip\" }," +
                "        \"ipv6_column\": { \"type\": \"ip\" }" +
                "      }" +
                "    }" +
                "  }" +
                "}";

        createIndex(indexName, mapping);

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
                .build());

        MaterializedResult rows = computeActual("" +
                "SELECT " +
                "boolean_column, " +
                "float_column, " +
                "double_column, " +
                "integer_column, " +
                "long_column, " +
                "keyword_column, " +
                "text_column, " +
                "binary_column, " +
                "timestamp_column, " +
                "ipv4_column, " +
                "ipv6_column " +
                "FROM types");

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(true, 1.0f, 1.0d, 1, 1L, "cool", "some text", new byte[] {(byte) 0xCA, (byte) 0xFE},
                        LocalDateTime.of(1970, 1, 1, 0, 0), "1.2.3.4", "2001:db8::1:0:0:1")
                .build();

        assertEquals(rows.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testFilters()
            throws IOException
    {
        String indexName = "filter_pushdown";

        String mapping = "" +
                "{" +
                "  \"mappings\": {" +
                "    \"doc\": {" +
                "      \"properties\": {" +
                "        \"boolean_column\":   { \"type\": \"boolean\" }," +
                "        \"float_column\":     { \"type\": \"float\" }," +
                "        \"double_column\":    { \"type\": \"double\" }," +
                "        \"integer_column\":   { \"type\": \"integer\" }," +
                "        \"long_column\":      { \"type\": \"long\" }," +
                "        \"keyword_column\":   { \"type\": \"keyword\" }," +
                "        \"text_column\":      { \"type\": \"text\" }," +
                "        \"binary_column\":    { \"type\": \"binary\" }," +
                "        \"timestamp_column\": { \"type\": \"date\" }," +
                "        \"ipv4_column\": { \"type\": \"ip\" }," +
                "        \"ipv6_column\": { \"type\": \"ip\" }" +
                "      }" +
                "    }" +
                "  }" +
                "}";

        createIndex(indexName, mapping);

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
                .build());

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
    public void testLimitPushdown()
            throws IOException
    {
        String indexName = "limit_pushdown";

        index(indexName, ImmutableMap.of("c1", "v1"));
        index(indexName, ImmutableMap.of("c1", "v2"));
        index(indexName, ImmutableMap.of("c1", "v3"));
        assertEquals(computeActual("SELECT * FROM limit_pushdown").getRowCount(), 3);
        assertEquals(computeActual("SELECT * FROM limit_pushdown LIMIT 1").getRowCount(), 1);
        assertEquals(computeActual("SELECT * FROM limit_pushdown LIMIT 2").getRowCount(), 2);
    }

    @Test
    public void testDataTypesNested()
            throws IOException
    {
        String indexName = "types_nested";

        String mapping = "" +
                "{" +
                "  \"mappings\": {" +
                "    \"doc\": {" +
                "      \"properties\": {" +
                "        \"field\": {" +
                "          \"properties\": {" +
                "            \"boolean_column\":   { \"type\": \"boolean\" }," +
                "            \"float_column\":     { \"type\": \"float\" }," +
                "            \"double_column\":    { \"type\": \"double\" }," +
                "            \"integer_column\":   { \"type\": \"integer\" }," +
                "            \"long_column\":      { \"type\": \"long\" }," +
                "            \"keyword_column\":   { \"type\": \"keyword\" }," +
                "            \"text_column\":      { \"type\": \"text\" }," +
                "            \"binary_column\":    { \"type\": \"binary\" }," +
                "            \"timestamp_column\": { \"type\": \"date\" }," +
                "            \"ipv4_column\": { \"type\": \"ip\" }," +
                "            \"ipv6_column\": { \"type\": \"ip\" }" +
                "          }" +
                "        }" +
                "      }" +
                "    }" +
                "  }" +
                "}";

        createIndex(indexName, mapping);

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
                        .build()));

        MaterializedResult rows = computeActual("" +
                "SELECT " +
                "field.boolean_column, " +
                "field.float_column, " +
                "field.double_column, " +
                "field.integer_column, " +
                "field.long_column, " +
                "field.keyword_column, " +
                "field.text_column, " +
                "field.binary_column, " +
                "field.timestamp_column, " +
                "field.ipv4_column, " +
                "field.ipv6_column " +
                "FROM types_nested");

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(true, 1.0f, 1.0d, 1, 1L, "cool", "some text", new byte[] {(byte) 0xCA, (byte) 0xFE},
                        LocalDateTime.of(1970, 1, 1, 0, 0), "1.2.3.4", "2001:db8::1:0:0:1")
                .build();

        assertEquals(rows.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testNestedTypeDataTypesNested()
            throws IOException
    {
        String indexName = "nested_type_nested";

        String mapping = "" +
                "{" +
                "  \"mappings\": {" +
                "    \"doc\": {" +
                "      \"properties\": {" +
                "        \"nested_field\": {" +
                "          \"type\":\"nested\"," +
                "          \"properties\": {" +
                "            \"boolean_column\":   { \"type\": \"boolean\" }," +
                "            \"float_column\":     { \"type\": \"float\" }," +
                "            \"double_column\":    { \"type\": \"double\" }," +
                "            \"integer_column\":   { \"type\": \"integer\" }," +
                "            \"long_column\":      { \"type\": \"long\" }," +
                "            \"keyword_column\":   { \"type\": \"keyword\" }," +
                "            \"text_column\":      { \"type\": \"text\" }," +
                "            \"binary_column\":    { \"type\": \"binary\" }," +
                "            \"timestamp_column\": { \"type\": \"date\" }," +
                "            \"ipv4_column\": { \"type\": \"ip\" }," +
                "            \"ipv6_column\": { \"type\": \"ip\" }" +
                "          }" +
                "        }" +
                "      }" +
                "    }" +
                "  }" +
                "}";

        createIndex(indexName, mapping);

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
                        .build()));

        MaterializedResult rows = computeActual("" +
                "SELECT " +
                "nested_field.boolean_column, " +
                "nested_field.float_column, " +
                "nested_field.double_column, " +
                "nested_field.integer_column, " +
                "nested_field.long_column, " +
                "nested_field.keyword_column, " +
                "nested_field.text_column, " +
                "nested_field.binary_column, " +
                "nested_field.timestamp_column, " +
                "nested_field.ipv4_column, " +
                "nested_field.ipv6_column " +
                "FROM nested_type_nested");

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(true, 1.0f, 1.0d, 1, 1L, "cool", "some text", new byte[] {(byte) 0xCA, (byte) 0xFE},
                        LocalDateTime.of(1970, 1, 1, 0, 0), "1.2.3.4", "2001:db8::1:0:0:1")
                .build();

        assertEquals(rows.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testQueryString()
    {
        MaterializedResult actual = computeActual("SELECT count(*) FROM \"orders: +packages -slyly\"");

        MaterializedResult expected = resultBuilder(getSession(), ImmutableList.of(BIGINT))
                .row(1639L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testMixedCase()
            throws IOException
    {
        String indexName = "mixed_case";
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("Name", "john")
                .put("AGE", 32)
                .build());

        assertQuery(
                "SELECT name, age FROM mixed_case",
                "VALUES ('john', 32)");

        assertQuery(
                "SELECT name, age FROM mixed_case WHERE name = 'john'",
                "VALUES ('john', 32)");
    }

    @Test
    public void testQueryStringError()
    {
        assertQueryFails("SELECT count(*) FROM \"orders: ++foo AND\"", "\\QFailed to parse query [ ++foo and]\\E");
    }

    @Test
    public void testAlias()
            throws IOException
    {
        addAlias("orders", "orders_alias");

        assertQuery(
                "SELECT count(*) FROM orders_alias",
                "SELECT count(*) FROM orders");
    }

    @Test
    public void testMultiIndexAlias()
            throws IOException
    {
        addAlias("nation", "multi_alias");
        addAlias("region", "multi_alias");

        assertQuery(
                "SELECT count(*) FROM multi_alias",
                "SELECT (SELECT count(*) FROM region) + (SELECT count(*) FROM nation)");
    }

    private void index(String index, Map<String, Object> document)
            throws IOException
    {
        client.index(new IndexRequest(index, "doc")
                .source(document)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
    }

    private void addAlias(String index, String alias)
            throws IOException
    {
        client.getLowLevelClient()
                .performRequest("PUT", format("/%s/_alias/%s", index, alias));

        refreshIndex(alias);
    }

    private void createIndex(String indexName, @Language("JSON") String mapping)
            throws IOException
    {
        client.getLowLevelClient()
                .performRequest("PUT", "/" + indexName, ImmutableMap.of(), new NStringEntity(mapping, ContentType.APPLICATION_JSON));
    }

    private void refreshIndex(String index)
            throws IOException
    {
        client.getLowLevelClient()
                .performRequest("GET", format("/%s/_refresh", index));
    }
}
