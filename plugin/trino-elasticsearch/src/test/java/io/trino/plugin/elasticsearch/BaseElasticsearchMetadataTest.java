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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Catalog-wide Elasticsearch metadata tests run on a server that is not shared with the parallel connector suite.
 *
 * <p>These tests intentionally enumerate the global Elasticsearch index namespace or temporarily expose metadata that
 * is invalid for Trino. Keeping them in a dedicated context prevents them from observing transient indices owned by
 * unrelated tests and prevents intentionally invalid fixtures from leaking into unrelated metadata scans.</p>
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public abstract class BaseElasticsearchMetadataTest
        extends AbstractTestQueryFramework
{
    private ElasticsearchServer server;
    private RestClient client;

    protected BaseElasticsearchMetadataTest(ElasticsearchServer server)
    {
        this.server = requireNonNull(server, "server is null");
        this.client = server.getClient();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return ElasticsearchQueryRunner.builder(server)
                .setInitialTables(ImmutableList.of(NATION, ORDERS, REGION))
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

    @Test
    public void testSelectInformationSchemaTables()
    {
        assertInformationSchemaTables();
    }

    @Test
    public void testSelectInformationSchemaColumns()
    {
        assertInformationSchemaColumns();
    }

    @Test
    public void testSelectInformationSchemaForMultiIndexAlias()
            throws IOException
    {
        String aliasName = "multi_alias_" + randomNameSuffix();
        addAlias("nation", aliasName);
        addAlias("region", aliasName);
        try {
            assertInformationSchemaTables();
            assertInformationSchemaColumns();
        }
        finally {
            removeAlias("nation", aliasName);
            removeAlias("region", aliasName);
        }
    }

    @Test
    public void testAsRawJsonAndIsArraySameFieldException()
            throws IOException
    {
        String indexName = "raw_json_array_exception_" + randomNameSuffix();

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
        try {
            assertQueryReturnsEmptyResult("SHOW TABLES LIKE '" + indexName + "'");
            index(indexName, ImmutableMap.of("array_raw_field", "test"));

            assertThatThrownBy(() -> computeActual("SELECT array_raw_field FROM " + indexName))
                    .hasMessage("A column, (array_raw_field) cannot be declared as a Trino array and also be rendered as json.");
        }
        finally {
            deleteIndex(indexName);
        }
    }

    @Test
    public void testEmptyIndexWithMappings()
            throws IOException
    {
        String indexName = "test_empty_index_with_mappings_" + randomNameSuffix();

        @Language("JSON")
        String mappings =
                """
                {
                  "properties": {
                    "dummy_column": { "type": "long" }
                  }
                }
                """;

        createIndex(indexName, mappings);
        try {
            assertThat(query(format("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", indexName)))
                    .matches("VALUES VARCHAR 'dummy_column'");
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains(indexName);
            assertQueryReturnsEmptyResult("SELECT * FROM " + indexName);
        }
        finally {
            deleteIndex(indexName);
        }
    }

    @Test
    public void testEmptyIndexNoMappings()
            throws IOException
    {
        String indexName = "test_empty_index_" + randomNameSuffix();
        createIndex(indexName);
        try {
            assertTableDoesNotExist(indexName);
        }
        finally {
            deleteIndex(indexName);
        }
    }

    @Test
    public void testEmptyAliasNoMappings()
            throws IOException
    {
        String indexName = "test_empty_index_for_alias_" + randomNameSuffix();
        String aliasName = "test_empty_alias_" + randomNameSuffix();
        createIndex(indexName);
        try {
            addAlias(indexName, aliasName);
            assertTableDoesNotExist(aliasName);
        }
        finally {
            deleteIndex(indexName);
        }
    }

    @Test
    public void testMissingIndex()
    {
        assertTableDoesNotExist("nonexistent_table_" + randomNameSuffix());
    }

    private void assertInformationSchemaTables()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String schemaPattern = schema.replaceAll("^.", "_");

        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "' AND table_name = 'orders'", "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schema + "' AND table_name LIKE '%rders'", "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '%rders'", "VALUES 'orders'");
        assertQuery(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema LIKE '" + schema + "' AND table_name LIKE '%orders'",
                "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");

        assertQuery(
                "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema = 'information_schema' OR rand() = 42 ORDER BY 1",
                "VALUES " +
                        "('applicable_roles'), " +
                        "('columns'), " +
                        "('enabled_roles'), " +
                        "('roles'), " +
                        "('schemata'), " +
                        "('table_privileges'), " +
                        "('tables'), " +
                        "('views')");
    }

    private void assertInformationSchemaColumns()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String schemaPattern = schema.replaceAll(".$", "_");
        String ordersTableWithColumns = ordersTableWithColumns();

        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
        assertQuery("SELECT table_name FROM information_schema.columns WHERE table_name = 'orders' GROUP BY table_name", "VALUES 'orders'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '_rder_'", ordersTableWithColumns);
        assertThat(query(
                "SELECT table_name, column_name FROM information_schema.columns " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '%orders%'"))
                .skippingTypesCheck()
                .containsAll(ordersTableWithColumns);

        assertQuerySucceeds("SELECT * FROM information_schema.columns");
        assertQuery("SELECT DISTINCT table_name, column_name FROM information_schema.columns WHERE table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "'");
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_name LIKE '%'");
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");

        assertQuery(
                "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema = 'information_schema' OR rand() = 42 ORDER BY 1",
                "VALUES " +
                        "('applicable_roles'), " +
                        "('columns'), " +
                        "('enabled_roles'), " +
                        "('roles'), " +
                        "('schemata'), " +
                        "('table_privileges'), " +
                        "('tables'), " +
                        "('views')");
    }

    private static String ordersTableWithColumns()
    {
        return """
               VALUES
               ('orders', 'orderkey'),
               ('orders', 'custkey'),
               ('orders', 'orderstatus'),
               ('orders', 'totalprice'),
               ('orders', 'orderdate'),
               ('orders', 'orderpriority'),
               ('orders', 'clerk'),
               ('orders', 'shippriority'),
               ('orders', 'comment')
               """;
    }

    private void assertTableDoesNotExist(String name)
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        assertQueryReturnsEmptyResult(format("SELECT * FROM information_schema.columns WHERE table_name = '%s'", name));
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).doesNotContain(name);
        assertQueryFails("SELECT * FROM " + name, ".*Table '" + catalogName + ".tpch." + name + "' does not exist");
    }

    private void createIndex(String indexName)
            throws IOException
    {
        client.performRequest(new Request("PUT", "/" + indexName));
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

    private void addAlias(String index, String alias)
            throws IOException
    {
        client.performRequest(new Request("PUT", format("/%s/_alias/%s", index, alias)));
        refreshIndex(alias);
    }

    private void removeAlias(String index, String alias)
            throws IOException
    {
        client.performRequest(new Request("DELETE", format("/%s/_alias/%s", index, alias)));
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
