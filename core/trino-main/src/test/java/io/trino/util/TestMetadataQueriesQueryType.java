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
package io.trino.util;

import io.trino.Session;
import io.trino.SessionTestUtils;
import io.trino.client.ClientCapabilities;
import io.trino.execution.QueryPreparer;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.sql.parser.SqlParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.resourcegroups.QueryType.DESCRIBE;
import static io.trino.spi.resourcegroups.QueryType.SELECT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.StatementUtils.getQueryType;
import static java.util.Arrays.stream;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;

@TestInstance(PER_CLASS)
public class TestMetadataQueriesQueryType
{
    private Session session;
    private QueryPreparer queryPreparer;

    @BeforeAll
    public void init()
    {
        session = SessionTestUtils.TEST_SESSION;
        queryPreparer = new QueryPreparer(new SqlParser());
    }

    @AfterAll
    public void teardown()
    {
        session = null;
        queryPreparer = null;
    }

    @Test
    public void testMetadataQueriesQueryType()
    {
        PreparedQuery preparedQuery;
        preparedQuery = queryPreparer.prepareQuery(session, "SELECT current_user");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));

        preparedQuery = queryPreparer.prepareQuery(session, "SELECT version()");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));

        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "SELECT TABLE_CAT\n" +
                "FROM system.jdbc.catalogs\n" +
                "ORDER BY TABLE_CAT");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));

        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "SELECT TABLE_SCHEM, TABLE_CATALOG\n" +
                "FROM system.jdbc.schemas\n" +
                "ORDER BY TABLE_CATALOG, TABLE_SCHEM");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));

        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS,\n" +
                "  TYPE_CAT, TYPE_SCHEM, TYPE_NAME, " +
                "  SELF_REFERENCING_COL_NAME, REF_GENERATION\n" +
                "FROM system.jdbc.tables");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));

        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "SELECT TABLE_TYPE\n" +
                "FROM system.jdbc.table_types\n" +
                "ORDER BY TABLE_TYPE");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));

        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "SELECT *\n" +
                "FROM \"hive\".\"information_schema\".\"tables\" t\n" +
                "WHERE table_catalog LIKE '%'\n" +
                "UNION ALL \n" +
                "SELECT *\n" +
                "FROM \"system\".\"information_schema\".\"tables\" t\n" +
                "WHERE table_catalog LIKE '%'\n" +
                "UNION ALL \n" +
                "SELECT *\n" +
                "FROM \"tpcds\".\"information_schema\".\"tables\" t\n" +
                "WHERE table_catalog LIKE '%'\n" +
                "UNION ALL \n" +
                "SELECT *\n" +
                "FROM \"tpch\".\"information_schema\".\"tables\" t\n" +
                "WHERE table_catalog LIKE '%'\n");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));

        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "SELECT\n" +
                "  CASE WHEN mv.name IS NOT NULL THEN 'materialized_view'\n" +
                "  WHEN t.table_type = 'BASE TABLE' THEN 'table'\n" +
                "  WHEN t.table_type = 'VIEW' THEN 'view'\n" +
                "  ELSE t.table_type\n" +
                "  END AS table_type\n" +
                "FROM tpch.information_schema.tables t\n" +
                "LEFT JOIN system.metadata.materialized_views mv\n" +
                "  ON mv.catalog_name = t.table_catalog AND mv.schema_name = t.table_schema AND mv.name = t.table_name\n" +
                "WHERE t.table_schema = 'some_table_schema'\n" +
                "  AND (mv.catalog_name is null or mv.catalog_name =  'some_catalog')\n" +
                "  AND (mv.schema_name is null or mv.schema_name =  'some_schema')\n" +
                "  AND t.table_name = 'some_table'");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));

        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "WITH tables AS (\n" +
                "  SELECT\n" +
                "    table_catalog AS \"table_database\",\n" +
                "    table_schema AS \"table_schema\",\n" +
                "    table_name AS \"table_name\",\n" +
                "    table_type AS \"table_type\",\n" +
                "    null as \"table_owner\"\n" +
                "  FROM hive.information_schema.tables\n" +
                "  WHERE\n" +
                "    table_schema != 'information_schema'\n" +
                "    AND\n" +
                "    table_schema IN ('test')\n" +
                "),\n" +
                "columns AS (\n" +
                "  SELECT\n" +
                "    table_catalog AS \"table_database\",\n" +
                "    table_schema AS \"table_schema\",\n" +
                "    table_name AS \"table_name\",\n" +
                "    column_name AS \"column_name\",\n" +
                "    ordinal_position AS \"column_index\",\n" +
                "    data_type AS \"column_type\",\n" +
                "    comment AS \"column_comment\"\n" +
                "  FROM hive.information_schema.columns\n" +
                "  WHERE\n" +
                "    table_schema != 'information_schema'\n" +
                "    AND\n" +
                "    table_schema IN ('test')\n" +
                "),\n" +
                "table_comment AS (\n" +
                "  SELECT\n" +
                "    catalog_name AS \"table_database\",\n" +
                "    schema_name AS \"table_schema\",\n" +
                "    table_name AS \"table_name\",\n" +
                "    comment AS \"table_comment\"\n" +
                "  FROM system.metadata.table_comments\n" +
                "  WHERE\n" +
                "    catalog_name = 'hive'\n" +
                "    AND\n" +
                "    schema_name != 'information_schema'\n" +
                "    AND\n" +
                "    schema_name = 'test'\n" +
                ")\n" +
                "SELECT\n" +
                "  table_database,\n" +
                "  table_schema,\n" +
                "  table_name,\n" +
                "  table_type,\n" +
                "  table_owner,\n" +
                "  column_name,\n" +
                "  column_index,\n" +
                "  column_type,\n" +
                "  column_comment,\n" +
                "  table_comment\n" +
                "FROM tables\n" +
                "JOIN columns USING (\"table_database\", \"table_schema\", \"table_name\")\n" +
                "JOIN table_comment USING (\"table_database\", \"table_schema\", \"table_name\")\n" +
                "ORDER BY 1,2,3");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));

        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "WITH info AS (\n" +
                "  SELECT table_catalog, table_schema, table_name\n" +
                "  FROM tpch.information_schema.tables t\n" +
                "  WHERE table_type = 'BASE TABLE'\n" +
                "    AND table_catalog = 'tpch'\n" +
                "    AND table_schema = 'tiny'\n" +
                "    AND table_name = 'nation'\n" +
                ")\n" +
                "SELECT i.table_catalog, i.table_schema, i.table_name\n" +
                "FROM info i\n");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));

        Session tempSession = getSessionWithCatalog("system");
        preparedQuery = queryPreparer.prepareQuery(tempSession, "" +
                "SELECT TABLE_CAT\n" +
                "FROM jdbc.catalogs\n" +
                "ORDER BY TABLE_CAT");
        assertEquals(getQueryType(tempSession, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));
        tempSession = null;

        tempSession = getSessionWithCatalogAndSchema("system", "jdbc");
        preparedQuery = queryPreparer.prepareQuery(tempSession, "" +
                "SELECT TABLE_CAT\n" +
                "FROM catalogs\n" +
                "ORDER BY TABLE_CAT");
        assertEquals(getQueryType(tempSession, preparedQuery.getStatement()), Optional.ofNullable(DESCRIBE));
        tempSession = null;
    }

    @Test
    public void testNonMetadataQueriesQueryType()
    {
        PreparedQuery preparedQuery;
        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "SELECT custkey, name, mktsegment\n" +
                "FROM tpch.tiny.customer");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(SELECT));

        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "SELECT 1 AS col1\n" +
                "FROM system.jdbc.catalogs c\n" +
                "UNION\n" +
                "SELECT 1 AS col1\n" +
                "FROM tpch.tiny.nation");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(SELECT));

        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "WITH info AS (\n" +
                "  SELECT table_catalog, table_schema, table_name\n" +
                "  FROM tpch.information_schema.tables t\n" +
                "  WHERE table_type = 'BASE TABLE'\n" +
                "    AND table_catalog = 'tpch'\n" +
                "    AND table_schema = 'tiny'\n" +
                "    AND table_name = 'nation'\n" +
                ")\n" +
                "SELECT i.table_catalog, i.table_schema, i.table_name, n.nationkey, n.name, n.regionkey\n" +
                "FROM tpch.tiny.nation n\n" +
                "CROSS JOIN info i");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(SELECT));

        Session tempSession = getSessionWithCatalog("hive");
        preparedQuery = queryPreparer.prepareQuery(tempSession, "" +
                "SELECT TABLE_CAT\n" +
                "FROM jdbc.catalogs\n" +
                "ORDER BY TABLE_CAT");
        assertEquals(getQueryType(tempSession, preparedQuery.getStatement()), Optional.ofNullable(SELECT));
        tempSession = null;

        preparedQuery = queryPreparer.prepareQuery(session, "" +
                "SELECT * FROM TABLE(postgres.system.query(query => 'SELECT 1'))");
        assertEquals(getQueryType(session, preparedQuery.getStatement()), Optional.ofNullable(SELECT));
    }

    private Session getSessionWithCatalog(String catalog)
    {
        return testSessionBuilder()
                .setCatalog(catalog)
                .setClientCapabilities(stream(ClientCapabilities.values())
                        .map(ClientCapabilities::toString)
                        .collect(toImmutableSet()))
                .build();
    }

    private Session getSessionWithCatalogAndSchema(String catalog, String schema)
    {
        return testSessionBuilder()
                .setCatalog(catalog)
                .setSchema(schema)
                .setClientCapabilities(stream(ClientCapabilities.values())
                        .map(ClientCapabilities::toString)
                        .collect(toImmutableSet()))
                .build();
    }
}
