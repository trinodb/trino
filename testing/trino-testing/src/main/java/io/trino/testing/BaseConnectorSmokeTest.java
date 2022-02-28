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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.regex.Pattern;

import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_UPDATE;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A connector smoke test exercising various connector functionalities without going in depth on any of them.
 * A connector should implement {@link BaseConnectorTest} and use this class to exercise some configuration variants.
 */
public abstract class BaseConnectorSmokeTest
        extends AbstractTestQueryFramework
{
    protected static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableList.of(NATION, REGION);

    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return connectorBehavior.hasBehaviorByDefault(this::hasBehavior);
    }

    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA " + schemaName;
    }

    /**
     * Ensure the tests are run with {@link DistributedQueryRunner}. E.g. {@link LocalQueryRunner} takes some
     * shortcuts, not exercising certain aspects.
     */
    @Test
    public void ensureDistributedQueryRunner()
    {
        assertThat(getQueryRunner().getNodeCount()).as("query runner node count")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    @Override
    public void ensureTestNamingConvention()
    {
        // Enforce a naming convention to make code navigation easier.
        assertThat(getClass().getName())
                .endsWith("ConnectorSmokeTest");
    }

    @Test
    public void testSelect()
    {
        assertQuery("SELECT name FROM region");
    }

    @Test
    public void testPredicate()
    {
        assertQuery("SELECT name, regionkey FROM nation WHERE nationkey = 10");
        assertQuery("SELECT name, regionkey FROM nation WHERE nationkey BETWEEN 5 AND 15");
        assertQuery("SELECT name, regionkey FROM nation WHERE name = 'EGYPT'");
    }

    @Test
    public void testLimit()
    {
        assertQuery("SELECT name FROM region LIMIT 5");
    }

    @Test
    public void testTopN()
    {
        assertQuery("SELECT regionkey FROM nation ORDER BY name LIMIT 3");
    }

    @Test
    public void testAggregation()
    {
        assertQuery("SELECT sum(regionkey) FROM nation");
        assertQuery("SELECT sum(nationkey) FROM nation GROUP BY regionkey");
    }

    @Test
    public void testHaving()
    {
        assertQuery("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 58", "VALUES (4, 58)");
    }

    @Test
    public void testJoin()
    {
        assertQuery("SELECT n.name, r.name FROM nation n JOIN region r on n.regionkey = r.regionkey");
    }

    @Test
    public void testCreateTable()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            assertQueryFails("CREATE TABLE xxxx (a bigint, b double)", "This connector does not support creating tables");
            return;
        }

        String tableName = "test_create_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double)");
        assertThat(query("SELECT a, b FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableAsSelect()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            assertQueryFails("CREATE TABLE xxxx AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", "This connector does not support creating tables with data");
            return;
        }

        String tableName = "test_create_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + tableName))
                .matches("VALUES (BIGINT '42', -385e-1)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsert()
    {
        if (!hasBehavior(SUPPORTS_INSERT)) {
            assertQueryFails("INSERT INTO region (regionkey) VALUES (42)", "This connector does not support inserts");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test INSERT without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_", "(a bigint, b double)")) {
            assertUpdate("INSERT INTO " + table.getName() + " (a, b) VALUES (42, -38.5)", 1);
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + table.getName()))
                    .matches("VALUES (BIGINT '42', -385e-1)");
        }
    }

    @Test
    public void verifySupportsDeleteDeclaration()
    {
        if (hasBehavior(SUPPORTS_DELETE)) {
            // Covered by testDeleteAllDataFromTable
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_delete", "AS SELECT * FROM region")) {
            assertQueryFails("DELETE FROM " + table.getName(), "This connector does not support deletes");
        }
    }

    @Test
    public void verifySupportsRowLevelDeleteDeclaration()
    {
        if (hasBehavior(SUPPORTS_ROW_LEVEL_DELETE)) {
            // Covered by testRowLevelDelete
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_row_level_delete", "AS SELECT * FROM region")) {
            assertQueryFails("DELETE FROM " + table.getName() + " WHERE regionkey = 2", "This connector does not support deletes");
        }
    }

    @Test
    public void testDeleteAllDataFromTable()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_DELETE));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_all_data", "AS SELECT * FROM region")) {
            // not using assertUpdate as some connectors provide update count and some do not
            getQueryRunner().execute("DELETE FROM " + table.getName());
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 0");
        }
    }

    @Test
    public void testRowLevelDelete()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_row_delete", "AS SELECT * FROM region")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 2", 1);
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE regionkey = 2"))
                    .returnsEmptyResult();
            assertThat(query("SELECT cast(regionkey AS integer) FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES 0, 1, 3, 4");
        }
    }

    @Test
    public void testUpdate()
    {
        if (!hasBehavior(SUPPORTS_UPDATE)) {
            // Note this change is a no-op, if actually run
            assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", "This connector does not support updates");
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_update", "AS TABLE tpch.tiny.nation")) {
            String tableName = table.getName();
            assertUpdate("UPDATE " + tableName + " SET nationkey = 100 + nationkey WHERE regionkey = 2", 5);
            assertThat(query("SELECT * FROM " + tableName))
                    .skippingTypesCheck()
                    .matches("SELECT IF(regionkey=2, nationkey + 100, nationkey) nationkey, name, regionkey, comment FROM tpch.tiny.nation");
        }
    }

    @Test
    public void testCreateSchema()
    {
        String schemaName = "test_schema_create_" + randomTableSuffix();
        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            assertQueryFails(createSchemaSql(schemaName), "This connector does not support creating schemas");
            return;
        }

        assertUpdate(createSchemaSql(schemaName));
        assertThat(query("SHOW SCHEMAS"))
                .skippingTypesCheck()
                .containsAll(format("VALUES '%s', '%s'", getSession().getSchema().orElseThrow(), schemaName));
        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testRenameSchema()
    {
        if (!hasBehavior(SUPPORTS_RENAME_SCHEMA)) {
            String schemaName = getSession().getSchema().orElseThrow();
            assertQueryFails(
                    format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomTableSuffix()),
                    "This connector does not support renaming schemas");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            throw new SkipException("Skipping as connector does not support CREATE SCHEMA");
        }

        String schemaName = "test_rename_schema_" + randomTableSuffix();
        try {
            assertUpdate("CREATE SCHEMA " + schemaName);
            assertUpdate("ALTER SCHEMA " + schemaName + " RENAME TO " + schemaName + "_renamed");
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName + "_renamed");
        }
    }

    @Test
    public void testRenameTable()
    {
        if (!hasBehavior(SUPPORTS_RENAME_TABLE)) {
            assertQueryFails("ALTER TABLE nation RENAME TO yyyy", "This connector does not support renaming tables");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test ALTER TABLE RENAME without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        String oldTable = "test_rename_old_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + oldTable + " (a bigint, b double)");

        String newTable = "test_rename_new_" + randomTableSuffix();
        assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);

        assertThat(query("SHOW TABLES LIKE '" + oldTable + "'"))
                .returnsEmptyResult();
        assertThat(query("SELECT a, b FROM " + newTable))
                .returnsEmptyResult();

        if (hasBehavior(SUPPORTS_INSERT)) {
            assertUpdate("INSERT INTO " + newTable + " (a, b) VALUES (42, -38.5)", 1);
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + newTable))
                    .matches("VALUES (BIGINT '42', -385e-1)");
        }

        assertUpdate("DROP TABLE " + newTable);
        assertThat(query("SHOW TABLES LIKE '" + newTable + "'"))
                .returnsEmptyResult();
    }

    @Test
    public void testRenameTableAcrossSchemas()
    {
        if (!hasBehavior(SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS)) {
            if (!hasBehavior(SUPPORTS_RENAME_TABLE)) {
                throw new SkipException("Skipping since rename table is not supported at all");
            }
            assertQueryFails("ALTER TABLE nation RENAME TO other_schema.yyyy", "This connector does not support renaming tables across schemas");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            throw new AssertionError("Cannot test ALTER TABLE RENAME across schemas without CREATE SCHEMA, the test needs to be implemented in a connector-specific way");
        }

        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test ALTER TABLE RENAME across schemas without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        String oldTable = "test_rename_old_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + oldTable + " (a bigint, b double)");

        String schemaName = "test_schema_" + randomTableSuffix();
        assertUpdate(createSchemaSql(schemaName));

        String newTable = schemaName + ".test_rename_new_" + randomTableSuffix();
        assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);

        assertThat(query("SHOW TABLES LIKE '" + oldTable + "'"))
                .returnsEmptyResult();
        assertThat(query("SELECT a, b FROM " + newTable))
                .returnsEmptyResult();

        if (hasBehavior(SUPPORTS_INSERT)) {
            assertUpdate("INSERT INTO " + newTable + " (a, b) VALUES (42, -38.5)", 1);
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + newTable))
                    .matches("VALUES (BIGINT '42', -385e-1)");
        }

        assertUpdate("DROP TABLE " + newTable);
        assertThat(query("SHOW TABLES LIKE '" + newTable + "'"))
                .returnsEmptyResult();

        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testSelectInformationSchemaTables()
    {
        assertThat(query(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", getSession().getSchema().orElseThrow())))
                .skippingTypesCheck()
                .containsAll("VALUES 'nation', 'region'");
    }

    @Test
    public void testSelectInformationSchemaColumns()
    {
        assertThat(query(format("SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = 'region'", getSession().getSchema().orElseThrow())))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment'");
    }

    // SHOW CREATE TABLE exercises table properties and comments, which may be skipped during regular SELECT execution
    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches(format(
                        "CREATE TABLE %s.%s.region \\(\n" +
                                "   regionkey (bigint|decimal\\(19, 0\\)),\n" +
                                "   name varchar(\\(\\d+\\))?,\n" +
                                "   comment varchar(\\(\\d+\\))?\n" +
                                "\\)",
                        Pattern.quote(getSession().getCatalog().orElseThrow()),
                        Pattern.quote(getSession().getSchema().orElseThrow())));
    }

    @Test
    public void testView()
    {
        if (!hasBehavior(SUPPORTS_CREATE_VIEW)) {
            assertQueryFails("CREATE VIEW nation_v AS SELECT * FROM nation", "This connector does not support creating views");
            return;
        }

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String viewName = "test_view_" + randomTableSuffix();
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM nation");

        assertThat(query("SELECT * FROM " + viewName))
                .skippingTypesCheck()
                .matches("SELECT * FROM nation");

        assertThat(((String) computeScalar("SHOW CREATE VIEW " + viewName)))
                .matches("(?s)" +
                        "CREATE VIEW \\Q" + catalogName + "." + schemaName + "." + viewName + "\\E" +
                        ".* AS\n" +
                        "SELECT \\*\n" +
                        "FROM\n" +
                        "  nation");

        assertUpdate("DROP  VIEW " + viewName);
    }

    @Test
    public void testMaterializedView()
    {
        if (!hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW)) {
            assertQueryFails("CREATE MATERIALIZED VIEW nation_mv AS SELECT * FROM nation", "This connector does not support creating materialized views");
            return;
        }

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String viewName = "test_materialized_view_" + randomTableSuffix();
        assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " AS SELECT * FROM nation");

        // reading
        assertThat(query("SELECT * FROM " + viewName))
                .skippingTypesCheck()
                .matches("SELECT * FROM nation");

        // details
        assertThat(((String) computeScalar("SHOW CREATE MATERIALIZED VIEW " + viewName)))
                .matches("(?s)" +
                        "CREATE MATERIALIZED VIEW \\Q" + catalogName + "." + schemaName + "." + viewName + "\\E" +
                        ".* AS\n" +
                        "SELECT \\*\n" +
                        "FROM\n" +
                        "  nation");

        assertUpdate("DROP MATERIALIZED VIEW " + viewName);
    }
}
