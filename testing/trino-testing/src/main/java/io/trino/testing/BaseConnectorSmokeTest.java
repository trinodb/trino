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
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.regex.Pattern;

import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MERGE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TRUNCATE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_UPDATE;
import static io.trino.testing.TestingNames.randomNameSuffix;
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

    /**
     * Make sure to group related behaviours together in the order and grouping they are declared in {@link TestingConnectorBehavior}.
     * If required, annotate the method with {@code @SuppressWarnings("DuplicateBranchesInSwitch")}.
     */
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

        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " " + getCreateTableDefaultDefinition());
        assertThat(query("SELECT a, b FROM " + tableName))
                .returnsEmptyResult();
        assertUpdate("DROP TABLE " + tableName);
    }

    protected String getCreateTableDefaultDefinition()
    {
        return "(a bigint, b double)";
    }

    protected String expectedValues(String values)
    {
        return format("SELECT CAST(a AS bigint), CAST(b AS double) FROM (VALUES %s) AS t (a, b)", values);
    }

    @Test
    public void testCreateTableAsSelect()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            assertQueryFails("CREATE TABLE xxxx AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", "This connector does not support creating tables with data");
            return;
        }

        String tableName = "test_create_" + randomNameSuffix();
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

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_", getCreateTableDefaultDefinition())) {
            assertUpdate("INSERT INTO " + table.getName() + " (a, b) VALUES (42, -38.5), (13, 99.9)", 2);
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + table.getName()))
                    .matches(expectedValues("(42, -38.5), (13, 99.9)"));
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
            assertQueryFails("DELETE FROM " + table.getName(), MODIFYING_ROWS_MESSAGE);
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
            assertQueryFails("DELETE FROM " + table.getName() + " WHERE regionkey = 2", MODIFYING_ROWS_MESSAGE);
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
    public void testTruncateTable()
    {
        if (!hasBehavior(SUPPORTS_TRUNCATE)) {
            assertQueryFails("TRUNCATE TABLE nation", "This connector does not support truncating tables");
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_truncate", "AS SELECT * FROM region")) {
            assertUpdate("TRUNCATE TABLE " + table.getName());
            assertThat(query("TABLE " + table.getName()))
                    .returnsEmptyResult();
        }
    }

    @Test
    public void testUpdate()
    {
        if (!hasBehavior(SUPPORTS_UPDATE)) {
            // Note this change is a no-op, if actually run
            assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", MODIFYING_ROWS_MESSAGE);
            return;
        }

        if (!hasBehavior(SUPPORTS_INSERT)) {
            throw new AssertionError("Cannot test UPDATE without INSERT");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_update_", getCreateTableDefaultDefinition())) {
            assertUpdate("INSERT INTO " + table.getName() + " (a, b) SELECT regionkey, regionkey * 2.5 FROM region", "SELECT count(*) FROM region");
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches(expectedValues("(0, 0.0), (1, 2.5), (2, 5.0), (3, 7.5), (4, 10.0)"));

            assertUpdate("UPDATE " + table.getName() + " SET b = b + 1.2 WHERE a % 2 = 0", 3);
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches(expectedValues("(0, 1.2), (1, 2.5), (2, 6.2), (3, 7.5), (4, 11.2)"));
        }
    }

    @Test
    public void testMerge()
    {
        if (!hasBehavior(SUPPORTS_MERGE)) {
            // Note this change is a no-op, if actually run
            assertQueryFails("MERGE INTO nation n USING nation s ON (n.nationkey = s.nationkey) " +
                            "WHEN MATCHED AND n.regionkey < 1 THEN UPDATE SET nationkey = 5",
                    MODIFYING_ROWS_MESSAGE);
            return;
        }

        if (!hasBehavior(SUPPORTS_INSERT)) {
            throw new AssertionError("Cannot test MERGE without INSERT");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_merge_", getCreateTableDefaultDefinition())) {
            assertUpdate("INSERT INTO " + table.getName() + " (a, b) SELECT regionkey, regionkey * 2.5 FROM region", "SELECT count(*) FROM region");
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches(expectedValues("(0, 0.0), (1, 2.5), (2, 5.0), (3, 7.5), (4, 10.0)"));

            assertUpdate("MERGE INTO " + table.getName() + " t " +
                    "USING (VALUES (0, 1.3), (2, 2.9), (3, 0.0), (4, -5.0), (5, 5.7)) AS s (a, b) " +
                    "ON (t.a = s.a) " +
                    "WHEN MATCHED AND s.b > 0 THEN UPDATE SET b = t.b + s.b " +
                    "WHEN MATCHED AND s.b = 0 THEN DELETE " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.b)",
                    4);
            assertThat(query("SELECT a, b FROM " + table.getName()))
                    .matches(expectedValues("(0, 1.3), (1, 2.5), (2, 7.9), (4, 10.0), (5, 5.7)"));
        }
    }

    @Test
    public void testCreateSchema()
    {
        String schemaName = "test_schema_create_" + randomNameSuffix();
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
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_SCHEMA));

        Session newSession = Session.builder(getSession())
                .setIdentity(Identity.ofUser("ADMIN"))
                .build();
        String schemaName = "test_schema_create_uppercase_owner_name_" + randomNameSuffix();
        assertUpdate(newSession, createSchemaSql(schemaName));
        assertThat(query(newSession, "SHOW SCHEMAS"))
                .skippingTypesCheck()
                .containsAll(format("VALUES '%s'", schemaName));
        assertUpdate(newSession, "DROP SCHEMA " + schemaName);
    }

    @Test
    public void testRenameSchema()
    {
        if (!hasBehavior(SUPPORTS_RENAME_SCHEMA)) {
            String schemaName = getSession().getSchema().orElseThrow();
            assertQueryFails(
                    format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomNameSuffix()),
                    "This connector does not support renaming schemas");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            throw new SkipException("Skipping as connector does not support CREATE SCHEMA");
        }

        String schemaName = "test_rename_schema_" + randomNameSuffix();
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
            throws Exception
    {
        if (!hasBehavior(SUPPORTS_RENAME_TABLE)) {
            assertQueryFails("ALTER TABLE nation RENAME TO yyyy", "This connector does not support renaming tables");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test ALTER TABLE RENAME without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        String oldTable = "test_rename_old_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + oldTable + " " + getCreateTableDefaultDefinition());

        String newTable = "test_rename_new_" + randomNameSuffix();
        try {
            assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);
        }
        catch (Throwable e) {
            try (AutoCloseable ignore = () -> assertUpdate("DROP TABLE " + oldTable)) {
                throw e;
            }
        }

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
            throws Exception
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

        String oldTable = "test_rename_old_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + oldTable + " " + getCreateTableDefaultDefinition());

        String schemaName = "test_schema_" + randomNameSuffix();
        assertUpdate(createSchemaSql(schemaName));

        String newTable = schemaName + ".test_rename_new_" + randomNameSuffix();
        try {
            assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);
        }
        catch (Throwable e) {
            try (AutoCloseable ignore = () -> assertUpdate("DROP TABLE " + oldTable)) {
                throw e;
            }
        }

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

    /**
     * This seemingly duplicate test of {@link BaseConnectorTest#testShowInformationSchemaTables()}
     * is used in the context of this class in order to be able to test
     * against a wider range of connector configurations.
     */
    @Test
    public void testShowInformationSchemaTables()
    {
        assertThat(query("SHOW TABLES FROM information_schema"))
                .skippingTypesCheck()
                .containsAll("VALUES 'applicable_roles', 'columns', 'enabled_roles', 'roles', 'schemata', 'table_privileges', 'tables', 'views'");
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
        String viewName = "test_view_" + randomNameSuffix();
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
        String viewName = "test_materialized_view_" + randomNameSuffix();
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

        // information_schema.tables (no filtering on table_name so that ConnectorMetadata.listViews is exercised)
        assertThat(query("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = '" + schemaName + "'"))
                .containsAll("VALUES (VARCHAR '" + viewName + "', VARCHAR 'BASE TABLE')");

        // information_schema.views
        assertThat(computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = '" + schemaName + "'").getOnlyColumnAsSet())
                .doesNotContain(viewName);
        assertThat(query("SELECT table_name FROM information_schema.views WHERE table_schema = '" + schemaName + "' AND table_name = '" + viewName + "'"))
                .returnsEmptyResult();

        // materialized view-specific listings
        assertThat(query("SELECT name FROM system.metadata.materialized_views WHERE catalog_name = '" + catalogName + "' AND schema_name = '" + schemaName + "'"))
                .containsAll("VALUES VARCHAR '" + viewName + "'");

        assertUpdate("DROP MATERIALIZED VIEW " + viewName);
    }

    @Test
    public void testCommentView()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_VIEW)) {
            if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
                try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM region")) {
                    assertQueryFails("COMMENT ON VIEW " + view.getName() + " IS 'new comment'", "This connector does not support setting view comments");
                }
                return;
            }
            throw new SkipException("Skipping as connector does not support CREATE VIEW");
        }

        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE VIEW " + view.getName())).contains("COMMENT 'new comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("new comment");

            // comment updated
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'updated comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("updated comment");

            // comment set to empty
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS ''");
            assertThat(getTableComment(view.getName())).isEmpty();

            // comment deleted
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'a comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("a comment");
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS NULL");
            assertThat(getTableComment(view.getName())).isNull();
        }
    }

    @Test
    public void testCommentViewColumn()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_VIEW_COLUMN)) {
            if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
                try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM region")) {
                    assertQueryFails("COMMENT ON COLUMN " + view.getName() + ".regionkey IS 'new region key comment'", "This connector does not support setting view column comments");
                }
                return;
            }
            throw new SkipException("Skipping as connector does not support CREATE VIEW");
        }

        String viewColumnName = "regionkey";
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'new region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("new region key comment");

            // comment updated
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'updated region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("updated region key comment");

            // comment set to empty
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS ''");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("");

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS NULL");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo(null);
        }
    }

    protected String getTableComment(String tableName)
    {
        return (String) computeScalar(format("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = '%s' AND schema_name = '%s' AND table_name = '%s'", getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), tableName));
    }

    protected String getColumnComment(String tableName, String columnName)
    {
        return (String) computeScalar(format(
                "SELECT comment FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'",
                getSession().getSchema().orElseThrow(),
                tableName,
                columnName));
    }
}
