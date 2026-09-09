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
package io.trino.plugin.firebird;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.firebird.FirebirdTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN_WITH_COMMENT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MERGE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NATIVE_QUERY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NEGATIVE_DATE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NOT_NULL_CONSTRAINT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_COLUMN;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

@Isolated
final class TestFirebirdConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingFirebirdServer firebirdServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        firebirdServer = closeAfterClass(new TestingFirebirdServer());

        return FirebirdQueryRunner.builder(firebirdServer)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DEFAULT_COLUMN_VALUE -> true;

            case SUPPORTS_ADD_FIELD,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_NEGATIVE_DATE, // min date is 0001-01-01
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_REFRESH_VIEW,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SCHEMA,
                 SUPPORTS_TRUNCATE -> false;

            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageMatching("(?s)unsuccessful metadata update;.*|deadlock; update conflicts with concurrent update;.*");
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageMatching("(?s)unsuccessful metadata update;.*|Failed to insert data: deadlock; update conflicts with concurrent update;.*");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup setup)
    {
        String type = setup.getTrinoTypeName();
        if (type.equals("time") || type.equals("time(6)")) {
            // Firebird connector does not support TIME type yet
            return Optional.empty();
        }
        if (type.equals("timestamp") || type.equals("timestamp(3)") || type.equals("timestamp(6)")) {
            // Firebird connector does not support TIMESTAMP type yet
            return Optional.empty();
        }
        if (type.equals("timestamp(3) with time zone") || type.equals("timestamp(6) with time zone")) {
            // Firebird connector does not support TIMESTAMP type yet
            return Optional.empty();
        }
        if (type.equals("date") && setup.getSampleValueLiteral().equals("DATE '1582-10-05'")) {
            // Firebird does not support DATE '1582-10-05', causing test to fail
            return Optional.empty();
        }
        if (type.equals("char(3)") && setup.getSampleValueLiteral().equals("'ab'")) {
            // Firebird fills char(3) with spaces, causing test to fail
            return Optional.of(new DataMappingTestSetup("char(3)", "'abc'", "'zzz'"));
        }
        if (type.equals("varbinary")) {
            // Firebird connector does not support VARBINARY type yet
            return Optional.empty();
        }
        return Optional.of(setup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "test_default_cols",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default INT DEFAULT 43," +
                        "col_nonnull_default BIGINT DEFAULT 42 NOT NULL," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "test_unsupported_col",
                "(one BIGINT, two GEOMETRY, three VARCHAR(10))");
    }

    @Test
    @Override
    public void testMergeMultipleRowsMatchFails()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_multiple_fail_target_" + randomNameSuffix();
        String sourceTable = "merge_multiple_fail_source_" + randomNameSuffix();

        createTableForWrites("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable, Optional.of(canonicalize("customer")));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Antioch')", targetTable), 2);

        createTableForWrites("CREATE TABLE %s (id INT, customer VARCHAR, purchases INT, address VARCHAR)", sourceTable, Optional.empty());

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (1, 'Aaron', 6, 'Adelphi'), (2, 'Aaron', 8, 'Ashland')", sourceTable), 2);

        assertQueryFails(
                format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN UPDATE SET address = s.address",
                "One MERGE target table row matched more than one source row");

        assertUpdate(
                format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Adelphi' THEN UPDATE SET address = s.address",
                1);
        assertQuery("SELECT customer, purchases, address FROM " + targetTable, "VALUES ('Aaron', 5, 'Adelphi'), ('Bill', 7, 'Antioch')");

        assertUpdate("DROP TABLE " + sourceTable);
        // assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    @Override //
    public void testMergeNonNullableColumns()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE) && hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT));

        String targetTable = "merge_non_nullable_target_" + randomNameSuffix();

        createTableForWrites("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR NOT NULL)", targetTable, Optional.of(canonicalize("nation_name")));

        assertUpdate(format("INSERT INTO %s (nation_name, region_name) VALUES ('FRANCE', 'EUROPE'), ('ALGERIA', 'AFRICA'), ('GERMANY', 'EUROPE')", targetTable), 3);

        // Show that updating using a null value fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('ALGERIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                " WHEN MATCHED THEN UPDATE SET region_name = NULL"))
                .hasMessage("NULL value not allowed for NOT NULL column: " + canonicalize("region_name"));

        // Show that inserting using a null value fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('IMAGINARIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                " WHEN NOT MATCHED THEN INSERT (nation_name, region_name) VALUES ('IMAGINARIA', NULL)"))
                .hasMessage("NULL value not allowed for NOT NULL column: " + canonicalize("region_name"));

        // Show that inserting using an implicit null value fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('IMAGINARIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                // The region_name is implicitly assigned null
                " WHEN NOT MATCHED THEN INSERT (nation_name) VALUES ('IMAGINARIA')"))
                .hasMessage("NULL value not allowed for NOT NULL column: " + canonicalize("region_name"));

        // Show that if the updated value is provided by a function unpredicatably computing null,
        // the merge fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('ALGERIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                " WHEN MATCHED THEN UPDATE SET region_name = CAST(TRY(5/0) AS VARCHAR)"))
                .hasMessage("NULL value not allowed for NOT NULL column: " + canonicalize("region_name"));

        // assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    @Override
    public void testVarcharCharComparison()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        try (TestTable table = newTrinoTable(
                "test_varchar_char",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS varchar(3))), " +
                        "   (0, CAST('' AS varchar(3)))," +
                        "   (1, CAST(' ' AS varchar(3))), " +
                        "   (2, CAST('  ' AS varchar(3))), " +
                        "   (3, CAST('   ' AS varchar(3)))," +
                        "   (4, CAST('x' AS varchar(3)))," +
                        "   (5, CAST('x ' AS varchar(3)))," +
                        "   (6, CAST('x  ' AS varchar(3)))")) {
            // The char value is coerced to varchar by trimming trailing spaces, then compared as varchar
            // (no blank padding): char '  ' becomes '', matching only the empty varchar.
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS char(2))",
                    "VALUES (0, ''), (1, ' '), (2, '  '), (3, '   ')");

            // char 'x ' becomes 'x', matching only the exact 'x'.
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS char(2))",
                    "VALUES (4, 'x'), (5, 'x '), (6, 'x  ')");
        }
    }

    @Test
    @Override
    public void testCreateTableAsSelectWithUnicode()
    {
        assertThatThrownBy(super::testCreateTableAsSelectWithUnicode)
                .hasStackTraceContaining("Data truncation");
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        return switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            case "char(25) -> char(20)" -> Optional.of(setup.withNewColumnType("char(50)"));
            case "char(20) -> varchar" -> Optional.of(setup.withNewColumnType("varchar(20)").withNewValueLiteral("'char-to-varchar     '"));
            case "varchar -> char(20)" -> Optional.of(setup.withNewColumnType("char(255)"));
            case "varchar(25) -> varchar(20)" -> Optional.of(setup.withNewColumnType("varchar(100)"));
            case "varchar(100) -> varchar(50)" -> Optional.of(setup.withNewColumnType("varchar(255)"));

            case "bigint -> integer",
                 "bigint -> smallint",
                 "bigint -> tinyint",
                 "map(integer, varchar) -> map(bigint, varchar)",
                 "map(varchar, integer) -> map(varchar, bigint)",
                 "map(integer, row(x integer)) -> map(integer, row(\"x\" bigint))" -> Optional.of(setup.asUnsupported());

            case "time(3) -> time(6)",
                 "time(6) -> time(3)",
                 "timestamp(3) -> timestamp(6)",
                 "timestamp(6) -> timestamp(3)",
                 "timestamp(3) with time zone -> timestamp(6) with time zone",
                 "array(integer) -> array(bigint)",
                 "array(array(integer)) -> array(array(bigint))",
                 "row(x integer) -> row(\"x\" bigint)",
                 "row(x integer) -> row(\"y\" integer)",
                 "row(x integer) -> row(\"x\" integer, \"y\" integer)",
                 "row(x integer, y integer) -> row(\"x\" integer, \"z\" integer)",
                 "row(x integer, y integer) -> row(\"x\" integer)",
                 "row(x integer, y integer) -> row(\"y\" integer, \"x\" integer)",
                 "row(x integer, y integer) -> row(\"z\" integer, \"y\" integer, \"x\" integer)",
                 "row(x row(nested integer)) -> row(\"x\" row(\"nested\" bigint))",
                 "row(x row(a integer, b integer)) -> row(\"x\" row(\"b\" integer, \"a\" integer))" -> Optional.empty();
            default -> Optional.of(setup);
        };
    }

    @Override
    protected void verifySetColumnTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("unsuccessful metadata update; ALTER TABLE.*");
    }

    @Test
    @Override
    public void testVarcharEqualityPushdownIgnoresTrailingSpaces()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        // Trino compares varchar with NO PAD, so 'a' and 'a ' are distinct; equality must return only the exact match
        // even when pushed to a remote that compares with PAD SPACE.
        try (TestTable table = newTrinoTable("test_varchar_pad_space", "(v varchar(5))", ImmutableList.of("'a'", "'a '"))) {
            assertThat(query("SELECT v FROM " + table.getName() + " WHERE v = 'a'"))
                    .skippingTypesCheck()
                    .matches("VALUES ('a'), ('a ')");
            assertThat(query("SELECT v FROM " + table.getName() + " WHERE v = 'a '"))
                    .skippingTypesCheck()
                    .matches("VALUES ('a'), ('a ')");
        }
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return format("Failed to insert data: \\(conn=.*\\) Incorrect date value: '%s'.*", date);
    }

    @Test
    @Override // I can't get this test to work because I get a JDBC driver error immediately when trying to create the table.
    public void testMergeDefaultNullIntoNotNullColumn()
    {
        assertThatThrownBy(super::testMergeDefaultNullIntoNotNullColumn)
                .hasStackTraceContaining("unsuccessful metadata update;");
    }

    @Test
    void testViews()
    {
        // Firebird requires the FROM clause with SELECT
        try (TestView view = new TestView(onRemoteDatabase(), "test_view", "SELECT 'O' as status FROM " + delimited("nation") + " FETCH FIRST ROW ONLY")) {
            assertQuery("SELECT status FROM " + view.getName(), "SELECT 'O'");
        }
    }

    @Test
    @Override // Override because Firebird requires a FROM clause in SELECT queries
    public void testViewCaseSensitivity()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String upperCaseView = "test_view_uppercase_" + randomNameSuffix();
        String mixedCaseView = "test_view_mixedcase_" + randomNameSuffix();
        computeActual("CREATE VIEW " + upperCaseView + " AS SELECT 123 X FROM " + delimited("nation") + " FETCH FIRST ROW ONLY");
        computeActual("CREATE VIEW " + mixedCaseView + " AS SELECT 456 XyZ FROM " + delimited("nation") + " FETCH FIRST ROW ONLY");
        assertQuery("SELECT * FROM " + upperCaseView, "SELECT 123 X");
        assertQuery("SELECT * FROM " + mixedCaseView, "SELECT 456 XyZ");

        assertUpdate("DROP VIEW " + upperCaseView);
        assertUpdate("DROP VIEW " + mixedCaseView);
    }

    @Test
    @Override // Override because Firebird requires a FROM clause in SELECT queries
    public void testView()
    {
        if (!hasBehavior(SUPPORTS_CREATE_VIEW)) {
            assertQueryFails("CREATE VIEW nation_v AS SELECT * FROM nation", "This connector does not support creating views");
            return;
        }

        @Language("SQL") String query = format(
                "SELECT %s, %s, (%s / 2) HALF FROM PUBLIC.%s",
                delimited(getSqlIdentifier("orderkey")),
                delimited(getSqlIdentifier("orderstatus")),
                delimited(getSqlIdentifier("totalprice")),
                delimited(getSqlIdentifier("orders")));
        // FIXME: Firebird need a FROM table name in SELECT
        @Language("SQL") String viewQuery = " AS SELECT 123 x FROM " + delimited("nation") + " FETCH FIRST ROW ONLY";

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String testView = "test_view_" + randomNameSuffix();
        String testViewWithComment = "test_view_with_comment_" + randomNameSuffix();
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()) // prime the cache, if any
                .doesNotContain(canonicalize(testView));
        assertUpdate("CREATE VIEW " + testView + viewQuery);
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .contains(canonicalize(testView));
        assertUpdate("CREATE OR REPLACE VIEW " + testView + " AS " + query);

        assertUpdate("CREATE VIEW " + testViewWithComment + " COMMENT 'orders'" + viewQuery);
        assertUpdate("CREATE OR REPLACE VIEW " + testViewWithComment + " COMMENT 'orders' AS " + query);

        // verify comment
        assertThat((String) computeScalar("SHOW CREATE VIEW " + testViewWithComment)).contains("COMMENT 'orders'");
        assertThat(query(
                "SELECT table_name, comment FROM system.metadata.table_comments " +
                        "WHERE catalog_name = '" + catalogName + "' AND " +
                        "schema_name = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + canonicalize(testView) + "', null), ('" + canonicalize(testViewWithComment) + "', 'orders')");

        // reading
        assertQuery("SELECT * FROM " + testView, query);
        assertQuery("SELECT * FROM " + testViewWithComment, query);

        assertQuery(
                format("SELECT * FROM %1$s a JOIN %1$s b on a.%2$s = b.%2$s", testView, delimited("orderkey")),
                format("SELECT * FROM (%1$s) a JOIN (%1$s) b ON a.%2$s = b.%2$s", query, delimited("orderkey")));

        assertQuery(format("WITH %1$s AS (SELECT * FROM %1$s LIMIT 0) SELECT * FROM %2$s", delimited("orders"), testView), query);

        String name = format("%s.%s.%s", catalogName, schemaName, testView);
        assertQuery("SELECT * FROM " + name, query);

        assertUpdate("DROP VIEW " + testViewWithComment);

        // information_schema.views without table_name filter
        assertThat(query(
                format("SELECT %s, regexp_replace(%s, '[\\s\"]', '') FROM %s.%s WHERE %s = '%s'",
                        delimited("table_name"),
                        delimited("view_definition"),
                        delimited("information_schema"),
                        delimited("views"),
                        delimited("table_schema"),
                        schemaName)))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + canonicalize(testView) + "', '" + query.replaceAll("[\\s\"]", "") + "')");
        // information_schema.views with table_name filter
        assertQuery(
                format("SELECT %1$s, regexp_replace(%2$s, '[\\s\"]', '') FROM %3$s.%4$s WHERE %5$s = '%6$s' AND %1$s = '%7$s'",
                        delimited("table_name"),
                        delimited("view_definition"),
                        delimited("information_schema"),
                        delimited("views"),
                        delimited("table_schema"),
                        schemaName,
                        canonicalize(testView)),
                "VALUES ('" + canonicalize(testView) + "', '" + query.replaceAll("[\\s\"]", "") + "')");

        // table listing
        assertThat(query("SHOW TABLES"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + canonicalize(testView) + "'");
        // information_schema.tables without table_name filter
        assertThat(query(
                format("SELECT %s, %s FROM %s.%s WHERE %s = '%s'",
                        delimited("table_name"),
                        delimited("table_type"),
                        delimited("information_schema"),
                        delimited("tables"),
                        delimited("table_schema"),
                        schemaName)))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + canonicalize(testView) + "', 'VIEW')");
        // information_schema.tables with table_name filter
        assertQuery(
                format("SELECT %1$s, %2$s FROM %3$s.%4$s WHERE %5$s = '%6$s' and %1$s = '%7$s'",
                        delimited("table_name"),
                        delimited("table_type"),
                        delimited("information_schema"),
                        delimited("tables"),
                        delimited("table_schema"),
                        schemaName,
                        canonicalize(testView)),
                "VALUES ('" + canonicalize(testView) + "', 'VIEW')");

        // system.jdbc.tables without filter
        assertThat(query(
                format(
                        "SELECT %s, %s, %s FROM system.jdbc.tables",
                        delimited(canonicalize("table_schem")),
                        delimited(canonicalize("table_name")),
                        delimited(canonicalize("table_type")))))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + schemaName + "', '" + canonicalize(testView) + "', 'VIEW')");

        // system.jdbc.tables with table prefix filter
        assertQuery(
                format("SELECT %1$s, %2$s, %3$s FROM system.jdbc.tables WHERE %4$s = '%5$s' AND %1$s = '%6$s' AND %2$s = '%7$s'",
                        delimited(canonicalize("table_schem")),
                        delimited(canonicalize("table_name")),
                        delimited(canonicalize("table_type")),
                        delimited(canonicalize("table_cat")),
                        catalogName,
                        schemaName,
                        canonicalize(testView)),
                "VALUES ('" + schemaName + "', '" + canonicalize(testView) + "', 'VIEW')");

        // column listing
        assertThat(query("SHOW COLUMNS FROM " + testView))
                .result()
                .projected("Column") // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'orderkey', 'orderstatus', '" + canonicalize("half") + "'");

        assertThat(query("DESCRIBE " + testView))
                .result()
                .projected("Column") // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'orderkey', 'orderstatus', '" + canonicalize("half") + "'");

        // information_schema.columns without table_name filter
        assertThat(query(
                format(
                        "SELECT %s, %s FROM %s.%s WHERE %s = '%s'",
                        delimited("table_name"),
                        delimited("column_name"),
                        delimited("information_schema"),
                        delimited("columns"),
                        delimited("table_schema"),
                        schemaName)))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + canonicalize(testView) + "') " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', '" + canonicalize("half") + "'])");

        // information_schema.columns with table_name filter
        assertThat(query(
                format("SELECT %1$s, %2$s FROM %3$s.%4$s WHERE %5$s = '%6$s' and %1$s = '%7$s'",
                        delimited("table_name"),
                        delimited("column_name"),
                        delimited("information_schema"),
                        delimited("columns"),
                        delimited("table_schema"),
                        schemaName,
                        canonicalize(testView))))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + canonicalize(testView) + "') " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', '" + canonicalize("half") + "'])");

        // view-specific listings
        assertThat(query(
                format("SELECT %s FROM %s.%s WHERE %s = '%s'",
                        delimited("table_name"),
                        delimited("information_schema"),
                        delimited("views"),
                        delimited("table_schema"),
                        schemaName)))
                .skippingTypesCheck()
                .containsAll("VALUES '" + canonicalize(testView) + "'");

        // system.jdbc.columns without filter
        assertThat(query(
                format("SELECT %s, %s, %s FROM system.jdbc.columns",
                        delimited(canonicalize("table_schem")),
                        delimited(canonicalize("table_name")),
                        delimited(canonicalize("column_name")))))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + canonicalize(testView) + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', '" + canonicalize("half") + "'])");

        // system.jdbc.columns with schema filter
        assertThat(query(
                format("SELECT %1$s, %2$s, %3$s " +
                                "FROM system.jdbc.columns " +
                                "WHERE %1$s LIKE '%4$s'",
                        delimited(canonicalize("table_schem")),
                        delimited(canonicalize("table_name")),
                        delimited(canonicalize("column_name")),
                        "%" + schemaName + "%")))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + canonicalize(testView) + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', '" + canonicalize("half") + "'])");

        // system.jdbc.columns with table filter
        assertThat(query(
                format("SELECT %1$s, %2$s, %3$s FROM system.jdbc.columns WHERE %2$s LIKE '%4$s'",
                        delimited(canonicalize("table_schem")),
                        delimited(canonicalize("table_name")),
                        delimited(canonicalize("column_name")),
                        "%" + canonicalize(testView) + "%")))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + canonicalize(testView) + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', '" + canonicalize("half") + "'])");

        assertUpdate("DROP VIEW " + testView);
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .doesNotContain(canonicalize(testView));
    }

    @Test
    @Override // Override because Firebird does not support row expression
    public void testShowCreateView()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));
        checkState(getSession().getCatalog().isPresent(), "catalog is not set");
        checkState(getSession().getSchema().isPresent(), "schema is not set");

        String viewName = "test_show_create_view" + randomNameSuffix();
        assertUpdate("DROP VIEW IF EXISTS " + viewName);
        String ddl = format(
                """
                CREATE VIEW %1$s.%2$s.%3$s SECURITY INVOKER AS
                SELECT
                  PUBLIC."%4$s"."%5$s"
                , PUBLIC."%4$s"."%6$s"
                , PUBLIC."%4$s"."%7$s"
                , PUBLIC."%4$s"."%8$s"
                FROM
                  PUBLIC."%4$s"\
                """,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                viewName,
                getSqlIdentifier("nation"),
                getSqlIdentifier("nationkey"),
                getSqlIdentifier("name"),
                getSqlIdentifier("regionkey"),
                getSqlIdentifier("comment"));
        assertUpdate(ddl);

        assertThat(computeScalar("SHOW CREATE VIEW " + viewName)).isEqualTo(ddl);

        assertUpdate("DROP VIEW " + viewName);
    }

    @Test
    public void testReplaceView()
    {
        if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
            try (TestView testView = new TestView(getQueryRunner()::execute, "test_view", " SELECT * FROM " + delimited("nation"))) {
                assertQueryFails("ALTER VIEW %s REFRESH".formatted(testView.getName()), "This connector does not support refreshing view definition");
            }
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_TABLE) && !hasBehavior(SUPPORTS_ADD_COLUMN)) {
            throw new AssertionError("Cannot test ALTER VIEW REFRESH without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        try (TestTable table = newTrinoTable("test_table", "(id BIGINT, column_to_dropped BIGINT, column_to_be_renamed BIGINT, column_with_comment BIGINT)", ImmutableList.of("1, 2, 3, 4"))) {
            String viewDefinition = " SELECT * FROM %s".formatted(table.getName());
            try (TestView view = new TestView(getQueryRunner()::execute, "test_view", viewDefinition)) {
                assertQueryReturnsEmptyResult("SELECT * FROM " + view.getName() + " EXCEPT CORRESPONDING SELECT * FROM " + table.getName());

                assertUpdate("ALTER TABLE %s ADD COLUMN new_column BIGINT DEFAULT 5".formatted(table.getName()));
                assertQuery(
                        "SELECT * FROM " + view.getName(),
                        "VALUES (1, 2, 3, 4)");

                assertUpdate("CREATE OR REPLACE VIEW %s AS %s".formatted(view.getName(), viewDefinition));
                assertQuery(
                        "SELECT * FROM " + view.getName(),
                        "VALUES (1, 2, 3, 4, 5)");

                if (hasBehavior(SUPPORTS_RENAME_COLUMN)) {
                    assertQueryFails(
                            "ALTER TABLE %s RENAME COLUMN column_to_be_renamed TO renamed_column".formatted(table.getName()),
                            "dependent objects exist:.*");
                }

                if (hasBehavior(SUPPORTS_COMMENT_ON_COLUMN)) {
                    assertUpdate("COMMENT ON COLUMN %s.column_with_comment IS 'test comment'".formatted(view.getName()));
                    assertThat(getColumnComment(view.getName(), "column_with_comment")).isEqualTo("test comment");
                }

                if (hasBehavior(SUPPORTS_DROP_COLUMN)) {
                    assertQueryFails("ALTER TABLE %s DROP COLUMN column_to_dropped".formatted(table.getName()),
                            "column is referenced in:.*");
                }
            }
        }
    }

    @Test
    @Override // Override because for Firebird an empty or null comment are same
    public void testCommentViewColumn()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_VIEW_COLUMN)) {
            if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
                try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM region")) {
                    assertQueryFails("COMMENT ON COLUMN " + view.getName() + ".regionkey IS 'new region key comment'", "This connector does not support setting view column comments");
                }
                return;
            }
            abort("Skipping as connector does not support CREATE VIEW");
        }

        String viewColumnName = "regionkey";
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM " + delimited("region"))) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'new region key comment'");
            assertThat(getColumnComment(canonicalize(view.getName()), viewColumnName)).isEqualTo("new region key comment");

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS NULL");
            assertThat(getColumnComment(canonicalize(view.getName()), viewColumnName)).isEqualTo(null);

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'updated region key comment'");
            assertThat(getColumnComment(canonicalize(view.getName()), viewColumnName)).isEqualTo("updated region key comment");

            // comment set to empty
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS ''");
            assertThat(getColumnComment(canonicalize(view.getName()), viewColumnName)).isEqualTo(null);
        }
    }

    @Test
    @Override // Overridden because Firebird does not support changing type on table column held by view
    public void testCompatibleTypeChangeForView()
    {
        assertThatThrownBy(super::testCompatibleTypeChangeForView)
                .hasStackTraceContaining("unsuccessful metadata update; cannot delete;");
    }

    @Test
    @Override // Overridden because for views, all comparisons must be made without regard to case or double cote.
    public void testViewMetadata()
    {
        testViewMetadata("", "INVOKER");
        testViewMetadata(" SECURITY DEFINER", "INVOKER");
        testViewMetadata(" SECURITY INVOKER", "INVOKER");
    }

    private void testViewMetadata(String securityClauseInCreate, String securityClauseInShowCreate)
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String viewName = "meta_test_view_" + randomNameSuffix();

        @Language("SQL") String query = "SELECT CAST('123' AS BIGINT) X, 'foo' Y FROM PUBLIC." + delimited(getSqlIdentifier("nation")) + " FETCH FIRST ROW ONLY";
        assertUpdate("CREATE VIEW " + viewName + securityClauseInCreate + " AS " + query);

        // test INFORMATION_SCHEMA.TABLES
        MaterializedResult actual = computeActual(format(
                "SELECT %s, %s FROM %s.%s WHERE %s = '%s'",
                delimited("table_name"),
                delimited("table_type"),
                delimited("information_schema"),
                delimited("tables"),
                delimited("table_schema"),
                getSession().getSchema().get()));

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("customer", "BASE TABLE")
                .row(canonicalize(viewName), "VIEW")
                .row("nation", "BASE TABLE")
                .row("orders", "BASE TABLE")
                .row("region", "BASE TABLE")
                .build();

        assertContains(actual, expected);

        // test SHOW TABLES
        actual = computeActual("SHOW TABLES");

        MaterializedResult.Builder builder = resultBuilder(getSession(), actual.getTypes());
        for (MaterializedRow row : expected.getMaterializedRows()) {
            builder.row(row.getField(0));
        }
        expected = builder.build();

        assertContains(actual, expected);

        // test INFORMATION_SCHEMA.VIEWS
        actual = computeActual(format(
                "SELECT %1$s, %2$s FROM %3$s.%4$s WHERE %5$s = '%6$s' and %1$s = '%7$s'",
                delimited("table_name"),
                delimited("view_definition"),
                delimited("information_schema"),
                delimited("views"),
                delimited("table_schema"),
                getSession().getSchema().get(),
                canonicalize(viewName)));

        expected = resultBuilder(getSession(), actual.getTypes())
                .row(canonicalize(viewName), formatSqlText(query))
                .build();

        assertThat(getComparableQuery(actual.toString(), "[\\s\"]")).isEqualTo(getComparableQuery(expected.toString(), "[\\s\"]"));

        // test SHOW COLUMNS
        assertThat(query("SHOW COLUMNS FROM " + viewName))
                .result().matches(resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row(canonicalize("x"), "bigint", "", "")
                        .row(canonicalize("y"), "char(3)", "", "")
                        .build());

        // test SHOW CREATE VIEW
        String expectedSql = formatSqlText(format(
                "CREATE VIEW %s.%s.%s SECURITY %s AS %s",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                viewName,
                securityClauseInShowCreate,
                query)).trim();

        actual = computeActual("SHOW CREATE VIEW " + viewName);

        assertThat(getComparableQuery(getOnlyElement(actual.getOnlyColumnAsSet()), "\"")).isEqualTo(getComparableQuery(expectedSql, "\""));

        actual = computeActual(format("SHOW CREATE VIEW %s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), viewName));

        assertThat(getComparableQuery(getOnlyElement(actual.getOnlyColumnAsSet()), "\"")).isEqualTo(getComparableQuery(expectedSql, "\""));

        assertUpdate("DROP VIEW " + viewName);
    }

    private String getComparableQuery(Object query, String pattern)
    {
        return getComparableQuery((String) query, pattern);
    }

    private String getComparableQuery(String query, String pattern)
    {
        return query.replaceAll(pattern, "");
    }

    @Test
    @Override // Overridden because for Firebird empty comment and NULL comment are same
    public void testCommentView()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_VIEW)) {
            if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
                try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM " + delimited("region"))) {
                    assertQueryFails("COMMENT ON VIEW " + view.getName() + " IS 'new comment'", "This connector does not support setting view comments");
                }
                return;
            }
            abort("Skipping as connector does not support CREATE VIEW");
        }

        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM " + delimited("region"))) {
            // comment set
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE VIEW " + view.getName())).contains("COMMENT 'new comment'");
            assertThat(getTableComment(canonicalize(view.getName()))).isEqualTo("new comment");

            // comment deleted
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS NULL");
            assertThat(getTableComment(canonicalize(view.getName()))).isEqualTo(null);

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'updated comment'");
            assertThat(getTableComment(canonicalize(view.getName()))).isEqualTo("updated comment");

            // comment set to empty
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS ''");
            assertThat(getTableComment(canonicalize(view.getName()))).isEqualTo(null);
        }

        String viewName = "test_comment_view" + randomNameSuffix();
        try {
            // comment set when creating a table
            assertUpdate("CREATE VIEW " + viewName + " COMMENT 'new view comment' AS SELECT * FROM " + delimited("region"));
            assertThat(getTableComment(canonicalize(viewName))).isEqualTo("new view comment");
        }
        finally {
            assertUpdate("DROP VIEW IF EXISTS " + viewName);
        }
    }

    @Test
    @Override // overridden because a table to which a view is attached cannot be deleted.
    public void testCompatibleTypeChangeForView2()
    {
        assertThatThrownBy(super::testCompatibleTypeChangeForView)
                .hasStackTraceContaining("unsuccessful metadata update; cannot delete;");
    }

    @Test
    @Override // Overridden because Firebird throw integrity constraint violation exception
    public void testInsertDefaultNullIntoNotNullColumn()
    {
        assertThatThrownBy(super::testInsertDefaultNullIntoNotNullColumn)
                .hasStackTraceContaining("unsuccessful metadata update;");
    }

    @Test
    void testPredicatePushdownForNumerics()
    {
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "124");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "<=", "123.321");
        predicatePushdownTest("DECIMAL(9, 3)", "123.321", "=", "123.321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456790");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "<=", "123456789.987654321");
        predicatePushdownTest("DECIMAL(30, 10)", "123456789.987654321", "=", "123456789.987654321");
        predicatePushdownTest("FLOAT", "123456789.987654321", "<=", "CAST(123456789.99 AS REAL)");
        predicatePushdownTest("DOUBLE PRECISION", "123456789.987654321", "<=", "CAST(123456789.99 AS DOUBLE)");
        predicatePushdownTest("NUMERIC(5,3)", "5.0", "=", "CAST(5.0 AS DECIMAL(5,3))");
    }

    @Test
    void testPredicatePushdownForChars()
    {
        // FIXME: Firebird does not support the <= operator with the CHAR type.
        predicatePushdownTest("CHAR(1)", "'0'", "=", "'0'");
        // predicatePushdownTest("CHAR(1)", "'0'", "<=", "'0'");
        predicatePushdownTest("CHAR(7)", "'my_char'", "=", "CAST('my_char' AS CHAR(7))");
    }

    private void predicatePushdownTest(String columnType, String columnLiteral, String operator, String filterLiteral)
    {
        String tableName = "test_pdown_" + columnType.replaceAll("[^a-zA-Z0-9]", "");
        try (TestTable table = new TestTable(onRemoteDatabase(), tableName, format("(c %s)", columnType))) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES (%s)", table.getName(), columnLiteral));

            assertThat(query(format("SELECT * FROM %s WHERE c %s %s", table.getName(), operator, filterLiteral)))
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override // Override because Firebird dont support negative date and the predicate of '-0001-01-01' match '0002-01-01'
    public void testInsertNegativeDate()
    {
        if (!hasBehavior(SUPPORTS_INSERT)) {
            assertQueryFails("INSERT INTO orders (orderdate) VALUES (DATE '-0001-01-01')", "This connector does not support inserts");
            return;
        }
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test INSERT negative dates without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }
        if (!hasBehavior(SUPPORTS_NEGATIVE_DATE)) {
            try (TestTable table = newTrinoTable("insert_date", "(dt DATE)")) {
                assertUpdate(format("INSERT INTO %s VALUES (DATE '-0001-01-01')", table.getName()), 1);
                assertQuery("SELECT * FROM " + table.getName(), "VALUES DATE '0002-01-01'");
                assertQuery(format("SELECT * FROM %s WHERE dt = DATE '-0001-01-01'", table.getName()), "VALUES DATE '0002-01-01'");
            }
            return;
        }

        try (TestTable table = newTrinoTable("insert_date", "(dt DATE)")) {
            assertUpdate(format("INSERT INTO %s VALUES (DATE '-0001-01-01')", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES DATE '-0001-01-01'");
            assertQuery(format("SELECT * FROM %s WHERE dt = DATE '-0001-01-01'", table.getName()), "VALUES DATE '-0001-01-01'");
        }
    }

    @Test
    @Override // Override because Firebird requires named columns and FROM clause in SELECT
    public void testNativeQuerySimple()
    {
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 res FROM RDB$DATABASE'))", "VALUES 1");
    }

    @Test
    @Override // Override test because for Firebird the predicate of '-1996-09-14' match '1997-09-14'
    public void testDateYearOfEraPredicate()
    {
        assertQuery(
                format(
                        "SELECT %1$s FROM %2$s WHERE %1$s = DATE '1997-09-14'",
                        delimited("orderdate"),
                        delimited("orders")),
                "VALUES DATE '1997-09-14'");
        assertQuery(
                format(
                        "SELECT %1$s FROM %2$s WHERE %1$s = DATE '-1996-09-14'",
                        delimited("orderdate"),
                        delimited("orders")),
                "VALUES DATE '1997-09-14'");
    }

    @Test
    void testNativeMultipleInClauses()
    {
        String longInClauses = range(0, 10)
                .mapToObj(value -> getLongInClause(value * 1_000, 1_000))
                .collect(joining(" OR "));
        onRemoteDatabase().execute(format("SELECT count(*) FROM %s WHERE %s", delimited("orders"), longInClauses));
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return format("%s IN (%s)", delimited("orderkey"), longValues);
    }

    @Override // Override because Firebird does not support multi rows insertion...
    protected SqlExecutor onRemoteDatabase()
    {
        return firebirdServer.getSqlExecutor();
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        // Firebird returns a ResultSet metadata with no columns for INSERT statement.
        // This is unusual, because other connectors don't produce a ResultSet metadata for INSERT at all.
        // The query fails because there are no columns, but even if columns were not required, the query would fail
        // to execute in Firebird because the connector wraps it in additional syntax, which causes syntax error.
        try (TestTable testTable = simpleTable()) {
            assertThat(query(format("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3)'))", testTable.getName())))
                    .nonTrinoExceptionFailure().hasMessageContaining("descriptor has no fields");
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES 1, 2");
        }
    }

    @Test
    @Override // Override because Firebird requires the FROM clause on the RDB$DATABASE
    public void testNativeQueryParameters()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query_simple", "SELECT * FROM TABLE(system.query(query => ?))")
                .addPreparedStatement("my_query", "SELECT * FROM TABLE(system.query(query => format('SELECT %s FROM %s', ?, ?)))")
                .build();
        assertQuery(session, "EXECUTE my_query_simple USING 'SELECT 1 a FROM RDB$DATABASE'", "VALUES 1");
        assertQuery(session, "EXECUTE my_query USING 'a', '(SELECT 2 a FROM RDB$DATABASE) t'", "VALUES 2");
    }

    @Test
    @Override // Override because Firebird returns a ResultSet metadata with no columns for CREATE statement.
    public void testNativeQueryCreateStatement()
    {
        skipTestUnless(hasBehavior(SUPPORTS_NATIVE_QUERY));
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .nonTrinoExceptionFailure().hasMessageContaining("descriptor has no fields");
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
    }

    @Test
    @Override // Override because if you add an empty comment ("''") in Firebird, it will be saved as NULL in the database.
    public void testAddColumnWithComment()
    {
        if (!hasBehavior(SUPPORTS_ADD_COLUMN)) {
            // Covered by testAddColumn
            return;
        }
        if (!hasBehavior(SUPPORTS_ADD_COLUMN_WITH_COMMENT)) {
            assertQueryFails("ALTER TABLE nation ADD COLUMN test_add_col_desc bigint COMMENT 'test column comment'", "This connector does not support adding columns with comments");
            return;
        }

        try (TestTable table = newTrinoTable("test_add_col_desc_", "(a_varchar varchar)")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar COMMENT 'test new column comment'");
            assertThat(getColumnComment(canonicalize(tableName), canonicalize("b_varchar"))).isEqualTo("test new column comment");

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN empty_comment varchar COMMENT ''");
            assertThat(getColumnComment(canonicalize(tableName), canonicalize("empty_comment"))).isEqualTo(null);
        }
    }

    @Test
    @Override // I can't get this test to work because I get a JDBC driver error immediately when trying to create the table.
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        assertThatThrownBy(super::testInsertInPresenceOfNotSupportedColumn)
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("(?s)java.sql.SQLSyntaxErrorException: unsuccessful metadata update; CREATE TABLE .* failed;.*");
    }

    @Test
    @Override // I can't get this test to work because I get a JDBC driver error immediately when trying to create the table.
    public void testNativeQuerySelectUnsupportedType()
    {
        assertThatThrownBy(super::testNativeQuerySelectUnsupportedType)
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("(?s)java.sql.SQLSyntaxErrorException: unsuccessful metadata update; CREATE TABLE .* failed;.*");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(63);
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(63);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Dynamic SQL Error; SQL error code = -104; Name longer than database column size;");
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Dynamic SQL Error; SQL error code = -104; Name longer than database column size;");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("(?s).*validation error for column .*\"%s\".*", columnName.toUpperCase(ENGLISH));
    }

    @Override
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return "Failed to insert data: value exceeds the range for valid dates.*";
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        // Firebird trim any trailing space
        if (columnName.equals("atrailingspace ")) {
            return Optional.empty();
        }
        return Optional.of(columnName);
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("unsuccessful metadata update;");
    }

    @Override
    protected TestTable createTestTableForWrites(String namePrefix, String tableDefinition, String primaryKey)
    {
        return super.createTestTableForWrites(namePrefix, getTableDefinitionWithPrimaryKey(tableDefinition, primaryKey), primaryKey);
    }

    @Override
    protected TestTable createTestTableForWrites(String namePrefix, String tableDefinition, List<String> rowsToInsert, String primaryKey)
    {
        return super.createTestTableForWrites(namePrefix, getTableDefinitionWithPrimaryKey(tableDefinition, primaryKey), rowsToInsert, primaryKey);
    }

    @Override
    protected void createTableForWrites(String createTable, String tableName, Optional<String> primaryKey, OptionalInt updateCount)
    {
        super.createTableForWrites(createTableWithPrimaryKey(createTable, primaryKey), tableName, primaryKey, updateCount);
    }

    private String createTableWithPrimaryKey(String createTable, Optional<String> primaryKey)
    {
        return primaryKey.map(key -> getTableDefinitionWithPrimaryKey(createTable, key)).orElse(createTable);
    }

    private String getTableDefinitionWithPrimaryKey(String createTable, String primaryKey)
    {
        String primaryKeys = String.join("','", primaryKey.split(","));
        String constraint = format(" WITH (%s = ARRAY['%s']) ", PRIMARY_KEY_PROPERTY, primaryKeys);
        Matcher matcher = Pattern.compile("\\s*AS\\s+").matcher(createTable);
        if (matcher.find()) {
            return matcher.replaceFirst(match -> constraint + match.group(0));
        }
        return createTable + constraint;
    }

    private String getSqlIdentifier(String value)
    {
        return value.toUpperCase(ENGLISH);
    }

    private String delimited(String value)
    {
        return value;
    }
}
