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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.trino.plugin.jdbc.H2QueryRunner.createH2QueryRunner;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Single-threaded because H2 DDL operations can sometimes take a global lock, leading to apparent deadlocks
// like in https://github.com/trinodb/trino/issues/7209.
@Test(singleThreaded = true)
public class TestJdbcConnectorTest
        extends BaseJdbcConnectorTest
{
    private Map<String, String> properties;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        properties = ImmutableMap.<String, String>builder()
                .putAll(TestingH2JdbcModule.createProperties())
                .buildOrThrow();
        return createH2QueryRunner(REQUIRED_TPCH_TABLES, properties);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
                return false;

            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    @Test(dataProvider = "largeInValuesCount")
    public void testLargeIn(int valuesCount)
    {
        throw new SkipException("This test should pass with H2, but takes too long (currently over a mninute) and is not that important");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.test_unsupported_column_present",
                "(one bigint, two geometry, three varchar(10))");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeBaseName = dataMappingTestSetup.getTrinoTypeName().replaceAll("\\([^()]*\\)", "");
        switch (typeBaseName) {
            case "boolean":
            case "decimal":
            case "varbinary":
            case "time":
            case "timestamp":
            case "timestamp with time zone":
                return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: Unsupported delete");
    }

    @Test
    public void testUnknownTypeAsIgnored()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_failure_on_unknown_type_as_ignored",
                "(int_column int, geometry_column GEOMETRY)",
                ImmutableList.of(
                        "1, NULL",
                        "2, 'POINT(7 52)'"))) {
            Session ignoreUnsupportedType = unsupportedTypeHandling(IGNORE);
            assertQuery(ignoreUnsupportedType, "SELECT int_column FROM " + table.getName(), "VALUES 1, 2");
            assertQuery(ignoreUnsupportedType, "SELECT * FROM " + table.getName(), "VALUES 1, 2");
            assertQuery(
                    ignoreUnsupportedType,
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name LIKE 'test_failure_on_unknown_type_as_ignored%'",
                    "VALUES ('int_column', 'integer')");
            assertQuery(
                    ignoreUnsupportedType,
                    "DESCRIBE " + table.getName(),
                    "VALUES ('int_column', 'integer', '', '')");

            assertUpdate(ignoreUnsupportedType, format("INSERT INTO %s (int_column) VALUES (3)", table.getName()), 1);
            assertQuery(ignoreUnsupportedType, "SELECT * FROM " + table.getName(), "VALUES 1, 2, 3");
        }
    }

    @Test
    public void testUnknownTypeAsVarchar()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_failure_on_unknown_type_as_varchar",
                "(int_column int, geometry_column GEOMETRY)",
                ImmutableList.of(
                        "1, NULL",
                        "2, 'POINT(7 52)'"))) {
            Session convertToVarcharUnsupportedTypes = unsupportedTypeHandling(CONVERT_TO_VARCHAR);
            assertQuery(convertToVarcharUnsupportedTypes, "SELECT int_column FROM " + table.getName(), "VALUES 1, 2");
            assertQuery(convertToVarcharUnsupportedTypes, "SELECT * FROM " + table.getName(), "VALUES (1, NULL), (2, 'POINT (7 52)')");

            // predicate pushdown
            assertQuery(
                    convertToVarcharUnsupportedTypes,
                    format("SELECT int_column FROM %s WHERE geometry_column = 'POINT (7 52)'", table.getName()),
                    "VALUES 2");
            assertQuery(
                    convertToVarcharUnsupportedTypes,
                    format("SELECT int_column FROM %s WHERE geometry_column = 'invalid data'", table.getName()),
                    "SELECT 1 WHERE false");

            assertQuery(
                    convertToVarcharUnsupportedTypes,
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name LIKE 'test_failure_on_unknown_type_as_varchar%'",
                    "VALUES ('int_column', 'integer'), ('geometry_column', 'varchar')");
            assertQuery(
                    convertToVarcharUnsupportedTypes,
                    "DESCRIBE " + table.getName(),
                    "VALUES ('int_column', 'integer', '', ''), ('geometry_column', 'varchar', '','')");

            assertUpdate(
                    convertToVarcharUnsupportedTypes,
                    format("INSERT INTO %s (int_column) VALUES (3)", table.getName()),
                    1);
            assertQueryFails(
                    convertToVarcharUnsupportedTypes,
                    format("INSERT INTO %s (int_column, geometry_column) VALUES (3, 'POINT (7 52)')", table.getName()),
                    "Underlying type that is mapped to VARCHAR is not supported for INSERT: GEOMETRY");

            assertQuery(
                    convertToVarcharUnsupportedTypes,
                    "SELECT * FROM " + table.getName(),
                    "VALUES (1, NULL), (2, 'POINT (7 52)'), (3, NULL)");
        }
    }

    @Test
    public void testTableWithOnlyUnsupportedColumns()
    {
        Session session = Session.builder(getSession())
                .setSchema("public")
                .build();
        try (TestTable table = new TestTable(onRemoteDatabase(), "unsupported_table", "(geometry_column GEOMETRY)", ImmutableList.of("NULL", "'POINT(7 52)'"))) {
            // SELECT all tables to avoid any optimizations that could skip the table listing
            assertThat(getQueryRunner().execute("SELECT table_name FROM information_schema.tables").getOnlyColumn())
                    .contains(table.getName());
            assertQuery(
                    format("SELECT count(*) FROM information_schema.tables WHERE table_name = '%s'", table.getName()),
                    "SELECT 1");
            assertQuery(
                    format("SELECT count(*) FROM information_schema.columns WHERE table_name = '%s'", table.getName()),
                    "SELECT 0");
            assertQuery(
                    session,
                    format("SHOW TABLES LIKE '%s'", table.getName()),
                    format("SELECT '%s'", table.getName()));
            String unsupportedTableErrorMessage = "Table 'public.*' has no supported columns.*";
            assertQueryFails(
                    session,
                    "SELECT * FROM " + table.getName(),
                    unsupportedTableErrorMessage);
            assertQueryFails(
                    session,
                    "SHOW CREATE TABLE " + table.getName(),
                    unsupportedTableErrorMessage);
            assertQueryFails(
                    session,
                    "SHOW COLUMNS FROM " + table.getName(),
                    unsupportedTableErrorMessage);
            assertQueryFails(
                    session,
                    "DESCRIBE " + table.getName(),
                    unsupportedTableErrorMessage);
        }
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("NULL not allowed for column \"%s\"(?s).*", columnName.toUpperCase(ENGLISH));
    }

    @Override
    protected JdbcSqlExecutor onRemoteDatabase()
    {
        return new JdbcSqlExecutor(properties.get("connection-url"), new Properties());
    }

    private Session unsupportedTypeHandling(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("jdbc", UNSUPPORTED_TYPE_HANDLING, unsupportedTypeHandling.name())
                .build();
    }
}
