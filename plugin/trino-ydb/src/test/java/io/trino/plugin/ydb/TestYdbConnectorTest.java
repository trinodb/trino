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
package io.trino.plugin.ydb;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

final class TestYdbConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingYdbServer ydbServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ydbServer = closeAfterClass(new TestingYdbServer());
        return YdbQueryRunner.builder(ydbServer)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return new JdbcSqlExecutor(ydbServer.getJdbcUrl());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_MERGE,
                 SUPPORTS_NATIVE_QUERY,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    void testDefaultSchema()
    {
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("default", "information_schema");
        assertThat(computeActual("SHOW TABLES FROM default").getOnlyColumnAsSet()).contains("orders");
        assertQueryFails("SELECT * FROM missing.orders", ".*Schema 'missing' does not exist");
        assertQueryFails("SELECT * FROM \"%\".orders", ".*Schema '%' does not exist");
    }

    @Test
    void testUnsignedPredicatePushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_unsigned_pushdown",
                "(id Int32 NOT NULL, u8 Uint8, u16 Uint16, u32 Uint32, u64 Uint64, PRIMARY KEY (id))")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " (id, u8, u16, u32, u64) VALUES " +
                    "(1, 0ut, 0us, 0u, 0ul), (2, 255ut, 65535us, 4294967295u, 18446744073709551615ul)");

            assertThat(query("SELECT id FROM " + table.getName() + " WHERE u8 > -1")).isFullyPushedDown().matches("VALUES 1, 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE u8 = 255")).isFullyPushedDown().matches("VALUES 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE u8 = 300")).isFullyPushedDown().returnsEmptyResult();
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE u8 IN (0, 300)")).isFullyPushedDown().matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE u16 > -1 AND u16 < 70000")).isFullyPushedDown().matches("VALUES 1, 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE u16 = 65535")).isFullyPushedDown().matches("VALUES 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE u32 >= 4294967295")).isFullyPushedDown().matches("VALUES 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE u32 < 4294967296 AND u32 > -1")).isFullyPushedDown().matches("VALUES 1, 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE u64 = DECIMAL '18446744073709551615'")).isFullyPushedDown().matches("VALUES 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE u64 < 1")).isFullyPushedDown().matches("VALUES 1");
            // A bound outside the Uint64 range cannot be bound as a parameter
            assertQueryFails("SELECT id FROM " + table.getName() + " WHERE u64 > -1", "Value must be between 0 and 18446744073709551615 in YDB: -1");
        }
    }

    @Test
    void testDatePredicatePushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_date_pushdown",
                "(id Int32 NOT NULL, d Date, d32 Date32, PRIMARY KEY (id))")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " (id, d, d32) VALUES " +
                    "(1, Date('1970-01-01'), Date32('1969-12-31')), (2, Date('2105-12-31'), Date32('2200-01-01'))");

            assertThat(query("SELECT id FROM " + table.getName() + " WHERE d > DATE '1960-01-01'")).isFullyPushedDown().matches("VALUES 1, 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE d = DATE '2105-12-31'")).isFullyPushedDown().matches("VALUES 2");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE d32 < DATE '1970-01-01'")).isFullyPushedDown().matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE d32 >= DATE '2200-01-01'")).isFullyPushedDown().matches("VALUES 2");
            // A bound outside the Date32 range cannot be bound as a parameter
            assertQueryFails("SELECT id FROM " + table.getName() + " WHERE d > date_add('year', -200000, DATE '1970-01-01')", "(?s).*Invalid Date value.*");
        }
    }

    // YDB rejects column names that need delimiting
    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return requiresDelimiting(columnName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String trinoTypeName = dataMappingTestSetup.getTrinoTypeName();
        // YDB does not have a CHAR type
        if (trinoTypeName.equals("char(3)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        if (trinoTypeName.equals("time")
                || trinoTypeName.equals("time(6)")
                || trinoTypeName.equals("timestamp(3)")
                || trinoTypeName.equals("timestamp(6)")
                || trinoTypeName.equals("timestamp(3) with time zone")
                || trinoTypeName.equals("timestamp(6) with time zone")) {
            // The connector does not map YDB time and timestamp types
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        // YDB does not have a CHAR type
        if (dataMappingTestSetup.getTrinoTypeName().equals("char(1)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        // YDB Text is unbounded, so all string columns map to varchar
        return MaterializedResult.resultBuilder(VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // YDB Text is unbounded, so all string columns map to varchar
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(computeScalar("SHOW CREATE TABLE orders")).isEqualTo(
                """
                CREATE TABLE %s.%s.orders (
                   orderkey bigint,
                   custkey bigint,
                   orderstatus varchar,
                   totalprice double,
                   orderdate date,
                   orderpriority varchar,
                   clerk varchar,
                   shippriority integer,
                   comment varchar
                )""".formatted(catalog, schema));
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        abort("YDB does not have a CHAR type");
    }

    @Test
    @Override
    public void testExecuteProcedure()
    {
        // Override because the connector is read-only and YDB tables require a primary key
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;
        assertQuerySucceeds("CALL system.execute('CREATE TABLE " + tableName + " (id Int32 NOT NULL, a Int32, PRIMARY KEY (id))')");
        try {
            assertQuerySucceeds("CALL system.execute('INSERT INTO " + tableName + " (id, a) VALUES (1, 1)')");
            assertQuery("SELECT a FROM " + schemaTableName, "VALUES 1");
            assertQuerySucceeds("CALL system.execute('UPDATE " + tableName + " SET a = 2')");
            assertQuery("SELECT a FROM " + schemaTableName, "VALUES 2");
            assertQuerySucceeds("CALL system.execute('DELETE FROM " + tableName + "')");
            assertQueryReturnsEmptyResult("SELECT a FROM " + schemaTableName);
            assertQuerySucceeds("CALL system.execute('DROP TABLE " + tableName + "')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            assertQuerySucceeds("CALL system.execute('DROP TABLE IF EXISTS " + tableName + "')");
        }
    }

    @Test
    @Override
    public void testExecuteProcedureWithNamedArgument()
    {
        // Override because the connector is read-only and YDB tables require a primary key
        String tableName = "test_execute" + randomNameSuffix();
        assertQuerySucceeds("CALL system.execute('CREATE TABLE " + tableName + " (id Int32 NOT NULL, PRIMARY KEY (id))')");
        try {
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
            assertQuerySucceeds("CALL system.execute(query => 'DROP TABLE " + tableName + "')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            assertQuerySucceeds("CALL system.execute('DROP TABLE IF EXISTS " + tableName + "')");
        }
    }

    // YDB limits table and column names to 255 characters
    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e.getMessage()).contains("too long");
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e.getMessage()).contains("too long");
    }
}
