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

import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.ResourceLock;
import tech.ydb.test.junit5.YdbHelperExtension;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.assertj.core.api.Assertions.assertThat;

@ResourceLock("YDB_HELPER")
public class TestYdbConnectorTest
        extends BaseConnectorTest
{
    @RegisterExtension
    static final YdbHelperExtension ydb = new YdbHelperExtension().failIfUnavailable();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return YdbQueryRunner.builder(ydb)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Test
    public void testDefaultSchema()
    {
        assertThat(computeActual("SHOW CATALOGS").getOnlyColumnAsSet()).contains("local");
        assertThat(computeActual("SHOW SCHEMAS FROM local").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("default", "information_schema");
        assertThat(computeActual("SHOW TABLES FROM local.default").getOnlyColumnAsSet()).contains("orders");
        assertQuerySucceeds("SELECT orderkey FROM local.default.orders LIMIT 1");
        assertQueryFails("SELECT * FROM local.missing.orders", ".*Schema 'missing' does not exist");
        assertQueryFails("SELECT * FROM local.\"%\".orders", ".*Schema '%' does not exist");
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_TABLE,
                 SUPPORTS_INSERT,
                 SUPPORTS_UPDATE,
                 SUPPORTS_DELETE,
                 SUPPORTS_MERGE,
                 SUPPORTS_ADD_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_TRUNCATE,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_DROP_SCHEMA_CASCADE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ADD_COLUMN_WITH_POSITION,
                 SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_ARRAY,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_SET_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_DROP_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return requiresDelimiting(columnName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(BaseConnectorTest.DataMappingTestSetup dataMappingTestSetup)
    {
        String trinoTypeName = dataMappingTestSetup.getTrinoTypeName();
        if (trinoTypeName.equals("char(3)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        else if (trinoTypeName.equals("time")
                || trinoTypeName.equals("time(6)")
                || trinoTypeName.equals("timestamp(3) with time zone")
                || trinoTypeName.equals("timestamp(6) with time zone")) {
            // Нет time в YQL
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        if (dataMappingTestSetup.getTrinoTypeName().equals("char(1)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        // В YQL строки произвольной длины
        return MaterializedResult.resultBuilder(this.getSession(), new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR}).row(new Object[] {"orderkey", "bigint", "", ""}).row(new Object[] {"custkey", "bigint", "", ""}).row(new Object[] {"orderstatus", "varchar", "", ""}).row(new Object[] {"totalprice", "double", "", ""}).row(new Object[] {"orderdate", "date", "", ""}).row(new Object[] {"orderpriority", "varchar", "", ""}).row(new Object[] {"clerk", "varchar", "", ""}).row(new Object[] {"shippriority", "integer", "", ""}).row(new Object[] {"comment", "varchar", "", ""}).build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // В YQL строки произвольной длины
        String catalog = this.getSession().getCatalog().orElseThrow();
        String schema = this.getSession().getSchema().orElseThrow();
        Assertions.assertThat(this.computeScalar("SHOW CREATE TABLE orders")).isEqualTo(String.format("CREATE TABLE %s.%s.orders (\n   orderkey bigint,\n   custkey bigint,\n   orderstatus varchar,\n   totalprice double,\n   orderdate date,\n   orderpriority varchar,\n   clerk varchar,\n   shippriority integer,\n   comment varchar\n)", catalog, schema));
    }

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

    @Test
    public void testNativeTypeReads()
    {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable table = new TestTable(
                executor,
                "native_types_",
                "(id Int64, u Uint64, s Utf8, b String, d Date32, t Timestamp64, n Decimal(22,9), PRIMARY KEY (id))")) {
            executor.execute("INSERT INTO " + table.getName() + " (id, u, s, b, d, t, n) VALUES " +
                    "(1, 18446744073709551615ul, 'hello'u, String('\\x00\\x80\\xff'), Date32('1969-12-31'), " +
                    "Timestamp64('1969-12-31T23:59:59.999999Z'), Decimal('1.25', 22, 9)), " +
                    "(2, NULL, NULL, NULL, NULL, NULL, NULL)");
            assertQuery("SELECT id, u, s, b, d, t, n FROM " + table.getName(),
                    "VALUES (CAST(1 AS BIGINT), CAST('18446744073709551615' AS DECIMAL(20,0)), 'hello', X'0080FF', DATE '1969-12-31', " +
                            "TIMESTAMP '1969-12-31 23:59:59.999999', CAST('1.25' AS DECIMAL(22,9))), " +
                            "(CAST(2 AS BIGINT), NULL, NULL, NULL, NULL, NULL, NULL)");
        }
    }

    @Test
    public void testReadOnlyWritesRejected()
    {
        JdbcSqlExecutor executor = new JdbcSqlExecutor(YdbQueryRunner.buildJdbcUrl(ydb));
        try (TestTable table = new TestTable(
                executor,
                "read_only_",
                "(id Int64, value Utf8, PRIMARY KEY (id))")) {
            executor.execute("INSERT INTO " + table.getName() + " (id, value) VALUES (1, 'original'u)");
            String copy = table.getName() + "_copy";
            try {
                for (String sql : List.of(
                        "CREATE TABLE " + copy + " (id bigint)",
                        "INSERT INTO " + table.getName() + " VALUES (2, 'new')",
                        "UPDATE " + table.getName() + " SET value = 'changed' WHERE id = 1",
                        "DELETE FROM " + table.getName() + " WHERE id = 1",
                        "MERGE INTO " + table.getName() + " t USING (VALUES (1, 'changed')) s(id, value) " +
                                "ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value",
                        "ALTER TABLE " + table.getName() + " ADD COLUMN added bigint",
                        "ALTER TABLE " + table.getName() + " RENAME TO " + copy,
                        "DROP TABLE " + table.getName())) {
                    assertThat(query(sql)).failure().hasErrorCode(NOT_SUPPORTED);
                }
                assertQuery("SELECT id, value FROM " + table.getName(), "VALUES (CAST(1 AS BIGINT), 'original')");
            }
            finally {
                executor.execute("DROP TABLE IF EXISTS " + copy);
            }
        }
    }
}
