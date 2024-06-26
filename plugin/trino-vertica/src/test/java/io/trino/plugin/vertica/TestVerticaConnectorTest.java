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

package io.trino.plugin.vertica;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.vertica.VerticaQueryRunner.createVerticaQueryRunner;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestVerticaConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingVerticaServer verticaServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verticaServer = closeAfterClass(new TestingVerticaServer());
        return createVerticaQueryRunner(
                verticaServer,
                ImmutableMap.of(),
                ImmutableMap.of(),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                    SUPPORTS_TOPN_PUSHDOWN,
                    SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                    SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                    SUPPORTS_DROP_COLUMN,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                    SUPPORTS_ARRAY,
                    SUPPORTS_ROW_TYPE,
                    SUPPORTS_SET_COLUMN_TYPE,
                    SUPPORTS_AGGREGATION_PUSHDOWN ->
                    false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                this::execute,
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        // shippriority column's type is BIGINT, not INTEGER
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertThat(actualColumns).isEqualTo(expectedColumns);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // shippriority column's type is BIGINT, not INTEGER
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE vertica.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority bigint,\n" +
                        "   comment varchar(79)\n" +
                        ")");
    }

    @Test
    @Override
    public void testShowColumns()
    {
        // shippriority column's type is BIGINT, not INTEGER
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    @Override
    public void testCreateTableAsSelectWithUnicode()
    {
        assertThatThrownBy(super::testCreateTableAsSelectWithUnicode)
                .hasStackTraceContaining("ERROR: String of 3 octets is too long for type Long Varchar(1) for column unicode");

        assertCreateTableAsSelect(
                "SELECT CAST('\u2603' AS VARCHAR(3)) unicode",
                "SELECT 1");
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        // TODO Fix this failure
        assertThatThrownBy(super::testInsertIntoNotNullColumn)
                .hasMessageContaining("Cannot set a NOT NULL column");
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format(".*Cannot set a NOT NULL column \\(%s\\) to a NULL value in INSERT/UPDATE statement", columnName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("date")) {
            // Vertica adds 10 days during julian-gregorian switch
            if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-05'")) {
                return Optional.empty();
            }
            return Optional.of(dataMappingTestSetup);
        }

        if (typeName.equals("time")
                || typeName.equals("time(6)")
                || typeName.equals("timestamp")
                || typeName.equals("timestamp(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")
                || typeName.equals("varbinary")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("real")
                || typeName.equals("double")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Maximum limit is 128 octets");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Maximum limit is 128 octets");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Maximum limit is 128 octets");
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(
                "\\[Vertica]\\[VJDBC]\\(2505\\) ROLLBACK: Cannot set column \"b_varchar\" in table " +
                        "\"tpch.test_add_nn.*\" to NOT NULL since it contains null values");
    }

    @Override
    protected TestTable simpleTable()
    {
        TestTable table = new TestTable(onRemoteDatabase(), format("%s.simple_table", getSession().getSchema().orElseThrow()), "(col BIGINT)");
        assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2", 2);
        return table;
    }

    private void execute(String sql)
    {
        verticaServer.execute(sql);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return verticaServer::execute;
    }
}
