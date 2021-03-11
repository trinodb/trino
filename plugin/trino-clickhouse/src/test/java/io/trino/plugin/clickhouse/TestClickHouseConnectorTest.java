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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

public class TestClickHouseConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingClickHouseServer clickhouseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = closeAfterClass(new TestingClickHouseServer());
        return createClickHouseQueryRunner(
                clickhouseServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("clickhouse.map-string-as-varchar", "true")
                        .put("allow-drop-table", "true")
                        .build(),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean supportsDelete()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        return false;
    }

    @Override
    @Test(dataProvider = "testColumnNameDataProvider")
    public void testColumnName(String columnName)
    {
        // TODO (https://github.com/trinodb/trino/issues/7101) enable the test
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRenameColumn()
    {
        // TODO (https://github.com/trinodb/trino/issues/7101) enable the test
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDropColumn()
    {
        // TODO (https://github.com/trinodb/trino/issues/7101) enable the test
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAddColumn()
    {
        // TODO (https://github.com/trinodb/trino/issues/7101) enable the test
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE clickhouse.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")");
    }

    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                clickhouseServer::execute,
                "tpch.tbl",
                "(col_required Int64," +
                        "col_nullable Nullable(Int64)," +
                        "col_default Nullable(Int64) DEFAULT 43," +
                        "col_nonnull_default Int64 DEFAULT 42," +
                        "col_required2 Int64) ENGINE=Log");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        switch (dataMappingTestSetup.getTrinoTypeName()) {
            case "boolean":
                // ClickHouse does not have built-in support for boolean type and we map boolean to tinyint.
                // Querying the column with a boolean predicate subsequently fails with "Cannot apply operator: tinyint = boolean"
                return Optional.empty();

            case "real":
                // TODO (https://github.com/trinodb/trino/issues/7101) enable the test
                return Optional.empty();

            case "varbinary":
                // here in this test class we map ClickHouse String into varchar, so varbinary ends up a varchar
                return Optional.empty();

            case "date":
                // TODO (https://github.com/trinodb/trino/issues/7101) enable the test
                return Optional.empty();

            case "time":
            case "timestamp":
            case "timestamp(3) with time zone":
                return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }
}
