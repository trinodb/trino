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
package io.prestosql.plugin.cassandra;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;

import java.util.Optional;

import static io.prestosql.plugin.cassandra.CassandraQueryRunner.createCassandraQueryRunner;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCassandraDistributedQueries
        extends AbstractTestDistributedQueries
{
    private CassandraServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = new CassandraServer();
        return createCassandraQueryRunner(server, ImmutableMap.of(), TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        server.close();
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    public void testCreateSchema()
    {
        // Cassandra does not support creating schemas
    }

    @Override
    public void testRenameTable()
    {
        // Cassandra does not support renaming tables
    }

    @Override
    public void testAddColumn()
    {
        // Cassandra does not support adding columns
    }

    @Override
    public void testRenameColumn()
    {
        // Cassandra does not support renaming columns
    }

    @Override
    public void testDropColumn()
    {
        // Cassandra does not support dropping columns
    }

    @Override
    public void testInsertWithCoercion()
    {
        // TODO
        assertThatThrownBy(super::testInsertWithCoercion)
                .hasMessage("unsupported type: decimal(5,3)");
        throw new SkipException("TODO change test to use supported types");
    }

    @Override
    public void testInsertArray()
    {
        // TODO
        assertThatThrownBy(super::testInsertArray)
                .hasMessage("unsupported type: array(double)");
        throw new SkipException("Unsupported");
    }

    @Override
    public void testDelete()
    {
        // Cassandra connector currently does not support delete
    }

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Override
    public void testCommentTable()
    {
        // Cassandra connector currently does not support comment on table
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Cassandra connector does not support column default values");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getPrestoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp")
                || typeName.equals("decimal(5,3)")
                || typeName.equals("decimal(15,3)")
                || typeName.equals("char(3)")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected String dataMappingTableName(String prestoTypeName)
    {
        return "presto_tmp_" + System.nanoTime();
    }
}
