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
package io.trino.plugin.kudu;

import io.trino.testing.AbstractTestDistributedQueries;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;

public class TestKuduDistributedQueries
        extends AbstractTestDistributedQueries
{
    private TestingKuduServer kuduServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer();
        return createKuduQueryRunnerTpch(kuduServer, Optional.of(""), REQUIRED_TPCH_TABLES);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        kuduServer.close();
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
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
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Kudu connector does not support column default values");
    }

    @Override
    public void testInsert()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
    }

    @Override
    public void testCommentTable()
    {
        // TODO
        throw new SkipException("TODO");
    }

    @Override
    public void testCommentColumn()
    {
        // TODO
        throw new SkipException("TODO");
    }

    @Override
    public void testAddColumn()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testCreateTable()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testInsertUnicode()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testDropNonEmptySchema()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testDelete()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    protected void skipTestUnlessSupportsDeletes()
    {
        // TODO Remove override once kudu connector can create tables with default partitions
        if (!supportsDelete()) {
            assertQueryFails("DELETE FROM region", "This connector does not support deletes");
            throw new SkipException("This connector does not support deletes");
        }
    }

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "nullable, encoding=auto, compression=default", "")
                .row("custkey", "bigint", "nullable, encoding=auto, compression=default", "")
                .row("orderstatus", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("totalprice", "double", "nullable, encoding=auto, compression=default", "")
                .row("orderdate", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("orderpriority", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("clerk", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("shippriority", "integer", "nullable, encoding=auto, compression=default", "")
                .row("comment", "varchar", "nullable, encoding=auto, compression=default", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Override
    @Test
    public void testWrittenStats()
    {
        // TODO Kudu connector supports CTAS and inserts, but the test would fail
        throw new SkipException("TODO");
    }

    @Override
    public void testColumnName(String columnName)
    {
        // TODO (https://github.com/trinodb/trino/issues/3477) enable the test
        throw new SkipException("TODO");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("date") // date gets stored as varchar
                || typeName.equals("varbinary") // TODO (https://github.com/trinodb/trino/issues/3416)
                || (typeName.startsWith("char") && dataMappingTestSetup.getSampleValueLiteral().contains(" "))) { // TODO: https://github.com/trinodb/trino/issues/3597
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }
}
