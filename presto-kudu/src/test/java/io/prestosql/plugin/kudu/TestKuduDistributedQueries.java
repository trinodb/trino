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
package io.prestosql.plugin.kudu;

import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;

import java.util.Optional;

import static io.prestosql.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;

public class TestKuduDistributedQueries
        extends AbstractTestDistributedQueries
{
    private TestingKuduServer kuduServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer();
        return createKuduQueryRunnerTpch(kuduServer, Optional.of(""), TpchTable.getTables());
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
    public void testDataMappingSmokeTest(DataMappingTestSetup dataMappingTestSetup)
    {
        // TODO Support these test once kudu connector can create tables with default partitions
    }

    @Override
    public void testPredicatePushdown()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_is_null (" +
                "id INT WITH (primary_key=true), " +
                "col_nullable bigint with (nullable=true)" +
                ") WITH (" +
                " partition_by_hash_columns = ARRAY['id'], " +
                " partition_by_hash_buckets = 2" +
                ")");

        assertUpdate("INSERT INTO test_is_null VALUES (1, 1)", 1);
        assertUpdate("INSERT INTO test_is_null(id) VALUES (2)", 1);
        assertQuery("SELECT id FROM test_is_null WHERE col_nullable = 1 OR col_nullable IS NULL", "VALUES (1), (2)");
    }

    @Override
    public void testAddColumn()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
    }

    @Override
    public void testCreateTable()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
    }

    @Override
    public void testInsertUnicode()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
    }

    @Override
    public void testInsertWithCoercion()
    {
        // Override because of non-canonical varchar mapping
    }

    @Override
    public void testDelete()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
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
    public void testCommentTable()
    {
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }

    @Override
    public void testWrittenStats()
    {
        // TODO Kudu connector supports CTAS and inserts, but the test would fail
    }
}
