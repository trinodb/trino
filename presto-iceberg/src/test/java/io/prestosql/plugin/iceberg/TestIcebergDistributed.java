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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;

public class TestIcebergDistributed
        extends AbstractTestDistributedQueries
{
    public static class Main
    {
        public static void main(String[] args)
                throws Exception
        {
            Logging.initialize();

            for (long attempt = 1; ; attempt++) {
                System.out.println("XYZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ attempt = " + attempt);

                TestIcebergDistributed test = new TestIcebergDistributed();
                try {
                    System.out.println("XYZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ init()");
                    test.init();

                    System.out.println("XYZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ testScalarSubquery()");
//                test.testScalarSubquery();

                    for (int i = 0; i < 30; i++) {
                        test.assertQuery("SELECT DISTINCT orderkey FROM lineitem " +
                                "WHERE orderkey BETWEEN" +
                                "   (SELECT avg(orderkey) FROM orders) - 10 " +
                                "   AND" +
                                "   (SELECT avg(orderkey) FROM orders) + 10");
                    }
                }
                finally {
                    System.out.println("XYZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ close()");
                    test.close();
                }
            }
        }
    }

    @Test public void testFooBar1() { testFooBar(); }
    @Test public void testFooBar2() { testFooBar(); }
    @Test public void testFooBar3() { testFooBar(); }
    @Test public void testFooBar4() { testFooBar(); }
    @Test public void testFooBar5() { testFooBar(); }
    @Test public void testFooBar6() { testFooBar(); }

    @Test
    public void testFooBar()
    {
        super.testCorrelatedJoin();
        super.testScalarSubquery();
        super.testCorrelatedExistsSubqueries();
        super.testCorrelatedInPredicateSubqueries();
        super.testCorrelatedScalarSubqueries();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of());
    }

    @Override
    protected boolean supportsViews()
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
        throw new SkipException("Iceberg connector does not support column default values");
    }

    @Override
    public void testDelete()
    {
        // TODO (https://github.com/prestosql/presto/pull/4639#issuecomment-700737583)
    }

    @Override
    public void testRenameTable()
    {
        assertQueryFails("ALTER TABLE orders RENAME TO rename_orders", "Rename not supported for Iceberg tables");
    }

    @Override
    public void testInsertWithCoercion()
    {
        // Iceberg does not support parameterized varchar
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getPrestoTypeName();
        if (typeName.equals("tinyint")
                || typeName.equals("smallint")
                || typeName.startsWith("char(")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.startsWith("decimal(")
                || typeName.equals("time")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        if (typeName.equals("timestamp")) {
            return Optional.of(new DataMappingTestSetup("timestamp(6)", "TIMESTAMP '2020-02-12 15:03:00'", "TIMESTAMP '2199-12-31 23:59:59.999999'"));
        }

        if (typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(new DataMappingTestSetup("timestamp(6) with time zone", "TIMESTAMP '2020-02-12 15:03:00 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999999 +12:00'"));
        }

        return Optional.of(dataMappingTestSetup);
    }
}
