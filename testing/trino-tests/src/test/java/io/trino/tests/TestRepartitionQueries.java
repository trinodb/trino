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
package io.trino.tests;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import static java.lang.String.format;

public class TestRepartitionQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }

    @Test
    public void testBoolean()
    {
        testRepartitioning("CASE WHEN mod(custkey + nationkey, 11) = 0 THEN FALSE ELSE TRUE END");
    }

    @Test
    public void testSmallInt()
    {
        testRepartitioning("CAST(custkey AS SMALLINT)");
    }

    @Test
    public void testInteger()
    {
        testRepartitioning("CAST(custkey AS INTEGER)");
    }

    @Test
    public void testBigInt()
    {
        testRepartitioning("CAST(custkey AS BIGINT)");
    }

    @Test
    public void testIpAddress()
    {
        testRepartitioning(
                "CAST (FORMAT('%s.%s.%s.%s', custkey % 255, nationkey % 255, nationkey, nationkey) as ipaddress)",
                "CAST (column_under_test as VARCHAR)",
                "custkey % 255 || '.' || nationkey % 255 || '.' || nationkey || '.' || nationkey");
    }

    @Test
    public void testBigintWithNulls()
    {
        testRepartitioning("CASE WHEN mod(custkey + nationkey, 11) = 0 THEN NULL ELSE custkey END");
    }

    @Test
    public void testVarchar()
    {
        testRepartitioning("comment");
    }

    @Test
    public void testArrayOfBigInt()
    {
        testRepartitioning("ARRAY[custkey, nationkey, nationkey * 2]");
    }

    @Test
    public void testArrayOfArray()
    {
        testRepartitioning(
                "ARRAY[ARRAY[custkey, nationkey], ARRAY[nationkey * 2]]",
                "array_join(column_under_test[1], ',') || ',' ||  array_join(column_under_test[2], ',')",
                "custkey || ',' || nationkey || ',' || nationkey * 2");
    }

    @Test
    public void testStruct()
    {
        testRepartitioning(
                "CAST(" +
                        "    ROW (" +
                        "        custkey," +
                        "        name," +
                        "        acctbal" +
                        "    ) AS ROW(" +
                        "        l_custkey BIGINT," +
                        "        l_name VARCHAR(25)," +
                        "        l_acctbal DOUBLE" +
                        "    )" +
                        ")",
                "FORMAT('%s%s%s', column_under_test.l_custkey, column_under_test.l_name, column_under_test.l_acctbal)",
                "custkey || name || acctbal");
    }

    @Test
    public void testStructWithNulls()
    {
        testRepartitioning(
                "CAST (" +
                        "CASE WHEN mod(custkey + nationkey, 11) = 0 THEN NULL ELSE ROW (custkey, name) END " +
                        "AS ROW(l_custkey BIGINT, l_name VARCHAR(25))" +
                        ")",
                "CASE WHEN column_under_test IS NULL THEN '' ELSE FORMAT('%s%s', column_under_test.l_custkey, column_under_test.l_name) END",
                "CASE WHEN mod(custkey + nationkey, 11) = 0 THEN '' ELSE custkey || name END");
    }

    @Test
    public void testMaps()
    {
        testRepartitioning(
                "MAP(ARRAY[1, 2], ARRAY[custkey, nationkey])",
                "array_join(map_values(column_under_test), ',')",
                "custkey || ',' || nationkey");
    }

    private void testRepartitioning(String columnExpression)
    {
        testRepartitioning(columnExpression, "column_under_test", columnExpression);
    }

    private void testRepartitioning(String columnExpression, String columnProjection, String actualColumnExpression)
    {
        // the join does not filter out any rows so the query just returns columnExpression,
        // formatted using columnProjection (used in case columnExpression is not supported by h2).
        // expected query uses actualColumnExpression to format the value in h2 in the same way as columnExpression
        assertQuery(format("WITH custkey_ex AS (" +
                                "  SELECT" +
                                "    custkey," +
                                "    nationkey," +
                                "    %s AS column_under_test" +
                                "  FROM customer" +
                                ")" +
                                "SELECT %s FROM (" +
                                "  SELECT c.column_under_test " +
                                "  FROM custkey_ex c, nation n " +
                                "  WHERE c.nationkey = n.nationkey" +
                                ")",
                        columnExpression, columnProjection),
                format("SELECT %s AS column_under_test FROM customer", actualColumnExpression));
    }
}
