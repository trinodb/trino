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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.testing.AbstractTestJoinQueries;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.Map;

import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;

public class TestSortMergeJoinQueries
        extends AbstractTestJoinQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }

    @Test
    public void testSortMergeJoin()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("optimizer.prefer-sort-merge-join", "true")
                .put("spill-enabled", "true")
                .put("spiller-spill-path", Paths.get(System.getProperty("java.io.tmpdir"), "trino", "spills").toString())
                .put("spiller-max-used-space-threshold", "1.0")
                .buildOrThrow();
        try (QueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setExtraProperties(properties)
                .withSplitsPerNode(10)
                .build()) {
            ImmutableSet<String> joinTypes = ImmutableSet.<String>builder()
                    .add("INNER JOIN")
                    .add("LEFT JOIN")
                    .add("RIGHT JOIN")
                    .add("FULL JOIN")
                    .build();
            for (String joinType : joinTypes) {
                testJoinOneKey(queryRunner, joinType);
                testJoinTwoKey(queryRunner, joinType);
                testJoinWithNullKeyAndDupKey(queryRunner, joinType);
            }
        }
    }

    private void testJoinOneKey(QueryRunner queryRunner, String joinType)
    {
        String sql = "SELECT * FROM supplier s " + joinType + " lineitem l ON s.suppkey = l.suppkey";
        MaterializedResult actual = queryRunner.execute(sql);
        MaterializedResult expected = getQueryRunner().execute(sql);
        assertEqualsIgnoreOrder(actual, expected, "For query: \n " + sql);
    }

    private void testJoinWithNullKeyAndDupKey(QueryRunner queryRunner, String joinType)
    {
        String leftTable = "(select suppkey as new_suppkey,* from supplier\n" +
                "UNION ALL\n" +
                "select suppkey as new_suppkey,* from supplier where suppkey in (1,6,7,10,11,12,40,50,80)\n" +
                "UNION ALL\n" +
                "select NULL as new_suppkey,* from supplier where suppkey in (22,23,24,33,88,90,91))";
        String rightTable = "(select suppkey as new_suppkey,* from lineitem\n" +
                "UNION ALL\n" +
                "select suppkey as new_suppkey,* from lineitem where suppkey in (7,12,40,50,90)\n" +
                "UNION ALL\n" +
                "select NULL as new_suppkey,* from lineitem where suppkey in (30,31,32,33,34,45,46,77,78))";

        String sql = String.format("SELECT * FROM %s s %s %s l ON s.new_suppkey = l.new_suppkey", leftTable, joinType, rightTable);
        MaterializedResult actual = queryRunner.execute(sql);
        MaterializedResult expected = getQueryRunner().execute(sql);
        assertEqualsIgnoreOrder(actual, expected, "For query: \n " + sql);
    }

    private void testJoinTwoKey(QueryRunner queryRunner, String joinType)
    {
        String leftTable = "(select suppkey as new_suppkey,* from supplier\n" +
                "UNION ALL\n" +
                "select suppkey as new_suppkey,* from supplier where suppkey in (1,6,7,10,11,12,40,50,80)\n" +
                "UNION ALL\n" +
                "select NULL as new_suppkey,* from supplier where suppkey in (22,23,24,33,88,90,91))";
        String rightTable = "(select suppkey as new_suppkey,* from lineitem\n" +
                "UNION ALL\n" +
                "select suppkey as new_suppkey,* from lineitem where suppkey in (7,12,40,50,90)\n" +
                "UNION ALL\n" +
                "select NULL as new_suppkey,* from lineitem where suppkey in (30,31,32,33,34,45,46,77,78))";

        String sql = String.format("SELECT * FROM %s s %s %s l ON s.new_suppkey = l.new_suppkey and s.suppkey = l.suppkey", leftTable, joinType, rightTable);
        MaterializedResult actual = queryRunner.execute(sql);
        MaterializedResult expected = getQueryRunner().execute(sql);
        assertEqualsIgnoreOrder(actual, expected, "For query: \n " + sql);
    }
}
