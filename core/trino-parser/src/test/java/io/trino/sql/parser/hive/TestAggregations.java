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
package io.trino.sql.parser.hive;

import org.testng.annotations.Test;

public class TestAggregations
        extends SQLTester
{
    @Test
    public void testGroupBy()
    {
        String sql = "SELECT a, b, count(1) as cnt from tb1 group by a, b";

        checkASTNode(sql);
    }

    @Test
    public void testGroupByCube()
    {
        String prestoSql = "SELECT a, b, count(1) as cnt from tb1 group by CUBE(a, b)";
        String hiveSql = "SELECT a, b, count(1) as cnt from tb1 group by a, b with CUBE";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testGroupByRollup()
    {
        String prestoSql = "SELECT a, b, count(1) as cnt from tb1 group by ROLLUP(a, b)";
        String hiveSql = "SELECT a, b, count(1) as cnt from tb1 group by a, b with ROLLUP";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testGroupByGroupingSets()
    {
        String prestoSql = "SELECT a, b, c, count(1) as cnt from tb1 group by GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ( ))";
        String hiveSql = "SELECT a, b, c, count(1) as cnt from tb1 GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ( ))";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testHaving()
    {
        String sql = "SELECT a from tb1 group by a having COUNT(b) > 25";

        checkASTNode(sql);
    }
}
