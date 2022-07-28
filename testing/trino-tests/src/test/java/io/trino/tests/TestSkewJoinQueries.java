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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

public class TestSkewJoinQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.of("optimizer.join-reordering-strategy", "NONE"))
                .amendSession(builder -> builder.setSystemProperty("skewed_join_metadata", "[[\"tpch.tiny.orders\", \"orderkey\", \"1\"]]"))
                .build();
    }

    @Test
    public void testEnsureSkewedKeyExists()
    {
        assertQuery("SELECT orderkey FROM orders WHERE orderkey = 1 LIMIT 1", "VALUES 1");
    }

    @Test
    public void testSkewedLeftJoin()
    {
        String sql = """
                SELECT o.orderkey, SUM(l.suppkey) FROM orders o LEFT JOIN lineitem l ON o.orderkey = l.orderkey
                WHERE l.suppkey > 10 GROUP BY o.orderkey ORDER BY 2 DESC LIMIT 10
                """;
        assertExplain("EXPLAIN " + sql, "\\QInnerJoin[criteria = (\"orderkey\" = \"orderkey_1\") AND (\"randpart\" = \"skewpart\"), hash");
        assertQuery(sql);
    }

    @Test
    public void testSkewedInnerJoin()
    {
        String sql = """
                SELECT o.orderkey, SUM(l.suppkey) FROM orders o INNER JOIN lineitem l ON o.orderkey = l.orderkey
                WHERE l.suppkey > 10 GROUP BY o.orderkey ORDER BY 2 DESC LIMIT 10
                """;
        assertExplain("EXPLAIN " + sql, "\\QInnerJoin[criteria = (\"orderkey\" = \"orderkey_1\") AND (\"randpart\" = \"skewpart\"), hash");
        assertQuery(sql);
    }

    @Test
    public void testNestedInnerJoin()
    {
        assertQuery("""
            SELECT o.orderkey, SUM(l.suppkey) FROM orders o INNER JOIN lineitem l ON o.orderkey = l.orderkey
            WHERE l.suppkey > 10 GROUP BY o.orderkey ORDER BY 2 DESC LIMIT 10""");
    }
}
