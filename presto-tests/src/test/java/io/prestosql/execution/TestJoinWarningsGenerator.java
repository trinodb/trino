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

package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import io.prestosql.client.Warning;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.WarningCode;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.DistributedQueryRunner;
import io.prestosql.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.spi.connector.StandardWarningCode.BETTER_JOIN_ORDERING;
import static org.testng.Assert.assertTrue;

public class TestJoinWarningsGenerator
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setup() throws Exception
    {
        queryRunner = TpchQueryRunnerBuilder.builder().build();
    }

    @Test
    public void testJoinWarningsGenerator()
    {
        List<Warning> expectedWarnings = ImmutableList.of(
            toClientWarning(new PrestoWarning(BETTER_JOIN_ORDERING, "Try swapping the join ordering for Tables [tpch:orders:sf0.01] InnerJoin Stage IDs [2] in StageId 1"))
        );
        MaterializedResult result = queryRunner.execute("SELECT * FROM orders o JOIN lineitem l ON o.orderkey = l.orderkey AND o.orderkey>10");
        assertWarning(result.getWarnings(), expectedWarnings);

        expectedWarnings = ImmutableList.of(
            toClientWarning(new PrestoWarning(BETTER_JOIN_ORDERING, "Try swapping the join ordering for Stage IDs [3] InnerJoin Stage IDs [4] in StageId 2"))
        );
        MaterializedResult result1 = queryRunner.execute("SELECT * FROM part p, orders o, lineitem l WHERE p.partkey = l.partkey AND l.orderkey = o.orderkey");
        assertWarning(result1.getWarnings(), expectedWarnings);

        expectedWarnings = ImmutableList.of(
            toClientWarning(new PrestoWarning(BETTER_JOIN_ORDERING, "Try swapping the join in StageId 1 where the left side has sources Tables [tpch:orders:sf0.01, tpch:lineitem:sf0.01] and right side has sources Stage IDs [2]"))
        );
        MaterializedResult result2 = queryRunner.execute("SELECT * FROM ((SELECT orderkey FROM orders o) UNION (SELECT orderkey FROM lineitem l)) CROSS JOIN lineitem l LIMIT 5");
        assertWarning(result2.getWarnings(), expectedWarnings);

        expectedWarnings = ImmutableList.of(
            toClientWarning(new PrestoWarning(BETTER_JOIN_ORDERING, "Try swapping the join ordering for Tables [tpch:orders:sf0.01] InnerJoin Stage IDs [2] in StageId 1")),
            toClientWarning(new PrestoWarning(BETTER_JOIN_ORDERING, "Try swapping the join in StageId 1 where the left side has sources Tables [tpch:orders:sf0.01],Stage IDs [2] and right side has sources Stage IDs [3]"))
        );
        MaterializedResult result3 = queryRunner.execute("SELECT * FROM ((SELECT * FROM orders WHERE orderkey < 10) AS a JOIN (SELECT * FROM lineitem) AS b ON a.orderkey = b.orderkey) CROSS JOIN (SELECT * FROM orders WHERE orderkey <10)");
        assertWarning(result3.getWarnings(), expectedWarnings);
    }

    @Test
    public void testSemiJoins()
    {
        List<Warning> expectedWarnings = ImmutableList.of(
          toClientWarning(new PrestoWarning(BETTER_JOIN_ORDERING, "Try swapping the join ordering for Stage IDs [2] SemiJoin Tables [tpch:orders:sf0.01] in StageId 1"))
        );
        MaterializedResult result = queryRunner.execute("SELECT orderkey FROM orders WHERE orderkey IN (SELECT orderkey FROM orders) AND orderkey < 10");
        assertWarning(result.getWarnings(), expectedWarnings);
    }

    private void assertWarning(List<Warning> warningList, List<Warning> expectedWarnings)
    {
        assertTrue(warningList.size() == expectedWarnings.size());
        for (Warning warning : warningList) {
            assertTrue(expectedWarnings.contains(warning));
        }
    }

    private Warning toClientWarning(PrestoWarning warning)
    {
        WarningCode code = warning.getWarningCode();
        return new Warning(new Warning.Code(code.getCode(), code.getName()), warning.getMessage());
    }
}
