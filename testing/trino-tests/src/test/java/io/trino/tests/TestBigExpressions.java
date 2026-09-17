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
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

public class TestBigExpressions
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunner.builder().setWorkerCount(1).build();
    }

    @Test
    public void testComplexSwitch()
    {
        assertQueryFails("SELECT %s FROM nation".formatted(generateCase("comment", 5, 5)),
                "Failed to execute query; there may be too many columns used or expressions are too complex");
    }

    private static String generateCase(String column, int whenCases, int depth)
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder("CASE ");
        for (int i = 0; i < whenCases; i++) {
            sb.append(" WHEN %s IN ('%s') THEN (%s)".formatted(
                    column,
                    random.nextInt(1000),
                    depth == 0 ? random.nextInt() : generateCase(column, whenCases, depth - 1)));
        }
        return sb.append(" ELSE -1 END").toString();
    }
}
