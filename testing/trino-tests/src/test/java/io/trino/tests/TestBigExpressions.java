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

import java.util.concurrent.atomic.AtomicInteger;

public class TestBigExpressions
        extends AbstractTestQueryFramework
{
    private static final String PROJECTION_TOO_COMPLEX = "Failed to execute query; there may be too many columns used or expressions are too complex";
    private static final String FILTER_TOO_COMPLEX = "Query exceeded maximum filters\\. Please reduce the number of filters referenced and re-run the query\\.";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunner.builder().setWorkerCount(1).build();
    }

    @Test
    public void testComplexSwitchInProjection()
    {
        assertQueryFails("SELECT %s FROM nation".formatted(generateCase("comment", 5, 5)), PROJECTION_TOO_COMPLEX);
    }

    @Test
    public void testComplexSwitchInFilter()
    {
        assertQueryFails("SELECT nationkey FROM nation WHERE %s > 0".formatted(generateCase("comment", 5, 5)), FILTER_TOO_COMPLEX);
    }

    /**
     * Builds a nested CASE with {@code whenCases ^ (depth + 1)} branches, each holding a distinct literal
     * so that no clause is folded away before compilation.
     */
    private static String generateCase(String column, int whenCases, int depth)
    {
        return generateCase(column, whenCases, depth, new AtomicInteger());
    }

    private static String generateCase(String column, int whenCases, int depth, AtomicInteger literals)
    {
        StringBuilder builder = new StringBuilder("CASE ");
        for (int i = 0; i < whenCases; i++) {
            String result;
            if (depth == 0) {
                result = String.valueOf(literals.getAndIncrement());
            }
            else {
                result = generateCase(column, whenCases, depth - 1, literals);
            }
            builder.append(" WHEN %s IN ('%s') THEN (%s)".formatted(column, literals.getAndIncrement(), result));
        }
        return builder.append(" ELSE -1 END").toString();
    }
}
