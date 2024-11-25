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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;

import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.gen.columnar.FilterEvaluator.isReorderingSafe;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.ir.Logical.Operator.OR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFilterReorderingSafety
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final PlannerContext PLANNER_CONTEXT = FUNCTION_RESOLUTION.getPlannerContext();
    private static final Reference REF_A = new Reference(BIGINT, "a");
    private static final Reference REF_B = new Reference(BIGINT, "b");
    private static final Expression SAFE_A = new Comparison(GREATER_THAN, REF_A, new Constant(BIGINT, 0L));
    private static final Expression SAFE_B = new Comparison(GREATER_THAN, REF_B, new Constant(BIGINT, 0L));

    @Test
    void testAllTermsSafe()
    {
        assertThat(isReorderingSafe(PLANNER_CONTEXT, ImmutableList.of(SAFE_A, SAFE_B))).isTrue();
    }

    @Test
    void testUnsafeTermDisablesReordering()
    {
        ResolvedFunction divide = FUNCTION_RESOLUTION.resolveOperator(DIVIDE, ImmutableList.of(BIGINT, BIGINT));
        Expression unsafe = new Comparison(GREATER_THAN, call(divide, new Constant(BIGINT, 100L), REF_B), new Constant(BIGINT, 0L));
        assertThat(isReorderingSafe(PLANNER_CONTEXT, ImmutableList.of(SAFE_A, unsafe))).isFalse();
    }

    @Test
    void testNestedLogicalWithUnsafeTerm()
    {
        ResolvedFunction divide = FUNCTION_RESOLUTION.resolveOperator(DIVIDE, ImmutableList.of(BIGINT, BIGINT));
        Expression unsafe = new Comparison(GREATER_THAN, call(divide, new Constant(BIGINT, 100L), REF_B), new Constant(BIGINT, 0L));
        Expression nested = new Logical(OR, ImmutableList.of(SAFE_B, unsafe));
        assertThat(isReorderingSafe(PLANNER_CONTEXT, ImmutableList.of(SAFE_A, nested))).isFalse();
    }

    @Test
    void testSafeDivisionByNonZeroConstant()
    {
        ResolvedFunction divide = FUNCTION_RESOLUTION.resolveOperator(DIVIDE, ImmutableList.of(BIGINT, BIGINT));
        Expression safeDiv = new Comparison(GREATER_THAN, call(divide, REF_A, new Constant(BIGINT, 2L)), new Constant(BIGINT, 0L));
        assertThat(isReorderingSafe(PLANNER_CONTEXT, ImmutableList.of(SAFE_B, safeDiv))).isTrue();
    }
}
