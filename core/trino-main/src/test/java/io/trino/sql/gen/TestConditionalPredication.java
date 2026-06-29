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
package io.trino.sql.gen;

import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.ConditionalPredication.isCheap;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.Logical.Operator.AND;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConditionalPredication
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();

    private static final Reference A = new Reference(BIGINT, "a");
    private static final Reference B = new Reference(BIGINT, "b");

    @Test
    void testLeavesAreCheap()
    {
        assertThat(isCheap(A)).isTrue();
        assertThat(isCheap(new Constant(BIGINT, 1L))).isTrue();
        assertThat(isCheap(new IsNull(A))).isTrue();
    }

    @Test
    void testOperatorCallsAreCheap()
    {
        // comparisons and arithmetic are operators -> cheap
        assertThat(isCheap(comparison(FUNCTIONS.getMetadata(), LESS_THAN, A, B))).isTrue();
        assertThat(isCheap(call(FUNCTIONS.resolveOperator(ADD, List.of(BIGINT, BIGINT)), A, B))).isTrue();
    }

    @Test
    void testFunctionCallsAndCastsAreNotCheap()
    {
        Expression length = FUNCTIONS.functionCallBuilder("length")
                .addArgument(VARCHAR, new Reference(VARCHAR, "s"))
                .build();
        assertThat(isCheap(length)).isFalse();
        assertThat(isCheap(new Cast(new Reference(VARCHAR, "s"), BIGINT))).isFalse();
    }

    @Test
    void testCompositeIsCheapOnlyWhenEveryChildIsCheap()
    {
        Reference predicate = new Reference(BOOLEAN, "p");
        Expression cheapTerm = comparison(FUNCTIONS.getMetadata(), LESS_THAN, A, B);
        Expression expensiveTerm = new Cast(new Reference(VARCHAR, "s"), BOOLEAN);

        assertThat(isCheap(new Logical(AND, List.of(predicate, cheapTerm)))).isTrue();
        assertThat(isCheap(new Logical(AND, List.of(predicate, expensiveTerm)))).isFalse();
    }
}
