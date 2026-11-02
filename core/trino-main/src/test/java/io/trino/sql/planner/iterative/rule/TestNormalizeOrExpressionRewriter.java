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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.iterative.rule.NormalizeOrExpressionRewriter.normalizeOrExpression;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNormalizeOrExpressionRewriter
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction RANDOM_INTEGER = FUNCTIONS.resolveFunction("random", fromTypes(INTEGER));

    private static final Reference X = new Reference(INTEGER, "x");
    private static final Constant ONE = new Constant(INTEGER, 1L);
    private static final Constant TWO = new Constant(INTEGER, 2L);
    private static final Call RANDOM = new Call(RANDOM_INTEGER, ImmutableList.of(new Constant(INTEGER, 5L)));

    @Test
    public void testMergeEqualitiesIntoIn()
    {
        assertThat(normalizeOrExpression(or(
                comparison(EQUAL, X, ONE),
                comparison(EQUAL, X, TWO))))
                .isEqualTo(new In(X, ImmutableList.of(ONE, TWO)));

        assertThat(normalizeOrExpression(or(
                new In(X, ImmutableList.of(ONE)),
                comparison(EQUAL, X, TWO))))
                .isEqualTo(new In(X, ImmutableList.of(ONE, TWO)));
    }

    @Test
    public void testMergeDeduplicatesValues()
    {
        assertThat(normalizeOrExpression(or(
                comparison(EQUAL, X, ONE),
                comparison(EQUAL, X, ONE))))
                .isEqualTo(new In(X, ImmutableList.of(ONE)));

        assertThat(normalizeOrExpression(or(
                new In(X, ImmutableList.of(ONE)),
                comparison(EQUAL, X, ONE))))
                .isEqualTo(new In(X, ImmutableList.of(ONE)));
    }

    @Test
    public void testDoesNotMergeNonDeterministicOperand()
    {
        Expression equalities = or(
                comparison(EQUAL, RANDOM, ONE),
                comparison(EQUAL, RANDOM, TWO));
        assertThat(normalizeOrExpression(equalities)).isEqualTo(equalities);

        Expression inAndEquality = or(
                new In(RANDOM, ImmutableList.of(ONE)),
                comparison(EQUAL, RANDOM, TWO));
        assertThat(normalizeOrExpression(inAndEquality)).isEqualTo(inAndEquality);
    }

    @Test
    public void testDoesNotMergeNonDeterministicValue()
    {
        Expression equalities = or(
                comparison(EQUAL, X, RANDOM),
                comparison(EQUAL, X, RANDOM));
        assertThat(normalizeOrExpression(equalities)).isEqualTo(equalities);

        Expression inAndEquality = or(
                new In(X, ImmutableList.of(RANDOM)),
                comparison(EQUAL, X, RANDOM));
        assertThat(normalizeOrExpression(inAndEquality)).isEqualTo(inAndEquality);
    }

    @Test
    public void testRetainsNonDeterministicDisjunctSharingOperandWithMergedDisjuncts()
    {
        assertThat(normalizeOrExpression(or(
                comparison(EQUAL, X, ONE),
                comparison(EQUAL, X, TWO),
                comparison(EQUAL, X, RANDOM))))
                .isEqualTo(or(
                        comparison(EQUAL, X, RANDOM),
                        new In(X, ImmutableList.of(ONE, TWO))));

        assertThat(normalizeOrExpression(or(
                new In(X, ImmutableList.of(ONE)),
                comparison(EQUAL, X, TWO),
                new In(X, ImmutableList.of(RANDOM)))))
                .isEqualTo(or(
                        new In(X, ImmutableList.of(RANDOM)),
                        new In(X, ImmutableList.of(ONE, TWO))));
    }

    private static Expression or(Expression... terms)
    {
        return new Logical(OR, ImmutableList.copyOf(terms));
    }
}
