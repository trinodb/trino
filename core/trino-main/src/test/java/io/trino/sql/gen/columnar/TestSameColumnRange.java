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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.TestingIr.comparison;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSameColumnRange
{
    private static final Reference A = new Reference(BIGINT, "a");
    private static final Reference B = new Reference(BIGINT, "b");

    private static final Expression A_LOWER = comparison(GREATER_THAN_OR_EQUAL, A, constant(1));
    private static final Expression A_UPPER = comparison(LESS_THAN, A, constant(10));
    private static final Expression B_LOWER = comparison(GREATER_THAN, B, constant(1));
    private static final Expression B_UPPER = comparison(LESS_THAN_OR_EQUAL, B, constant(10));
    private static final Expression A_EQUAL = comparison(EQUAL, A, constant(5));

    @Test
    public void testPairIsFusedInPlace()
    {
        assertThat(SameColumnRange.fuseTerms(ImmutableList.of(A_LOWER, A_UPPER)))
                .containsExactly(range(A_LOWER, A_UPPER));
        // the fused term takes the slot of the earlier bound, and the lower bound always comes first
        assertThat(SameColumnRange.fuseTerms(ImmutableList.of(A_UPPER, A_EQUAL, A_LOWER)))
                .containsExactly(range(A_LOWER, A_UPPER), A_EQUAL);
    }

    @Test
    public void testIndependentColumns()
    {
        assertThat(SameColumnRange.fuseTerms(ImmutableList.of(A_LOWER, B_UPPER, B_LOWER, A_UPPER)))
                .containsExactly(range(A_LOWER, A_UPPER), range(B_LOWER, B_UPPER));
    }

    @Test
    public void testNothingToFuse()
    {
        List<Expression> terms = ImmutableList.of(A_LOWER, B_UPPER, A_EQUAL);
        assertThat(SameColumnRange.fuseTerms(terms)).isEqualTo(terms);

        // two bounds in the same direction, and a comparison between two columns
        terms = ImmutableList.of(A_LOWER, comparison(GREATER_THAN, A, constant(2)), comparison(LESS_THAN, A, B));
        assertThat(SameColumnRange.fuseTerms(terms)).isEqualTo(terms);

        // a null constant is never a bound
        terms = ImmutableList.of(A_LOWER, comparison(LESS_THAN, A, new Constant(BIGINT, null)));
        assertThat(SameColumnRange.fuseTerms(terms)).isEqualTo(terms);

        // only primitive columns are fused
        Reference text = new Reference(VARCHAR, "text");
        terms = ImmutableList.of(
                comparison(GREATER_THAN_OR_EQUAL, text, new Constant(VARCHAR, utf8Slice("a"))),
                comparison(LESS_THAN, text, new Constant(VARCHAR, utf8Slice("m"))));
        assertThat(SameColumnRange.fuseTerms(terms)).isEqualTo(terms);
    }

    @Test
    public void testThirdBoundStaysSeparate()
    {
        Expression secondUpper = comparison(LESS_THAN, A, constant(20));
        assertThat(SameColumnRange.fuseTerms(ImmutableList.of(A_LOWER, A_UPPER, secondUpper)))
                .containsExactly(range(A_LOWER, A_UPPER), secondUpper);
    }

    @Test
    public void testIsFusedRange()
    {
        assertThat(SameColumnRange.isFusedRange(range(A_LOWER, A_UPPER))).isTrue();
        assertThat(SameColumnRange.isFusedRange(range(A_UPPER, A_LOWER))).isFalse();
        assertThat(SameColumnRange.isFusedRange(range(A_LOWER, B_UPPER))).isFalse();
        assertThat(SameColumnRange.isFusedRange(range(A_LOWER, A_EQUAL))).isFalse();
        assertThat(SameColumnRange.isFusedRange(new Logical(AND, ImmutableList.of(A_LOWER, A_UPPER, A_EQUAL)))).isFalse();
        assertThat(SameColumnRange.isFusedRange(A_LOWER)).isFalse();
    }

    private static Expression range(Expression lower, Expression upper)
    {
        return new Logical(AND, ImmutableList.of(lower, upper));
    }

    private static Constant constant(long value)
    {
        return new Constant(BIGINT, value);
    }
}
