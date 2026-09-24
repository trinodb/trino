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
package io.trino.sql.ir;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionFormatter
{
    @Test
    public void testReference()
    {
        assertFormattedExpression(
                new Reference(BIGINT, "abc"),
                "abc");
        assertFormattedExpression(
                new Reference(BIGINT, "with a space"),
                "\"with a space\"");
        assertFormattedExpression(
                new Reference(BIGINT, "with \" quote, $ dollar and ' apostrophe"),
                "\"with \"\" quote, $ dollar and ' apostrophe\"");
    }

    @Test
    public void testEveryExpressionConsidered()
            throws Exception
    {
        assertAllMethodsOverridden(IrVisitor.class, ExpressionFormatter.Formatter.class, Set.of(
                // process(..) have reasonable defaults
                IrVisitor.class.getMethod("process", Expression.class),
                IrVisitor.class.getMethod("process", Expression.class, Object.class /*context*/)));
    }

    @Test
    public void testLet()
    {
        assertFormattedExpression(
                new Let(new Symbol(BIGINT, "x"),
                        new Constant(BIGINT, 1L),
                        new Reference(BIGINT, "x")),
                "LET(x::[bigint] = bigint '1', x)");
    }

    @Test
    public void testSecureExpressionIsRedacted()
    {
        SecureExpression secure = new SecureExpression(comparison(
                GREATER_THAN,
                new Reference(BIGINT, "policy_secret"),
                new Constant(BIGINT, 100000L)));

        assertFormattedExpression(secure, SecureExpression.REDACTED);
    }

    @Test
    public void testSecureTermsOfLogicalExpressionPrintAsOneMarker()
    {
        // How many secure terms a conjunction holds depends on how the policy relates to the query, so only one marker is printed
        SecureExpression policy = new SecureExpression(comparison(GREATER_THAN, new Reference(BIGINT, "policy_secret"), new Constant(BIGINT, 100000L)));
        SecureExpression derived = new SecureExpression(comparison(GREATER_THAN, new Reference(BIGINT, "policy_secret"), new Constant(BIGINT, 100001L)));
        Expression visible = comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L));

        assertFormattedExpression(new Logical(AND, ImmutableList.of(policy, derived)), SecureExpression.REDACTED);
        assertFormattedExpression(new Logical(OR, ImmutableList.of(policy, derived)), SecureExpression.REDACTED);
        assertFormattedExpression(new Logical(AND, ImmutableList.of(visible, policy, derived)), "((bigint '1' < x) AND [REDACTED])");
        assertFormattedExpression(new Logical(AND, ImmutableList.of(policy, visible, derived)), "([REDACTED] AND (bigint '1' < x))");
        assertFormattedExpression(new Logical(AND, ImmutableList.of(policy, new Logical(OR, ImmutableList.of(derived, policy)))), SecureExpression.REDACTED);
        assertFormattedExpression(new Logical(AND, ImmutableList.of(visible, new Logical(OR, ImmutableList.of(derived, policy)))), "((bigint '1' < x) AND [REDACTED])");
        assertFormattedExpression(new Logical(AND, ImmutableList.of(visible, visible)), "((bigint '1' < x) AND (bigint '1' < x))");
        assertFormattedExpression(new Logical(AND, ImmutableList.of(visible, visible, policy, derived)), "((bigint '1' < x) AND (bigint '1' < x) AND [REDACTED])");
        assertFormattedExpression(new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(visible, policy)), derived)), "(((bigint '1' < x) OR [REDACTED]) AND [REDACTED])");
    }

    private void assertFormattedExpression(Expression expression, String expected)
    {
        assertThat(ExpressionFormatter.formatExpression(expression)).as("formatted expression")
                .isEqualTo(expected);
    }
}
