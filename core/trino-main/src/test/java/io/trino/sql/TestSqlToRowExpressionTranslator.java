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
package io.trino.sql;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NullLiteral;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.util.Map;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.relational.Expressions.constant;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlToRowExpressionTranslator
{
    private final LiteralEncoder literalEncoder = new LiteralEncoder(PLANNER_CONTEXT);

    @Test
    @Timeout(10)
    public void testPossibleExponentialOptimizationTime()
    {
        Expression expression = new LongLiteral("1");
        ImmutableMap.Builder<NodeRef<Expression>, Type> types = ImmutableMap.builder();
        types.put(NodeRef.of(expression), BIGINT);
        for (int i = 0; i < 100; i++) {
            expression = new CoalesceExpression(expression, new LongLiteral("2"));
            types.put(NodeRef.of(expression), BIGINT);
        }
        translateAndOptimize(expression, types.buildOrThrow());
    }

    @Test
    public void testOptimizeDecimalLiteral()
    {
        // Short decimal
        assertThat(translateAndOptimize(new Cast(new NullLiteral(), dataType("decimal(7,2)"))))
                .isEqualTo(constant(null, createDecimalType(7, 2)));
        assertThat(translateAndOptimize(new DecimalLiteral("42")))
                .isEqualTo(constant(42L, createDecimalType(2, 0)));
        assertThat(translateAndOptimize(new Cast(new LongLiteral("42"), dataType("decimal(7,2)"))))
                .isEqualTo(constant(4200L, createDecimalType(7, 2)));
        assertThat(translateAndOptimize(simplifyExpression(new Cast(new LongLiteral("42"), dataType("decimal(7,2)")))))
                .isEqualTo(constant(4200L, createDecimalType(7, 2)));

        // Long decimal
        assertThat(translateAndOptimize(new Cast(new NullLiteral(), dataType("decimal(35,2)"))))
                .isEqualTo(constant(null, createDecimalType(35, 2)));
        assertThat(translateAndOptimize(new DecimalLiteral("123456789012345678901234567890")))
                .isEqualTo(constant(Decimals.valueOf(new BigDecimal("123456789012345678901234567890")), createDecimalType(30, 0)));
        assertThat(translateAndOptimize(new Cast(new DecimalLiteral("123456789012345678901234567890"), dataType("decimal(35,2)"))))
                .isEqualTo(constant(Decimals.valueOf(new BigDecimal("123456789012345678901234567890.00")), createDecimalType(35, 2)));
        assertThat(translateAndOptimize(simplifyExpression(new Cast(new DecimalLiteral("123456789012345678901234567890"), dataType("decimal(35,2)")))))
                .isEqualTo(constant(Decimals.valueOf(new BigDecimal("123456789012345678901234567890.00")), createDecimalType(35, 2)));
    }

    private RowExpression translateAndOptimize(Expression expression)
    {
        return translateAndOptimize(expression, getExpressionTypes(expression));
    }

    private RowExpression translateAndOptimize(Expression expression, Map<NodeRef<Expression>, Type> types)
    {
        return SqlToRowExpressionTranslator.translate(
                expression,
                types,
                ImmutableMap.of(),
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                PLANNER_CONTEXT.getTypeManager(),
                TEST_SESSION,
                true);
    }

    private Expression simplifyExpression(Expression expression)
    {
        // Testing simplified expressions is important, since simplification may create CASTs or function calls that cannot be simplified by the ExpressionOptimizer

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(expression);
        IrExpressionInterpreter interpreter = new IrExpressionInterpreter(expression, PLANNER_CONTEXT, TEST_SESSION, expressionTypes);
        Object value = interpreter.optimize(NoOpSymbolResolver.INSTANCE);
        return literalEncoder.toExpression(value, expressionTypes.get(NodeRef.of(expression)));
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Expression expression)
    {
        return new IrTypeAnalyzer(PLANNER_CONTEXT).getTypes(TEST_SESSION, TypeProvider.empty(), expression);
    }
}
