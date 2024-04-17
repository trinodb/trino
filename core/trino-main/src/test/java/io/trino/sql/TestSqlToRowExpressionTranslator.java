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
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.relational.Expressions.constant;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlToRowExpressionTranslator
{
    @Test
    @Timeout(10)
    public void testPossibleExponentialOptimizationTime()
    {
        Expression expression = new Constant(BIGINT, 1L);
        for (int i = 0; i < 100; i++) {
            expression = new Coalesce(expression, new Constant(BIGINT, 2L));
        }
        translateAndOptimize(expression);
    }

    @Test
    public void testOptimizeDecimalLiteral()
    {
        // Short decimal
        assertThat(translateAndOptimize(new Constant(createDecimalType(7, 2), null)))
                .isEqualTo(constant(null, createDecimalType(7, 2)));
        assertThat(translateAndOptimize(new Constant(createDecimalType(2), Decimals.valueOf(42))))
                .isEqualTo(constant(42L, createDecimalType(2, 0)));
        assertThat(translateAndOptimize(simplifyExpression(new Cast(new Constant(INTEGER, 42L), createDecimalType(7, 2)))))
                .isEqualTo(constant(4200L, createDecimalType(7, 2)));

        // Long decimal
        assertThat(translateAndOptimize(new Constant(createDecimalType(35, 2), null)))
                .isEqualTo(constant(null, createDecimalType(35, 2)));
        assertThat(translateAndOptimize(new Constant(createDecimalType(30), Decimals.valueOf(new BigDecimal("123456789012345678901234567890")))))
                .isEqualTo(constant(Decimals.valueOf(new BigDecimal("123456789012345678901234567890")), createDecimalType(30, 0)));
        assertThat(translateAndOptimize(simplifyExpression(new Cast(new Constant(createDecimalType(30), Decimals.valueOf(new BigDecimal("123456789012345678901234567890"))), createDecimalType(35, 2)))))
                .isEqualTo(constant(Decimals.valueOf(new BigDecimal("123456789012345678901234567890.00")), createDecimalType(35, 2)));
    }

    private RowExpression translateAndOptimize(Expression expression)
    {
        return SqlToRowExpressionTranslator.translate(
                expression,
                ImmutableMap.of(),
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getTypeManager());
    }

    private Expression simplifyExpression(Expression expression)
    {
        // Testing simplified expressions is important, since simplification may create CASTs or function calls that cannot be simplified by the ExpressionOptimizer
        return new IrExpressionInterpreter(expression, PLANNER_CONTEXT, TEST_SESSION).optimize();
    }
}
