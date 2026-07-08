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
package io.trino.sql.ir.optimizer;

import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.SimplifyStackedCast;
import io.trino.sql.planner.SymbolAllocator;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ir.Cast.Kind.CONVERT;
import static io.trino.sql.ir.Cast.Kind.REINTERPRET;
import static io.trino.sql.ir.optimizer.IrExpressionOptimizer.newOptimizer;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyStackedCast
{
    @Test
    void testCollapse()
    {
        assertThat(optimize(
                new Cast(new Cast(new Reference(createVarcharType(5), "x"), createVarcharType(20), REINTERPRET), createVarcharType(10))))
                .isEqualTo(Optional.of(new Cast(new Reference(createVarcharType(5), "x"), createVarcharType(10))));

        assertThat(optimize(
                new Cast(new Cast(new Reference(createVarcharType(5), "x"), createVarcharType(20), REINTERPRET), BIGINT)))
                .describedAs("cast to a different type family")
                .isEqualTo(Optional.of(new Cast(new Reference(createVarcharType(5), "x"), BIGINT)));

        assertThat(optimize(
                new Cast(new Cast(new Reference(createDecimalType(10, 2), "x"), createDecimalType(12, 2), REINTERPRET), createDecimalType(14, 2))))
                .describedAs("widening decimal chain")
                .isEqualTo(Optional.of(new Cast(new Reference(createDecimalType(10, 2), "x"), createDecimalType(14, 2))));
    }

    @Test
    void testDoesNotFire()
    {
        assertThat(optimize(
                new Cast(new Cast(new Reference(createVarcharType(20), "x"), createVarcharType(5)), createVarcharType(10))))
                .describedAs("narrowing inner cast may truncate or fail")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Cast(new Cast(new Reference(BIGINT, "x"), DOUBLE), createVarcharType(20))))
                .describedAs("inner cast changes the value representation")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new Cast(new Cast(new Reference(createVarcharType(5), "x"), createVarcharType(20), CONVERT), createVarcharType(10))))
                .describedAs("type-only-shaped inner cast tagged CONVERT is not a reinterpretation")
                .isEqualTo(Optional.empty());

        assertThat(optimize(new Cast(new Reference(createVarcharType(5), "x"), createVarcharType(10))))
                .describedAs("single cast")
                .isEqualTo(Optional.empty());
    }

    @Test
    void testCollapsesFullyWithRedundantCast()
    {
        assertThat(newOptimizer(PLANNER_CONTEXT).process(
                new Cast(new Cast(new Reference(createVarcharType(5), "x"), createVarcharType(20), REINTERPRET), createVarcharType(5)),
                testSession(),
                new SymbolAllocator(),
                ImmutableMap.of()))
                .isEqualTo(Optional.of(new Reference(createVarcharType(5), "x")));
    }

    private Optional<Expression> optimize(Expression expression)
    {
        return new SimplifyStackedCast(PLANNER_CONTEXT).apply(expression, testSession(), new SymbolAllocator(), ImmutableMap.of());
    }
}
