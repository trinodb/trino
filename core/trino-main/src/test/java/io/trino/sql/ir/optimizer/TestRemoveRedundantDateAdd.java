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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantDateAdd;
import jakarta.annotation.Nullable;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRemoveRedundantDateAdd
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution(createTestTransactionManager(), PLANNER_CONTEXT);

    @Test
    void testRemoveNoopDateAdd()
    {
        for (Type type : ImmutableList.of(DATE, createTimestampType(0), createTimestampType(9), createTimestampWithTimeZoneType(3))) {
            Reference value = new Reference(type, "x");
            assertThat(optimize(dateAdd("day", 0L, value)))
                    .describedAs("date_add('day', 0, %s)".formatted(type))
                    .isEqualTo(Optional.of(value));
        }

        for (Type type : ImmutableList.of(createTimeType(3), createTimeWithTimeZoneType(12), createTimestampType(6))) {
            Reference value = new Reference(type, "x");
            assertThat(optimize(dateAdd("second", 0L, value)))
                    .describedAs("date_add('second', 0, %s)".formatted(type))
                    .isEqualTo(Optional.of(value));
        }
    }

    @Test
    void testUnitMixedCase()
    {
        Reference value = new Reference(createTimestampType(3), "x");
        assertThat(optimize(dateAdd("Quarter", 0L, value)))
                .isEqualTo(Optional.of(value));
        assertThat(optimize(dateAdd("qUArTeR", 0L, value)))
                .isEqualTo(Optional.of(value));
    }

    @Test
    void testNonZeroAmount()
    {
        assertThat(optimize(dateAdd("day", 1L, new Reference(createTimestampType(3), "x"))))
                .isEmpty();
    }

    // Keep date_add with invalid unit. Do not mask failures.
    @Test
    void testInvalidUnit()
    {
        assertThat(optimize(dateAdd("millisecond", 0L, new Reference(DATE, "x"))))
                .describedAs("date_add rejects sub-day units for date")
                .isEmpty();

        assertThat(optimize(dateAdd("day", 0L, new Reference(createTimeType(3), "x"))))
                .describedAs("date_add rejects units above hour for time")
                .isEmpty();

        assertThat(optimize(dateAdd("millennium", 0L, new Reference(createTimestampType(3), "x"))))
                .describedAs("date_add rejects unknown units")
                .isEmpty();
    }

    // handled by other optimizers
    @Test
    void testNullArguments()
    {
        Reference value = new Reference(createTimestampType(3), "x");
        assertThat(optimize(dateAdd(null, 0L, value)))
                .describedAs("null unit")
                .isEmpty();
        assertThat(optimize(dateAdd("day", null, value)))
                .describedAs("null amount")
                .isEmpty();

        Constant nullValue = new Constant(createTimestampType(3), null);
        assertThat(optimize(dateAdd("day", 0L, nullValue)))
                .describedAs("null value")
                .isEqualTo(Optional.of(nullValue));
    }

    @Test
    void testNonConstantUnit()
    {
        Reference value = new Reference(createTimestampType(3), "x");
        Call call = new Call(
                FUNCTIONS.resolveFunction("date_add", fromTypes(VARCHAR, BIGINT, value.type())),
                ImmutableList.of(new Reference(VARCHAR, "unit"), new Constant(BIGINT, 0L), value));
        assertThat(optimize(call))
                .isEmpty();
    }

    @Test
    void testNonConstantAmount()
    {
        Reference value = new Reference(createTimestampType(3), "x");
        Call call = new Call(
                FUNCTIONS.resolveFunction("date_add", fromTypes(VARCHAR, BIGINT, value.type())),
                ImmutableList.of(new Constant(VARCHAR, utf8Slice("day")), new Reference(BIGINT, "amount"), value));
        assertThat(optimize(call))
                .isEmpty();
    }

    private static Call dateAdd(@Nullable String unit, @Nullable Long amount, Expression value)
    {
        return new Call(
                FUNCTIONS.resolveFunction("date_add", fromTypes(VARCHAR, BIGINT, value.type())),
                ImmutableList.of(
                        new Constant(VARCHAR, unit == null ? null : utf8Slice(unit)),
                        new Constant(BIGINT, amount),
                        value));
    }

    private static Optional<Expression> optimize(Expression expression)
    {
        return new RemoveRedundantDateAdd().apply(expression, testSession(), emptySymbolAllocator(), ImmutableMap.of());
    }
}
