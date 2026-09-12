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
package io.trino.lib;

import org.junit.jupiter.api.Test;
import org.weakref.solver.FunctionResolver;
import org.weakref.solver.TypeLibrary;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.symbol;

public class TemporalArithmeticTest
{
    private static final TypeLibrary LIBRARY = TrinoPreset.library();

    @Test
    void testDatePlusIntervalYearToMonthReturnsDate()
    {
        assertThat(LIBRARY.resolveFunction("+", List.of(symbol("date"), symbol("interval_year_to_month"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("date")));
    }

    @Test
    void testIntervalPlusDateReturnsDate()
    {
        // Symmetric registration.
        assertThat(LIBRARY.resolveFunction("+", List.of(symbol("interval_year_to_month"), symbol("date"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("date")));
    }

    @Test
    void testDateMinusDateReturnsInterval()
    {
        assertThat(LIBRARY.resolveFunction("-", List.of(symbol("date"), symbol("date"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("interval_day_to_second")));
    }

    @Test
    void testTimestampPlusIntervalPreservesPrecision()
    {
        assertThat(LIBRARY.resolveFunction(
                "+",
                List.of(apply("timestamp", literal(6)), symbol("interval_day_to_second"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(apply("timestamp", literal(6))));
    }

    @Test
    void testTimestampMinusTimestampReturnsInterval()
    {
        assertThat(LIBRARY.resolveFunction(
                "-",
                List.of(apply("timestamp", literal(3)), apply("timestamp", literal(6)))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("interval_day_to_second")));
    }

    @Test
    void testIntervalTimesBigintReturnsInterval()
    {
        assertThat(LIBRARY.resolveFunction("*", List.of(symbol("interval_day_to_second"), symbol("bigint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("interval_day_to_second")));
    }

    @Test
    void testUnaryNegateOnInterval()
    {
        assertThat(LIBRARY.resolveFunction("-", List.of(symbol("interval_year_to_month"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("interval_year_to_month")));
    }

    @Test
    void testIntervalPlusIntervalSameKind()
    {
        assertThat(LIBRARY.resolveFunction("+", List.of(symbol("interval_day_to_second"), symbol("interval_day_to_second"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("interval_day_to_second")));
    }
}
