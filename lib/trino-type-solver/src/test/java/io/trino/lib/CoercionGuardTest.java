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
import org.weakref.solver.TypeSystem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.symbol;

/**
 * {@code coercionPlan} must honour a rule's {@code NumericRelation} guards for ground
 * types: a structural rule match is not enough. These cases all have a matching rule whose
 * guard fails, so the implicit coercion must be rejected — matching what the full solver
 * engine decides for the same pair.
 */
class CoercionGuardTest
{
    private final TypeSystem typeSystem = TrinoPreset.typeSystem();

    @Test
    void testGuardedNarrowingIsNotImplicit()
    {
        // varchar(50) -> varchar(5): guard 50 <= 5 fails.
        assertThat(typeSystem.coercionPlan(apply("varchar", literal(50)), apply("varchar", literal(5)))).isEmpty();
        // char(10) -> char(3): guard 10 <= 3 fails.
        assertThat(typeSystem.coercionPlan(apply("char", literal(10)), apply("char", literal(3)))).isEmpty();
        // decimal(38,0) -> decimal(10,2): guard 38 <= 8 fails.
        assertThat(typeSystem.coercionPlan(apply("decimal", literal(38), literal(0)), apply("decimal", literal(10), literal(2)))).isEmpty();
        // timestamp(6) -> timestamp(3): guard 6 <= 3 fails.
        assertThat(typeSystem.coercionPlan(apply("timestamp", literal(6)), apply("timestamp", literal(3)))).isEmpty();
        // integer -> decimal(10,2): guard 10 - 2 >= 10 fails (only 8 integer digits).
        assertThat(typeSystem.coercionPlan(symbol("integer"), apply("decimal", literal(10), literal(2)))).isEmpty();
    }

    @Test
    void testGuardedWideningRemainsImplicit()
    {
        // varchar(5) -> varchar(50): guard 5 <= 50 holds.
        assertThat(typeSystem.coercionPlan(apply("varchar", literal(5)), apply("varchar", literal(50)))).isPresent();
        // decimal(10,2) -> decimal(38,4): guards 8 <= 34 and 2 <= 4 hold.
        assertThat(typeSystem.coercionPlan(apply("decimal", literal(10), literal(2)), apply("decimal", literal(38), literal(4)))).isPresent();
        // timestamp(3) -> timestamp(6): guard 3 <= 6 holds.
        assertThat(typeSystem.coercionPlan(apply("timestamp", literal(3)), apply("timestamp", literal(6)))).isPresent();
        // integer -> decimal(12,2): guard 12 - 2 >= 10 holds.
        assertThat(typeSystem.coercionPlan(symbol("integer"), apply("decimal", literal(12), literal(2)))).isPresent();
    }
}
