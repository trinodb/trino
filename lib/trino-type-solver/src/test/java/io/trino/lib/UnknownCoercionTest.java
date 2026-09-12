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
import static org.weakref.solver.Expression.field;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.row;
import static org.weakref.solver.Expression.symbol;

public class UnknownCoercionTest
{
    private final TypeSystem typeSystem = TrinoPreset.typeSystem();

    @Test
    void testUnknownCoercesToPrimitive()
    {
        assertThat(typeSystem.coercionPlan(symbol("unknown"), symbol("integer"))).isPresent();
        assertThat(typeSystem.coercionPlan(symbol("unknown"), symbol("bigint"))).isPresent();
        assertThat(typeSystem.coercionPlan(symbol("unknown"), symbol("tinyint"))).isPresent();
    }

    @Test
    void testUnknownCoercesToDecimal()
    {
        assertThat(typeSystem.coercionPlan(symbol("unknown"), apply("decimal", literal(10), literal(2)))).isPresent();
    }

    @Test
    void testUnknownCoercesToVarchar()
    {
        assertThat(typeSystem.coercionPlan(symbol("unknown"), apply("varchar", literal(20)))).isPresent();
    }

    @Test
    void testUnknownCoercesToArray()
    {
        assertThat(typeSystem.coercionPlan(symbol("unknown"), apply("array", symbol("bigint")))).isPresent();
    }

    @Test
    void testUnknownCoercesToMap()
    {
        assertThat(typeSystem.coercionPlan(symbol("unknown"), apply("map", symbol("integer"), symbol("varchar")))).isPresent();
    }

    @Test
    void testUnknownCoercesToRow()
    {
        assertThat(typeSystem.coercionPlan(symbol("unknown"), row(field("a", symbol("integer")), field("b", symbol("bigint"))))).isPresent();
    }
}
