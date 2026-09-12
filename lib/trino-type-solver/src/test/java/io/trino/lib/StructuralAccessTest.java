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
import static org.weakref.solver.Expression.field;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.row;
import static org.weakref.solver.Expression.symbol;

public class StructuralAccessTest
{
    private static final TypeLibrary LIBRARY = TrinoPreset.library();

    @Test
    void testArraySubscriptReturnsElementType()
    {
        assertThat(LIBRARY.resolveFunction("subscript", List.of(apply("array", symbol("integer")), symbol("bigint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("integer")));
    }

    @Test
    void testArraySubscriptNarrowsIntegerIndexToBigint()
    {
        // integer arg widens to bigint for the subscript signature.
        assertThat(LIBRARY.resolveFunction("subscript", List.of(apply("array", symbol("varchar")), symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("varchar")));
    }

    @Test
    void testMapSubscriptReturnsValueType()
    {
        assertThat(LIBRARY.resolveFunction(
                "subscript",
                List.of(apply("map", symbol("bigint"), symbol("double")), symbol("bigint"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, outcome ->
                        assertThat(outcome.resolution().returnType()).isEqualTo(symbol("double")));
    }

    @Test
    void testJsonToRowCastWithCastableFields()
    {
        // row(x:integer, y:varchar): both field types are castable from json.
        assertThat(LIBRARY.resolveCast(
                symbol("json"),
                row(field("x", symbol("integer")), field("y", apply("varchar", literal(20))))))
                .isPresent();
    }

    @Test
    void testJsonToRowCastRejectsNonCastableField()
    {
        // row(x:date): date has no inward cast from json → compositional guard fails.
        assertThat(LIBRARY.resolveCast(
                symbol("json"),
                row(field("x", symbol("date")))))
                .isEmpty();
    }
}
