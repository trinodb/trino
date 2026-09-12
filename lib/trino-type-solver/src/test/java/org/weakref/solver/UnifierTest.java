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
package org.weakref.solver;

import org.junit.jupiter.api.Test;
import org.weakref.solver.Unifier.Failure;
import org.weakref.solver.Unifier.Success;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.symbol;
import static org.weakref.solver.Expression.variable;
import static org.weakref.solver.Unifier.unify;

public class UnifierTest
{
    @Test
    void testSymbol()
    {
        assertThat(unify(symbol("a"), symbol("a")))
                .isEqualTo(new Success(Map.of()));

        assertThat(unify(symbol("a"), symbol("b")))
                .isEqualTo(new Failure(symbol("a"), symbol("b")));
    }

    @Test
    void testVariable()
    {
        assertThat(unify(variable("@v"), symbol("a")))
                .isEqualTo(new Success(Map.of("@v", symbol("a"))));

        assertThat(unify(symbol("a"), variable("@v")))
                .isEqualTo(new Success(Map.of("@v", symbol("a"))));

        assertThat(unify(variable("@v"), variable("@t")))
                .isEqualTo(new Success(Map.of("@v", variable("@t"))));

        assertThat(unify(
                variable("@v"),
                apply("varchar", variable("@t"))))
                .isEqualTo(new Success(Map.of("@v", apply("varchar", variable("@t")))));
    }

    @Test
    void testApplication()
    {
        assertThat(unify(
                apply("varchar", variable("@t")),
                apply("decimal", literal(10), literal(20))))
                .isEqualTo(new Failure(apply("varchar", variable("@t")), apply("decimal", literal(10), literal(20))));

        assertThat(unify(
                apply("varchar", variable("@t")),
                apply("varchar", variable("@u"))))
                .isEqualTo(new Success(Map.of("@t", variable("@u"))));

        assertThat(unify(
                apply("varchar", variable("@t")),
                apply("varchar", literal(10))))
                .isEqualTo(new Success(Map.of("@t", literal(10))));

        assertThat(unify(
                apply("decimal", variable("@t"), variable("@t")),
                apply("decimal", literal(10), literal(20))))
                .isEqualTo(new Failure(literal(10), literal(20)));

        assertThat(unify(
                apply("map", variable("@t"), variable("@t")),
                apply("map",
                        apply("decimal", List.of(literal(10), variable("@s"))),
                        apply("decimal", List.of(variable("@p"), literal(20))))))
                .isEqualTo(new Success(Map.of(
                        "@p", literal(10),
                        "@s", literal(20),
                        "@t", apply("decimal", literal(10), literal(20)))));

        assertThat(unify(
                apply("map", variable("@t"), variable("@t")),
                apply("map",
                        apply("decimal", List.of(variable("@p"), variable("@s"))),
                        apply("decimal", List.of(literal(10), literal(20))))))
                .isEqualTo(new Success(Map.of(
                        "@p", literal(10),
                        "@s", literal(20),
                        "@t", apply("decimal", literal(10), literal(20)))));

        assertThat(unify(
                apply("map",
                        variable("@t"),
                        apply("array", List.of(variable("@t")))),
                apply("map",
                        symbol("bigint"),
                        symbol("bigint"))))
                .isEqualTo(new Failure(apply("array", symbol("bigint")), symbol("bigint")));
    }

    @Test
    void testRecursive()
    {
        assertThat(unify(
                apply("array", variable("@t")),
                variable("@t")))
                .isEqualTo(new Failure(apply("array", variable("@t")), variable("@t")));

        assertThat(unify(
                variable("@t"),
                apply("array", variable("@t"))))
                .isEqualTo(new Failure(variable("@t"), apply("array", variable("@t"))));
    }
}
