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
import static org.weakref.solver.Expression.function;
import static org.weakref.solver.Expression.symbol;

/**
 * Tests for function signature shapes that exercise less-trodden paths in the solver:
 * nested container return types, type vars bound from row field positions inside a parametric
 * type, lambdas with concrete return types, and sharing a single type var across multiple
 * positions (including across two containers and a lambda).
 */
public class ExoticSignatureTest
{
    private static final TypeLibrary LIBRARY = TrinoPreset.library();

    @Test
    void testCombinationsReturnsNestedArray()
    {
        // combinations<T>(array(T), integer) -> array(array(T))
        // With T=varchar, the solver must construct array(array(varchar)) in the return position.
        assertThat(LIBRARY.resolveFunction(
                "combinations",
                List.of(apply("array", symbol("varchar")), symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType())
                                .isEqualTo(apply("array", apply("array", symbol("varchar")))));
    }

    @Test
    void testCombinationsBindsElementFromInputArray()
    {
        // Sanity: different element type flows through to the nested return.
        assertThat(LIBRARY.resolveFunction(
                "combinations",
                List.of(apply("array", symbol("bigint")), symbol("integer"))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType())
                                .isEqualTo(apply("array", apply("array", symbol("bigint")))));
    }

    @Test
    void testMultimapFromEntriesBindsKAndVFromRowFields()
    {
        // multimap_from_entries<K, V>(array(row(K, V))) -> map(K, array(V))
        // With input array(row(varchar, integer)), the solver must bind K=varchar and V=integer
        // from the row's field positions, then construct map(varchar, array(integer)) on return.
        assertThat(LIBRARY.resolveFunction(
                "multimap_from_entries",
                List.of(apply("array", apply("row", symbol("varchar"), symbol("integer"))))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType())
                                .isEqualTo(apply("map", symbol("varchar"), apply("array", symbol("integer")))));
    }

    @Test
    void testMultimapFromEntriesWithDistinctKVTypes()
    {
        // Distinct types at K and V positions catch any accidental cross-binding.
        assertThat(LIBRARY.resolveFunction(
                "multimap_from_entries",
                List.of(apply("array", apply("row", symbol("bigint"), symbol("boolean"))))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType())
                                .isEqualTo(apply("map", symbol("bigint"), apply("array", symbol("boolean")))));
    }

    @Test
    void testArraySortComparatorBindsElementType()
    {
        // array_sort<T>(array(T), function(T, T) -> integer) -> array(T).
        // The lambda return type is concrete integer; the args share T with the input array.
        assertThat(LIBRARY.resolveFunction(
                "array_sort",
                List.of(
                        apply("array", symbol("varchar")),
                        function(List.of(symbol("varchar"), symbol("varchar")), symbol("integer")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType())
                                .isEqualTo(apply("array", symbol("varchar"))));
    }

    @Test
    void testArraySortNaturalOrderStillWorks()
    {
        // Single-arg array_sort overload — orderable T binds normally.
        assertThat(LIBRARY.resolveFunction("array_sort", List.of(apply("array", symbol("integer")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType()).isEqualTo(apply("array", symbol("integer"))));
    }

    @Test
    void testMapZipWithSharedKeyTypeAcrossMapsAndLambda()
    {
        // map_zip_with<K, V1, V2, V3>(map(K, V1), map(K, V2), function(K, V1, V2) -> V3) -> map(K, V3).
        // K=varchar appears in positions 1, 2, and the first lambda arg. V1=integer, V2=bigint, V3=double.
        // The solver must unify K across all three occurrences and bind V3 from the lambda return.
        assertThat(LIBRARY.resolveFunction(
                "map_zip_with",
                List.of(
                        apply("map", symbol("varchar"), symbol("integer")),
                        apply("map", symbol("varchar"), symbol("bigint")),
                        function(
                                List.of(symbol("varchar"), symbol("integer"), symbol("bigint")),
                                symbol("double")))))
                .isInstanceOfSatisfying(FunctionResolver.Resolved.class, r ->
                        assertThat(r.resolution().returnType())
                                .isEqualTo(apply("map", symbol("varchar"), symbol("double"))));
    }

    @Test
    void testMapZipWithRejectsMismatchedKeyTypes()
    {
        // Two maps with incompatible key types: K would have to be both varchar and bigint — no match.
        assertThat(LIBRARY.resolveFunction(
                "map_zip_with",
                List.of(
                        apply("map", symbol("varchar"), symbol("integer")),
                        apply("map", symbol("bigint"), symbol("bigint")),
                        function(
                                List.of(symbol("varchar"), symbol("integer"), symbol("bigint")),
                                symbol("double")))))
                .isNotInstanceOf(FunctionResolver.Resolved.class);
    }
}
