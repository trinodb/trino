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
import static org.weakref.solver.Expression.symbol;

public class CommonSupertypeTest
{
    private final TypeSystem typeSystem = TrinoPreset.typeSystem();

    @Test
    void testIdenticalTypesAreTheirOwnSupertype()
    {
        assertThat(typeSystem.getCommonSupertype(symbol("integer"), symbol("integer")))
                .hasValue(symbol("integer"));
    }

    @Test
    void testIntegerAndBigintShareBigint()
    {
        assertThat(typeSystem.getCommonSupertype(symbol("integer"), symbol("bigint")))
                .hasValue(symbol("bigint"));
    }

    @Test
    void testTinyintAndDoubleShareDouble()
    {
        assertThat(typeSystem.getCommonSupertype(symbol("tinyint"), symbol("double")))
                .hasValue(symbol("double"));
    }

    @Test
    void testIntegerAndRealShareReal()
    {
        assertThat(typeSystem.getCommonSupertype(symbol("integer"), symbol("real")))
                .hasValue(symbol("real"));
    }

    @Test
    void testIncomparableTypesHaveNoSupertype()
    {
        assertThat(typeSystem.getCommonSupertype(symbol("integer"), symbol("boolean")))
                .isEmpty();
    }

    @Test
    void testUnknownWithConcreteTypeReturnsConcreteType()
    {
        assertThat(typeSystem.getCommonSupertype(symbol("unknown"), symbol("integer")))
                .hasValue(symbol("integer"));
        assertThat(typeSystem.getCommonSupertype(symbol("bigint"), symbol("unknown")))
                .hasValue(symbol("bigint"));
    }
}
