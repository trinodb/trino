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
package io.trino.typesolver;

import io.trino.spi.type.CharType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.typesolver.Expression.apply;
import static io.trino.typesolver.Expression.function;
import static io.trino.typesolver.Expression.literal;
import static io.trino.typesolver.Expression.symbol;
import static io.trino.typesolver.Expression.variable;
import static io.trino.typesolver.TrinoPreset.CharVarcharCoercion.LEGACY;
import static org.assertj.core.api.Assertions.assertThat;

public class CharVarcharCoercionTest
{
    @Test
    void testSqlStandardCoercion()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem();

        assertThat(typeSystem.coercionPlan(apply("char", literal(5)), apply("varchar", literal(10)))).isPresent();
        assertThat(typeSystem.coercionPlan(apply("char", literal(5)), symbol("varchar"))).isPresent();
        assertThat(typeSystem.coercionPlan(apply("varchar", literal(5)), apply("char", literal(10)))).isEmpty();
    }

    @Test
    void testLegacyCoercion()
    {
        TypeSystem typeSystem = TrinoPreset.typeSystem(LEGACY);

        assertThat(typeSystem.coercionPlan(apply("varchar", literal(5)), apply("char", literal(10)))).isPresent();
        assertThat(typeSystem.coercionPlan(symbol("varchar"), apply("char", literal((int) CharType.MAX_LENGTH)))).isPresent();
        assertThat(typeSystem.coercionPlan(apply("char", literal(5)), apply("varchar", literal(10)))).isEmpty();
    }

    @Test
    void testSqlStandardMixedCommonTypeIsBounded()
    {
        TypeLibrary.Builder builder = TypeLibrary.builder();
        TrinoPreset.typeConstructors().forEach(builder::registerType);
        TrinoPreset.coercionRules().forEach(builder::registerCoercion);
        TrinoPreset.castRules().forEach(builder::registerCast);
        builder.registerCoercion(new UnboundedVarcharSentinelCoercion());

        CompiledSignature commonType = new CompiledSignature(
                List.of(variable("T")),
                List.of(),
                function(List.of(variable("T"), variable("T")), variable("T")));

        assertThat(commonType.matchFunctionCallOutcome(
                List.of(apply("char", literal(5)), apply("varchar", literal(3))),
                builder.build().typeSystem()))
                .isInstanceOfSatisfying(CompiledSignature.Satisfied.class, result -> assertThat(result.result().returnType())
                        .isEqualTo(apply("varchar", literal(5))));
    }
}
