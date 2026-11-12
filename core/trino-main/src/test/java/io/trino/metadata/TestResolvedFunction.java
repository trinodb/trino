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
package io.trino.metadata;

import org.junit.jupiter.api.Test;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static org.assertj.core.api.Assertions.assertThat;

final class TestResolvedFunction
{
    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();

    @Test
    void testEqualInstancesHashEqually()
    {
        ResolvedFunction function = functionResolution.resolveFunction("sum", fromTypes(createDecimalType(10, 2)));
        ResolvedFunction copy = new ResolvedFunction(
                function.signature(),
                function.catalogHandle(),
                function.functionId(),
                function.functionKind(),
                function.deterministic(),
                function.neverFails(),
                function.functionNullability(),
                function.typeDependencies(),
                function.functionDependencies());

        assertThat(copy).isNotSameAs(function);
        assertThat(copy).isEqualTo(function);
        assertThat(copy.hashCode()).isEqualTo(function.hashCode());
    }

    @Test
    void testBindingsOfGenericFunctionShareHash()
    {
        ResolvedFunction narrow = functionResolution.resolveFunction("sum", fromTypes(createDecimalType(10, 2)));
        ResolvedFunction wide = functionResolution.resolveFunction("sum", fromTypes(createDecimalType(20, 3)));

        assertThat(wide.functionId()).isEqualTo(narrow.functionId());
        assertThat(wide).isNotEqualTo(narrow);
        assertThat(wide.hashCode()).isEqualTo(narrow.hashCode());
    }
}
