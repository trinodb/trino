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
package io.trino.operator;

import io.trino.operator.aggregation.ParametricAggregationImplementation;
import io.trino.operator.scalar.ParametricScalar;

import static org.assertj.core.api.Assertions.assertThat;

abstract class TestAnnotationEngine
{
    void assertImplementationCount(ParametricScalar scalar, int exact, int specialized, int generic)
    {
        assertImplementationCount(scalar.getImplementations(), exact, specialized, generic);
    }

    void assertImplementationCount(ParametricImplementationsGroup<?> implementations, int exact, int specialized, int generic)
    {
        assertThat(implementations.getExactImplementations()).hasSize(exact);
        assertThat(implementations.getSpecializedImplementations()).hasSize(specialized);
        assertThat(implementations.getGenericImplementations()).hasSize(generic);
    }

    void assertDependencyCount(ParametricAggregationImplementation implementation, int input, int combine, int output)
    {
        assertThat(implementation.getInputDependencies()).hasSize(input);
        assertThat(implementation.getCombineDependencies()).hasSize(combine);
        assertThat(implementation.getOutputDependencies()).hasSize(output);
    }
}
