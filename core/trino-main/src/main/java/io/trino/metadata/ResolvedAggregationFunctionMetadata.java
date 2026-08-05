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

import io.trino.spi.type.TypeDescriptor;

import java.util.List;

/// Aggregation-specific metadata of a resolved function: whether it is order sensitive, and its ground
/// intermediate (serialized state) types. The declared, possibly variable-bearing form is
/// [io.trino.spi.function.AggregationFunctionMetadata].
public record ResolvedAggregationFunctionMetadata(boolean orderSensitive, List<TypeDescriptor> intermediateTypes)
{
    public ResolvedAggregationFunctionMetadata
    {
        intermediateTypes = List.copyOf(intermediateTypes);
    }

    /// A decomposable aggregation can run as a partial step followed by a final step, exchanging its
    /// intermediate state between the two.
    public boolean isDecomposable()
    {
        return !intermediateTypes.isEmpty();
    }
}
