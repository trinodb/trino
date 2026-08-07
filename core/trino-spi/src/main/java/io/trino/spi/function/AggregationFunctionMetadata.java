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
package io.trino.spi.function;

import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.TypeTemplates;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class AggregationFunctionMetadata
{
    private final boolean orderSensitive;
    private final List<TypeTemplate> intermediateTypes;
    private final Optional<AggregationDecomposition> decomposition;

    private AggregationFunctionMetadata(boolean orderSensitive, List<TypeTemplate> intermediateTypes, Optional<AggregationDecomposition> decomposition)
    {
        this.orderSensitive = orderSensitive;
        this.intermediateTypes = List.copyOf(requireNonNull(intermediateTypes, "intermediateTypes is null"));
        this.decomposition = requireNonNull(decomposition, "decomposition is null");
        if (decomposition.isPresent() && intermediateTypes.isEmpty()) {
            throw new IllegalArgumentException("intermediateTypes must be set when decomposition is present");
        }
    }

    public boolean isOrderSensitive()
    {
        return orderSensitive;
    }

    public boolean isDecomposable()
    {
        return !intermediateTypes.isEmpty();
    }

    public Optional<AggregationDecomposition> getDecomposition()
    {
        return decomposition;
    }

    public List<TypeTemplate> getIntermediateTypes()
    {
        return intermediateTypes;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", AggregationFunctionMetadata.class.getSimpleName() + "[", "]")
                .add("orderSensitive=" + orderSensitive)
                .add("intermediateTypes=" + intermediateTypes)
                .toString();
    }

    public static AggregationFunctionMetadataBuilder builder()
    {
        return new AggregationFunctionMetadataBuilder();
    }

    public static class AggregationFunctionMetadataBuilder
    {
        private boolean orderSensitive;
        private final List<TypeTemplate> intermediateTypes = new ArrayList<>();
        private Optional<AggregationDecomposition> decomposition = Optional.empty();

        private AggregationFunctionMetadataBuilder() {}

        public AggregationFunctionMetadataBuilder orderSensitive()
        {
            this.orderSensitive = true;
            return this;
        }

        public AggregationFunctionMetadataBuilder intermediateType(Type type)
        {
            this.intermediateTypes.add(TypeTemplates.fromTypeDescriptor(type.getTypeDescriptor()));
            return this;
        }

        public AggregationFunctionMetadataBuilder intermediateType(TypeDescriptor type)
        {
            this.intermediateTypes.add(TypeTemplates.fromTypeDescriptor(type));
            return this;
        }

        public AggregationFunctionMetadataBuilder intermediateType(TypeTemplate type)
        {
            this.intermediateTypes.add(requireNonNull(type, "type is null"));
            return this;
        }

        public AggregationFunctionMetadataBuilder decomposition(AggregationDecomposition decomposition)
        {
            this.decomposition = Optional.of(requireNonNull(decomposition, "decomposition is null"));
            return this;
        }

        public AggregationFunctionMetadata build()
        {
            return new AggregationFunctionMetadata(orderSensitive, intermediateTypes, decomposition);
        }
    }
}
