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

import io.trino.spi.Experimental;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

@Experimental(eta = "2022-10-31")
public class AggregationFunctionMetadata
{
    private final boolean orderSensitive;
    private final List<TypeSignature> intermediateTypes;

    private AggregationFunctionMetadata(boolean orderSensitive, List<TypeSignature> intermediateTypes)
    {
        this.orderSensitive = orderSensitive;
        this.intermediateTypes = List.copyOf(requireNonNull(intermediateTypes, "intermediateTypes is null"));
    }

    public boolean isOrderSensitive()
    {
        return orderSensitive;
    }

    public boolean isDecomposable()
    {
        return !intermediateTypes.isEmpty();
    }

    public List<TypeSignature> getIntermediateTypes()
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
        private final List<TypeSignature> intermediateTypes = new ArrayList<>();

        private AggregationFunctionMetadataBuilder() {}

        public AggregationFunctionMetadataBuilder orderSensitive()
        {
            this.orderSensitive = true;
            return this;
        }

        public AggregationFunctionMetadataBuilder intermediateType(Type type)
        {
            this.intermediateTypes.add(type.getTypeSignature());
            return this;
        }

        public AggregationFunctionMetadataBuilder intermediateType(TypeSignature type)
        {
            this.intermediateTypes.add(requireNonNull(type, "type is null"));
            return this;
        }

        public AggregationFunctionMetadata build()
        {
            return new AggregationFunctionMetadata(orderSensitive, intermediateTypes);
        }
    }
}
