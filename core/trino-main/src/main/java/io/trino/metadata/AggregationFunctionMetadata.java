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

import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class AggregationFunctionMetadata
{
    private final boolean orderSensitive;
    private final Optional<TypeSignature> intermediateType;

    public AggregationFunctionMetadata(boolean orderSensitive, TypeSignature... intermediateTypes)
    {
        this.orderSensitive = orderSensitive;

        if (intermediateTypes.length == 0) {
            intermediateType = Optional.empty();
        }
        else if (intermediateTypes.length == 1) {
            intermediateType = Optional.of(intermediateTypes[0]);
        }
        else {
            intermediateType = Optional.of(new TypeSignature(StandardTypes.ROW, Arrays.stream(intermediateTypes)
                    .map(TypeSignatureParameter::anonymousField)
                    .collect(toImmutableList())));
        }
    }

    public AggregationFunctionMetadata(boolean orderSensitive, Optional<TypeSignature> intermediateType)
    {
        this.orderSensitive = orderSensitive;
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
    }

    public boolean isOrderSensitive()
    {
        return orderSensitive;
    }

    public boolean isDecomposable()
    {
        return intermediateType.isPresent();
    }

    public Optional<TypeSignature> getIntermediateType()
    {
        return intermediateType;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("orderSensitive", orderSensitive)
                .add("intermediateType", intermediateType)
                .toString();
    }
}
