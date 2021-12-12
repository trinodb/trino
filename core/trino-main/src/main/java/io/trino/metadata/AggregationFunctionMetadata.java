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

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.TypeSignature;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AggregationFunctionMetadata
{
    private final boolean orderSensitive;
    private final List<TypeSignature> intermediateTypes;

    public AggregationFunctionMetadata(boolean orderSensitive, TypeSignature... intermediateTypes)
    {
        this(orderSensitive, ImmutableList.copyOf(requireNonNull(intermediateTypes, "intermediateTypes is null")));
    }

    public AggregationFunctionMetadata(boolean orderSensitive, List<TypeSignature> intermediateTypes)
    {
        this.orderSensitive = orderSensitive;
        this.intermediateTypes = ImmutableList.copyOf(requireNonNull(intermediateTypes, "intermediateTypes is null"));
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
        return toStringHelper(this)
                .add("orderSensitive", orderSensitive)
                .add("intermediateTypes", intermediateTypes)
                .toString();
    }
}
