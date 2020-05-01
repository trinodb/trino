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
package io.prestosql.metadata;

import io.prestosql.spi.type.TypeSignature;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AggregationFunctionMetadata
{
    private final boolean orderSensitive;
    private final Optional<TypeSignature> intermediateType;

    public AggregationFunctionMetadata(boolean orderSensitive, Optional<TypeSignature> intermediateType)
    {
        this.orderSensitive = orderSensitive;
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
    }

    public boolean isOrderSensitive()
    {
        return orderSensitive;
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
