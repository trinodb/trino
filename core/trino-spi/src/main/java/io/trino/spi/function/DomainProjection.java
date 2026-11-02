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

import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;

import java.util.Optional;

import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static java.util.Objects.requireNonNull;

/// Registers a scalar function's preimage provider and validates the results it returns.
/// A preimage maps a set of function results to values of one input argument: for example,
/// `year(date)` maps dates to years, and its preimage maps the year 2025 to the dates in 2025.
/// Attach this object to [FunctionMetadata.Builder#domainProjection] when registering a function.
///
/// The function must be deterministic, return null on null arguments, and return a non-null
/// result whenever it succeeds on non-null arguments. Requests fix every argument except
/// the selected input to a non-null constant. The provider decides which such calls it supports.
/// This wrapper checks result types, null membership, and the requested exactness; the
/// provider remains responsible for the correctness of the computed input values.
public final class DomainProjection
{
    private final DomainPreimage provider;

    public DomainProjection(DomainPreimage provider)
    {
        this.provider = requireNonNull(provider, "provider is null");
    }

    public boolean isComparisonIdentity(Context context)
    {
        return provider.isComparisonIdentity(context);
    }

    public Optional<NullableValue> comparisonConstant(Context context, NullableValue constant)
    {
        if (!context.signature().getReturnType().equals(constant.getType())) {
            throw new IllegalArgumentException("projection context and constant must match the bound signature");
        }
        Optional<NullableValue> result = provider.comparisonConstant(context, constant);
        result.ifPresent(value -> {
            if (!value.getType().equals(context.signature().getArgumentTypes().get(context.inputArgument())) || value.isNull() != constant.isNull()) {
                throw new IllegalArgumentException("comparison constant must match the projected type and nullness");
            }
        });
        return result;
    }

    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        if (!context.signature().getReturnType().equals(resultDomain.getType())) {
            throw new IllegalArgumentException("projection context and result domain must match the bound signature");
        }
        Optional<PreimageResult> result = provider.preimage(context, resultDomain);
        result.ifPresent(value -> {
            if (!value.inputDomain().getType().equals(context.signature().getArgumentTypes().get(context.inputArgument()))) {
                throw new IllegalArgumentException("preimage type must match the input argument");
            }
            if (value.exactness() == EXACT && value.inputDomain().isNullAllowed() != resultDomain.isNullAllowed()) {
                throw new IllegalArgumentException("exact preimage violates the null-reflecting contract");
            }
            if (resultDomain.isNullAllowed() && !value.inputDomain().isNullAllowed()) {
                throw new IllegalArgumentException("preimage excludes a matching null input");
            }
        });
        return result.filter(value -> context.requiredExactness() != EXACT || value.exactness() == EXACT);
    }
}
