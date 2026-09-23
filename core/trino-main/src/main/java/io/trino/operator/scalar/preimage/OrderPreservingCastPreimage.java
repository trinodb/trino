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
package io.trino.operator.scalar.preimage;

import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;

/// Projects casts whose successful values preserve order and have a unique input at each boundary.
public final class OrderPreservingCastPreimage
        implements DomainPreimage
{
    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        Type source = context.signature().getArgumentTypes().getFirst();
        Type target = resultDomain.getType();
        if (!CastPreimages.eligible(context, source, target)) {
            return Optional.empty();
        }
        return project(context, resultDomain, source.getRange());
    }

    static Optional<PreimageResult> project(Context context, Domain resultDomain, Optional<Type.Range> sourceRange)
    {
        Type source = context.signature().getArgumentTypes().getFirst();
        Type target = resultDomain.getType();
        Optional<Function<Object, Object>> forward = context.functions().coercion(source, target);
        Optional<Function<Object, Object>> reverse = context.functions().coercion(target, source);
        if (forward.isEmpty() || reverse.isEmpty()) {
            return Optional.empty();
        }
        Comparator<Object> comparison = context.functions().resultComparator();
        return CastPreimages.compute(() -> CastPreimages.orderedRanges(
                resultDomain,
                source,
                (value, lower, inclusive) -> CastPreimages.roundTripBound(source, sourceRange, forward.get(), reverse.get(), comparison, value, lower, inclusive)));
    }
}
