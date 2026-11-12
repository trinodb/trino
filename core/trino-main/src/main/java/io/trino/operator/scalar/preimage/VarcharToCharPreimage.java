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

import io.airlift.slice.Slice;
import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.setCodePointAt;
import static io.trino.spi.function.PreimageResult.Exactness.CONSERVATIVE;
import static io.trino.spi.function.PreimageResult.Exactness.EXACT;

public final class VarcharToCharPreimage
        implements DomainPreimage
{
    @Override
    public Optional<PreimageResult> preimage(Context context, Domain resultDomain)
    {
        return CastPreimages.compute(() -> project(context, resultDomain));
    }

    private static Optional<PreimageResult> project(Context context, Domain resultDomain)
    {
        Type source = context.signature().getArgumentTypes().getFirst();
        Type target = resultDomain.getType();
        if (source instanceof VarcharType varcharType && target instanceof CharType charType) {
            Optional<PreimageResult> exact = Optional.empty();
            if (!varcharType.isUnbounded() && varcharType.getBoundedLength() <= charType.getLength()) {
                exact = CastPreimages.equality(resultDomain, source, value -> {
                    int length = countCodePoints((Slice) value);
                    if (length < varcharType.getBoundedLength()) {
                        return Optional.empty();
                    }
                    return Optional.of(length == varcharType.getBoundedLength() ? ValueSet.of(source, value) : ValueSet.none(source));
                });
            }
            if (exact.isPresent() || context.requiredExactness() == EXACT) {
                return exact;
            }
            return prefixes(resultDomain, varcharType);
        }
        return Optional.empty();
    }

    /// Bounds the strings that the cast maps to one char value. The cast truncates and trims
    /// trailing spaces, so every string it maps to a value starts with that value.
    private static Optional<PreimageResult> prefixes(Domain resultDomain, VarcharType source)
    {
        ValueSet values = resultDomain.getValues();
        if (!values.isSingleValue()) {
            return Optional.empty();
        }
        Slice value = (Slice) values.getSingleValue();
        if (!source.isUnbounded() && source.getBoundedLength() < countCodePoints(value)) {
            // No string of the source type is long enough to cast to the value
            return Optional.of(new PreimageResult(Domain.create(ValueSet.none(source), resultDomain.isNullAllowed()), EXACT));
        }
        return prefixRange(source, value)
                .map(range -> new PreimageResult(Domain.create(ValueSet.ofRanges(range), resultDomain.isNullAllowed()), CONSERVATIVE));
    }

    /// Strings starting with `prefix`, bounded above by the prefix up to its last ASCII
    /// character, incremented. Keeping the bound ASCII leaves its byte length unchanged.
    private static Optional<Range> prefixRange(VarcharType source, Slice prefix)
    {
        int lastIncrementable = -1;
        for (int position = 0; position < prefix.length(); position += lengthOfCodePoint(prefix, position)) {
            if (getCodePointAt(prefix, position) < 127) {
                lastIncrementable = position;
            }
        }
        if (lastIncrementable == -1) {
            return Optional.empty();
        }
        Slice upper = prefix.slice(0, lastIncrementable + lengthOfCodePoint(prefix, lastIncrementable)).copy();
        setCodePointAt(getCodePointAt(prefix, lastIncrementable) + 1, upper, lastIncrementable);
        return Optional.of(Range.range(source, prefix, true, upper, false));
    }
}
