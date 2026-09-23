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
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.spi.function.DomainPreimage;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.spi.function.PreimageResult.Exactness.CONSERVATIVE;
import static io.trino.spi.function.PreimageResult.Exactness.EXACT;
import static io.trino.type.CharVarcharCoercion.LEGACY;

public final class CharToVarcharPreimage
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
        if (source instanceof CharType charType && target instanceof VarcharType varcharType) {
            if (!varcharType.isUnbounded() && varcharType.getBoundedLength() < charType.getLength()) {
                return Optional.empty();
            }
            if (((FullConnectorSession) context.session()).getCharVarcharCoercion() == LEGACY) {
                return CharacterPreimages.legacyChar(context, resultDomain, charType);
            }
            Optional<PreimageResult> equality = CastPreimages.equality(resultDomain, source, value -> {
                @SuppressWarnings("ArgumentSelectionDefectChecker") // Reverse the ordinary cast to calculate an input boundary.
                Optional<Function<Object, Object>> reverse = context.functions().coercion(target, source);
                if (reverse.isEmpty()) {
                    return Optional.empty();
                }
                Object input = reverse.get().apply(value);
                Object output = context.functions().invoke(List.of(input));
                return Optional.of(value.equals(output) ? ValueSet.of(source, input) : ValueSet.none(source));
            });
            if (equality.isPresent() || context.requiredExactness() == EXACT) {
                return equality;
            }
            return ordering(context, resultDomain, charType, varcharType);
        }
        return Optional.empty();
    }

    /// Bounds the char values whose varchar form satisfies a single ordering range. Char
    /// comparison pads with spaces while varchar comparison does not, so the cast is not
    /// order-preserving and the result is a superset.
    private static Optional<PreimageResult> ordering(Context context, Domain resultDomain, CharType charType, VarcharType varcharType)
    {
        ValueSet values = resultDomain.getValues();
        if (values.isNone() || values.isAll() || values.getRanges().getRangeCount() != 1) {
            return Optional.empty();
        }
        Range range = values.getRanges().getSpan();
        if (range.isLowUnbounded() == range.isHighUnbounded()) {
            return Optional.empty();
        }
        @SuppressWarnings("ArgumentSelectionDefectChecker") // Reverse the ordinary cast to calculate an input boundary.
        Optional<Function<Object, Object>> reverse = context.functions().coercion(varcharType, charType);
        if (reverse.isEmpty()) {
            return Optional.empty();
        }
        boolean upper = range.isLowUnbounded();
        Slice constant = (Slice) (upper ? range.getHighBoundedValue() : range.getLowBoundedValue());
        boolean inclusive = upper ? range.isHighInclusive() : range.isLowInclusive();
        // The bound is the char value the constant is rounded down to
        Slice bound = (Slice) reverse.get().apply(constant);
        Slice boundAsVarchar = (Slice) context.functions().invoke(List.of(bound));
        for (int i = 0; i < bound.length(); i++) {
            if (Byte.toUnsignedInt(bound.getByte(i)) < ' ') {
                // Char comparison pads with spaces, so a character below the space makes a value sort before its
                // own prefix as a char and after it as a varchar. Such a bound is not usable.
                return Optional.empty();
            }
        }
        // The bound satisfies the comparison exactly when its own varchar form does, and no other char value
        // shares its position in the char ordering.
        int compared = boundAsVarchar.compareTo(constant);
        boolean boundSatisfies = upper
                ? (inclusive ? compared <= 0 : compared < 0)
                : (inclusive ? compared >= 0 : compared > 0);
        Range input;
        if (upper) {
            // A char value below the bound casts to a varchar below the constant, so the comparison holds for no
            // value above the bound.
            input = boundSatisfies ? Range.lessThanOrEqual(charType, bound) : Range.lessThan(charType, bound);
        }
        else if (countCodePoints(bound) == charType.getLength()) {
            // No char value extends the bound
            input = boundSatisfies ? Range.greaterThanOrEqual(charType, bound) : Range.greaterThan(charType, bound);
        }
        else {
            // A value extending the bound casts to a varchar above the constant while sorting below the bound as a
            // char, when the extension starts below the space. Every such value is above the bound with its last
            // character decremented, which in turn is above every value below the bound.
            int lastByte = bound.length() == 0 ? 0 : Byte.toUnsignedInt(bound.getByte(bound.length() - 1));
            if (lastByte <= '!' || lastByte > 0x7F) {
                // The last character does not decrement within ASCII: the bound is empty, the character is a '!',
                // which would decrement to a trailing space, or it is not ASCII.
                return Optional.empty();
            }
            Slice lowered = Slices.wrappedBuffer(bound.getBytes());
            lowered.setByte(lowered.length() - 1, lastByte - 1);
            input = Range.greaterThan(charType, lowered);
        }
        return Optional.of(new PreimageResult(Domain.create(ValueSet.ofRanges(input), resultDomain.isNullAllowed()), CONSERVATIVE));
    }
}
