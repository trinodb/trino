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

import io.airlift.slice.InvalidUtf8Exception;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.trino.spi.function.DomainPreimage.Context;
import io.trino.spi.function.PreimageResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.setCodePointAt;
import static io.trino.spi.function.PreimageResult.Exactness.EXACT;

/// Domain boundaries for the legacy space-padding CHAR-to-VARCHAR policy.
final class CharacterPreimages
{
    private CharacterPreimages() {}

    public static Optional<PreimageResult> legacyChar(Context context, Domain result, CharType source)
    {
        try {
            List<Range> ranges = new ArrayList<>();
            for (Range range : result.getValues().getRanges().getOrderedRanges()) {
                ValueSet low = range.isLowUnbounded() ? ValueSet.all(source) : bound(context, source, (Slice) range.getLowBoundedValue(), true, range.isLowInclusive());
                ValueSet high = range.isHighUnbounded() ? ValueSet.all(source) : bound(context, source, (Slice) range.getHighBoundedValue(), false, range.isHighInclusive());
                ranges.addAll(low.intersect(high).getRanges().getOrderedRanges());
            }
            return Optional.of(new PreimageResult(Domain.create(ValueSet.copyOfRanges(source, ranges), result.isNullAllowed()), EXACT));
        }
        catch (InvalidUtf8Exception _) {
            return Optional.empty();
        }
    }

    private static ValueSet bound(Context context, CharType source, Slice value, boolean lower, boolean inclusive)
    {
        Slice input = floor(source.getLength(), value);
        Slice output = (Slice) context.functions().invoke(List.of(input));
        int roundTrip = output.compareTo(value);
        if (roundTrip > 0) {
            // The boundary lies below the smallest source value.
            return lower ? ValueSet.all(source) : ValueSet.none(source);
        }
        boolean include = roundTrip == 0 ? inclusive : !lower;
        return ValueSet.ofRanges(lower
                ? (include ? Range.greaterThanOrEqual(source, input) : Range.greaterThan(source, input))
                : (include ? Range.lessThanOrEqual(source, input) : Range.lessThan(source, input)));
    }

    private static Slice floor(int length, Slice slice)
    {
        IntList codePoints = toCodePoints(slice);

        // if Varchar(x) value length (including spaces) is greater than the CHAR length, we can just truncate it
        if (codePoints.size() >= length) {
            // char(length) slice representation doesn't contain trailing spaces
            codePoints.size(Math.min(length, codePoints.size()));
            trimTrailing(codePoints, ' ');
            return codePointsToSliceUtf8(codePoints);
        }

        // A shorter VARCHAR sorts before its padded CHAR representation. Decrement
        // the last nonzero code point and fill the remaining positions with the maximum.
        trimTrailing(codePoints, '\0');

        if (codePoints.isEmpty()) {
            // A shorter all-zero VARCHAR lies below every padded CHAR value.
            return Slices.allocate(length);
        }

        int lastCodePoint = codePoints.getInt(codePoints.size() - 1) - 1;
        // Skip the surrogate code points, which are not valid Unicode scalar values.
        if (lastCodePoint == Character.MAX_SURROGATE) {
            lastCodePoint = Character.MIN_SURROGATE - 1;
        }
        codePoints.set(codePoints.size() - 1, lastCodePoint);
        int toAdd = length - codePoints.size();
        for (int i = 0; i < toAdd; i++) {
            codePoints.add(Character.MAX_CODE_POINT);
        }

        verify(codePoints.getInt(codePoints.size() - 1) != ' '); // no trailing spaces to trim

        return codePointsToSliceUtf8(codePoints);
    }

    private static void trimTrailing(IntList codePoints, int codePointToTrim)
    {
        int endIndex = codePoints.size();
        while (endIndex > 0 && codePoints.getInt(endIndex - 1) == codePointToTrim) {
            endIndex--;
        }
        codePoints.size(endIndex);
    }

    private static IntList toCodePoints(Slice slice)
    {
        IntList codePoints = new IntArrayList(slice.length());
        for (int offset = 0; offset < slice.length(); ) {
            int codePoint = getCodePointAt(slice, offset);
            offset += lengthOfCodePoint(slice, offset);
            codePoints.add(codePoint);
        }
        return codePoints;
    }

    private static Slice codePointsToSliceUtf8(IntList codePoints)
    {
        int bufferLength = 0;
        for (int codePoint : codePoints) {
            bufferLength += SliceUtf8.lengthOfCodePoint(codePoint);
        }

        Slice result = Slices.wrappedBuffer(new byte[bufferLength]);
        int offset = 0;
        for (int codePoint : codePoints) {
            setCodePointAt(codePoint, result, offset);
            offset += lengthOfCodePoint(codePoint);
        }

        return result;
    }
}
