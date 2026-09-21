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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.type.CharVarcharCoercion;
import it.unimi.dsi.fastutil.ints.IntList;

import static com.google.common.base.Verify.verify;
import static io.trino.operator.scalar.CharacterStringCasts.codePointsToSliceUtf8;
import static io.trino.operator.scalar.CharacterStringCasts.toCodePoints;
import static io.trino.operator.scalar.CharacterStringCasts.trimTrailing;
import static java.lang.Math.toIntExact;

/**
 * Legacy saturated floor cast from {@code VARCHAR} to {@code CHAR}, matching
 * {@link CharVarcharCoercion#LEGACY} which re-pads values with spaces to their declared {@code CHAR}
 * length when casting back. The legacy space-padded coercion is monotone in both directions, so a
 * saturated floor cast exists; it is registered only when the
 * {@code deprecated.legacy-varchar-to-char-coercion} configuration property is set. The default
 * coercion has no varchar to char saturated floor cast, because {@code CAST(char AS varchar)}
 * returns the unpadded value while {@code char} values compare as if space-padded, so the cast is
 * not monotone and domain bounds cannot be translated through it.
 */
public final class LegacyVarcharToCharSaturatedFloorCast
{
    private LegacyVarcharToCharSaturatedFloorCast() {}

    @ScalarOperator(OperatorType.SATURATED_FLOOR_CAST)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToCharSaturatedFloorCast(@LiteralParameter("y") long y, @SqlType("varchar(x)") Slice slice)
    {
        IntList codePoints = toCodePoints(slice);

        // if Varchar(x) value length (including spaces) is greater than y, we can just truncate it
        if (codePoints.size() >= y) {
            // char(y) slice representation doesn't contain trailing spaces
            codePoints.size(Math.min(toIntExact(y), codePoints.size()));
            trimTrailing(codePoints, ' ');
            return codePointsToSliceUtf8(codePoints);
        }

        /*
         * Value length is smaller than same-represented char(y) value because input varchar has length lower than y.
         * We decrement last character in input (in fact, we decrement last non-zero character) and pad the value with
         * max code point up to y characters.
         */
        trimTrailing(codePoints, '\0');

        if (codePoints.isEmpty()) {
            // No non-zero characters in input and input is shorter than y. Input value is smaller than any char(4) casted back to varchar, so we return the smallest char(4) possible
            return Slices.allocate(toIntExact(y));
        }

        int lastCodePoint = codePoints.getInt(codePoints.size() - 1) - 1;
        /*
         * UTF-8 reserve codepoints from 0xD800 to 0xDFFF for encoding UTF-16
         * If the lastCodePoint after -1 operation is in this range, it will lead to an InvalidCodePointException
         * Since the codePoint is originally valid, so the only case will be 0XE00 - 1
         * So we let it go through this range and become 0xD7FF
         */
        if (lastCodePoint == Character.MAX_SURROGATE) {
            lastCodePoint = Character.MIN_SURROGATE - 1;
        }
        codePoints.set(codePoints.size() - 1, lastCodePoint);
        int toAdd = toIntExact(y) - codePoints.size();
        for (int i = 0; i < toAdd; i++) {
            codePoints.add(Character.MAX_CODE_POINT);
        }

        verify(codePoints.getInt(codePoints.size() - 1) != ' '); // no trailing spaces to trim

        return codePointsToSliceUtf8(codePoints);
    }
}
