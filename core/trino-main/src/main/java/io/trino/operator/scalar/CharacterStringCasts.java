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
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.setCodePointAt;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Math.toIntExact;

public final class CharacterStringCasts
{
    private CharacterStringCasts() {}

    @ScalarOperator(value = OperatorType.CAST, neverFails = true)
    @SqlType("varchar(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToVarcharCast(@LiteralParameter("x") long x, @LiteralParameter("y") long y, @SqlType("varchar(x)") Slice slice)
    {
        if (x > y) {
            return truncateToLength(slice, toIntExact(y));
        }
        return slice;
    }

    @ScalarOperator(value = OperatorType.CAST, neverFails = true)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice charToCharCast(@LiteralParameter("x") long x, @LiteralParameter("y") long y, @SqlType("char(x)") Slice slice)
    {
        if (x > y) {
            return truncateToLength(slice, toIntExact(y));
        }
        return slice;
    }

    @ScalarOperator(value = OperatorType.CAST, neverFails = true)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToCharCast(@LiteralParameter("y") long y, @SqlType("varchar(x)") Slice slice)
    {
        return truncateToLengthAndTrimSpaces(slice, toIntExact(y));
    }

    @ScalarOperator(OperatorType.SATURATED_FLOOR_CAST)
    @SqlType("varchar(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToVarcharSaturatedFloorCast(@LiteralParameter("y") long y, @SqlType("varchar(x)") Slice slice)
    {
        if (countCodePoints(slice) <= y) {
            return slice;
        }

        IntList codePoints = toCodePoints(slice);
        codePoints.size(toIntExact(y));
        return codePointsToSliceUtf8(codePoints);
    }

    static void trimTrailing(IntList codePoints, int codePointToTrim)
    {
        int endIndex = codePoints.size();
        while (endIndex > 0 && codePoints.getInt(endIndex - 1) == codePointToTrim) {
            endIndex--;
        }
        codePoints.size(endIndex);
    }

    static IntList toCodePoints(Slice slice)
    {
        IntList codePoints = new IntArrayList(slice.length());
        for (int offset = 0; offset < slice.length(); ) {
            int codePoint = getCodePointAt(slice, offset);
            offset += lengthOfCodePoint(slice, offset);
            codePoints.add(codePoint);
        }
        return codePoints;
    }

    public static Slice codePointsToSliceUtf8(IntList codePoints)
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
