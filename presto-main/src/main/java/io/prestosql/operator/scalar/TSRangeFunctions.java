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
package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TSRange;
import io.prestosql.spi.type.TSRangeType;
import io.prestosql.type.DateTimes;

import static io.prestosql.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.prestosql.spi.type.TSRangeType.combine;
import static io.prestosql.spi.type.TSRangeType.createTSRangeSlice;
import static io.prestosql.spi.type.TSRangeType.sliceToTSRange;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MICROS;

public class TSRangeFunctions
{
    private TSRangeFunctions()
    {
    }

    @Description("Create half-open tsrange [lower,upper)")
    @ScalarFunction(value = "tsrange")
    @LiteralParameters("p")
    @SqlType("tsrange(p)")
    public static Slice tsrangeHalfOpenFromTimestamp(@SqlType("timestamp(p)") long lower, @SqlType("timestamp(p)") long upper)
    { // postgres default is [lower,upper)
        return createTSRangeSlice(lower, upper, true, false);
    }

    @Description("Create half-open tsrange [lower,upper) from epochMillis")
    @ScalarFunction(value = "tsrange")
    @SqlType("tsrange(3)") // assume milliseconds precision if created with bigint
    public static Slice tsrangeHalfOpenFromEpoch(@SqlType(StandardTypes.BIGINT) long lower, @SqlType(StandardTypes.BIGINT) long upper)
    {
        return createTSRangeSlice(lower, upper, true, false);
    }

    @Description("Utility constructor")
    @ScalarFunction(value = "tsrange")
    @LiteralParameters("p")
    @SqlType("tsrange(p)")
    public static Slice tsrangeFromTimestamp(@SqlType("timestamp(p)") long lower, @SqlType("timestamp(p)") long upper,
            @SqlType(StandardTypes.BOOLEAN) boolean lowerClosed, @SqlType(StandardTypes.BOOLEAN) boolean upperClosed)
    {
        return createTSRangeSlice(lower, upper, lowerClosed, upperClosed);
    }

    @Description("Utility constructor from epochMillis")
    @ScalarFunction(value = "tsrange")
    @SqlType("tsrange(3)")
    public static Slice tsrangeFromEpoch(@SqlType(StandardTypes.BIGINT) long lower,
            @SqlType(StandardTypes.BIGINT) long upper, @SqlType(StandardTypes.BOOLEAN) boolean lowerClosed, @SqlType(StandardTypes.BOOLEAN) boolean upperClosed)
    {
        return createTSRangeSlice(lower, upper, lowerClosed, upperClosed);
    }

    @Description("Creates the smallest interval containing both intervals")
    @ScalarFunction(value = "combine")
    @LiteralParameters("p")
    @SqlType("tsrange(p)")
    public static Slice combineTSRanges(@SqlType("tsrange(p)") Slice left, @SqlType("tsrange(p)") Slice right)
    {
        return combine(left, right);
    }

    @Description("Returns true if lower bound is closed")
    @ScalarFunction(value = "lower_inc")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean leftClosed(@SqlType("tsrange(p)") Slice slice)
    {
        final TSRange tsRange = sliceToTSRange(slice);
        if (tsRange.isEmpty()) {
            throw new PrestoException(CONSTRAINT_VIOLATION, "Empty range has no bounds");
        }
        return tsRange.isLowerClosed();
    }

    @Description("Returns true if upper bound is closed")
    @ScalarFunction(value = "upper_inc")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean rightClosed(@SqlType("tsrange(p)") Slice slice)
    {
        final TSRange tsRange = sliceToTSRange(slice);
        if (tsRange.isEmpty()) {
            throw new PrestoException(CONSTRAINT_VIOLATION, "Empty range has no bounds");
        }
        return tsRange.isUpperClosed();
    }

    @Description("Returns lower timestamp from tsrange")
    @ScalarFunction(value = "lower")
    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long lower(@LiteralParameter("p") long precision, @SqlType("tsrange(p)") Slice slice)
    {
        final TSRange tsRange = sliceToTSRange(slice);
        if (tsRange.isEmpty()) {
            throw new PrestoException(CONSTRAINT_VIOLATION, "Cannot obtain element of empty tsrange");
        }
        return DateTimes.rescale(tsRange.getLower(), (int) precision, TIMESTAMP_MICROS.getPrecision());
    }

    @Description("Returns upper timestamp from tsrange")
    @ScalarFunction(value = "upper")
    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long upper(@LiteralParameter("p") long precision, @SqlType("tsrange(p)") Slice slice)
    {
        final TSRange tsRange = sliceToTSRange(slice);
        if (tsRange.isEmpty()) {
            throw new PrestoException(CONSTRAINT_VIOLATION, "Cannot obtain element of empty tsrange");
        }
        return DateTimes.rescale(tsRange.getUpper(), (int) precision, TIMESTAMP_MICROS.getPrecision());
    }

    @Description("Returns lower epoch from tsrange")
    @ScalarFunction(value = "lowerEpoch")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long lowerEpoch(@LiteralParameter("p") long precision, @SqlType("tsrange(p)") Slice slice)
    {
        final TSRange tsRange = sliceToTSRange(slice);
        if (tsRange.isEmpty()) {
            throw new PrestoException(CONSTRAINT_VIOLATION, "Cannot obtain element of empty tsrange");
        }
        return DateTimes.rescale(tsRange.getLower(), (int) precision, 3);
    }

    @Description("Returns upper epoch from tsrange")
    @ScalarFunction(value = "upperEpoch")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long upperEpoch(@LiteralParameter("p") long precision, @SqlType("tsrange(p)") Slice slice)
    {
        final TSRange tsRange = sliceToTSRange(slice);
        if (tsRange.isEmpty()) {
            throw new PrestoException(CONSTRAINT_VIOLATION, "Cannot obtain element of empty tsrange");
        }
        return DateTimes.rescale(tsRange.getUpper(), (int) precision, 3);
    }

    @Description("Returns true if left tsrange is strictly left from right tsrange")
    @ScalarFunction(value = "strictlyLeft")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean strictlyLeft(@SqlType("tsrange(p)") Slice left, @SqlType("tsrange(p)") Slice right)
    {
        return TSRangeType.strictlyLeft(left, right);
    }

    @Description("Returns true if left is strictly right from right")
    @ScalarFunction(value = "strictlyRight")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean strictlyRight(@SqlType("tsrange(p)") Slice left, @SqlType("tsrange(p)") Slice right)
    {
        return TSRangeType.strictlyLeft(right, left);
    }

    @Description("Returns true if left is adjacent to other tsrange")
    @ScalarFunction(value = "adjacent")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean adjacent(@SqlType("tsrange(p)") Slice left, @SqlType("tsrange(p)") Slice right)
    {
        return TSRangeType.adjacent(left, right);
    }

    @Description("Returns true if tsrange is empty")
    @ScalarFunction(value = "isEmpty")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isEmpty(@SqlType("tsrange(p)") Slice slice)
    {
        final TSRange tsRange = sliceToTSRange(slice);
        return tsRange.isEmpty();
    }

    @Description("Returns true if left contains right")
    @ScalarFunction(value = "containsTSRange")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean containsTSRange(@SqlType("tsrange(p)") Slice left, @SqlType("tsrange(p)") Slice right)
    {
        return TSRangeType.contains(left, right);
    }

    @Description("Returns true if tsrange contains timestamp")
    @ScalarFunction(value = "containsTimestamp")
    @LiteralParameters({"p1", "p2"})
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean containsTimestamp(@LiteralParameter("p1") long tsrangePrecision, @LiteralParameter("p2") long tsPrecision,
            @SqlType("tsrange(p1)") Slice slice, @SqlType("timestamp(p2)") long ts)
    {
        final TSRange tsRange = sliceToTSRange(slice);
        if (tsRange.isEmpty()) {
            return false;
        }
        else { // somehow timestamp(3) returns microseconds
            final long element = DateTimes.rescale(ts, (int) tsPrecision, (int) tsrangePrecision - 3);
            final long lower = tsRange.getLower();
            final long upper = tsRange.getUpper();
            return (element > lower || (element == lower && tsRange.isLowerClosed())) && (element < upper || (element == upper && tsRange.isUpperClosed()));
        }
    }

    @Description("Returns true if tsrange contains epochMillis")
    @ScalarFunction(value = "containsElement")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean containsEpochMillis(@LiteralParameter("p") long scale, @SqlType("tsrange(p)") Slice slice, @SqlType(StandardTypes.BIGINT) long element)
    {
        final TSRange tsRange = sliceToTSRange(slice);
        if (tsRange.isEmpty()) {
            return false;
        }
        else {
            final long lower = DateTimes.rescale(tsRange.getLower(), (int) scale, 3);
            final long upper = DateTimes.rescale(tsRange.getUpper(), (int) scale, 3);
            return (element > lower || (element == lower && tsRange.isLowerClosed())) && (element < upper || (element == upper && tsRange.isUpperClosed()));
        }
    }

    @Description("Returns true if left and right have points in common")
    @ScalarFunction(value = "overlap")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean overlap(@SqlType("tsrange(p)") Slice leftSlice, @SqlType("tsrange(p)") Slice rightSlice)
    {
        final TSRange left = sliceToTSRange(leftSlice);
        final TSRange right = sliceToTSRange(rightSlice);

        if (left.isEmpty()) {
            return false;
        }

        if (right.isEmpty()) {
            return false;
        }

        if (left.getUpper() == right.getLower()) {
            return left.isUpperClosed() && right.isLowerClosed();
        }

        if (left.getLower() == right.getUpper()) {
            return left.isLowerClosed() && right.isUpperClosed();
        }

        if (left.getUpper() < right.getLower()) {
            return false;
        }
        return left.getLower() <= right.getUpper();
    }

    @Description("Returns true if right contains an upper bound for left")
    @ScalarFunction(value = "doesNotExtendToTheRightOf")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean doesNotExtendToTheRight(@SqlType("tsrange(p)") Slice left, @SqlType("tsrange(p)") Slice right)
    {
        if (sliceToTSRange(left).isEmpty() || sliceToTSRange(right).isEmpty()) {
            return false;
        }
        return TSRangeType.compareUpper(left, right) <= 0;
    }

    @Description("Returns true if right tsrange contains a lower bound for left")
    @ScalarFunction(value = "doesNotExtendToTheLeftOf")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean doesNotExtendToTheLeft(@SqlType("tsrange(p)") Slice left, @SqlType("tsrange(p)") Slice right)
    {
        if (sliceToTSRange(left).isEmpty() || sliceToTSRange(right).isEmpty()) {
            return false;
        }
        return TSRangeType.compareLower(left, right) >= 0;
    }
}
