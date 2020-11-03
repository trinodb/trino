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
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.TSRange;
import io.prestosql.spi.type.TSRangeType;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.MULTIPLY;
import static io.prestosql.spi.function.OperatorType.SUBTRACT;

public class TSRangeOperators
{
    private TSRangeOperators()
    {
    }

    @ScalarOperator(ADD)
    @LiteralParameters("p")
    @SqlType("tsrange(p)")
    public static Slice union(@SqlType("tsrange(p)") Slice slice, @SqlType("tsrange(p)") Slice other)
    {
        if (TSRangeType.sliceToTSRange(other).isEmpty()) {
            return slice;
        }
        else if (!TSRangeType.adjacent(slice, other) && TSRangeType.sliceToTSRange(intersection(slice, other)).isEmpty()) {
            throw new PrestoException(CONSTRAINT_VIOLATION, "Result of tsrange union would not be contiguous");
        }
        return TSRangeType.combine(slice, other);
    }

    @ScalarOperator(SUBTRACT)
    @LiteralParameters("p")
    @SqlType("tsrange(p)")
    public static Slice difference(@SqlType("tsrange(p)") Slice slice, @SqlType("tsrange(p)") Slice other)
    {
        if (TSRangeType.compareLower(slice, other) < 0 && TSRangeType.compareUpper(slice, other) > 0 && TSRangeType.contains(slice, other)) {
            throw new PrestoException(CONSTRAINT_VIOLATION, "Result of tsrange union would not be contiguous");
        }
        else if (TSRangeType.contains(other, slice)) {
            return TSRangeType.empty();
        }
        Slice intersection = intersection(slice, other);
        if (TSRangeType.sliceToTSRange(intersection).isEmpty()) {
            return slice;
        }
        // cut off intersection from tsrange; 2 cases intersection is on the left or the right
        if (TSRangeType.compareLower(slice, intersection) < 0) {
            return TSRangeType.createTSRangeSlice(TSRangeType.getLower(slice), -TSRangeType.getLower(intersection));
        }
        else {
            return TSRangeType.createTSRangeSlice(-TSRangeType.getUpper(intersection), TSRangeType.getUpper(slice));
        }
    }

    @ScalarOperator(MULTIPLY)
    @LiteralParameters("p")
    @SqlType("tsrange(p)")
    public static Slice intersection(@SqlType("tsrange(p)") Slice slice, @SqlType("tsrange(p)") Slice other)
    {
        final TSRange left = TSRangeType.sliceToTSRange(slice);
        if (left.isEmpty()) {
            return TSRangeType.empty();
        }

        final TSRange right = TSRangeType.sliceToTSRange(other);
        if (right.isEmpty()) {
            return TSRangeType.empty();
        }

        if (left.getLower() > right.getUpper() || left.getUpper() < right.getLower()) {
            return TSRangeType.empty();
        }

        // special case: adjacent in numbers but not in boundaries
        if ((left.getLower() == right.getUpper() && !(left.isLowerClosed() && right.isUpperClosed())) || (left.getUpper() == right.getLower() && !(left.isUpperClosed() && right.isLowerClosed()))) {
            return TSRangeType.empty();
        }

        final boolean lb;
        final long lv;
        if (left.getLower() < right.getLower()) {
            lv = right.getLower();
            lb = right.isLowerClosed();
        }
        else if (left.getLower() == right.getLower()) {
            lv = left.getLower();
            lb = left.isLowerClosed() && right.isLowerClosed();
        }
        else {
            lv = left.getLower();
            lb = left.isLowerClosed();
        }
        final boolean rb;
        final long rv;
        if (left.getUpper() < right.getUpper()) {
            rv = left.getUpper();
            rb = left.isUpperClosed();
        }
        else if (left.getUpper() == right.getUpper()) {
            rv = left.getUpper();
            rb = left.isUpperClosed() && right.isUpperClosed();
        }
        else {
            rv = right.getUpper();
            rb = right.isUpperClosed();
        }

        return TSRangeType.createTSRangeSlice(lv, rv, lb, rb);
    }

    @ScalarOperator(CAST)
    @LiteralParameters({"p", "x"})
    @SqlType("tsrange(p)")
    public static Slice castFromVarcharToTSRange(@SqlType("varchar(x)") Slice slice)
    {
        TSRange tsRange;
        try {
            tsRange = TSRange.fromString(slice.toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Cannot cast value to tsrange: " + slice.toStringUtf8());
        }
        return TSRangeType.tsrangeLongToSlice(tsRange);
    }

    @ScalarOperator(CAST)
    @LiteralParameters({"p", "x"})
    @SqlType("varchar(x)")
    public static Slice castTSRangeToVarchar(@SqlType("tsrange(p)") Slice slice)
    {
        return utf8Slice(TSRangeType.sliceToTSRange(slice).toString());
    }
}
