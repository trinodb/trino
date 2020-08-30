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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.util.DateTimeUtils.parseDate;
import static io.prestosql.util.DateTimeUtils.printDate;

public final class DateOperators
{
    private DateOperators() {}

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToSlice(@SqlType(StandardTypes.DATE) long value)
    {
        return utf8Slice(printDate((int) value));
    }

    @ScalarFunction("date")
    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long castFromSlice(@SqlType("varchar(x)") Slice value)
    {
        try {
            return parseDate(trim(value).toStringUtf8());
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to date: " + value.toStringUtf8(), e);
        }
    }
}
