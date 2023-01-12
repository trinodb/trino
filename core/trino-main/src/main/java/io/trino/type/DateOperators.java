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
package io.trino.type;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.time.DateTimeException;

import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.util.DateTimeUtils.parseDate;
import static io.trino.util.DateTimeUtils.printDate;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class DateOperators
{
    private DateOperators() {}

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType(StandardTypes.DATE) long value)
    {
        String stringValue = printDate(toIntExact(value));
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", stringValue, x));
    }

    @ScalarFunction("date")
    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(StandardTypes.DATE)
    public static long castFromVarchar(@SqlType("varchar(x)") Slice value)
    {
        // Note: update DomainTranslator.Visitor.createVarcharCastToDateComparisonExtractionResult whenever CAST behavior changes.

        try {
            return parseDate(trim(value).toStringUtf8());
        }
        catch (IllegalArgumentException | ArithmeticException | DateTimeException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to date: " + value.toStringUtf8(), e);
        }
    }
}
