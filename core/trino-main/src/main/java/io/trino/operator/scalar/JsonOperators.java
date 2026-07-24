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
import io.trino.json.Json;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonItemEncoding.TypeTag;
import io.trino.json.JsonItems;
import io.trino.operator.scalar.time.TimeOperators;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TrinoNumber;
import io.trino.type.DateOperators;
import io.trino.util.JsonCastException;

import java.math.BigDecimal;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.DATE;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.StandardTypes.NUMBER;
import static io.trino.spi.type.StandardTypes.REAL;
import static io.trino.spi.type.StandardTypes.SMALLINT;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.util.DateTimeUtils.printDate;
import static io.trino.util.JsonUtil.itemAsBigint;
import static io.trino.util.JsonUtil.itemAsBoolean;
import static io.trino.util.JsonUtil.itemAsDouble;
import static io.trino.util.JsonUtil.itemAsInteger;
import static io.trino.util.JsonUtil.itemAsNumber;
import static io.trino.util.JsonUtil.itemAsReal;
import static io.trino.util.JsonUtil.itemAsSmallint;
import static io.trino.util.JsonUtil.itemAsTinyint;
import static io.trino.util.JsonUtil.itemAsVarchar;
import static java.lang.String.format;

public final class JsonOperators
{
    private JsonOperators() {}

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType(JSON) Json json)
    {
        Slice result;
        try {
            result = itemAsVarchar(json);
        }
        catch (JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to varchar(%s)", JsonItems.toText(json).toStringUtf8(), x), e);
        }
        if (result == null || countCodePoints(result) <= x) {
            return result;
        }
        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to varchar(%s)", JsonItems.toText(json).toStringUtf8(), x));
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(BIGINT)
    public static Long castToBigint(@SqlType(JSON) Json json)
    {
        try {
            return itemAsBigint(json);
        }
        catch (JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), BIGINT), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(INTEGER)
    public static Long castToInteger(@SqlType(JSON) Json json)
    {
        try {
            return itemAsInteger(json);
        }
        catch (ArithmeticException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), INTEGER), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(SMALLINT)
    public static Long castToSmallint(@SqlType(JSON) Json json)
    {
        try {
            return itemAsSmallint(json);
        }
        catch (IllegalArgumentException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), SMALLINT), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(TINYINT)
    public static Long castToTinyint(@SqlType(JSON) Json json)
    {
        try {
            return itemAsTinyint(json);
        }
        catch (IllegalArgumentException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), TINYINT), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(DOUBLE)
    public static Double castToDouble(@SqlType(JSON) Json json)
    {
        try {
            return itemAsDouble(json);
        }
        catch (JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), DOUBLE), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(REAL)
    public static Long castToReal(@SqlType(JSON) Json json)
    {
        try {
            return itemAsReal(json);
        }
        catch (JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), REAL), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(NUMBER)
    public static TrinoNumber castToNumber(@SqlType(JSON) Json json)
    {
        try {
            return itemAsNumber(json);
        }
        catch (JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), NUMBER), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(BOOLEAN)
    public static Boolean castToBoolean(@SqlType(JSON) Json json)
    {
        // NUMBER scalars (BigDecimal-precision values produced when raw input doesn't fit
        // BIGINT or DECIMAL) bypass Jackson's tokenizer because parser.getLongValue would
        // overflow on values like 1e309. SQL semantics: a non-zero numeric value is true.
        if (json.isScalar() && json.scalarType() == TypeTag.NUMBER) {
            TrinoNumber number = (TrinoNumber) json.materializeScalar().getObjectValue();
            return switch (number.toBigDecimal()) {
                case TrinoNumber.BigDecimalValue(BigDecimal d) -> d.signum() != 0;
                case TrinoNumber.NotANumber _ -> throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), BOOLEAN));
                case TrinoNumber.Infinity _ -> true;
            };
        }
        try {
            return itemAsBoolean(json);
        }
        catch (JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), BOOLEAN), e);
        }
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @LiteralParameters("x")
    @SqlType(JSON)
    public static Json castFromVarchar(@SqlType("varchar(x)") Slice value)
    {
        return JsonItemBuilder.encodeVarchar(value);
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(JSON)
    public static Json castFromTinyInt(@SqlType(TINYINT) long value)
    {
        return JsonItemBuilder.encode(w -> w.tinyintValue(value));
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(JSON)
    public static Json castFromSmallInt(@SqlType(SMALLINT) long value)
    {
        return JsonItemBuilder.encode(w -> w.smallintValue(value));
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(JSON)
    public static Json castFromInteger(@SqlType(INTEGER) long value)
    {
        return JsonItemBuilder.encode(w -> w.integerValue(value));
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(JSON)
    public static Json castFromBigint(@SqlType(BIGINT) long value)
    {
        return JsonItemBuilder.encodeBigint(value);
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(JSON)
    public static Json castFromDouble(@SqlType(DOUBLE) double value)
    {
        return JsonItemBuilder.encodeDouble(value);
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(JSON)
    public static Json castFromReal(@SqlType(REAL) long value)
    {
        return JsonItemBuilder.encode(w -> w.realBits((int) value));
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(JSON)
    public static Json castFromNumber(@SqlType(NUMBER) TrinoNumber value)
    {
        return JsonItemBuilder.encodeNumber(value);
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(JSON)
    public static Json castFromBoolean(@SqlType(BOOLEAN) boolean value)
    {
        return JsonItemBuilder.encodeBoolean(value);
    }

    @ScalarOperator(value = CAST, neverFails = true)
    @SqlType(JSON)
    public static Json castFromDate(@SqlType(DATE) long value)
    {
        return JsonItemBuilder.encodeVarchar(utf8Slice(printDate((int) value)));
    }

    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(DATE)
    public static Long castToDate(@SqlType(JSON) Json json)
    {
        if (json.isNull()) {
            return null;
        }
        if (json.isScalar() && json.scalarType() == TypeTag.DATE) {
            // A DATE item already carries the SQL value; casting it back is the identity.
            return json.materializeScalar().getLongValue();
        }
        if (!json.isScalar() || json.scalarType() != TypeTag.VARCHAR) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast JSON value to date; expected a JSON string or a JSON date");
        }
        Slice text = (Slice) json.materializeScalar().getObjectValue();
        return DateOperators.castFromVarchar(text);
    }

    @ScalarOperator(CAST)
    @SqlNullable
    @LiteralParameters("p")
    @SqlType("time(p)")
    public static Long castToTime(@LiteralParameter("p") long precision, @SqlType(JSON) Json json)
    {
        if (json.isNull()) {
            return null;
        }
        if (json.isScalar() && json.scalarType() == TypeTag.TIME) {
            // A TIME item carries picoseconds of the day at its own declared precision; round
            // it to the target precision the same way TIME-to-TIME casting does.
            long picos = json.materializeScalar().getLongValue();
            return round(picos, (int) (TimeType.MAX_PRECISION - precision)) % PICOSECONDS_PER_DAY;
        }
        if (!json.isScalar() || json.scalarType() != TypeTag.VARCHAR) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot cast JSON value to time; expected a JSON string or a JSON time");
        }
        Slice text = (Slice) json.materializeScalar().getObjectValue();
        return TimeOperators.castFromVarchar(precision, text);
    }
}
