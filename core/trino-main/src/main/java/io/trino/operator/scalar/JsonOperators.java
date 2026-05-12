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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.airlift.slice.Slice;
import io.trino.json.Json;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonItemEncoding.TypeTag;
import io.trino.json.JsonItems;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.TrinoNumber;
import io.trino.util.JsonCastException;

import java.io.IOException;
import java.math.BigDecimal;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
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
import static io.trino.util.DateTimeUtils.printDate;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.createJsonFactory;
import static io.trino.util.JsonUtil.currentTokenAsBigint;
import static io.trino.util.JsonUtil.currentTokenAsBoolean;
import static io.trino.util.JsonUtil.currentTokenAsDouble;
import static io.trino.util.JsonUtil.currentTokenAsInteger;
import static io.trino.util.JsonUtil.currentTokenAsNumber;
import static io.trino.util.JsonUtil.currentTokenAsReal;
import static io.trino.util.JsonUtil.currentTokenAsSmallint;
import static io.trino.util.JsonUtil.currentTokenAsTinyint;
import static io.trino.util.JsonUtil.currentTokenAsVarchar;
import static java.lang.String.format;

public final class JsonOperators
{
    private static final JsonMapper JSON_MAPPER = new JsonMapper(createJsonFactory());

    private JsonOperators() {}

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@LiteralParameter("x") long x, @SqlType(JSON) Json json)
    {
        try (JsonParser parser = jsonAsParser(json)) {
            parser.nextToken();
            Slice result = currentTokenAsVarchar(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to VARCHAR"); // check no trailing token
            if (result == null || countCodePoints(result) <= x) {
                return result;
            }
        }
        catch (IOException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to varchar(%s)", JsonItems.toText(json).toStringUtf8(), x), e);
        }
        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to varchar(%s)", JsonItems.toText(json).toStringUtf8(), x));
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(BIGINT)
    public static Long castToBigint(@SqlType(JSON) Json json)
    {
        try (JsonParser parser = jsonAsParser(json)) {
            parser.nextToken();
            Long result = currentTokenAsBigint(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to BIGINT"); // check no trailing token
            return result;
        }
        catch (InputCoercionException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Out of range for %s: %s", BIGINT, JsonItems.toText(json).toStringUtf8()), e);
        }
        catch (IOException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), BIGINT), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(INTEGER)
    public static Long castToInteger(@SqlType(JSON) Json json)
    {
        try (JsonParser parser = jsonAsParser(json)) {
            parser.nextToken();
            Long result = currentTokenAsInteger(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to INTEGER"); // check no trailing token
            return result;
        }
        catch (InputCoercionException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Out of range for %s: %s", INTEGER, JsonItems.toText(json).toStringUtf8()), e);
        }
        catch (ArithmeticException | IOException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), INTEGER), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(SMALLINT)
    public static Long castToSmallint(@SqlType(JSON) Json json)
    {
        try (JsonParser parser = jsonAsParser(json)) {
            parser.nextToken();
            Long result = currentTokenAsSmallint(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to SMALLINT"); // check no trailing token
            return result;
        }
        catch (InputCoercionException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Out of range for %s: %s", SMALLINT, JsonItems.toText(json).toStringUtf8()), e);
        }
        catch (IllegalArgumentException | IOException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), SMALLINT), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(TINYINT)
    public static Long castToTinyint(@SqlType(JSON) Json json)
    {
        try (JsonParser parser = jsonAsParser(json)) {
            parser.nextToken();
            Long result = currentTokenAsTinyint(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to TINYINT"); // check no trailing token
            return result;
        }
        catch (InputCoercionException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Out of range for %s: %s", TINYINT, JsonItems.toText(json).toStringUtf8()), e);
        }
        catch (IllegalArgumentException | IOException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), TINYINT), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(DOUBLE)
    public static Double castToDouble(@SqlType(JSON) Json json)
    {
        try (JsonParser parser = jsonAsParser(json)) {
            parser.nextToken();
            Double result = currentTokenAsDouble(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to DOUBLE"); // check no trailing token
            return result;
        }
        catch (IOException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), DOUBLE), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(REAL)
    public static Long castToReal(@SqlType(JSON) Json json)
    {
        try (JsonParser parser = jsonAsParser(json)) {
            parser.nextToken();
            Long result = currentTokenAsReal(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to REAL"); // check no trailing token
            return result;
        }
        catch (IOException | JsonCastException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", JsonItems.toText(json).toStringUtf8(), REAL), e);
        }
    }

    // fallible
    @ScalarOperator(CAST)
    @SqlNullable
    @SqlType(NUMBER)
    public static TrinoNumber castToNumber(@SqlType(JSON) Json json)
    {
        try (JsonParser parser = jsonAsParser(json)) {
            parser.nextToken();
            TrinoNumber result = currentTokenAsNumber(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to NUMBER"); // check no trailing token
            return result;
        }
        catch (IOException | JsonCastException e) {
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
        try (JsonParser parser = jsonAsParser(json)) {
            parser.nextToken();
            Boolean result = currentTokenAsBoolean(parser);
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to BOOLEAN"); // check no trailing token
            return result;
        }
        catch (IOException | JsonCastException e) {
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

    // TODO: every cast from JSON pays a tree materialization plus a token round-trip through
    // Jackson, which makes this the slowest cast hop. A naive `JsonItems.toText(json)` +
    // Jackson token parsing swap breaks precision for arbitrary-precision integers —
    // `JsonItems.toJsonNode` deliberately
    // special-cases integer-valued `TrinoNumber` via `BigIntegerNode` so the consumer sees
    // VALUE_NUMBER_INT (preserved as text) rather than VALUE_NUMBER_FLOAT (which forces a
    // Double conversion and silently loses precision for values like `1e309`). A correct
    // replacement is a direct typed-encoding walker that emits the exact value without going
    // through Jackson, or a custom token-source that maps NUMBER scalars to VALUE_NUMBER_INT
    // when they round-trip to an integer.
    private static JsonParser jsonAsParser(Json json)
    {
        JsonNode node = JsonItems.toJsonNode(json);
        return JSON_MAPPER.treeAsTokens(node);
    }
}
