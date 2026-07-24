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
package io.trino.jsonpath.ir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.primitives.Shorts;
import io.airlift.slice.Slice;
import io.trino.json.Json;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonItemEncoding.TypeTag;
import io.trino.json.TypedValue;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static io.trino.spi.type.Chars.padSpaces;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public final class SqlJsonLiteralConverter
{
    private SqlJsonLiteralConverter() {}

    /// Returns the [TypedValue] for a scalar [Json] item if its scalar type is one
    /// recognized by the path engine (boolean, varchar, or any numeric tag).
    /// Empty for non-scalar items (NULL, ARRAY, OBJECT, ERROR).
    public static Optional<TypedValue> getTypedValue(Json json)
    {
        if (!json.isScalar()) {
            return Optional.empty();
        }
        return Optional.of(json.materializeScalar());
    }

    /// Returns the [TypedValue] for a VARCHAR scalar [Json] item; empty for any other
    /// kind.
    public static Optional<TypedValue> getTextTypedValue(Json json)
    {
        if (!json.isScalar() || json.scalarType() != TypeTag.VARCHAR) {
            return Optional.empty();
        }
        return Optional.of(json.materializeScalar());
    }

    /// Returns the [TypedValue] for a numeric scalar [Json] item; empty for any other
    /// kind.
    public static Optional<TypedValue> getNumericTypedValue(Json json)
    {
        if (!json.isScalar()) {
            return Optional.empty();
        }
        return switch (json.scalarType()) {
            case BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, DECIMAL, NUMBER -> Optional.of(json.materializeScalar());
            case BOOLEAN, VARCHAR, DATE, TIME, TIME_WITH_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE -> Optional.empty();
        };
    }

    /// Encodes a [TypedValue] back into a self-contained [Json] payload. Used when
    /// the path engine needs to emit a SQL scalar as a JSON value (e.g., to be
    /// returned by JSON_QUERY when the result is a typed scalar). Empty when the SQL
    /// type has no JSON representation (datetime types, arbitrary user types).
    public static Optional<Json> getJson(TypedValue typedValue)
    {
        return Optional.ofNullable(switch (typedValue.type()) {
            case BooleanType _ -> JsonItemBuilder.encodeBoolean(typedValue.getBooleanValue());
            case CharType charType -> JsonItemBuilder.encodeVarchar(padSpaces((Slice) typedValue.getObjectValue(), charType));
            case VarcharType _ -> JsonItemBuilder.encodeVarchar((Slice) typedValue.getObjectValue());
            case BigintType _ -> JsonItemBuilder.encodeBigint(typedValue.getLongValue());
            case IntegerType _ -> JsonItemBuilder.encode(w -> w.integerValue(typedValue.getLongValue()));
            case SmallintType _ -> JsonItemBuilder.encode(w -> w.smallintValue(typedValue.getLongValue()));
            case TinyintType _ -> JsonItemBuilder.encode(w -> w.tinyintValue(typedValue.getLongValue()));
            case DecimalType decimalType -> decimalType.isShort()
                    ? JsonItemBuilder.encodeShortDecimal(decimalType.getPrecision(), decimalType.getScale(), typedValue.getLongValue())
                    : JsonItemBuilder.encodeLongDecimal(decimalType.getPrecision(), decimalType.getScale(), (Int128) typedValue.getObjectValue());
            case DoubleType _ -> JsonItemBuilder.encodeDouble(typedValue.getDoubleValue());
            case RealType _ -> JsonItemBuilder.encode(w -> w.realBits(toIntExact(typedValue.getLongValue())));
            case NumberType _ -> JsonItemBuilder.encodeNumber((TrinoNumber) typedValue.getObjectValue());
            default -> null;
        });
    }

    /// SPI-channel adapter: produces a [JsonNode] for callers that still return
    /// JsonNode through the JsonType binding (JSON_QUERY, JSON_OBJECT, JSON_ARRAY,
    /// json_table). Mirrors [#getJson]; empty when the SQL type has no JSON
    /// representation.
    public static Optional<JsonNode> getJsonNode(TypedValue typedValue)
    {
        return Optional.ofNullable(switch (typedValue.type()) {
            case BooleanType _ -> BooleanNode.valueOf(typedValue.getBooleanValue());
            case CharType charType -> TextNode.valueOf(padSpaces((Slice) typedValue.getObjectValue(), charType).toStringUtf8());
            case VarcharType _ -> TextNode.valueOf(((Slice) typedValue.getObjectValue()).toStringUtf8());
            case BigintType _ -> LongNode.valueOf(typedValue.getLongValue());
            case IntegerType _ -> IntNode.valueOf(toIntExact(typedValue.getLongValue()));
            case SmallintType _, TinyintType _ -> ShortNode.valueOf(Shorts.checkedCast(typedValue.getLongValue()));
            case DecimalType decimalType -> {
                BigInteger unscaledValue = decimalType.isShort()
                        ? BigInteger.valueOf(typedValue.getLongValue())
                        : ((Int128) typedValue.getObjectValue()).toBigInteger();
                yield DecimalNode.valueOf(new BigDecimal(unscaledValue, decimalType.getScale()));
            }
            case DoubleType _ -> DoubleNode.valueOf(typedValue.getDoubleValue());
            case RealType _ -> FloatNode.valueOf(intBitsToFloat(toIntExact(typedValue.getLongValue())));
            case NumberType _ -> switch (((TrinoNumber) typedValue.getObjectValue()).toBigDecimal()) {
                case TrinoNumber.BigDecimalValue(BigDecimal decimal) -> DecimalNode.valueOf(decimal);
                case TrinoNumber.NotANumber _ -> TextNode.valueOf("NaN");
                case TrinoNumber.Infinity(boolean negative) -> TextNode.valueOf(negative ? "-Infinity" : "+Infinity");
            };
            default -> null;
        });
    }
}
