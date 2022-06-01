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
package io.trino.json.ir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.primitives.Shorts;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_JSON_LITERAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class SqlJsonLiteralConverter
{
    private SqlJsonLiteralConverter() {}

    public static Optional<TypedValue> getTypedValue(JsonNode jsonNode)
    {
        if (jsonNode.getNodeType() == JsonNodeType.BOOLEAN) {
            return Optional.of(new TypedValue(BOOLEAN, jsonNode.booleanValue()));
        }
        if (jsonNode.getNodeType() == JsonNodeType.STRING) {
            return Optional.of(new TypedValue(VARCHAR, utf8Slice(jsonNode.textValue())));
        }
        return getNumericTypedValue(jsonNode);
    }

    public static Optional<TypedValue> getTextTypedValue(JsonNode jsonNode)
    {
        if (jsonNode.getNodeType() == JsonNodeType.STRING) {
            return Optional.of(new TypedValue(VARCHAR, utf8Slice(jsonNode.textValue())));
        }
        return Optional.empty();
    }

    public static Optional<TypedValue> getNumericTypedValue(JsonNode jsonNode)
    {
        if (jsonNode.getNodeType() == JsonNodeType.NUMBER) {
            if (jsonNode instanceof BigIntegerNode) {
                if (jsonNode.canConvertToInt()) {
                    return Optional.of(new TypedValue(INTEGER, jsonNode.longValue()));
                }
                if (jsonNode.canConvertToLong()) {
                    return Optional.of(new TypedValue(BIGINT, jsonNode.longValue()));
                }
                throw conversionError(jsonNode, "value too big");
            }
            if (jsonNode instanceof DecimalNode) {
                BigDecimal jsonDecimal = jsonNode.decimalValue();
                int precision = jsonDecimal.precision();
                if (precision > MAX_PRECISION) {
                    throw conversionError(jsonNode, "precision too big");
                }
                int scale = jsonDecimal.scale();
                DecimalType decimalType = createDecimalType(precision, scale);
                Object value = decimalType.isShort() ? encodeShortScaledValue(jsonDecimal, scale) : encodeScaledValue(jsonDecimal, scale);
                return Optional.of(TypedValue.fromValueAsObject(decimalType, value));
            }
            if (jsonNode instanceof DoubleNode) {
                return Optional.of(new TypedValue(DOUBLE, jsonNode.doubleValue()));
            }
            if (jsonNode instanceof FloatNode) {
                return Optional.of(new TypedValue(REAL, floatToRawIntBits(jsonNode.floatValue())));
            }
            if (jsonNode instanceof IntNode) {
                return Optional.of(new TypedValue(INTEGER, jsonNode.longValue()));
            }
            if (jsonNode instanceof LongNode) {
                return Optional.of(new TypedValue(BIGINT, jsonNode.longValue()));
            }
            if (jsonNode instanceof ShortNode) {
                return Optional.of(new TypedValue(SMALLINT, jsonNode.longValue()));
            }
        }

        return Optional.empty();
    }

    public static Optional<JsonNode> getJsonNode(TypedValue typedValue)
    {
        Type type = typedValue.getType();
        if (type.equals(BOOLEAN)) {
            return Optional.of(BooleanNode.valueOf(typedValue.getBooleanValue()));
        }
        if (type instanceof CharType) {
            return Optional.of(TextNode.valueOf(padSpaces((Slice) typedValue.getObjectValue(), (CharType) typedValue.getType()).toStringUtf8()));
        }
        if (type instanceof VarcharType) {
            return Optional.of(TextNode.valueOf(((Slice) typedValue.getObjectValue()).toStringUtf8()));
        }
        if (type.equals(BIGINT)) {
            return Optional.of(LongNode.valueOf(typedValue.getLongValue()));
        }
        if (type.equals(INTEGER)) {
            return Optional.of(IntNode.valueOf(toIntExact(typedValue.getLongValue())));
        }
        if (type.equals(SMALLINT)) {
            return Optional.of(ShortNode.valueOf(Shorts.checkedCast(typedValue.getLongValue())));
        }
        if (type.equals(TINYINT)) {
            return Optional.of(ShortNode.valueOf(Shorts.checkedCast(typedValue.getLongValue())));
        }
        if (type instanceof DecimalType) {
            BigInteger unscaledValue;
            if (((DecimalType) type).isShort()) {
                unscaledValue = BigInteger.valueOf(typedValue.getLongValue());
            }
            else {
                unscaledValue = ((Int128) typedValue.getObjectValue()).toBigInteger();
            }
            return Optional.of(DecimalNode.valueOf(new BigDecimal(unscaledValue, ((DecimalType) type).getScale())));
        }
        if (type.equals(DOUBLE)) {
            return Optional.of(DoubleNode.valueOf(typedValue.getDoubleValue()));
        }
        if (type.equals(REAL)) {
            return Optional.of(FloatNode.valueOf(intBitsToFloat(toIntExact(typedValue.getLongValue()))));
        }

        return Optional.empty();
    }

    public static TrinoException conversionError(JsonNode jsonNode, String cause)
    {
        return new JsonLiteralConversionError(jsonNode, cause);
    }

    public static class JsonLiteralConversionError
            extends TrinoException
    {
        public JsonLiteralConversionError(JsonNode jsonNode, String cause)
        {
            super(INVALID_JSON_LITERAL, format("cannot convert %s to Trino value (%s)", jsonNode, cause));
        }
    }
}
