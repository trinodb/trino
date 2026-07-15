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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
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
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

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
            return switch (jsonNode) {
                case BigIntegerNode _ -> {
                    if (jsonNode.canConvertToInt()) {
                        yield Optional.of(new TypedValue(INTEGER, jsonNode.longValue()));
                    }
                    if (jsonNode.canConvertToLong()) {
                        yield Optional.of(new TypedValue(BIGINT, jsonNode.longValue()));
                    }
                    throw new JsonLiteralConversionException(jsonNode, "value too big");
                }
                case DecimalNode _ -> {
                    BigDecimal jsonDecimal = jsonNode.decimalValue();
                    int precision = jsonDecimal.precision();
                    if (precision > MAX_PRECISION) {
                        throw new JsonLiteralConversionException(jsonNode, "precision too big");
                    }
                    int scale = jsonDecimal.scale();
                    DecimalType decimalType = createDecimalType(precision, scale);
                    Object value = decimalType.isShort() ? encodeShortScaledValue(jsonDecimal, scale) : encodeScaledValue(jsonDecimal, scale);
                    yield Optional.of(TypedValue.fromValueAsObject(decimalType, value));
                }
                case DoubleNode _ -> Optional.of(new TypedValue(DOUBLE, jsonNode.doubleValue()));
                case FloatNode _ -> Optional.of(new TypedValue(REAL, floatToRawIntBits(jsonNode.floatValue())));
                case IntNode _ -> Optional.of(new TypedValue(INTEGER, jsonNode.longValue()));
                case LongNode _ -> Optional.of(new TypedValue(BIGINT, jsonNode.longValue()));
                case ShortNode _ -> Optional.of(new TypedValue(SMALLINT, jsonNode.longValue()));
                default -> Optional.empty();
            };
        }
        return Optional.empty();
    }

    public static Optional<JsonNode> getJsonNode(TypedValue typedValue)
    {
        return Optional.ofNullable(switch (typedValue.getType()) {
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
            default -> null;
        });
    }
}
