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
package io.trino.plugin.deltalake.kernel.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JsonRow
        implements Row
{
    private final Object[] parsedValues;
    private final StructType readSchema;

    public JsonRow(ObjectNode rootNode, StructType readSchema)
    {
        this.readSchema = readSchema;
        this.parsedValues = new Object[readSchema.length()];

        for (int i = 0; i < readSchema.length(); i++) {
            final StructField field = readSchema.at(i);
            final Object parsedValue = decodeField(rootNode, field);
            parsedValues[i] = parsedValue;
        }
    }

    @Override
    public StructType getSchema()
    {
        return readSchema;
    }

    @Override
    public boolean isNullAt(int ordinal)
    {
        return parsedValues[ordinal] == null;
    }

    @Override
    public boolean getBoolean(int ordinal)
    {
        return (boolean) parsedValues[ordinal];
    }

    @Override
    public byte getByte(int ordinal)
    {
        return (byte) parsedValues[ordinal];
    }

    @Override
    public short getShort(int ordinal)
    {
        return (short) parsedValues[ordinal];
    }

    @Override
    public int getInt(int ordinal)
    {
        return (int) parsedValues[ordinal];
    }

    @Override
    public long getLong(int ordinal)
    {
        return (long) parsedValues[ordinal];
    }

    @Override
    public float getFloat(int ordinal)
    {
        return (float) parsedValues[ordinal];
    }

    @Override
    public double getDouble(int ordinal)
    {
        return (double) parsedValues[ordinal];
    }

    @Override
    public String getString(int ordinal)
    {
        return (String) parsedValues[ordinal];
    }

    @Override
    public BigDecimal getDecimal(int ordinal)
    {
        return (BigDecimal) parsedValues[ordinal];
    }

    @Override
    public byte[] getBinary(int ordinal)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Row getStruct(int ordinal)
    {
        return (JsonRow) parsedValues[ordinal];
    }

    @Override
    public ArrayValue getArray(int ordinal)
    {
        return (ArrayValue) parsedValues[ordinal];
    }

    @Override
    public MapValue getMap(int ordinal)
    {
        return (MapValue) parsedValues[ordinal];
    }

    private static void throwIfTypeMismatch(String expType, boolean hasExpType, JsonNode jsonNode)
    {
        if (!hasExpType) {
            throw new RuntimeException(
                    String.format("Couldn't decode %s, expected a %s", jsonNode, expType));
        }
    }

    private static Object decodeElement(JsonNode jsonValue, DataType dataType)
    {
        if (jsonValue.isNull()) {
            return null;
        }

        if (dataType instanceof BooleanType) {
            throwIfTypeMismatch("boolean", jsonValue.isBoolean(), jsonValue);
            return jsonValue.booleanValue();
        }

        if (dataType instanceof ByteType) {
            throwIfTypeMismatch(
                    "byte",
                    jsonValue.canConvertToExactIntegral()
                            && jsonValue.canConvertToInt()
                            && jsonValue.intValue() <= Byte.MAX_VALUE
                            && jsonValue.canConvertToInt()
                            && jsonValue.intValue() >= Byte.MIN_VALUE,
                    jsonValue);
            return jsonValue.numberValue().byteValue();
        }

        if (dataType instanceof ShortType) {
            throwIfTypeMismatch(
                    "short",
                    jsonValue.canConvertToExactIntegral()
                            && jsonValue.canConvertToInt()
                            && jsonValue.intValue() <= Short.MAX_VALUE
                            && jsonValue.canConvertToInt()
                            && jsonValue.intValue() >= Short.MIN_VALUE,
                    jsonValue);
            return jsonValue.numberValue().shortValue();
        }

        if (dataType instanceof IntegerType) {
            throwIfTypeMismatch(
                    "integer", jsonValue.isIntegralNumber() && jsonValue.canConvertToInt(), jsonValue);
            return jsonValue.intValue();
        }

        if (dataType instanceof LongType) {
            throwIfTypeMismatch(
                    "long", jsonValue.isIntegralNumber() && jsonValue.canConvertToLong(), jsonValue);
            return jsonValue.numberValue().longValue();
        }

        if (dataType instanceof FloatType) {
            switch (jsonValue.getNodeType()) {
                case NUMBER:
                    throwIfTypeMismatch(
                            "float",
                            // floatValue() will be converted to +/-INF if it cannot be represented
                            // by a float
                            // Note it is still possible to lose precision in this conversion but
                            // checking for that requires converting to a float and back to BigDecimal
                            !Float.isInfinite(jsonValue.floatValue()),
                            jsonValue);
                    return jsonValue.floatValue();
                case STRING:
                    switch (jsonValue.asText()) {
                        case "NaN":
                            return Float.NaN;
                        case "+INF":
                        case "+Infinity":
                        case "Infinity":
                            return Float.POSITIVE_INFINITY;
                        case "-INF":
                        case "-Infinity":
                            return Float.NEGATIVE_INFINITY;
                    }
                default:
                    throwIfTypeMismatch("float", false, jsonValue);
            }
        }

        if (dataType instanceof DoubleType) {
            switch (jsonValue.getNodeType()) {
                case NUMBER:
                    throwIfTypeMismatch(
                            "double",
                            // doubleValue() will be converted to +/-INF if it cannot be represented by
                            // a double
                            // Note it is still possible to lose precision in this conversion but
                            // checking for that requires converting to a double and back to BigDecimal
                            !Double.isInfinite(jsonValue.doubleValue()),
                            jsonValue);
                    return jsonValue.doubleValue();
                case STRING:
                    switch (jsonValue.asText()) {
                        case "NaN":
                            return Double.NaN;
                        case "+INF":
                        case "+Infinity":
                        case "Infinity":
                            return Double.POSITIVE_INFINITY;
                        case "-INF":
                        case "-Infinity":
                            return Double.NEGATIVE_INFINITY;
                    }
                default:
                    throwIfTypeMismatch("double", false, jsonValue);
            }
        }

        if (dataType instanceof StringType) {
            throwIfTypeMismatch("string", jsonValue.isTextual(), jsonValue);
            return jsonValue.asText();
        }

        if (dataType instanceof DecimalType) {
            throwIfTypeMismatch("decimal", jsonValue.isNumber(), jsonValue);
            return jsonValue.decimalValue();
        }

        if (dataType instanceof DateType) {
            throwIfTypeMismatch("date", jsonValue.isTextual(), jsonValue);
            return InternalUtils.daysSinceEpoch(Date.valueOf(jsonValue.textValue()));
        }

        if (dataType instanceof TimestampType) {
            throwIfTypeMismatch("timestamp", jsonValue.isTextual(), jsonValue);
            Instant time = OffsetDateTime.parse(jsonValue.textValue()).toInstant();
            return ChronoUnit.MICROS.between(Instant.EPOCH, time);
        }

        if (dataType instanceof TimestampNTZType) {
            throwIfTypeMismatch("timestamp_ntz", jsonValue.isTextual(), jsonValue);
            throw new UnsupportedOperationException("not yet implemented");
        }

        if (dataType instanceof StructType) {
            throwIfTypeMismatch("object", jsonValue.isObject(), jsonValue);
            return new JsonRow((ObjectNode) jsonValue, (StructType) dataType);
        }

        if (dataType instanceof ArrayType) {
            throwIfTypeMismatch("array", jsonValue.isArray(), jsonValue);
            final ArrayType arrayType = ((ArrayType) dataType);
            final ArrayNode jsonArray = (ArrayNode) jsonValue;
            final Object[] elements = new Object[jsonArray.size()];
            for (int i = 0; i < jsonArray.size(); i++) {
                final JsonNode element = jsonArray.get(i);
                final Object parsedElement = decodeElement(element, arrayType.getElementType());
                if (parsedElement == null && !arrayType.containsNull()) {
                    throw new RuntimeException(
                            "Array type expects no nulls as elements, but " + "received `null` as array element");
                }
                elements[i] = parsedElement;
            }
            return new ArrayValue()
            {
                @Override
                public int getSize()
                {
                    return elements.length;
                }

                @Override
                public ColumnVector getElements()
                {
                    return DefaultGenericVector.fromArray(arrayType.getElementType(), elements);
                }
            };
        }

        if (dataType instanceof MapType) {
            throwIfTypeMismatch("map", jsonValue.isObject(), jsonValue);
            final MapType mapType = (MapType) dataType;
            if (!(mapType.getKeyType() instanceof StringType)) {
                throw new RuntimeException(
                        "MapType with a key type of `String` is supported, "
                                + "received a key type: "
                                + mapType.getKeyType());
            }
            List<Object> keys = new ArrayList<>(jsonValue.size());
            List<Object> values = new ArrayList<>(jsonValue.size());
            final Iterator<Map.Entry<String, JsonNode>> iter = jsonValue.fields();

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                String keyParsed = entry.getKey();
                Object valueParsed = decodeElement(entry.getValue(), mapType.getValueType());
                if (valueParsed == null && !mapType.isValueContainsNull()) {
                    throw new RuntimeException(
                            "Map type expects no nulls in values, but " + "received `null` as value");
                }
                keys.add(keyParsed);
                values.add(valueParsed);
            }
            return new MapValue()
            {
                @Override
                public int getSize()
                {
                    return jsonValue.size();
                }

                @Override
                public ColumnVector getKeys()
                {
                    return DefaultGenericVector.fromList(mapType.getKeyType(), keys);
                }

                @Override
                public ColumnVector getValues()
                {
                    return DefaultGenericVector.fromList(mapType.getValueType(), values);
                }
            };
        }

        throw new UnsupportedOperationException(
                String.format("Unsupported DataType %s for RootNode %s", dataType, jsonValue));
    }

    private static Object decodeField(ObjectNode rootNode, StructField field)
    {
        if (rootNode.get(field.getName()) == null || rootNode.get(field.getName()).isNull()) {
            if (field.isNullable()) {
                return null;
            }

            throw new RuntimeException(
                    String.format(
                            "Root node at key %s is null but field isn't nullable. Root node: %s",
                            field.getName(), rootNode));
        }

        return decodeElement(rootNode.get(field.getName()), field.getDataType());
    }
}
