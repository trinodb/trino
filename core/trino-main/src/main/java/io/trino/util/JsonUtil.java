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
package io.trino.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DuplicateMapKeyException;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;
import io.trino.type.BigintOperators;
import io.trino.type.BooleanOperators;
import io.trino.type.DoubleOperators;
import io.trino.type.JsonType;
import io.trino.type.UnknownType;
import io.trino.type.VarcharOperators;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.util.JsonUtils.jsonFactoryBuilder;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static io.trino.type.DateTimes.formatTimestamp;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.util.DateTimeUtils.printDate;
import static io.trino.util.JsonUtil.ObjectKeyProvider.createObjectKeyProvider;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;

public final class JsonUtil
{
    // StringReader outperforms InputStreamReader for small inputs. Limit based on Jackson benchmarks {@link https://github.com/FasterXML/jackson-benchmarks/pull/9}
    private static final int STRING_READER_LENGTH_LIMIT = 8192;

    private JsonUtil() {}

    // This object mapper is constructed without .configure(ORDER_MAP_ENTRIES_BY_KEYS, true) because
    // `OBJECT_MAPPER.writeValueAsString(parser.readValueAsTree());` preserves input order.
    // Be aware. Using it arbitrarily can produce invalid json (ordered by key is required in Trino).
    private static final ObjectMapper OBJECT_MAPPED_UNORDERED = new ObjectMapper(createJsonFactory());

    private static final int MAX_JSON_LENGTH_IN_ERROR_MESSAGE = 10_000;

    // Note: JsonFactory is mutable, instances cannot be shared openly.
    public static JsonFactory createJsonFactory()
    {
        return jsonFactoryBuilder().disable(CANONICALIZE_FIELD_NAMES).build();
    }

    public static JsonParser createJsonParser(JsonFactory factory, Slice json)
            throws IOException
    {
        // Jackson tries to detect the character encoding automatically when using InputStream
        // so we pass StringReader or an InputStreamReader instead.
        // Despite the https://github.com/FasterXML/jackson-core/pull/1081, the below performance optimization
        // is still valid for small inputs.
        if (json.length() < STRING_READER_LENGTH_LIMIT) {
            // StringReader is more performant than InputStreamReader for small inputs
            return factory.createParser(new StringReader(json.toStringUtf8()));
        }

        return factory.createParser(new InputStreamReader(json.getInput(), UTF_8));
    }

    public static JsonGenerator createJsonGenerator(JsonFactory factory, SliceOutput output)
            throws IOException
    {
        return factory.createGenerator((OutputStream) output);
    }

    public static String truncateIfNecessaryForErrorMessage(Slice json)
    {
        if (json.length() <= MAX_JSON_LENGTH_IN_ERROR_MESSAGE) {
            return json.toStringUtf8();
        }
        return json.slice(0, MAX_JSON_LENGTH_IN_ERROR_MESSAGE).toStringUtf8() + "...(truncated)";
    }

    public static boolean canCastToJson(Type type)
    {
        if (type instanceof UnknownType ||
                type instanceof BooleanType ||
                type instanceof TinyintType ||
                type instanceof SmallintType ||
                type instanceof IntegerType ||
                type instanceof BigintType ||
                type instanceof RealType ||
                type instanceof DoubleType ||
                type instanceof DecimalType ||
                type instanceof VarcharType ||
                type instanceof JsonType ||
                type instanceof TimestampType ||
                type instanceof DateType) {
            return true;
        }
        if (type instanceof ArrayType arrayType) {
            return canCastToJson(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return (mapType.getKeyType() instanceof UnknownType ||
                    isValidJsonObjectKeyType(mapType.getKeyType())) &&
                    canCastToJson(mapType.getValueType());
        }
        if (type instanceof RowType) {
            return type.getTypeParameters().stream().allMatch(JsonUtil::canCastToJson);
        }
        return false;
    }

    public static boolean canCastFromJson(Type type)
    {
        if (type instanceof BooleanType ||
                type instanceof TinyintType ||
                type instanceof SmallintType ||
                type instanceof IntegerType ||
                type instanceof BigintType ||
                type instanceof RealType ||
                type instanceof DoubleType ||
                type instanceof DecimalType ||
                type instanceof VarcharType ||
                type instanceof JsonType) {
            return true;
        }
        if (type instanceof ArrayType arrayType) {
            return canCastFromJson(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return isValidJsonObjectKeyType(mapType.getKeyType()) && canCastFromJson(mapType.getValueType());
        }
        if (type instanceof RowType) {
            return type.getTypeParameters().stream().allMatch(JsonUtil::canCastFromJson);
        }
        return false;
    }

    private static boolean isValidJsonObjectKeyType(Type type)
    {
        return type instanceof BooleanType ||
                type instanceof TinyintType ||
                type instanceof SmallintType ||
                type instanceof IntegerType ||
                type instanceof BigintType ||
                type instanceof RealType ||
                type instanceof DoubleType ||
                type instanceof DecimalType ||
                type instanceof VarcharType;
    }

    // transform the map key into string for use as JSON object key
    public interface ObjectKeyProvider
    {
        String getObjectKey(Block block, int position);

        static ObjectKeyProvider createObjectKeyProvider(Type type)
        {
            if (type.equals(UNKNOWN)) {
                return (block, position) -> null;
            }
            if (type.equals(BOOLEAN)) {
                return (block, position) -> BOOLEAN.getBoolean(block, position) ? "true" : "false";
            }
            if (type.equals(TINYINT)) {
                return (block, position) -> String.valueOf(TINYINT.getByte(block, position));
            }
            if (type.equals(SMALLINT)) {
                return (block, position) -> String.valueOf(SMALLINT.getShort(block, position));
            }
            if (type.equals(INTEGER)) {
                return (block, position) -> String.valueOf(INTEGER.getInt(block, position));
            }
            if (type.equals(BIGINT)) {
                return (block, position) -> String.valueOf(BIGINT.getLong(block, position));
            }
            if (type.equals(REAL)) {
                return (block, position) -> String.valueOf(REAL.getFloat(block, position));
            }
            if (type.equals(DOUBLE)) {
                return (block, position) -> String.valueOf(DOUBLE.getDouble(block, position));
            }
            if (type instanceof DecimalType decimalType) {
                if (decimalType.isShort()) {
                    return (block, position) -> Decimals.toString(decimalType.getLong(block, position), decimalType.getScale());
                }
                return (block, position) -> Decimals.toString(
                        ((Int128) decimalType.getObject(block, position)).toBigInteger(),
                        decimalType.getScale());
            }
            if (type instanceof VarcharType varcharType) {
                return (block, position) -> varcharType.getSlice(block, position).toStringUtf8();
            }

            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported type: %s", type));
        }
    }

    // given block and position, write to JsonGenerator
    public interface JsonGeneratorWriter
    {
        // write a Json value into the JsonGenerator, provided by block and position
        void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException;

        static JsonGeneratorWriter createJsonGeneratorWriter(Type type)
        {
            if (type instanceof UnknownType) {
                return new UnknownJsonGeneratorWriter();
            }
            if (type instanceof BooleanType) {
                return new BooleanJsonGeneratorWriter();
            }
            if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType || type instanceof BigintType) {
                return new LongJsonGeneratorWriter(type);
            }
            if (type instanceof RealType) {
                return new RealJsonGeneratorWriter();
            }
            if (type instanceof DoubleType) {
                return new DoubleJsonGeneratorWriter();
            }
            if (type instanceof DecimalType decimalType) {
                if (decimalType.isShort()) {
                    return new ShortDecimalJsonGeneratorWriter(decimalType);
                }
                return new LongDecimalJsonGeneratorWriter(decimalType);
            }
            if (type instanceof VarcharType) {
                return new VarcharJsonGeneratorWriter(type);
            }
            if (type instanceof JsonType) {
                return new JsonJsonGeneratorWriter();
            }
            if (type instanceof TimestampType timestampType) {
                return new TimestampJsonGeneratorWriter(timestampType);
            }
            if (type instanceof DateType) {
                return new DateGeneratorWriter();
            }
            if (type instanceof ArrayType arrayType) {
                return new ArrayJsonGeneratorWriter(
                        arrayType,
                        createJsonGeneratorWriter(arrayType.getElementType()));
            }
            if (type instanceof MapType mapType) {
                return new MapJsonGeneratorWriter(
                        mapType,
                        createObjectKeyProvider(mapType.getKeyType()),
                        createJsonGeneratorWriter(mapType.getValueType()));
            }
            if (type instanceof RowType rowType) {
                List<Type> fieldTypes = type.getTypeParameters();
                List<JsonGeneratorWriter> fieldWriters = new ArrayList<>(fieldTypes.size());
                for (int i = 0; i < fieldTypes.size(); i++) {
                    fieldWriters.add(createJsonGeneratorWriter(fieldTypes.get(i)));
                }
                return new RowJsonGeneratorWriter(rowType, fieldWriters);
            }

            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported type: %s", type));
        }
    }

    private static class UnknownJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            jsonGenerator.writeNull();
        }
    }

    private static class BooleanJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                boolean value = BOOLEAN.getBoolean(block, position);
                jsonGenerator.writeBoolean(value);
            }
        }
    }

    private static class LongJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final Type type;

        public LongJsonGeneratorWriter(Type type)
        {
            this.type = type;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                long value = type.getLong(block, position);
                jsonGenerator.writeNumber(value);
            }
        }
    }

    private static class RealJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                float value = REAL.getFloat(block, position);
                jsonGenerator.writeNumber(value);
            }
        }
    }

    private static class DoubleJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                double value = DOUBLE.getDouble(block, position);
                jsonGenerator.writeNumber(value);
            }
        }
    }

    private static class ShortDecimalJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final DecimalType type;

        public ShortDecimalJsonGeneratorWriter(DecimalType type)
        {
            this.type = type;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                BigDecimal value = BigDecimal.valueOf(type.getLong(block, position), type.getScale());
                jsonGenerator.writeNumber(value);
            }
        }
    }

    private static class LongDecimalJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final DecimalType type;

        public LongDecimalJsonGeneratorWriter(DecimalType type)
        {
            this.type = type;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                BigDecimal value = new BigDecimal(
                        ((Int128) type.getObject(block, position)).toBigInteger(),
                        type.getScale());
                jsonGenerator.writeNumber(value);
            }
        }
    }

    private static class VarcharJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final Type type;

        public VarcharJsonGeneratorWriter(Type type)
        {
            this.type = type;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                Slice value = type.getSlice(block, position);
                jsonGenerator.writeString(value.toStringUtf8());
            }
        }
    }

    private static class JsonJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                Slice value = JSON.getSlice(block, position);
                jsonGenerator.writeRawValue(value.toStringUtf8());
            }
        }
    }

    private static class TimestampJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final TimestampType type;

        public TimestampJsonGeneratorWriter(TimestampType type)
        {
            this.type = type;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                long epochMicros;
                int fraction;

                if (type.isShort()) {
                    epochMicros = type.getLong(block, position);
                    fraction = 0;
                }
                else {
                    LongTimestamp timestamp = (LongTimestamp) type.getObject(block, position);
                    epochMicros = timestamp.getEpochMicros();
                    fraction = timestamp.getPicosOfMicro();
                }

                jsonGenerator.writeString(formatTimestamp(type.getPrecision(), epochMicros, fraction, UTC));
            }
        }
    }

    private static class DateGeneratorWriter
            implements JsonGeneratorWriter
    {
        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                int value = DATE.getInt(block, position);
                jsonGenerator.writeString(printDate(value));
            }
        }
    }

    private static class ArrayJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final ArrayType type;
        private final JsonGeneratorWriter elementWriter;

        public ArrayJsonGeneratorWriter(ArrayType type, JsonGeneratorWriter elementWriter)
        {
            this.type = type;
            this.elementWriter = elementWriter;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                Block arrayBlock = type.getObject(block, position);
                jsonGenerator.writeStartArray();
                for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                    elementWriter.writeJsonValue(jsonGenerator, arrayBlock, i);
                }
                jsonGenerator.writeEndArray();
            }
        }
    }

    private static class MapJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final MapType type;
        private final ObjectKeyProvider keyProvider;
        private final JsonGeneratorWriter valueWriter;

        public MapJsonGeneratorWriter(MapType type, ObjectKeyProvider keyProvider, JsonGeneratorWriter valueWriter)
        {
            this.type = type;
            this.keyProvider = keyProvider;
            this.valueWriter = valueWriter;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                SqlMap sqlMap = type.getObject(block, position);

                int rawOffset = sqlMap.getRawOffset();
                Block rawKeyBlock = sqlMap.getRawKeyBlock();
                Block rawValueBlock = sqlMap.getRawValueBlock();

                Map<String, Integer> orderedKeyToValuePosition = new TreeMap<>();
                for (int i = 0; i < sqlMap.getSize(); i++) {
                    String objectKey = keyProvider.getObjectKey(rawKeyBlock, rawOffset + i);
                    orderedKeyToValuePosition.put(objectKey, i);
                }

                jsonGenerator.writeStartObject();
                for (Map.Entry<String, Integer> entry : orderedKeyToValuePosition.entrySet()) {
                    jsonGenerator.writeFieldName(entry.getKey());
                    valueWriter.writeJsonValue(jsonGenerator, rawValueBlock, rawOffset + entry.getValue());
                }
                jsonGenerator.writeEndObject();
            }
        }
    }

    private static class RowJsonGeneratorWriter
            implements JsonGeneratorWriter
    {
        private final RowType type;
        private final List<JsonGeneratorWriter> fieldWriters;

        public RowJsonGeneratorWriter(RowType type, List<JsonGeneratorWriter> fieldWriters)
        {
            this.type = type;
            this.fieldWriters = fieldWriters;
        }

        @Override
        public void writeJsonValue(JsonGenerator jsonGenerator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                jsonGenerator.writeNull();
            }
            else {
                SqlRow sqlRow = type.getObject(block, position);
                int rawIndex = sqlRow.getRawIndex();

                List<TypeSignatureParameter> typeSignatureParameters = type.getTypeSignature().getParameters();
                jsonGenerator.writeStartObject();
                for (int i = 0; i < sqlRow.getFieldCount(); i++) {
                    jsonGenerator.writeFieldName(typeSignatureParameters.get(i).getNamedTypeSignature().getName().orElse(""));
                    fieldWriters.get(i).writeJsonValue(jsonGenerator, sqlRow.getRawFieldBlock(i), rawIndex);
                }
                jsonGenerator.writeEndObject();
            }
        }
    }

    // utility classes and functions for cast from JSON
    public static Slice currentTokenAsVarchar(JsonParser parser)
            throws IOException
    {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING, FIELD_NAME -> Slices.utf8Slice(parser.getText());
            // Avoidance of loss of precision does not seem to be possible here because of Jackson implementation.
            case VALUE_NUMBER_FLOAT -> DoubleOperators.castToVarchar(UNBOUNDED_LENGTH, parser.getDoubleValue());
            // An alternative is calling getLongValue and then BigintOperators.castToVarchar.
            // It doesn't work as well because it can result in overflow and underflow exceptions for large integral numbers.
            case VALUE_NUMBER_INT -> Slices.utf8Slice(parser.getText());
            case VALUE_TRUE -> BooleanOperators.castToVarchar(true);
            case VALUE_FALSE -> BooleanOperators.castToVarchar(false);
            default -> throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.VARCHAR, parser.getText()));
        };
    }

    public static Long currentTokenAsBigint(JsonParser parser)
            throws IOException
    {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToBigint(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT -> DoubleOperators.castToLong(parser.getDoubleValue());
            case VALUE_NUMBER_INT -> parser.getLongValue();
            case VALUE_TRUE -> BooleanOperators.castToBigint(true);
            case VALUE_FALSE -> BooleanOperators.castToBigint(false);
            default -> throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.BIGINT, parser.getText()));
        };
    }

    public static Long currentTokenAsInteger(JsonParser parser)
            throws IOException
    {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToInteger(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT -> DoubleOperators.castToInteger(parser.getDoubleValue());
            case VALUE_NUMBER_INT -> (long) toIntExact(parser.getLongValue());
            case VALUE_TRUE -> BooleanOperators.castToInteger(true);
            case VALUE_FALSE -> BooleanOperators.castToInteger(false);
            default -> throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.INTEGER, parser.getText()));
        };
    }

    public static Long currentTokenAsSmallint(JsonParser parser)
            throws IOException
    {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToSmallint(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT -> DoubleOperators.castToSmallint(parser.getDoubleValue());
            case VALUE_NUMBER_INT -> (long) Shorts.checkedCast(parser.getLongValue());
            case VALUE_TRUE -> BooleanOperators.castToSmallint(true);
            case VALUE_FALSE -> BooleanOperators.castToSmallint(false);
            default -> throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.SMALLINT, parser.getText()));
        };
    }

    public static Long currentTokenAsTinyint(JsonParser parser)
            throws IOException
    {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToTinyint(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT -> DoubleOperators.castToTinyint(parser.getDoubleValue());
            case VALUE_NUMBER_INT -> (long) SignedBytes.checkedCast(parser.getLongValue());
            case VALUE_TRUE -> BooleanOperators.castToTinyint(true);
            case VALUE_FALSE -> BooleanOperators.castToTinyint(false);
            default -> throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.TINYINT, parser.getText()));
        };
    }

    public static Double currentTokenAsDouble(JsonParser parser)
            throws IOException
    {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToDouble(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT -> parser.getDoubleValue();
            // An alternative is calling getLongValue and then BigintOperators.castToDouble.
            // It doesn't work as well because it can result in overflow and underflow exceptions for large integral numbers.
            case VALUE_NUMBER_INT -> parser.getDoubleValue();
            case VALUE_TRUE -> BooleanOperators.castToDouble(true);
            case VALUE_FALSE -> BooleanOperators.castToDouble(false);
            default -> throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.DOUBLE, parser.getText()));
        };
    }

    public static Long currentTokenAsReal(JsonParser parser)
            throws IOException
    {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToFloat(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT -> (long) floatToRawIntBits(parser.getFloatValue());
            // An alternative is calling getLongValue and then BigintOperators.castToReal.
            // It doesn't work as well because it can result in overflow and underflow exceptions for large integral numbers.
            case VALUE_NUMBER_INT -> (long) floatToRawIntBits(parser.getFloatValue());
            case VALUE_TRUE -> BooleanOperators.castToReal(true);
            case VALUE_FALSE -> BooleanOperators.castToReal(false);
            default -> throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.REAL, parser.getText()));
        };
    }

    public static Boolean currentTokenAsBoolean(JsonParser parser)
            throws IOException
    {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToBoolean(Slices.utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT -> DoubleOperators.castToBoolean(parser.getDoubleValue());
            case VALUE_NUMBER_INT -> BigintOperators.castToBoolean(parser.getLongValue());
            case VALUE_TRUE -> true;
            case VALUE_FALSE -> false;
            default -> throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.BOOLEAN, parser.getText()));
        };
    }

    public static Long currentTokenAsShortDecimal(JsonParser parser, int precision, int scale)
            throws IOException
    {
        BigDecimal bigDecimal = currentTokenAsJavaDecimal(parser, precision, scale);
        return bigDecimal != null ? bigDecimal.unscaledValue().longValue() : null;
    }

    public static Int128 currentTokenAsLongDecimal(JsonParser parser, int precision, int scale)
            throws IOException
    {
        BigDecimal bigDecimal = currentTokenAsJavaDecimal(parser, precision, scale);
        if (bigDecimal == null) {
            return null;
        }
        return Int128.valueOf(bigDecimal.unscaledValue());
    }

    // TODO: Instead of having BigDecimal as an intermediate step,
    // an alternative way is to make currentTokenAsShortDecimal and currentTokenAsLongDecimal
    // directly return the Long or Slice representation of the cast result
    // by calling the corresponding cast-to-decimal function, similar to other JSON cast function.
    private static BigDecimal currentTokenAsJavaDecimal(JsonParser parser, int precision, int scale)
            throws IOException
    {
        BigDecimal result;
        switch (parser.getCurrentToken()) {
            case VALUE_NULL:
                return null;
            case VALUE_STRING:
            case FIELD_NAME:
                result = new BigDecimal(parser.getText());
                result = result.setScale(scale, HALF_UP);
                break;
            case VALUE_NUMBER_FLOAT:
            case VALUE_NUMBER_INT:
                result = parser.getDecimalValue();
                result = result.setScale(scale, HALF_UP);
                break;
            case VALUE_TRUE:
                result = BigDecimal.ONE.setScale(scale, HALF_UP);
                break;
            case VALUE_FALSE:
                result = BigDecimal.ZERO.setScale(scale, HALF_UP);
                break;
            default:
                throw new JsonCastException(format("Unexpected token when cast to DECIMAL(%s,%s): %s", precision, scale, parser.getText()));
        }

        if (result.precision() > precision) {
            // TODO: Should we use NUMERIC_VALUE_OUT_OF_RANGE instead?
            throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot cast input json to DECIMAL(%s,%s)", precision, scale));
        }
        return result;
    }

    // given a JSON parser, write to the BlockBuilder
    public interface BlockBuilderAppender
    {
        void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException;

        static BlockBuilderAppender createBlockBuilderAppender(Type type)
        {
            if (type instanceof BooleanType) {
                return new BooleanBlockBuilderAppender();
            }
            if (type instanceof TinyintType) {
                return new TinyintBlockBuilderAppender();
            }
            if (type instanceof SmallintType) {
                return new SmallintBlockBuilderAppender();
            }
            if (type instanceof IntegerType) {
                return new IntegerBlockBuilderAppender();
            }
            if (type instanceof BigintType) {
                return new BigintBlockBuilderAppender();
            }
            if (type instanceof RealType) {
                return new RealBlockBuilderAppender();
            }
            if (type instanceof DoubleType) {
                return new DoubleBlockBuilderAppender();
            }
            if (type instanceof DecimalType decimalType) {
                if (decimalType.isShort()) {
                    return new ShortDecimalBlockBuilderAppender(decimalType);
                }

                return new LongDecimalBlockBuilderAppender(decimalType);
            }
            if (type instanceof VarcharType) {
                return new VarcharBlockBuilderAppender(type);
            }
            if (type instanceof JsonType) {
                return (parser, blockBuilder) -> {
                    String json = OBJECT_MAPPED_UNORDERED.writeValueAsString(parser.readValueAsTree());
                    JSON.writeSlice(blockBuilder, Slices.utf8Slice(json));
                };
            }
            if (type instanceof ArrayType arrayType) {
                return new ArrayBlockBuilderAppender(createBlockBuilderAppender(arrayType.getElementType()));
            }
            if (type instanceof MapType mapType) {
                return new MapBlockBuilderAppender(
                        createBlockBuilderAppender(mapType.getKeyType()),
                        createBlockBuilderAppender(mapType.getValueType()));
            }
            if (type instanceof RowType rowType) {
                List<Field> rowFields = rowType.getFields();
                BlockBuilderAppender[] fieldAppenders = new BlockBuilderAppender[rowFields.size()];
                for (int i = 0; i < fieldAppenders.length; i++) {
                    fieldAppenders[i] = createBlockBuilderAppender(rowFields.get(i).getType());
                }
                return new RowBlockBuilderAppender(fieldAppenders, getFieldNameToIndex(rowFields));
            }

            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Unsupported type: %s", type));
        }
    }

    private static class BooleanBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Boolean result = currentTokenAsBoolean(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(blockBuilder, result);
            }
        }
    }

    private static class TinyintBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsTinyint(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                TINYINT.writeLong(blockBuilder, result);
            }
        }
    }

    private static class SmallintBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsInteger(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                SMALLINT.writeLong(blockBuilder, result);
            }
        }
    }

    private static class IntegerBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsInteger(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                INTEGER.writeLong(blockBuilder, result);
            }
        }
    }

    private static class BigintBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsBigint(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, result);
            }
        }
    }

    private static class RealBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsReal(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                REAL.writeLong(blockBuilder, result);
            }
        }
    }

    private static class DoubleBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Double result = currentTokenAsDouble(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                DOUBLE.writeDouble(blockBuilder, result);
            }
        }
    }

    private static class ShortDecimalBlockBuilderAppender
            implements BlockBuilderAppender
    {
        final DecimalType type;

        ShortDecimalBlockBuilderAppender(DecimalType type)
        {
            this.type = type;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Long result = currentTokenAsShortDecimal(parser, type.getPrecision(), type.getScale());

            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeLong(blockBuilder, result);
            }
        }
    }

    private static class LongDecimalBlockBuilderAppender
            implements BlockBuilderAppender
    {
        final DecimalType type;

        LongDecimalBlockBuilderAppender(DecimalType type)
        {
            this.type = type;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Int128 result = currentTokenAsLongDecimal(parser, type.getPrecision(), type.getScale());

            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeObject(blockBuilder, result);
            }
        }
    }

    private static class VarcharBlockBuilderAppender
            implements BlockBuilderAppender
    {
        final Type type;

        VarcharBlockBuilderAppender(Type type)
        {
            this.type = type;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            Slice result = currentTokenAsVarchar(parser);
            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeSlice(blockBuilder, result);
            }
        }
    }

    private static class ArrayBlockBuilderAppender
            implements BlockBuilderAppender
    {
        final BlockBuilderAppender elementAppender;

        ArrayBlockBuilderAppender(BlockBuilderAppender elementAppender)
        {
            this.elementAppender = elementAppender;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
                blockBuilder.appendNull();
                return;
            }

            if (parser.getCurrentToken() != START_ARRAY) {
                throw new JsonCastException(format("Expected a json array, but got %s", parser.getText()));
            }
            ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
                while (parser.nextToken() != END_ARRAY) {
                    elementAppender.append(parser, elementBuilder);
                }
            });
        }
    }

    private static class MapBlockBuilderAppender
            implements BlockBuilderAppender
    {
        final BlockBuilderAppender keyAppender;
        final BlockBuilderAppender valueAppender;

        MapBlockBuilderAppender(BlockBuilderAppender keyAppender, BlockBuilderAppender valueAppender)
        {
            this.keyAppender = keyAppender;
            this.valueAppender = valueAppender;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
                blockBuilder.appendNull();
                return;
            }

            if (parser.getCurrentToken() != START_OBJECT) {
                throw new JsonCastException(format("Expected a json object, but got %s", parser.getText()));
            }

            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) blockBuilder;
            mapBlockBuilder.strict();
            try {
                mapBlockBuilder.buildEntry((keyBuilder, valueBuilder) -> appendMap(parser, keyBuilder, valueBuilder));
            }
            catch (DuplicateMapKeyException e) {
                throw new JsonCastException("Duplicate keys are not allowed");
            }
        }

        private void appendMap(JsonParser parser, BlockBuilder keyBuilder, BlockBuilder valueBuilder)
                throws IOException
        {
            while (parser.nextToken() != END_OBJECT) {
                keyAppender.append(parser, keyBuilder);
                parser.nextToken();
                valueAppender.append(parser, valueBuilder);
            }
        }
    }

    private static class RowBlockBuilderAppender
            implements BlockBuilderAppender
    {
        final BlockBuilderAppender[] fieldAppenders;
        final Optional<Map<String, Integer>> fieldNameToIndex;

        RowBlockBuilderAppender(BlockBuilderAppender[] fieldAppenders, Optional<Map<String, Integer>> fieldNameToIndex)
        {
            this.fieldAppenders = fieldAppenders;
            this.fieldNameToIndex = fieldNameToIndex;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
                blockBuilder.appendNull();
                return;
            }

            if (parser.getCurrentToken() != START_ARRAY && parser.getCurrentToken() != START_OBJECT) {
                throw new JsonCastException(format("Expected a json array or object, but got %s", parser.getText()));
            }

            ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> parseJsonToSingleRowBlock(parser, fieldBuilders, fieldAppenders, fieldNameToIndex));
        }
    }

    public static Optional<Map<String, Integer>> getFieldNameToIndex(List<Field> rowFields)
    {
        if (rowFields.get(0).getName().isEmpty()) {
            return Optional.empty();
        }

        Map<String, Integer> fieldNameToIndex = new HashMap<>(rowFields.size());
        for (int i = 0; i < rowFields.size(); i++) {
            fieldNameToIndex.put(rowFields.get(i).getName().get(), i);
        }
        return Optional.of(fieldNameToIndex);
    }

    // TODO: Once CAST function supports cachedInstanceFactory or directly write to BlockBuilder,
    // JsonToRowCast::toRow can use RowBlockBuilderAppender::append to parse JSON and append to the block builder.
    // Thus there will be single call to this method, so this method can be inlined.
    private static void parseJsonToSingleRowBlock(
            JsonParser parser,
            List<BlockBuilder> fieldBuilders,
            BlockBuilderAppender[] fieldAppenders,
            Optional<Map<String, Integer>> fieldNameToIndex)
            throws IOException
    {
        if (parser.getCurrentToken() == START_ARRAY) {
            for (int i = 0; i < fieldAppenders.length; i++) {
                parser.nextToken();
                fieldAppenders[i].append(parser, fieldBuilders.get(i));
            }
            if (parser.nextToken() != JsonToken.END_ARRAY) {
                throw new JsonCastException(format("Expected json array ending, but got %s", parser.getText()));
            }
        }
        else {
            verify(parser.getCurrentToken() == START_OBJECT);
            if (fieldNameToIndex.isEmpty()) {
                throw new JsonCastException("Cannot cast a JSON object to anonymous row type. Input must be a JSON array.");
            }
            boolean[] fieldWritten = new boolean[fieldAppenders.length];
            int numFieldsWritten = 0;

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.currentToken() != FIELD_NAME) {
                    throw new JsonCastException(format("Expected a json field name, but got %s", parser.getText()));
                }
                String fieldName = parser.getText().toLowerCase(Locale.ENGLISH);
                Integer fieldIndex = fieldNameToIndex.get().get(fieldName);
                parser.nextToken();
                if (fieldIndex != null) {
                    if (fieldWritten[fieldIndex]) {
                        throw new JsonCastException("Duplicate field: " + fieldName);
                    }
                    fieldWritten[fieldIndex] = true;
                    numFieldsWritten++;
                    fieldAppenders[fieldIndex].append(parser, fieldBuilders.get(fieldIndex));
                }
                else {
                    parser.skipChildren();
                }
            }

            if (numFieldsWritten != fieldAppenders.length) {
                for (int i = 0; i < fieldWritten.length; i++) {
                    if (!fieldWritten[i]) {
                        fieldBuilders.get(i).appendNull();
                    }
                }
            }
        }
    }
}
