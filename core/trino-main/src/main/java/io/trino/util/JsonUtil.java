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
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.json.Json;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonItemEncoding;
import io.trino.json.JsonItems;
import io.trino.json.TypedValue;
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
import io.trino.spi.type.Int128Math;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.type.BigintOperators;
import io.trino.type.BooleanOperators;
import io.trino.type.DecimalCasts;
import io.trino.type.DoubleOperators;
import io.trino.type.JsonType;
import io.trino.type.NumberOperators;
import io.trino.type.RealOperators;
import io.trino.type.UnknownType;
import io.trino.type.VarcharOperators;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonFactory.Feature.INTERN_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonUtils.jsonFactoryBuilder;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
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
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;

public final class JsonUtil
{
    // java.io.Reader outperforms InputStreamReader for small inputs. Limit based on Jackson benchmarks {@link https://github.com/FasterXML/jackson-benchmarks/pull/9}
    private static final int STRING_READER_LENGTH_LIMIT = 8192;

    private JsonUtil() {}

    // This object mapper is constructed without .configure(ORDER_MAP_ENTRIES_BY_KEYS, true) because
    // `OBJECT_MAPPER.writeValueAsString(parser.readValueAsTree());` preserves input order.
    // Be aware. Using it arbitrarily can produce invalid json (ordered by key is required in Trino).

    private static final int MAX_JSON_LENGTH_IN_ERROR_MESSAGE = 10_000;

    // Note: JsonFactory is mutable, instances cannot be shared openly.
    public static JsonFactory createJsonFactory()
    {
        return jsonFactoryBuilder()
                .disable(CANONICALIZE_FIELD_NAMES)
                /*
                 * When multiple threads deserialize JSON responses concurrently,
                 * Jackson's default behavior of interning field names causes severe lock contention
                 * on the JVM's global String pool. This manifests as threads blocked waiting at
                 * {@code InternCache.intern()}.
                 *
                 * Disabling INTERN_FIELD_NAMES eliminates this contention with minimal performance
                 * impact - field name deduplication becomes slightly less memory-efficient, but the
                 * elimination of lock contention far outweighs this cost in high-concurrency scenarios.
                 *
                 * @see <a href="https://github.com/FasterXML/jackson-core/issues/332">Jackson issue on InternCache contention</a>
                 */
                .disable(INTERN_FIELD_NAMES)
                .build();
    }

    public static JsonParser createJsonParser(JsonMapper mapper, Slice json)
            throws IOException
    {
        // Jackson tries to detect the character encoding automatically when using InputStream
        // so we pass java.io.Reader or an InputStreamReader instead.
        // Despite the https://github.com/FasterXML/jackson-core/pull/1081, the below performance optimization
        // is still valid for small inputs.
        if (json.length() < STRING_READER_LENGTH_LIMIT) {
            // java.io.Reader is more performant than InputStreamReader for small inputs
            return mapper.createParser(Reader.of(json.toStringUtf8()));
        }

        return mapper.createParser(new InputStreamReader(json.getInput(), UTF_8));
    }

    public static JsonGenerator createJsonGenerator(JsonMapper mapper, SliceOutput output)
            throws IOException
    {
        return mapper.createGenerator((OutputStream) output);
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
                type instanceof NumberType ||
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
        if (type instanceof RowType rowType) {
            return rowType.getFieldTypes().stream().allMatch(JsonUtil::canCastToJson);
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
                type instanceof NumberType ||
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
        if (type instanceof RowType rowType) {
            return rowType.getFieldTypes().stream().allMatch(JsonUtil::canCastFromJson);
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
                return (_, _) -> null;
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
            if (type instanceof NumberType) {
                return new NumberJsonGeneratorWriter();
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
                List<Type> fieldTypes = rowType.getFieldTypes();
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

    private static class NumberJsonGeneratorWriter
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
                TrinoNumber value = (TrinoNumber) NUMBER.getObject(block, position);
                switch (value.toBigDecimal()) {
                    case TrinoNumber.NotANumber() -> jsonGenerator.writeString("NaN");
                    case TrinoNumber.Infinity(boolean negative) -> jsonGenerator.writeString(negative ? "-Infinity" : "+Infinity");
                    case TrinoNumber.BigDecimalValue(BigDecimal bigDecimal) -> jsonGenerator.writeNumber(bigDecimal);
                }
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
                for (Entry<String, Integer> entry : orderedKeyToValuePosition.entrySet()) {
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

                List<Field> fields = type.getFields();
                jsonGenerator.writeStartObject();
                for (int i = 0; i < sqlRow.getFieldCount(); i++) {
                    jsonGenerator.writeFieldName(fields.get(i).getName().orElse(""));
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
            case VALUE_STRING, FIELD_NAME -> utf8Slice(parser.getText());
            // Avoidance of loss of precision does not seem to be possible here because of Jackson implementation.
            case VALUE_NUMBER_FLOAT -> DoubleOperators.castToVarchar(UNBOUNDED_LENGTH, parser.getDoubleValue());
            // An alternative is calling getLongValue and then BigintOperators.castToVarchar.
            // It doesn't work as well because it can result in overflow and underflow exceptions for large integral numbers.
            case VALUE_NUMBER_INT -> utf8Slice(parser.getText());
            case VALUE_TRUE -> BooleanOperators.castToVarchar(UNBOUNDED_LENGTH, true);
            case VALUE_FALSE -> BooleanOperators.castToVarchar(UNBOUNDED_LENGTH, false);
            default -> throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.VARCHAR, parser.getText()));
        };
    }

    public static Long currentTokenAsBigint(JsonParser parser)
            throws IOException
    {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToBigint(utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT -> DoubleOperators.castToBigint(parser.getDoubleValue());
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
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToInteger(utf8Slice(parser.getText()));
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
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToSmallint(utf8Slice(parser.getText()));
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
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToTinyint(utf8Slice(parser.getText()));
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
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToDouble(utf8Slice(parser.getText()));
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
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToReal(utf8Slice(parser.getText()));
            case VALUE_NUMBER_FLOAT -> (long) floatToRawIntBits(parser.getFloatValue());
            // An alternative is calling getLongValue and then BigintOperators.castToReal.
            // It doesn't work as well because it can result in overflow and underflow exceptions for large integral numbers.
            case VALUE_NUMBER_INT -> (long) floatToRawIntBits(parser.getFloatValue());
            case VALUE_TRUE -> BooleanOperators.castToReal(true);
            case VALUE_FALSE -> BooleanOperators.castToReal(false);
            default -> throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.REAL, parser.getText()));
        };
    }

    @Nullable
    public static TrinoNumber currentTokenAsNumber(JsonParser parser)
            throws IOException
    {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToNumber(utf8Slice(parser.getText()));
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> TrinoNumber.from(parser.getDecimalValue());
            case VALUE_TRUE -> TrinoNumber.from(BigDecimal.ONE);
            case VALUE_FALSE -> TrinoNumber.from(BigDecimal.ZERO);
            default -> throw new JsonCastException(format("Unexpected token when cast to %s: %s", StandardTypes.NUMBER, parser.getText()));
        };
    }

    public static Boolean currentTokenAsBoolean(JsonParser parser)
            throws IOException
    {
        return switch (parser.currentToken()) {
            case VALUE_NULL -> null;
            case VALUE_STRING, FIELD_NAME -> VarcharOperators.castToBoolean(utf8Slice(parser.getText()));
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
            case VALUE_NULL -> {
                return null;
            }
            case VALUE_STRING, FIELD_NAME -> {
                result = new BigDecimal(parser.getText());
                result = result.setScale(scale, HALF_UP);
            }
            case VALUE_NUMBER_FLOAT, VALUE_NUMBER_INT -> {
                result = parser.getDecimalValue();
                result = result.setScale(scale, HALF_UP);
            }
            case VALUE_TRUE -> result = BigDecimal.ONE.setScale(scale, HALF_UP);
            case VALUE_FALSE -> result = BigDecimal.ZERO.setScale(scale, HALF_UP);
            default -> throw new JsonCastException(format("Unexpected token when cast to DECIMAL(%s,%s): %s", precision, scale, parser.getText()));
        }

        if (result.precision() > precision) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Cannot cast input json to DECIMAL(%s,%s)", precision, scale));
        }
        return result;
    }

    // ------------------------------------------------------------------------------------
    // Converting an SQL/JSON item to an SQL value.
    //
    // SQL:2023 9.48 GR 4(b)(iii): an SQL/JSON item has a data type IDT, and the value of the
    // conversion is CAST(X AS DT) where X is a variable of type IDT holding the item. So each
    // of these delegates to the same cast the engine would run on the item's own SQL type —
    // the JSON text form never enters into it.
    // ------------------------------------------------------------------------------------

    /// The scalar an item carries, or `null` for the SQL/JSON null. Arrays and objects have no
    /// scalar value, so converting one to a scalar SQL type is an error.
    private static TypedValue scalarOf(Json item, String targetType)
            throws JsonCastException
    {
        if (item.isNull()) {
            return null;
        }
        if (!item.isScalar()) {
            throw new JsonCastException(format("Unexpected %s when cast to %s", item.kind(), targetType));
        }
        return item.materializeScalar();
    }

    private static JsonCastException cannotCast(TypedValue value, String targetType)
    {
        return new JsonCastException(format("Unexpected %s when cast to %s", value.type().getDisplayName(), targetType));
    }

    public static Slice itemAsVarchar(Json item)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, StandardTypes.VARCHAR);
        if (value == null) {
            return null;
        }
        return switch (value.type()) {
            case BooleanType _ -> BooleanOperators.castToVarchar(UNBOUNDED_LENGTH, value.getBooleanValue());
            case VarcharType _ -> (Slice) value.getObjectValue();
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> utf8Slice(Long.toString(value.getLongValue()));
            case DoubleType _ -> DoubleOperators.castToVarchar(UNBOUNDED_LENGTH, value.getDoubleValue());
            case RealType _ -> RealOperators.castToVarchar(UNBOUNDED_LENGTH, value.getLongValue());
            case DecimalType decimalType -> decimalType.isShort()
                    ? DecimalCasts.shortDecimalToVarchar(value.getLongValue(), decimalType.getScale(), UNBOUNDED_LENGTH)
                    : DecimalCasts.longDecimalToVarchar((Int128) value.getObjectValue(), decimalType.getScale(), UNBOUNDED_LENGTH);
            case NumberType _ -> NumberOperators.castToVarchar(UNBOUNDED_LENGTH, (TrinoNumber) value.getObjectValue());
            case DateType _, TimeType _, TimeWithTimeZoneType _, TimestampType _, TimestampWithTimeZoneType _ -> utf8Slice(JsonItemEncoding.datetimeText(value));
            default -> throw cannotCast(value, StandardTypes.VARCHAR);
        };
    }

    public static Boolean itemAsBoolean(Json item)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, StandardTypes.BOOLEAN);
        if (value == null) {
            return null;
        }
        return switch (value.type()) {
            case BooleanType _ -> value.getBooleanValue();
            case VarcharType _ -> VarcharOperators.castToBoolean((Slice) value.getObjectValue());
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> BigintOperators.castToBoolean(value.getLongValue());
            case DoubleType _ -> DoubleOperators.castToBoolean(value.getDoubleValue());
            case RealType _ -> RealOperators.castToBoolean(value.getLongValue());
            case DecimalType decimalType -> decimalType.isShort()
                    ? DecimalCasts.shortDecimalToBoolean(value.getLongValue(), decimalType.getPrecision(), decimalType.getScale(), 0)
                    : DecimalCasts.longDecimalToBoolean((Int128) value.getObjectValue(), decimalType.getPrecision(), decimalType.getScale(), null);
            // 9.48 GR 4(b)(iii)(1): no cast exists from NUMBER to BOOLEAN, so the item cannot
            // be converted to the target type.
            default -> throw cannotCast(value, StandardTypes.BOOLEAN);
        };
    }

    public static Long itemAsBigint(Json item)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, StandardTypes.BIGINT);
        if (value == null) {
            return null;
        }
        return switch (value.type()) {
            case BooleanType _ -> BooleanOperators.castToBigint(value.getBooleanValue());
            case VarcharType _ -> VarcharOperators.castToBigint((Slice) value.getObjectValue());
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> value.getLongValue();
            case DoubleType _ -> DoubleOperators.castToBigint(value.getDoubleValue());
            case RealType _ -> RealOperators.castToBigint(value.getLongValue());
            case DecimalType decimalType -> decimalToBigint(value, decimalType);
            case NumberType _ -> NumberOperators.castToBigint((TrinoNumber) value.getObjectValue());
            default -> throw cannotCast(value, StandardTypes.BIGINT);
        };
    }

    public static Long itemAsInteger(Json item)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, StandardTypes.INTEGER);
        if (value == null) {
            return null;
        }
        return switch (value.type()) {
            case BooleanType _ -> BooleanOperators.castToInteger(value.getBooleanValue());
            case VarcharType _ -> VarcharOperators.castToInteger((Slice) value.getObjectValue());
            case IntegerType _, SmallintType _, TinyintType _ -> value.getLongValue();
            case BigintType _ -> BigintOperators.castToInteger(value.getLongValue());
            case DoubleType _ -> DoubleOperators.castToInteger(value.getDoubleValue());
            case RealType _ -> RealOperators.castToInteger(value.getLongValue());
            case DecimalType decimalType -> BigintOperators.castToInteger(decimalToBigint(value, decimalType));
            case NumberType _ -> NumberOperators.castToInteger((TrinoNumber) value.getObjectValue());
            default -> throw cannotCast(value, StandardTypes.INTEGER);
        };
    }

    public static Long itemAsSmallint(Json item)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, StandardTypes.SMALLINT);
        if (value == null) {
            return null;
        }
        return switch (value.type()) {
            case BooleanType _ -> BooleanOperators.castToSmallint(value.getBooleanValue());
            case VarcharType _ -> VarcharOperators.castToSmallint((Slice) value.getObjectValue());
            case SmallintType _, TinyintType _ -> value.getLongValue();
            case BigintType _, IntegerType _ -> BigintOperators.castToSmallint(value.getLongValue());
            case DoubleType _ -> DoubleOperators.castToSmallint(value.getDoubleValue());
            case RealType _ -> RealOperators.castToSmallint(value.getLongValue());
            case DecimalType decimalType -> BigintOperators.castToSmallint(decimalToBigint(value, decimalType));
            case NumberType _ -> NumberOperators.castToSmallint((TrinoNumber) value.getObjectValue());
            default -> throw cannotCast(value, StandardTypes.SMALLINT);
        };
    }

    public static Long itemAsTinyint(Json item)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, StandardTypes.TINYINT);
        if (value == null) {
            return null;
        }
        return switch (value.type()) {
            case BooleanType _ -> BooleanOperators.castToTinyint(value.getBooleanValue());
            case VarcharType _ -> VarcharOperators.castToTinyint((Slice) value.getObjectValue());
            case TinyintType _ -> value.getLongValue();
            case BigintType _, IntegerType _, SmallintType _ -> BigintOperators.castToTinyint(value.getLongValue());
            case DoubleType _ -> DoubleOperators.castToTinyint(value.getDoubleValue());
            case RealType _ -> RealOperators.castToTinyint(value.getLongValue());
            case DecimalType decimalType -> BigintOperators.castToTinyint(decimalToBigint(value, decimalType));
            case NumberType _ -> NumberOperators.castToTinyint((TrinoNumber) value.getObjectValue());
            default -> throw cannotCast(value, StandardTypes.TINYINT);
        };
    }

    public static Double itemAsDouble(Json item)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, StandardTypes.DOUBLE);
        if (value == null) {
            return null;
        }
        return switch (value.type()) {
            case BooleanType _ -> BooleanOperators.castToDouble(value.getBooleanValue());
            case VarcharType _ -> VarcharOperators.castToDouble((Slice) value.getObjectValue());
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> (double) value.getLongValue();
            case DoubleType _ -> value.getDoubleValue();
            case RealType _ -> RealOperators.castToDouble(value.getLongValue());
            case DecimalType decimalType -> decimalToDouble(value, decimalType);
            case NumberType _ -> NumberOperators.castToDouble((TrinoNumber) value.getObjectValue());
            default -> throw cannotCast(value, StandardTypes.DOUBLE);
        };
    }

    public static Long itemAsReal(Json item)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, StandardTypes.REAL);
        if (value == null) {
            return null;
        }
        // Each source casts straight to REAL. Going through DOUBLE first would round twice,
        // and the two roundings do not agree: BIGINT 4611686293305294849 lands on 0x5e800001
        // directly, but on 0x5e800000 via DOUBLE.
        return switch (value.type()) {
            case BooleanType _ -> BooleanOperators.castToReal(value.getBooleanValue());
            case VarcharType _ -> VarcharOperators.castToReal((Slice) value.getObjectValue());
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> BigintOperators.castToReal(value.getLongValue());
            case DoubleType _ -> DoubleOperators.castToReal(value.getDoubleValue());
            case RealType _ -> value.getLongValue();
            case DecimalType decimalType -> decimalType.isShort()
                    ? DecimalCasts.shortDecimalToReal(value.getLongValue(), decimalType.getPrecision(), decimalType.getScale(), longTenToNth(decimalType.getScale()))
                    : DecimalCasts.longDecimalToReal((Int128) value.getObjectValue(), decimalType.getPrecision(), decimalType.getScale(), Int128Math.powerOfTen(decimalType.getScale()));
            case NumberType _ -> NumberOperators.castToReal((TrinoNumber) value.getObjectValue());
            default -> throw cannotCast(value, StandardTypes.REAL);
        };
    }

    public static TrinoNumber itemAsNumber(Json item)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, StandardTypes.NUMBER);
        if (value == null) {
            return null;
        }
        return switch (value.type()) {
            case BooleanType _ -> TrinoNumber.from(value.getBooleanValue() ? BigDecimal.ONE : BigDecimal.ZERO);
            case VarcharType _ -> VarcharOperators.castToNumber((Slice) value.getObjectValue());
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> TrinoNumber.from(BigDecimal.valueOf(value.getLongValue()));
            case DoubleType _ -> DoubleOperators.castToNumber(value.getDoubleValue());
            case RealType _ -> RealOperators.castToNumber(value.getLongValue());
            case DecimalType decimalType -> TrinoNumber.from(javaDecimalOf(value, decimalType));
            case NumberType _ -> (TrinoNumber) value.getObjectValue();
            default -> throw cannotCast(value, StandardTypes.NUMBER);
        };
    }

    /// The item as a `DECIMAL(precision, scale)`, rounding half-up to the target scale the way
    /// the SQL cast does, and failing when the value does not fit the target precision.
    private static BigDecimal itemAsJavaDecimal(Json item, int precision, int scale)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, "DECIMAL");
        if (value == null) {
            return null;
        }
        BigDecimal result = switch (value.type()) {
            case BooleanType _ -> value.getBooleanValue() ? BigDecimal.ONE : BigDecimal.ZERO;
            case VarcharType _ -> new BigDecimal(((Slice) value.getObjectValue()).toStringUtf8());
            case BigintType _, IntegerType _, SmallintType _, TinyintType _ -> BigDecimal.valueOf(value.getLongValue());
            case DoubleType _ -> BigDecimal.valueOf(value.getDoubleValue());
            case RealType _ -> BigDecimal.valueOf(intBitsToFloat(toIntExact(value.getLongValue())));
            case DecimalType decimalType -> javaDecimalOf(value, decimalType);
            case NumberType _ -> numberAsJavaDecimal((TrinoNumber) value.getObjectValue());
            default -> throw cannotCast(value, format("DECIMAL(%s,%s)", precision, scale));
        };
        result = result.setScale(scale, HALF_UP);
        if (result.precision() > precision) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Cannot cast input json to DECIMAL(%s,%s)", precision, scale));
        }
        return result;
    }

    public static Long itemAsShortDecimal(Json item, int precision, int scale)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, "DECIMAL");
        if (value == null) {
            return null;
        }
        // The inexact sources cast straight to DECIMAL rather than through a BigDecimal built
        // here, so the rounding and the out-of-range errors are the cast's own.
        return switch (value.type()) {
            case DoubleType _ -> DecimalCasts.doubleToShortDecimal(value.getDoubleValue(), precision, scale, longTenToNth(scale));
            case RealType _ -> DecimalCasts.realToShortDecimal(value.getLongValue(), precision, scale, longTenToNth(scale));
            default -> itemAsJavaDecimal(item, precision, scale).unscaledValue().longValue();
        };
    }

    public static Int128 itemAsLongDecimal(Json item, int precision, int scale)
            throws JsonCastException
    {
        TypedValue value = scalarOf(item, "DECIMAL");
        if (value == null) {
            return null;
        }
        return switch (value.type()) {
            case DoubleType _ -> DecimalCasts.doubleToLongDecimal(value.getDoubleValue(), precision, scale, Int128Math.powerOfTen(scale));
            case RealType _ -> DecimalCasts.realToLongDecimal(value.getLongValue(), precision, scale, Int128Math.powerOfTen(scale));
            default -> Int128.valueOf(itemAsJavaDecimal(item, precision, scale).unscaledValue());
        };
    }

    private static BigDecimal numberAsJavaDecimal(TrinoNumber number)
            throws JsonCastException
    {
        return switch (number.toBigDecimal()) {
            case TrinoNumber.BigDecimalValue(BigDecimal decimal) -> decimal;
            case TrinoNumber.NotANumber _ -> throw new JsonCastException("Cannot cast NaN to DECIMAL");
            case TrinoNumber.Infinity _ -> throw new JsonCastException("Cannot cast Infinity to DECIMAL");
        };
    }

    private static BigDecimal javaDecimalOf(TypedValue value, DecimalType type)
    {
        BigInteger unscaled = type.isShort()
                ? BigInteger.valueOf(value.getLongValue())
                : ((Int128) value.getObjectValue()).toBigInteger();
        return new BigDecimal(unscaled, type.getScale());
    }

    private static long decimalToBigint(TypedValue value, DecimalType type)
    {
        int precision = type.getPrecision();
        int scale = type.getScale();
        if (type.isShort()) {
            return DecimalCasts.shortDecimalToBigint(value.getLongValue(), precision, scale, longTenToNth(scale));
        }
        return DecimalCasts.longDecimalToBigint((Int128) value.getObjectValue(), precision, scale, Int128Math.powerOfTen(scale));
    }

    private static double decimalToDouble(TypedValue value, DecimalType type)
    {
        int precision = type.getPrecision();
        int scale = type.getScale();
        if (type.isShort()) {
            return DecimalCasts.shortDecimalToDouble(value.getLongValue(), precision, scale, longTenToNth(scale));
        }
        return DecimalCasts.longDecimalToDouble((Int128) value.getObjectValue(), precision, scale, Int128Math.powerOfTen(scale));
    }

    // given a JSON parser, write to the BlockBuilder
    public interface BlockBuilderAppender
    {
        void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException;

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
            if (type instanceof NumberType) {
                return new NumberBlockBuilderAppender();
            }
            if (type instanceof VarcharType) {
                return new VarcharBlockBuilderAppender(type);
            }
            if (type instanceof JsonType) {
                return (value, blockBuilder) -> JSON.writeObject(blockBuilder, value);
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

    /// Casts JSON to a value by walking the Jackson token stream in place, without first
    /// materializing the value-model tree. This restores the streaming shape the cast had before
    /// the value model, so a raw-text JSON value (the common connector case) is not paid for with a
    /// throwaway tree. Correctness stays anchored to [BlockBuilderAppender]: every scalar is either
    /// converted by a token fast path that must be proven identical to the value-model conversion,
    /// or materialized as a single item via [JsonItems#parseItem] and handed to the matching
    /// value-model appender — so no cast semantics diverge.
    public interface StreamingBlockBuilderAppender
    {
        void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException;

        /// Builds a streaming appender for `type`, or returns `null` when the type contains a shape
        /// this path does not stream yet (row types), so the caller falls back to the value model.
        static StreamingBlockBuilderAppender create(Type type)
        {
            if (type instanceof DoubleType) {
                return new DoubleStreamingAppender();
            }
            if (type instanceof BigintType) {
                return new BigintStreamingAppender();
            }
            if (type instanceof IntegerType) {
                return new IntegerStreamingAppender();
            }
            if (type instanceof VarcharType) {
                return new VarcharStreamingAppender(type);
            }
            if (type instanceof ArrayType arrayType) {
                StreamingBlockBuilderAppender elementAppender = create(arrayType.getElementType());
                return elementAppender == null ? null : new ArrayStreamingAppender(elementAppender);
            }
            if (type instanceof MapType mapType) {
                StreamingBlockBuilderAppender valueAppender = create(mapType.getValueType());
                if (valueAppender == null) {
                    return null;
                }
                return new MapStreamingAppender(mapType.getKeyType(), BlockBuilderAppender.createBlockBuilderAppender(mapType.getKeyType()), valueAppender);
            }
            if (type instanceof RowType) {
                return null;
            }
            return new ScalarStreamingAppender(BlockBuilderAppender.createBlockBuilderAppender(type));
        }
    }

    /// Streams any scalar target by materializing the current item and reusing the value-model
    /// conversion, so the result is identical to the tree path but the container above it never
    /// allocated a tree. A non-scalar token here is a type error the value-model appender reports.
    private static class ScalarStreamingAppender
            implements StreamingBlockBuilderAppender
    {
        private final BlockBuilderAppender valueAppender;

        ScalarStreamingAppender(BlockBuilderAppender valueAppender)
        {
            this.valueAppender = valueAppender;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            valueAppender.append(JsonItems.parseItem(parser), blockBuilder);
        }
    }

    /// [Double] fast path: [JsonParser#getDoubleValue] on a numeric token yields the exact same
    /// double as [#itemAsDouble] — both round the token's exact decimal value to the nearest double
    /// once — so the DECIMAL/NUMBER materialization is skipped. Non-numeric tokens (null, boolean,
    /// string, or a mistyped container) defer to the value-model conversion.
    private static class DoubleStreamingAppender
            implements StreamingBlockBuilderAppender
    {
        private final BlockBuilderAppender valueAppender = new DoubleBlockBuilderAppender();

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            JsonToken token = parser.currentToken();
            if (token == JsonToken.VALUE_NUMBER_FLOAT || token == JsonToken.VALUE_NUMBER_INT) {
                double value = parser.getDoubleValue();
                // The value model encodes a JSON float as a decimal, which cannot carry a negative
                // zero, so every zero casts to +0.0. Normalize here so a raw-text value casts to the
                // same double as its typed-encoded form.
                DOUBLE.writeDouble(blockBuilder, value == 0 ? 0.0 : value);
            }
            else {
                valueAppender.append(JsonItems.parseItem(parser), blockBuilder);
            }
        }
    }

    /// [Bigint] fast path: an integer token that Jackson resolves to `int`/`long` casts to BIGINT
    /// as exactly its `long` value — the same as [#itemAsBigint] on the typed item — so the
    /// [TypedValue] is skipped. Wider integers, floats and other tokens defer to the value model.
    private static class BigintStreamingAppender
            implements StreamingBlockBuilderAppender
    {
        private final BlockBuilderAppender valueAppender = new BigintBlockBuilderAppender();

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            if (parser.currentToken() == JsonToken.VALUE_NUMBER_INT) {
                JsonParser.NumberType numberType = parser.getNumberType();
                if (numberType == JsonParser.NumberType.INT || numberType == JsonParser.NumberType.LONG) {
                    BIGINT.writeLong(blockBuilder, parser.getLongValue());
                    return;
                }
                // A wider integer falls through to the value model, which raises the same
                // out-of-range error a typed-encoded value would.
            }
            valueAppender.append(JsonItems.parseItem(parser), blockBuilder);
        }
    }

    /// [Integer] fast path: an integer token Jackson resolves to `int` is by definition within
    /// INTEGER range and casts to that value — matching [#itemAsInteger]. Everything else (wider
    /// integers that need the range check, floats, other tokens) defers to the value model.
    private static class IntegerStreamingAppender
            implements StreamingBlockBuilderAppender
    {
        private final BlockBuilderAppender valueAppender = new IntegerBlockBuilderAppender();

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            if (parser.currentToken() == JsonToken.VALUE_NUMBER_INT && parser.getNumberType() == JsonParser.NumberType.INT) {
                INTEGER.writeLong(blockBuilder, parser.getLongValue());
                return;
            }
            valueAppender.append(JsonItems.parseItem(parser), blockBuilder);
        }
    }

    /// [Varchar] fast path: a JSON string is the varchar value verbatim, and an integer token that
    /// fits `long` renders as its decimal string — both matching [#itemAsVarchar]. Floats (whose
    /// canonical rendering differs from the raw token) and other tokens defer to the value model.
    private static class VarcharStreamingAppender
            implements StreamingBlockBuilderAppender
    {
        private final Type type;
        private final BlockBuilderAppender valueAppender;

        VarcharStreamingAppender(Type type)
        {
            this.type = type;
            this.valueAppender = new VarcharBlockBuilderAppender(type);
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            JsonToken token = parser.currentToken();
            if (token == JsonToken.VALUE_STRING) {
                type.writeSlice(blockBuilder, utf8Slice(parser.getText()));
                return;
            }
            if (token == JsonToken.VALUE_NUMBER_INT) {
                JsonParser.NumberType numberType = parser.getNumberType();
                if (numberType == JsonParser.NumberType.INT || numberType == JsonParser.NumberType.LONG) {
                    type.writeSlice(blockBuilder, utf8Slice(Long.toString(parser.getLongValue())));
                    return;
                }
            }
            valueAppender.append(JsonItems.parseItem(parser), blockBuilder);
        }
    }

    private static class ArrayStreamingAppender
            implements StreamingBlockBuilderAppender
    {
        private final StreamingBlockBuilderAppender elementAppender;

        ArrayStreamingAppender(StreamingBlockBuilderAppender elementAppender)
        {
            this.elementAppender = elementAppender;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            JsonToken token = parser.currentToken();
            if (token == JsonToken.VALUE_NULL) {
                blockBuilder.appendNull();
                return;
            }
            if (token != JsonToken.START_ARRAY) {
                throw new JsonCastException(format("Expected a json array, but got %s", token));
            }
            ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
                for (JsonToken element = parser.nextToken(); element != JsonToken.END_ARRAY; element = parser.nextToken()) {
                    elementAppender.append(parser, elementBuilder);
                }
            });
        }
    }

    private static class MapStreamingAppender
            implements StreamingBlockBuilderAppender
    {
        private final Type keyType;
        private final boolean varcharKey;
        private final BlockBuilderAppender keyAppender;
        private final StreamingBlockBuilderAppender valueAppender;

        MapStreamingAppender(Type keyType, BlockBuilderAppender keyAppender, StreamingBlockBuilderAppender valueAppender)
        {
            this.keyType = keyType;
            this.varcharKey = keyType instanceof VarcharType;
            this.keyAppender = keyAppender;
            this.valueAppender = valueAppender;
        }

        @Override
        public void append(JsonParser parser, BlockBuilder blockBuilder)
                throws IOException
        {
            JsonToken token = parser.currentToken();
            if (token == JsonToken.VALUE_NULL) {
                blockBuilder.appendNull();
                return;
            }
            if (token != JsonToken.START_OBJECT) {
                throw new JsonCastException(format("Expected a json object, but got %s", token));
            }
            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) blockBuilder;
            mapBlockBuilder.strict();
            try {
                mapBlockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
                    for (JsonToken field = parser.nextToken(); field != JsonToken.END_OBJECT; field = parser.nextToken()) {
                        // An object key is a JSON string, and that is the item the key type
                        // converts from — the same rule any other scalar goes through. A varchar
                        // key is that string verbatim, so it writes straight through; any other key
                        // type is cast from the string, carried as a scalar item with no byte encoding.
                        Slice keyName = utf8Slice(parser.currentName());
                        if (varcharKey) {
                            keyType.writeSlice(keyBuilder, keyName);
                        }
                        else {
                            keyAppender.append(new TypedValue(VarcharType.VARCHAR, keyName), keyBuilder);
                        }
                        parser.nextToken();
                        valueAppender.append(parser, valueBuilder);
                    }
                });
            }
            catch (DuplicateMapKeyException e) {
                throw new JsonCastException("Duplicate keys are not allowed");
            }
        }
    }

    private static class BooleanBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            Boolean result = itemAsBoolean(value);
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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            Long result = itemAsTinyint(value);
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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            Long result = itemAsInteger(value);
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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            Long result = itemAsInteger(value);
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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            Long result = itemAsBigint(value);
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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            Long result = itemAsReal(value);
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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            Double result = itemAsDouble(value);
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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            Long result = itemAsShortDecimal(value, type.getPrecision(), type.getScale());

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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            Int128 result = itemAsLongDecimal(value, type.getPrecision(), type.getScale());

            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeObject(blockBuilder, result);
            }
        }
    }

    private static class NumberBlockBuilderAppender
            implements BlockBuilderAppender
    {
        @Override
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            TrinoNumber result = itemAsNumber(value);

            if (result == null) {
                blockBuilder.appendNull();
            }
            else {
                NUMBER.writeObject(blockBuilder, result);
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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            Slice result = itemAsVarchar(value);
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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            if (value.isNull()) {
                blockBuilder.appendNull();
                return;
            }
            if (!value.isArray()) {
                throw new JsonCastException(format("Expected a json array, but got %s", describe(value)));
            }
            ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
                for (Json element : elements(value)) {
                    elementAppender.append(element, elementBuilder);
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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            if (value.isNull()) {
                blockBuilder.appendNull();
                return;
            }
            if (!value.isObject()) {
                throw new JsonCastException(format("Expected a json object, but got %s", describe(value)));
            }

            MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) blockBuilder;
            mapBlockBuilder.strict();
            try {
                mapBlockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
                    for (Map.Entry<String, Json> member : members(value)) {
                        // An object key is a JSON string, and that is the item the key type
                        // converts from — the same rule any other scalar goes through.
                        keyAppender.append(JsonItemBuilder.encodeVarchar(utf8Slice(member.getKey())), keyBuilder);
                        valueAppender.append(member.getValue(), valueBuilder);
                    }
                });
            }
            catch (DuplicateMapKeyException e) {
                throw new JsonCastException("Duplicate keys are not allowed");
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
        public void append(Json value, BlockBuilder blockBuilder)
                throws JsonCastException
        {
            if (value.isNull()) {
                blockBuilder.appendNull();
                return;
            }
            if (!value.isArray() && !value.isObject()) {
                throw new JsonCastException(format("Expected a json array or object, but got %s", describe(value)));
            }
            ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> appendRow(value, fieldBuilders, fieldAppenders, fieldNameToIndex));
        }
    }

    private static void appendRow(
            Json value,
            List<BlockBuilder> fieldBuilders,
            BlockBuilderAppender[] fieldAppenders,
            Optional<Map<String, Integer>> fieldNameToIndex)
            throws JsonCastException
    {
        if (value.isArray()) {
            List<Json> elements = elements(value);
            if (elements.size() != fieldAppenders.length) {
                throw new JsonCastException(format("Expected a json array with %s elements, but got %s", fieldAppenders.length, elements.size()));
            }
            for (int i = 0; i < fieldAppenders.length; i++) {
                fieldAppenders[i].append(elements.get(i), fieldBuilders.get(i));
            }
            return;
        }

        if (fieldNameToIndex.isEmpty()) {
            throw new JsonCastException("Cannot cast a JSON object to anonymous row type. Input must be a JSON array.");
        }
        boolean[] fieldWritten = new boolean[fieldAppenders.length];
        for (Map.Entry<String, Json> member : members(value)) {
            Integer fieldIndex = fieldNameToIndex.get().get(member.getKey().toLowerCase(Locale.ENGLISH));
            if (fieldIndex == null) {
                continue;
            }
            if (fieldWritten[fieldIndex]) {
                throw new JsonCastException("Duplicate field: " + member.getKey().toLowerCase(Locale.ENGLISH));
            }
            fieldWritten[fieldIndex] = true;
            fieldAppenders[fieldIndex].append(member.getValue(), fieldBuilders.get(fieldIndex));
        }
        for (int i = 0; i < fieldWritten.length; i++) {
            if (!fieldWritten[i]) {
                fieldBuilders.get(i).appendNull();
            }
        }
    }

    private static List<Json> elements(Json array)
    {
        ImmutableList.Builder<Json> elements = ImmutableList.builder();
        array.forEachArrayElement(elements::add);
        return elements.build();
    }

    private static List<Map.Entry<String, Json>> members(Json object)
    {
        ImmutableList.Builder<Map.Entry<String, Json>> members = ImmutableList.builder();
        object.forEachObjectMember((key, member) -> members.add(new SimpleEntry<>(key, member)));
        return members.build();
    }

    private static String describe(Json value)
    {
        return value.isScalar() ? value.scalarType().toString() : value.kind().toString();
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
}
