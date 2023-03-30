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
package io.trino.hive.formats.line.openxjson;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hive.formats.DistinctMapKeys;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.plugin.base.type.TrinoTimestampEncoder;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SingleRowBlockWriter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.HiveFormatUtils.parseHiveDate;
import static io.trino.hive.formats.HiveFormatUtils.parseHiveTimestamp;
import static io.trino.hive.formats.HiveFormatUtils.scaleDecimal;
import static io.trino.hive.formats.line.openxjson.JsonWriter.canonicalizeJsonString;
import static io.trino.hive.formats.line.openxjson.JsonWriter.writeJsonArray;
import static io.trino.hive.formats.line.openxjson.JsonWriter.writeJsonObject;
import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.createTimestampEncoder;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Character.toLowerCase;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.StrictMath.floorDiv;
import static java.lang.StrictMath.floorMod;
import static java.lang.StrictMath.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.format.ResolverStyle.LENIENT;
import static java.util.HexFormat.isHexDigit;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public final class OpenXJsonDeserializer
        implements LineDeserializer
{
    private final List<Type> types;
    private final OpenXJsonOptions options;
    private final RowDecoder rowDecoder;

    public OpenXJsonDeserializer(List<Column> columns, OpenXJsonOptions options)
    {
        this.options = requireNonNull(options, "options is null");

        this.types = columns.stream()
                .map(Column::type)
                .collect(toImmutableList());

        List<DateTimeFormatter> timestampFormatters = options.getTimestampFormats().stream()
                .map(DateTimeFormatter::ofPattern)
                .map(formatter -> formatter.withZone(ZoneOffset.UTC))
                .collect(toImmutableList());

        rowDecoder = new RowDecoder(
                RowType.from(columns.stream()
                        .map(column -> field(column.name().toLowerCase(Locale.ROOT), column.type()))
                        .collect(toImmutableList())),
                options,
                columns.stream()
                        .map(Column::type)
                        .map(fieldType -> createDecoder(fieldType, options, timestampFormatters))
                        .collect(toImmutableList()));
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void deserialize(LineBuffer lineBuffer, PageBuilder builder)
            throws IOException
    {
        String line = new String(lineBuffer.getBuffer(), 0, lineBuffer.getLength(), UTF_8).trim();

        // only object and array lines are supported; everything else results in a null row
        Object jsonNode = null;
        if (line.startsWith("[") || line.startsWith("{")) {
            try {
                // fields names from JSON are not mapped
                jsonNode = JsonReader.readJson(line, originalValue -> new FieldName(originalValue, options.isCaseInsensitive(), options.isDotsInFieldNames(), ImmutableMap.of()));
            }
            catch (InvalidJsonException e) {
                if (!options.isIgnoreMalformedJson()) {
                    throw e;
                }
            }
        }

        if (jsonNode == null) {
            // if json did not parse all columns are null
            builder.declarePosition();
            for (int columnIndex = 0; columnIndex < types.size(); columnIndex++) {
                builder.getBlockBuilder(columnIndex).appendNull();
            }
        }
        else {
            rowDecoder.decode(jsonNode, builder);
        }
    }

    private static Decoder createDecoder(Type type, OpenXJsonOptions options, List<DateTimeFormatter> timestampFormatters)
    {
        if (BOOLEAN.equals(type)) {
            return new BooleanDecoder();
        }
        if (BIGINT.equals(type)) {
            return new BigintDecoder();
        }
        if (INTEGER.equals(type)) {
            return new IntegerDecoder();
        }
        if (SMALLINT.equals(type)) {
            return new SmallintDecoder();
        }
        if (TINYINT.equals(type)) {
            return new TinyintDecoder();
        }
        if (type instanceof DecimalType decimalType) {
            return new DecimalDecoder(decimalType);
        }
        if (REAL.equals(type)) {
            return new RealDecoder();
        }
        if (DOUBLE.equals(type)) {
            return new DoubleDecoder();
        }
        if (DATE.equals(type)) {
            return new DateDecoder();
        }
        if (type instanceof TimestampType timestampType) {
            return new TimestampDecoder(timestampType, timestampFormatters);
        }
        if (VARBINARY.equals(type)) {
            return new VarbinaryDecoder();
        }
        if (type instanceof VarcharType varcharType) {
            return new VarcharDecoder(varcharType);
        }
        if (type instanceof CharType charType) {
            return new CharDecoder(charType);
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayDecoder(createDecoder(arrayType.getElementType(), options, timestampFormatters));
        }
        if (type instanceof MapType mapType) {
            return new MapDecoder(
                    mapType,
                    createDecoder(mapType.getKeyType(), options, timestampFormatters),
                    createDecoder(mapType.getValueType(), options, timestampFormatters));
        }
        if (type instanceof RowType rowType) {
            return new RowDecoder(
                    rowType,
                    options,
                    rowType.getFields().stream()
                            .map(Field::getType)
                            .map(fieldType -> createDecoder(fieldType, options, timestampFormatters))
                            .collect(toImmutableList()));
        }
        throw new UnsupportedOperationException("Unsupported column type: " + type);
    }

    private abstract static class Decoder
    {
        public final void decode(Object jsonValue, BlockBuilder builder)
        {
            if (jsonValue == null) {
                builder.appendNull();
                return;
            }
            decodeValue(jsonValue, builder);
        }

        /**
         * Implementations only receive a Boolean, String, Integer, Long, Double, BigDecimal, Map, or List, and never null.
         */
        abstract void decodeValue(Object jsonValue, BlockBuilder builder);
    }

    private static class BooleanDecoder
            extends Decoder
    {
        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            BOOLEAN.writeBoolean(builder, parseBoolean(jsonValue));
        }

        private static boolean parseBoolean(Object jsonValue)
        {
            return Boolean.parseBoolean(jsonValue.toString());
        }
    }

    private static class BigintDecoder
            extends Decoder
    {
        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            if (!(jsonValue instanceof JsonString jsonString)) {
                throw invalidJson("%s can not be coerced to a BIGINT".formatted(jsonValue.getClass().getSimpleName()));
            }

            try {
                BIGINT.writeLong(builder, parseLong(jsonString.value()));
                return;
            }
            catch (NumberFormatException | ArithmeticException ignored) {
            }
            builder.appendNull();
        }
    }

    private static class IntegerDecoder
            extends Decoder
    {
        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            if (!(jsonValue instanceof JsonString jsonString)) {
                throw invalidJson("%s can not be coerced to an INTEGER".formatted(jsonValue.getClass().getSimpleName()));
            }

            try {
                long longValue = parseLong(jsonString.value());
                if ((int) longValue == longValue) {
                    INTEGER.writeLong(builder, longValue);
                    return;
                }
            }
            catch (NumberFormatException | ArithmeticException ignored) {
            }
            builder.appendNull();
        }
    }

    private static class SmallintDecoder
            extends Decoder
    {
        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            if (!(jsonValue instanceof JsonString jsonString)) {
                throw invalidJson("%s can not be coerced to a SMALLINT".formatted(jsonValue.getClass().getSimpleName()));
            }

            try {
                long longValue = parseLong(jsonString.value());
                if ((short) longValue == longValue) {
                    SMALLINT.writeLong(builder, longValue);
                    return;
                }
            }
            catch (NumberFormatException | ArithmeticException ignored) {
            }
            builder.appendNull();
        }
    }

    private static class TinyintDecoder
            extends Decoder
    {
        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            if (!(jsonValue instanceof JsonString jsonString)) {
                throw invalidJson("%s can not be coerced to a TINYINT".formatted(jsonValue.getClass().getSimpleName()));
            }

            try {
                long longValue = parseLong(jsonString.value());
                if ((byte) longValue == longValue) {
                    TINYINT.writeLong(builder, longValue);
                    return;
                }
            }
            catch (NumberFormatException | ArithmeticException ignored) {
            }
            builder.appendNull();
        }
    }

    private static class DecimalDecoder
            extends Decoder
    {
        private final DecimalType decimalType;

        public DecimalDecoder(DecimalType decimalType)
        {
            this.decimalType = decimalType;
        }

        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            if (!(jsonValue instanceof JsonString jsonString)) {
                throw invalidJson("%s can not be coerced to a %s".formatted(jsonValue.getClass().getSimpleName(), decimalType));
            }

            try {
                BigDecimal bigDecimal = scaleDecimal(new BigDecimal(jsonString.value()), decimalType);
                if (!overflows(bigDecimal, decimalType.getPrecision())) {
                    if (decimalType.isShort()) {
                        decimalType.writeLong(builder, bigDecimal.unscaledValue().longValueExact());
                    }
                    else {
                        decimalType.writeObject(builder, Int128.valueOf(bigDecimal.unscaledValue()));
                    }
                    return;
                }
            }
            catch (NumberFormatException ignored) {
            }
            builder.appendNull();
        }
    }

    private static class RealDecoder
            extends Decoder
    {
        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            if (!(jsonValue instanceof JsonString jsonString)) {
                throw invalidJson("%s can not be coerced to a REAL".formatted(jsonValue.getClass().getSimpleName()));
            }

            try {
                REAL.writeLong(builder, floatToRawIntBits(Float.parseFloat(jsonString.value())));
            }
            catch (NumberFormatException ignored) {
                builder.appendNull();
            }
        }
    }

    private static class DoubleDecoder
            extends Decoder
    {
        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            if (!(jsonValue instanceof JsonString jsonString)) {
                throw invalidJson("%s can not be coerced to a DOUBLE".formatted(jsonValue.getClass().getSimpleName()));
            }

            try {
                DOUBLE.writeDouble(builder, Double.parseDouble(jsonString.value()));
            }
            catch (NumberFormatException e) {
                builder.appendNull();
            }
        }
    }

    private static class DateDecoder
            extends Decoder
    {
        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            if (!(jsonValue instanceof JsonString jsonString)) {
                throw invalidJson("%s can not be coerced to a DATE".formatted(jsonValue.getClass().getSimpleName()));
            }

            String dateString = jsonString.value();
            try {
                DATE.writeLong(builder, toIntExact(parseHiveDate(dateString).toEpochDay()));
                return;
            }
            catch (DateTimeParseException | ArithmeticException ignored) {
            }
            try {
                DATE.writeLong(builder, toIntExact(parseDecimalHexOctalLong(dateString)));
                return;
            }
            catch (NumberFormatException | ArithmeticException ignored) {
            }
            builder.appendNull();
        }
    }

    private static class TimestampDecoder
            extends Decoder
    {
        private final TimestampType timestampType;
        private final List<DateTimeFormatter> timestampFormatters;
        private final TrinoTimestampEncoder<? extends Comparable<?>> timestampEncoder;

        public TimestampDecoder(TimestampType timestampType, List<DateTimeFormatter> timestampFormatters)
        {
            this.timestampType = timestampType;
            this.timestampFormatters = timestampFormatters;
            this.timestampEncoder = createTimestampEncoder(timestampType, UTC);
        }

        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            if (!(jsonValue instanceof JsonString jsonString)) {
                throw invalidJson("%s can not be coerced to a %s".formatted(jsonValue.getClass().getSimpleName(), timestampType));
            }
            try {
                DecodedTimestamp timestamp = parseTimestamp(jsonString.value(), timestampFormatters);
                timestampEncoder.write(timestamp, builder);
            }
            catch (DateTimeParseException | NumberFormatException | ArithmeticException e) {
                builder.appendNull();
            }
        }

        private static final int MIN_NUMERIC_TIMESTAMP_MILLIS_LENGTH = 13;

        public static DecodedTimestamp parseTimestamp(String value, List<DateTimeFormatter> timestampFormatters)
        {
            // first try specified formatters
            for (DateTimeFormatter formatter : timestampFormatters) {
                try {
                    ZonedDateTime zonedDateTime = formatter.parse(value, ZonedDateTime::from);
                    long epochSeconds = zonedDateTime.toEpochSecond();
                    return new DecodedTimestamp(epochSeconds, zonedDateTime.getNano());
                }
                catch (DateTimeParseException ignored) {
                }
            }

            // always try the build in timestamp formats

            // timestamp with time
            if (value.indexOf(':') > 0) {
                if (value.endsWith("z") || value.endsWith("Z") || HAS_TZ_OFFSET.matcher(value).matches()) {
                    try {
                        ZonedDateTime zonedDateTime = ZonedDateTime.parse(value, ZONED_DATE_TIME_PARSER_NO_COLON);
                        zonedDateTime = zonedDateTime.withZoneSameInstant(ZoneOffset.UTC);
                        return new DecodedTimestamp(zonedDateTime.toEpochSecond(), zonedDateTime.getNano());
                    }
                    catch (DateTimeParseException ignored) {
                    }
                    try {
                        ZonedDateTime zonedDateTime = ZonedDateTime.parse(value, ZONED_DATE_TIME_PARSER_WITH_COLON);
                        zonedDateTime = zonedDateTime.withZoneSameInstant(ZoneOffset.UTC);
                        return new DecodedTimestamp(zonedDateTime.toEpochSecond(), zonedDateTime.getNano());
                    }
                    catch (DateTimeParseException ignored) {
                    }
                }
                return parseHiveTimestamp(value);
            }

            if (!CharMatcher.anyOf("-+.0123456789").matchesAllOf(value)) {
                throw new DateTimeParseException("Invalid timestamp", value, 0);
            }

            // decimal seconds
            if (value.indexOf('.') >= 0) {
                long epochMillis = new BigDecimal(value)
                        .scaleByPowerOfTen(3)
                        .setScale(0, RoundingMode.DOWN)
                        .longValueExact();
                return ofEpochMilli(epochMillis);
            }

            // integer millis or seconds based on text length
            long timestampNumber = Long.parseLong(value);
            if (value.length() >= MIN_NUMERIC_TIMESTAMP_MILLIS_LENGTH) {
                return ofEpochMilli(timestampNumber);
            }
            return new DecodedTimestamp(timestampNumber, 0);
        }

        // There is no way to have a parser that supports a zone wih an optional colon, so we must have two copies of the parser
        @SuppressWarnings("SpellCheckingInspection")
        private static final DateTimeFormatter ZONED_DATE_TIME_PARSER_NO_COLON = new DateTimeFormatterBuilder()
                .parseLenient()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_TIME)
                .optionalStart()
                .appendOffset("+HHMM", "Z")
                .optionalEnd()
                .toFormatter()
                .withResolverStyle(LENIENT);

        private static final DateTimeFormatter ZONED_DATE_TIME_PARSER_WITH_COLON = new DateTimeFormatterBuilder()
                .parseLenient()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_TIME)
                .optionalStart()
                .appendOffset("+HH:MM", "Z")
                .optionalEnd()
                .toFormatter()
                .withResolverStyle(LENIENT);

        private static final Pattern HAS_TZ_OFFSET = Pattern.compile(".+([+\\-])\\d{2}:?\\d{2}$");

        private static DecodedTimestamp ofEpochMilli(long epochMillis)
        {
            long epochSeconds = floorDiv(epochMillis, (long) MILLISECONDS_PER_SECOND);
            long fractionalSecond = floorMod(epochMillis, (long) MILLISECONDS_PER_SECOND);
            int nanosOfSecond = toIntExact(fractionalSecond * (long) NANOSECONDS_PER_MILLISECOND);
            return new DecodedTimestamp(epochSeconds, nanosOfSecond);
        }
    }

    private static class VarbinaryDecoder
            extends Decoder
    {
        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            if (!(jsonValue instanceof JsonString jsonString)) {
                throw invalidJson("%s can not be coerced to a VARBINARY".formatted(jsonValue.getClass().getSimpleName()));
            }

            if (!jsonString.quoted()) {
                throw invalidJson("Unquoted JSON string is not allowed for VARBINARY: " + jsonValue.getClass().getSimpleName());
            }

            Slice binaryValue = Slices.wrappedBuffer(Base64.getDecoder().decode(jsonString.value()));
            VARBINARY.writeSlice(builder, binaryValue);
        }
    }

    private static class VarcharDecoder
            extends Decoder
    {
        private final VarcharType varcharType;

        public VarcharDecoder(VarcharType varcharType)
        {
            this.varcharType = varcharType;
        }

        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            String string;
            if (jsonValue instanceof Map<?, ?> jsonObject) {
                string = writeJsonObject(jsonObject);
            }
            else if (jsonValue instanceof List<?> jsonList) {
                string = writeJsonArray(jsonList);
            }
            else {
                JsonString jsonString = (JsonString) jsonValue;
                string = canonicalizeJsonString(jsonString);
            }
            varcharType.writeSlice(builder, truncateToLength(Slices.utf8Slice(string), varcharType));
        }
    }

    private static class CharDecoder
            extends Decoder
    {
        private final CharType charType;

        public CharDecoder(CharType charType)
        {
            this.charType = charType;
        }

        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            String string;
            if (jsonValue instanceof Map<?, ?> jsonObject) {
                string = writeJsonObject(jsonObject);
            }
            else if (jsonValue instanceof List<?> jsonList) {
                string = writeJsonArray(jsonList);
            }
            else {
                JsonString jsonString = (JsonString) jsonValue;
                string = canonicalizeJsonString(jsonString);
            }
            charType.writeSlice(builder, truncateToLengthAndTrimSpaces(Slices.utf8Slice(string), charType));
        }
    }

    private static class ArrayDecoder
            extends Decoder
    {
        private final Decoder elementDecoder;

        public ArrayDecoder(Decoder elementDecoder)
        {
            this.elementDecoder = elementDecoder;
        }

        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            // empty string is coerced to null; otherwise string is coerced to single element array below
            if (jsonValue instanceof JsonString stingValue && stingValue.value().isEmpty()) {
                builder.appendNull();
                return;
            }

            BlockBuilder elementBuilder = builder.beginBlockEntry();

            if (jsonValue instanceof List<?> jsonArray) {
                for (Object element : jsonArray) {
                    elementDecoder.decode(element, elementBuilder);
                }
            }
            else {
                // all other values are coerced to a single element array
                elementDecoder.decode(jsonValue, elementBuilder);
            }
            builder.closeEntry();
        }
    }

    private static class MapDecoder
            extends Decoder
    {
        private final Decoder keyDecoder;
        private final Decoder valueDecoder;
        private final Type keyType;

        private final DistinctMapKeys distinctMapKeys;
        private BlockBuilder keyBlockBuilder;

        public MapDecoder(MapType mapType, Decoder keyDecoder, Decoder valueDecoder)
        {
            this.keyType = mapType.getKeyType();
            this.keyDecoder = keyDecoder;
            this.valueDecoder = valueDecoder;

            this.distinctMapKeys = new DistinctMapKeys(mapType, true);
            this.keyBlockBuilder = mapType.getKeyType().createBlockBuilder(null, 128);
        }

        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            // string containing only whitespace is coerced to null; otherwise an exception is thrown
            if (jsonValue instanceof JsonString jsonString) {
                // string containing only whitespace is coerced to null
                if (!jsonString.value().trim().isEmpty()) {
                    throw invalidJson("Primitive can not be coerced to a MAP");
                }
                builder.appendNull();
                return;
            }

            checkArgument(jsonValue instanceof Map, "%s can not be coerced to a MAP", jsonValue.getClass().getSimpleName());
            Map<?, ?> jsonObject = (Map<?, ?>) jsonValue;
            Set<?> fieldNames = jsonObject.keySet();
            // field names strings are distinct, but after parsing the values may not be distinct (e.g., boolean)
            Block keyBlock = readKeys(fieldNames);
            boolean[] distinctKeys = distinctMapKeys.selectDistinctKeys(keyBlock);

            BlockBuilder entryBuilder = builder.beginBlockEntry();
            int keyIndex = 0;
            for (Object fieldName : fieldNames) {
                if (distinctKeys[keyIndex]) {
                    keyType.appendTo(keyBlock, keyIndex, entryBuilder);
                    valueDecoder.decode(jsonObject.get(fieldName), entryBuilder);
                }
                keyIndex++;
            }
            builder.closeEntry();
        }

        private Block readKeys(Collection<?> fieldNames)
        {
            for (Object fieldName : fieldNames) {
                // field names are always processed as a quoted JSON string even though they may
                // have not been quoted in the original JSON text
                JsonString jsonValue = new JsonString(fieldName.toString(), true);
                keyDecoder.decode(jsonValue, keyBlockBuilder);
            }

            Block keyBlock = keyBlockBuilder.build();
            keyBlockBuilder = keyType.createBlockBuilder(null, keyBlock.getPositionCount());
            return keyBlock;
        }
    }

    private static class RowDecoder
            extends Decoder
    {
        private final List<FieldName> fieldNames;
        private final List<Decoder> fieldDecoders;
        private final boolean dotsInKeyNames;

        public RowDecoder(RowType rowType, OpenXJsonOptions options, List<Decoder> fieldDecoders)
        {
            this.fieldNames = rowType.getFields().stream()
                    .map(field -> field.getName().orElseThrow())
                    .map(fieldName -> fieldName.toLowerCase(Locale.ROOT))
                    .map(originalValue -> new FieldName(originalValue, options))
                    .collect(toImmutableList());
            this.fieldDecoders = fieldDecoders;
            this.dotsInKeyNames = options.isDotsInFieldNames();
        }

        public void decode(Object jsonValue, PageBuilder builder)
                throws IOException
        {
            builder.declarePosition();
            decodeValue(jsonValue, builder::getBlockBuilder);
        }

        @Override
        void decodeValue(Object jsonValue, BlockBuilder builder)
        {
            SingleRowBlockWriter currentBuilder = (SingleRowBlockWriter) builder.beginBlockEntry();
            decodeValue(jsonValue, currentBuilder::getFieldBlockBuilder);
            builder.closeEntry();
        }

        private void decodeValue(Object jsonValue, IntFunction<BlockBuilder> fieldBuilders)
        {
            if (jsonValue instanceof JsonString jsonString) {
                decodeValueFromString(jsonString, fieldBuilders);
            }
            else if (jsonValue instanceof Map<?, ?> jsonObject) {
                decodeValueFromMap(jsonObject, fieldBuilders);
            }
            else if (jsonValue instanceof List<?> jsonArray) {
                decodeValueFromList(jsonArray, fieldBuilders);
            }
            else {
                throw invalidJson("Expected JSON object: " + jsonValue.getClass().getSimpleName());
            }
        }

        private void decodeValueFromString(JsonString jsonString, IntFunction<BlockBuilder> fieldBuilders)
        {
            // string containing only whitespace is coerced to a row with all fields set to  null; otherwise an exception is thrown
            if (!jsonString.value().trim().isEmpty()) {
                throw invalidJson("Primitive can not be coerced to a ROW");
            }

            for (int i = 0; i < fieldDecoders.size(); i++) {
                BlockBuilder blockBuilder = fieldBuilders.apply(i);
                blockBuilder.appendNull();
            }
        }

        private void decodeValueFromMap(Map<?, ?> jsonObject, IntFunction<BlockBuilder> fieldBuilders)
        {
            for (int i = 0; i < fieldDecoders.size(); i++) {
                FieldName fieldName = fieldNames.get(i);
                Object fieldValue = null;
                if (jsonObject.containsKey(fieldName)) {
                    fieldValue = jsonObject.get(fieldName);
                }
                else if (dotsInKeyNames) {
                    // check if any field matches any keys after dots have been replaced with underscores
                    for (Object key : jsonObject.keySet()) {
                        if (fieldName.originalValueMatchesDottedFieldName((FieldName) key)) {
                            fieldValue = jsonObject.get(key);
                            break;
                        }
                    }
                }

                BlockBuilder blockBuilder = fieldBuilders.apply(i);
                if (fieldValue == null) {
                    blockBuilder.appendNull();
                }
                else {
                    fieldDecoders.get(i).decode(fieldValue, blockBuilder);
                }
            }
        }

        private void decodeValueFromList(List<?> jsonArray, IntFunction<BlockBuilder> fieldBuilders)
        {
            for (int i = 0; i < fieldDecoders.size(); i++) {
                Object fieldValue = jsonArray.size() > i ? jsonArray.get(i) : null;
                BlockBuilder blockBuilder = fieldBuilders.apply(i);
                if (fieldValue == null) {
                    blockBuilder.appendNull();
                }
                else {
                    fieldDecoders.get(i).decode(fieldValue, blockBuilder);
                }
            }
        }
    }

    public static long parseLong(String stringValue)
            throws NumberFormatException, ArithmeticException
    {
        try {
            return parseDecimalHexOctalLong(stringValue);
        }
        catch (NumberFormatException ignored) {
        }

        BigDecimal bigDecimal = new BigDecimal(stringValue).setScale(0, RoundingMode.DOWN);
        return bigDecimal.longValueExact();
    }

    public static long parseDecimalHexOctalLong(String stringValue)
            throws NumberFormatException
    {
        if (isHex(stringValue)) {
            // Negative values will fail
            return Long.parseLong(stringValue.substring(2), 16);
        }
        if (isOctal(stringValue)) {
            // Negative values will fail
            return Long.parseLong(stringValue.substring(1), 8);
        }
        return Long.parseLong(stringValue);
    }

    private static boolean isHex(String s)
    {
        // This does not allow for `0x-123`
        return s.length() >= 3 &&
               s.charAt(0) == '0' &&
               toLowerCase(s.charAt(1)) == 'x' &&
               isHexDigit(s.charAt(2));
    }

    private static boolean isOctal(String s)
    {
        // This does not allow for `0-123`
        return s.length() >= 2 &&
               s.charAt(0) == '0' &&
               isOctalDigit(s.charAt(1));
    }

    private static boolean isOctalDigit(char c)
    {
        int digit = (int) c - (int) '0';
        return digit >= 0 && digit <= 8;
    }

    private static RuntimeException invalidJson(String message)
    {
        return new RuntimeException("Invalid JSON: " + message);
    }

    private static final class FieldName
    {
        private final String originalValue;
        private final String mappedName;
        private final boolean caseInsensitive;
        private final int hashCode;
        private final boolean canMatchDottedFieldName;
        private final boolean isDottedFieldName;

        public FieldName(String originalValue, OpenXJsonOptions options)
        {
            this(originalValue, options.isCaseInsensitive(), options.isDotsInFieldNames(), options.getFieldNameMappings());
        }

        public FieldName(String originalValue, boolean caseInsensitive, boolean dotsInFieldNames, Map<String, String> fieldNameMappings)
        {
            this.originalValue = requireNonNull(originalValue, "originalValue is null");
            this.mappedName = fieldNameMappings.getOrDefault(originalValue, originalValue);
            this.caseInsensitive = caseInsensitive;

            // the hashcode is always used, so just precompute it
            // equality is based on the mapped value
            if (caseInsensitive) {
                this.hashCode = mappedName.toLowerCase(Locale.ROOT).hashCode();
            }
            else {
                this.hashCode = mappedName.hashCode();
            }

            // only names containing underscores can match dotted field names
            canMatchDottedFieldName = dotsInFieldNames && originalValue.indexOf('_') >= 0;
            isDottedFieldName = dotsInFieldNames && originalValue.indexOf('.') >= 0;
        }

        /**
         * If the supplied name contains dots, replace all dots with underscores, and compare to
         * the original value using the set case sensitivity.  This strange behavior is follows
         * the logic of the original code.
         */
        public boolean originalValueMatchesDottedFieldName(FieldName dottedName)
        {
            // skip impossible matches
            if (!canMatchDottedFieldName || !dottedName.isDottedFieldName) {
                return false;
            }

            // substitute . with _ and check if name matches the *original* value
            // Note: this is not precomputed to save memory
            // Note: this could be sped up with a custom equality method
            String underscoreName = dottedName.originalValue.replace('.', '_');
            return caseInsensitive ? originalValue.equalsIgnoreCase(underscoreName) : originalValue.equals(underscoreName);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FieldName that = (FieldName) o;
            // equality is based on the mapped value
            return caseInsensitive ? mappedName.equalsIgnoreCase(that.mappedName) : mappedName.equals(that.mappedName);
        }

        @Override
        public int hashCode()
        {
            return hashCode;
        }

        @Override
        public String toString()
        {
            return originalValue;
        }
    }
}
