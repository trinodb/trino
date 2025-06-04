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
package io.trino.hive.formats.line.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hive.formats.DistinctMapKeys;
import io.trino.hive.formats.HiveFormatUtils;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.ValueBlock;
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
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.core.JsonFactory.Feature.INTERN_FIELD_NAMES;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.HiveFormatUtils.createTimestampParser;
import static io.trino.hive.formats.HiveFormatUtils.parseHiveDate;
import static io.trino.hive.formats.HiveFormatUtils.writeDecimal;
import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.createTimestampEncoder;
import static io.trino.plugin.base.util.JsonUtils.jsonFactoryBuilder;
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
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

/**
 * Deserializer that is bug for bug compatible with Hive JsonSerDe where possible. Known exceptions are:
 * <ul>
 *     <li>
 *         When a scalar value is actually a json object, Hive will process the open curly bracket for
 *         BOOLEAN, DECIMAL, CHAR, VARCHAR, and VARBINARY.  Then it continues processing field inside of
 *         the json object as if they are part of the outer json object.  When the closing curly bracket
 *         is encountered it pops a level, which can end parsing early.  This is clearly a bug resulting
 *         in corrupted data, and instead we throw an exception.
 *     </li>
 *     <li>
 *         Duplicate json object fields are supported, and like Hive. Hive parses each of these duplicate
 *         values, but this code only process the last value.  This means if one of the duplicates is
 *         invalid, Hive will fail, and this code will not.
 *     </li>
 * </ul>
 */
public class JsonDeserializer
        implements LineDeserializer
{
    private static final JsonFactory JSON_FACTORY = jsonFactoryBuilder()
            .disable(INTERN_FIELD_NAMES)
            .build();

    private final List<Type> types;
    private final RowDecoder rowDecoder;

    public JsonDeserializer(List<Column> columns, List<String> timestampFormats)
    {
        this.types = columns.stream()
                .map(Column::type)
                .collect(toImmutableList());

        Function<String, DecodedTimestamp> timestampParser = createTimestampParser(timestampFormats);

        ImmutableMap.Builder<Integer, Integer> ordinals = ImmutableMap.builderWithExpectedSize(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            ordinals.put(columns.get(i).ordinal(), i);
        }
        Map<Integer, Integer> topLevelOrdinalMap = ordinals.buildOrThrow();

        rowDecoder = new RowDecoder(
                RowType.from(columns.stream()
                        .map(column -> field(column.name().toLowerCase(Locale.ROOT), column.type()))
                        .collect(toImmutableList())),
                columns.stream()
                        .map(Column::type)
                        .map(fieldType -> createDecoder(fieldType, timestampParser))
                        .toArray(Decoder[]::new),
                topLevelOrdinalMap::get);
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
        JsonParser parser = JSON_FACTORY.createParser(lineBuffer.getBuffer(), 0, lineBuffer.getLength());
        parser.nextToken();

        rowDecoder.decode(parser, builder);

        // Calling close on the parser even though there is no real InputStream backing, it is necessary so that
        // entries in this parser instance's canonical field name cache can be reused on the next invocation
        parser.close();
    }

    private static Decoder createDecoder(Type type, Function<String, DecodedTimestamp> timestampParser)
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
            return new TimestampDecoder(timestampType, timestampParser);
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
            return new ArrayDecoder(arrayType, createDecoder(arrayType.getElementType(), timestampParser));
        }
        if (type instanceof MapType mapType) {
            return new MapDecoder(mapType, createDecoder(mapType.getValueType(), timestampParser), timestampParser);
        }
        if (type instanceof RowType rowType) {
            return new RowDecoder(
                    rowType,
                    rowType.getFields().stream()
                            .map(Field::getType)
                            .map(fieldType -> createDecoder(fieldType, timestampParser))
                            .toArray(Decoder[]::new),
                    IntUnaryOperator.identity());
        }
        throw new UnsupportedOperationException("Unsupported column type: " + type);
    }

    private abstract static class Decoder
    {
        private final Type type;
        private final boolean isScalarType;

        public Decoder(Type type)
        {
            this.type = requireNonNull(type, "type is null");
            this.isScalarType = isScalarType(type);
        }

        public final void decode(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            JsonToken currentToken = parser.currentToken();
            if (currentToken == VALUE_NULL) {
                builder.appendNull();
                return;
            }

            if (isScalarType && !currentToken.isScalarValue()) {
                throw invalidJson(type + " value must be a scalar json value");
            }
            decodeValue(parser, builder);
        }

        abstract void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException;

        private static boolean isScalarType(Type type)
        {
            return !(type instanceof ArrayType || type instanceof MapType || type instanceof RowType);
        }
    }

    private static class BooleanDecoder
            extends Decoder
    {
        public BooleanDecoder()
        {
            super(BOOLEAN);
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            // this does not use parser.getBoolean, because it only works with JSON boolean
            // literals, and the original Hive code implicitly converts any JSON type to boolean
            BOOLEAN.writeBoolean(builder, Boolean.parseBoolean(parser.getText()));
        }
    }

    private static class BigintDecoder
            extends Decoder
    {
        public BigintDecoder()
        {
            super(BIGINT);
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            BIGINT.writeLong(builder, parser.getLongValue());
        }
    }

    private static class IntegerDecoder
            extends Decoder
    {
        public IntegerDecoder()
        {
            super(INTEGER);
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            INTEGER.writeLong(builder, parser.getIntValue());
        }
    }

    private static class SmallintDecoder
            extends Decoder
    {
        public SmallintDecoder()
        {
            super(SMALLINT);
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            SMALLINT.writeLong(builder, parser.getShortValue());
        }
    }

    private static class TinyintDecoder
            extends Decoder
    {
        public TinyintDecoder()
        {
            super(TINYINT);
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            TINYINT.writeLong(builder, parser.getByteValue());
        }
    }

    private static class DecimalDecoder
            extends Decoder
    {
        private final DecimalType decimalType;

        public DecimalDecoder(DecimalType decimalType)
        {
            super(decimalType);
            this.decimalType = requireNonNull(decimalType, "decimalType is null");
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            String value = parser.getText();
            BigDecimal bigDecimal;
            try {
                bigDecimal = HiveFormatUtils.parseDecimal(value, decimalType);
            }
            catch (NumberFormatException _) {
                // parsing errors are simply ignored and the field is null
                builder.appendNull();
                return;
            }
            // out of bounds is an error
            if (overflows(bigDecimal, decimalType.getPrecision())) {
                throw new NumberFormatException(format("Cannot convert '%s' to %s. Value too large.", value, decimalType));
            }
            if (decimalType.isShort()) {
                decimalType.writeLong(builder, bigDecimal.unscaledValue().longValueExact());
            }
            else {
                decimalType.writeObject(builder, Int128.valueOf(bigDecimal.unscaledValue()));
            }
        }
    }

    private static class RealDecoder
            extends Decoder
    {
        public RealDecoder()
        {
            super(REAL);
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            REAL.writeLong(builder, floatToRawIntBits(parser.getFloatValue()));
        }
    }

    private static class DoubleDecoder
            extends Decoder
    {
        public DoubleDecoder()
        {
            super(DOUBLE);
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            DOUBLE.writeDouble(builder, parser.getDoubleValue());
        }
    }

    private static class DateDecoder
            extends Decoder
    {
        public DateDecoder()
        {
            super(DATE);
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            DATE.writeLong(builder, toIntExact(parseHiveDate(parser.getText()).toEpochDay()));
        }
    }

    private static class TimestampDecoder
            extends Decoder
    {
        private final TimestampType timestampType;
        private final Function<String, DecodedTimestamp> timestampParser;

        public TimestampDecoder(TimestampType timestampType, Function<String, DecodedTimestamp> timestampParser)
        {
            super(timestampType);
            this.timestampType = requireNonNull(timestampType, "timestampType is null");
            this.timestampParser = requireNonNull(timestampParser, "timestampParser is null");
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            DecodedTimestamp timestamp = timestampParser.apply(parser.getText());
            createTimestampEncoder(timestampType, UTC).write(timestamp, builder);
        }
    }

    private static class VarbinaryDecoder
            extends Decoder
    {
        // This must be created per instance because it is not thread safe
        private final CharsetDecoder charsetDecoder = createCharsetDecoder();

        public VarbinaryDecoder()
        {
            super(VARBINARY);
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            VARBINARY.writeSlice(builder, parseBinary(parser.getText(), charsetDecoder));
        }

        private static Slice parseBinary(String value, CharsetDecoder charsetDecoder)
                throws IOException
        {
            byte[] utf8 = value.getBytes(UTF_8);
            // This corrupts the data, but this is exactly what Hive does, so we get the same result as Hive
            String decode = charsetDecoder.decode(ByteBuffer.wrap(utf8, 0, utf8.length)).toString();
            return Slices.utf8Slice(decode);
        }

        private static CharsetDecoder createCharsetDecoder()
        {
            return UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
    }

    private static class VarcharDecoder
            extends Decoder
    {
        private final VarcharType varcharType;

        public VarcharDecoder(VarcharType varcharType)
        {
            super(varcharType);
            this.varcharType = requireNonNull(varcharType, "varcharType is null");
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            varcharType.writeSlice(builder, truncateToLength(Slices.utf8Slice(parser.getText()), varcharType));
        }
    }

    private static class CharDecoder
            extends Decoder
    {
        private final CharType charType;

        public CharDecoder(CharType charType)
        {
            super(charType);
            this.charType = requireNonNull(charType, "charType is null");
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            charType.writeSlice(builder, truncateToLengthAndTrimSpaces(Slices.utf8Slice(parser.getText()), charType));
        }
    }

    private static class ArrayDecoder
            extends Decoder
    {
        private final Decoder elementDecoder;

        public ArrayDecoder(ArrayType arrayType, Decoder elementDecoder)
        {
            super(arrayType);
            this.elementDecoder = requireNonNull(elementDecoder, "elementDecoder is null");
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            ((ArrayBlockBuilder) builder).buildEntry(elementBuilder -> {
                if (parser.currentToken() != START_ARRAY) {
                    throw invalidJson("start of array expected");
                }
                while (nextTokenRequired(parser) != JsonToken.END_ARRAY) {
                    elementDecoder.decode(parser, elementBuilder);
                }
            });
        }
    }

    private static class MapDecoder
            extends Decoder
    {
        private final Decoder valueDecoder;
        private final Type keyType;
        private final Type valueType;
        private final Function<String, DecodedTimestamp> timestampParser;

        private final CharsetDecoder charsetDecoder = VarbinaryDecoder.createCharsetDecoder();

        private final DistinctMapKeys distinctMapKeys;
        private BlockBuilder keyBlockBuilder;
        private BlockBuilder valueBlockBuilder;

        public MapDecoder(MapType mapType, Decoder valueDecoder, Function<String, DecodedTimestamp> timestampParser)
        {
            super(mapType);
            this.keyType = mapType.getKeyType();
            this.valueType = mapType.getValueType();
            this.valueDecoder = requireNonNull(valueDecoder, "valueDecoder is null");
            this.timestampParser = requireNonNull(timestampParser, "timestampParser is null");

            this.distinctMapKeys = new DistinctMapKeys(mapType, true);
            this.keyBlockBuilder = mapType.getKeyType().createBlockBuilder(null, 128);
            this.valueBlockBuilder = mapType.getValueType().createBlockBuilder(null, 128);
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            if (parser.currentToken() != START_OBJECT) {
                throw invalidJson("start of object expected");
            }

            // buffer the keys and values
            while (nextObjectField(parser)) {
                String keyText = parser.getText();
                serializeMapKey(keyText, keyType, keyBlockBuilder);
                parser.nextToken();
                valueDecoder.decode(parser, valueBlockBuilder);
            }
            ValueBlock keys = keyBlockBuilder.buildValueBlock();
            ValueBlock values = valueBlockBuilder.buildValueBlock();
            keyBlockBuilder = keyType.createBlockBuilder(null, keys.getPositionCount());
            valueBlockBuilder = valueType.createBlockBuilder(null, values.getPositionCount());

            // copy the distinct key entries to the output
            boolean[] distinctKeys = distinctMapKeys.selectDistinctKeys(keys);

            ((MapBlockBuilder) builder).buildEntry((keyBuilder, valueBuilder) -> {
                // this could use bulk append positions, but that would require converting the mask to a list of positions
                for (int index = 0; index < distinctKeys.length; index++) {
                    boolean distinctKey = distinctKeys[index];
                    if (distinctKey) {
                        keyBuilder.append(keys, index);
                        valueBuilder.append(values, index);
                    }
                }
            });
        }

        private void serializeMapKey(String value, Type type, BlockBuilder builder)
                throws IOException
        {
            if (BOOLEAN.equals(type)) {
                type.writeBoolean(builder, Boolean.parseBoolean(value));
            }
            else if (BIGINT.equals(type)) {
                type.writeLong(builder, Long.parseLong(value));
            }
            else if (INTEGER.equals(type)) {
                type.writeLong(builder, Integer.parseInt(value));
            }
            else if (SMALLINT.equals(type)) {
                type.writeLong(builder, Short.parseShort(value));
            }
            else if (TINYINT.equals(type)) {
                type.writeLong(builder, Byte.parseByte(value));
            }
            else if (type instanceof DecimalType decimalType) {
                writeDecimal(value, decimalType, builder);
            }
            else if (REAL.equals(type)) {
                type.writeLong(builder, floatToRawIntBits(Float.parseFloat(value)));
            }
            else if (DOUBLE.equals(type)) {
                type.writeDouble(builder, Double.parseDouble(value));
            }
            else if (DATE.equals(type)) {
                type.writeLong(builder, parseHiveDate(value).toEpochDay());
            }
            else if (type instanceof TimestampType timestampType) {
                DecodedTimestamp timestamp = timestampParser.apply(value);
                createTimestampEncoder(timestampType, UTC).write(timestamp, builder);
            }
            else if (VARBINARY.equals(type)) {
                type.writeSlice(builder, VarbinaryDecoder.parseBinary(value, charsetDecoder));
            }
            else if (type instanceof VarcharType varcharType) {
                type.writeSlice(builder, truncateToLength(Slices.utf8Slice(value), varcharType));
            }
            else if (type instanceof CharType charType) {
                type.writeSlice(builder, truncateToLengthAndTrimSpaces(Slices.utf8Slice(value), charType));
            }
            else {
                throw new UnsupportedOperationException("Unsupported map key type: " + type);
            }
        }
    }

    private static class RowDecoder
            extends Decoder
    {
        private static final Pattern INTERNAL_PATTERN = Pattern.compile("_col([0-9]+)");

        private final Map<String, Integer> fieldPositions;
        private final Decoder[] fieldDecoders;
        private final boolean[] fieldWritten;
        private final IntUnaryOperator ordinalToFieldPosition;

        public RowDecoder(RowType rowType, Decoder[] fieldDecoders, IntUnaryOperator ordinalToFieldPosition)
        {
            super(rowType);

            ImmutableMap.Builder<String, Integer> fieldPositions = ImmutableMap.builder();
            List<Field> fields = rowType.getFields();
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                fieldPositions.put(field.getName().orElseThrow().toLowerCase(Locale.ROOT), i);
            }
            this.fieldPositions = fieldPositions.buildOrThrow();
            this.fieldDecoders = requireNonNull(fieldDecoders, "fieldDecoders is null");
            checkArgument(this.fieldDecoders.length == fields.size(), "fieldDecoders size mismatch: %s <> %s", this.fieldDecoders.length, fields.size());
            checkArgument(Arrays.stream(this.fieldDecoders).noneMatch(Objects::isNull), "fieldDecoders contains null element");
            this.fieldWritten = new boolean[this.fieldDecoders.length];
            this.ordinalToFieldPosition = requireNonNull(ordinalToFieldPosition, "ordinalToFieldPosition is null");
        }

        public void decode(JsonParser parser, PageBuilder builder)
                throws IOException
        {
            builder.declarePosition();
            decodeValue(parser, builder.getPositionCount(), builder::getBlockBuilder);
        }

        @Override
        void decodeValue(JsonParser parser, BlockBuilder builder)
                throws IOException
        {
            ((RowBlockBuilder) builder).buildEntry(fieldBuilders -> decodeValue(parser, builder.getPositionCount(), fieldBuilders::get));
        }

        private void decodeValue(JsonParser parser, int currentPosition, IntFunction<BlockBuilder> fieldBuilders)
                throws IOException
        {
            if (parser.currentToken() != START_OBJECT) {
                throw invalidJson("start of object expected");
            }

            Arrays.fill(fieldWritten, false);

            while (nextObjectField(parser)) {
                String fieldName = parser.getText();
                int rowIndex = getFieldPosition(fieldName);
                if (rowIndex < 0) {
                    skipNextValue(parser);
                    continue;
                }

                BlockBuilder fieldBuilder = fieldBuilders.apply(rowIndex);
                if (fieldWritten[rowIndex]) {
                    fieldBuilder.resetTo(currentPosition);
                }

                nextTokenRequired(parser);
                fieldDecoders[rowIndex].decode(parser, fieldBuilder);
                fieldWritten[rowIndex] = true;
            }

            // write null to unset fields
            for (int i = 0; i < fieldWritten.length; i++) {
                if (!fieldWritten[i]) {
                    fieldBuilders.apply(i).appendNull();
                }
            }
        }

        private int getFieldPosition(String fieldName)
        {
            Integer fieldPosition = fieldPositions.get(fieldName.toLowerCase(Locale.ROOT));
            if (fieldPosition != null) {
                return fieldPosition;
            }

            // The above line should have been all the implementation that
            // we need, but due to a bug in that impl which recognizes
            // only single-digit columns, we need another impl here.
            Matcher matcher = INTERNAL_PATTERN.matcher(fieldName);
            if (!matcher.matches()) {
                return -1;
            }
            int ordinal = Integer.parseInt(matcher.group(1));
            return ordinalToFieldPosition.applyAsInt(ordinal);
        }
    }

    private static void skipNextValue(JsonParser parser)
            throws IOException
    {
        JsonToken valueToken = parser.nextToken();
        if ((valueToken == START_ARRAY) || (valueToken == START_OBJECT)) {
            // if the currently read token is a beginning of an array or object, move the stream forward
            // skipping any child tokens till we're at the corresponding END_ARRAY or END_OBJECT token
            parser.skipChildren();
        }
        // At the end of this function, the stream should be pointing to the last token that
        // corresponds to the value being skipped. This way, the next call to nextToken
        // will advance it to the next field name.
    }

    private static boolean nextObjectField(JsonParser parser)
            throws IOException
    {
        JsonToken token = nextTokenRequired(parser);
        if (token == FIELD_NAME) {
            return true;
        }
        if (token == END_OBJECT) {
            return false;
        }
        throw invalidJson("field name expected");
    }

    private static JsonToken nextTokenRequired(JsonParser parser)
            throws IOException
    {
        JsonToken token = parser.nextToken();
        if (token == null) {
            throw invalidJson("object is truncated");
        }
        return token;
    }

    private static IOException invalidJson(String message)
    {
        return new IOException("Invalid JSON: " + message);
    }
}
