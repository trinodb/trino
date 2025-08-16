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
package io.trino.hive.formats.esri;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.hive.formats.line.Column;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NUMBER_INT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.createTimestampEncoder;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.StrictMath.floorDiv;
import static java.lang.StrictMath.floorMod;
import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class EsriDeserializer
{
    private static final VarHandle INT_HANDLE_BIG_ENDIAN = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final String GEOMETRY_FIELD_NAME = "geometry";
    private static final String ATTRIBUTES_FIELD_NAME = "attributes";
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-M-d").withZone(UTC);
    private static final List<DateTimeFormatter> TIMESTAMP_FORMATTERS = List.of(
            DateTimeFormatter.ofPattern("yyyy-M-d HH:mm:ss.SSS").withZone(UTC),
            DateTimeFormatter.ofPattern("yyyy-M-d HH:mm:ss").withZone(UTC),
            DateTimeFormatter.ofPattern("yyyy-M-d HH:mm").withZone(UTC),
            DATE_FORMATTER);

    private final int geometryColumn;
    private final List<Column> columns;
    private final Map<String, Integer> columnIndex;
    private final List<Type> types;
    private final boolean[] fieldWritten;

    public EsriDeserializer(List<Column> columns)
    {
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.types = columns.stream()
                .map(Column::type)
                .collect(toImmutableList());
        this.fieldWritten = new boolean[columns.size()];

        for (Column column : columns) {
            validateSupportedType(column.type(), column.name());
        }

        ImmutableMap.Builder<String, Integer> columnNameBuilder = ImmutableMap.builder();
        for (int index = 0; index < columns.size(); index++) {
            columnNameBuilder.put(columns.get(index).name().toLowerCase(ENGLISH), index);
        }
        columnIndex = columnNameBuilder.buildOrThrow();

        int geometryColumn = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).type() == VARBINARY) {
                if (geometryColumn >= 0) {
                    throw new IllegalArgumentException("Multiple binary columns defined. Define only one binary column for geometries");
                }
                geometryColumn = i;
            }
        }
        this.geometryColumn = geometryColumn;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public void deserialize(PageBuilder pageBuilder, JsonParser parser)
            throws IOException
    {
        if (parser.currentToken() != START_OBJECT) {
            throw invalidJson("start of object expected");
        }

        Arrays.fill(fieldWritten, false);
        while (nextObjectField(parser)) {
            String fieldName = parser.currentName();
            if (nextTokenRequired(parser) == VALUE_NULL) {
                continue;
            }
            if (GEOMETRY_FIELD_NAME.equals(fieldName)) {
                parseGeometry(parser, pageBuilder);
            }
            else if (ATTRIBUTES_FIELD_NAME.equals(fieldName)) {
                parseAttributes(parser, pageBuilder);
            }
            else {
                skipCurrentValue(parser);
            }
        }

        pageBuilder.declarePosition();

        for (int i = 0; i < columns.size(); i++) {
            if (!fieldWritten[i]) {
                pageBuilder.getBlockBuilder(i).appendNull();
            }
        }
    }

    private BlockBuilder getBlockBuilderForWrite(PageBuilder pageBuilder, int fieldIndex)
    {
        // if the geometry column is already written, overwrite the last value
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(fieldIndex);
        if (fieldWritten[fieldIndex]) {
            blockBuilder.resetTo(blockBuilder.getPositionCount() - 1);
        }
        fieldWritten[fieldIndex] = true;
        return blockBuilder;
    }

    private void parseGeometry(JsonParser parser, PageBuilder pageBuilder)
            throws IOException
    {
        if (parser.currentToken() != START_OBJECT) {
            throw invalidJson("geometry is not an object");
        }

        // if geometry is not mapped to a column, skip it
        if (geometryColumn <= -1) {
            skipCurrentValue(parser);
            return;
        }

        MapGeometry mapGeometry = GeometryEngine.jsonToGeometry(parser);
        OGCGeometry ogcGeometry = OGCGeometry.createFromEsriGeometry(mapGeometry.getGeometry(), mapGeometry.getSpatialReference());
        Geometry geometry = ogcGeometry.getEsriGeometry();
        if (geometry == null) {
            throw new IllegalArgumentException("Could not parse geometry");
        }

        byte[] shape = GeometryEngine.geometryToEsriShape(geometry);
        if (shape == null) {
            throw new IllegalArgumentException("Could not serialize geometry shape");
        }

        byte[] shapeHeader = new byte[4 + 1 + shape.length];
        // write the Spatial Reference System Identifier (a.k.a, the well-known ID)
        INT_HANDLE_BIG_ENDIAN.set(shapeHeader, 0, ogcGeometry.SRID());
        // write the geometry type
        OGCType ogcType = switch (ogcGeometry.geometryType()) {
            case "Point" -> OGCType.ST_POINT;
            case "LineString" -> OGCType.ST_LINESTRING;
            case "Polygon" -> OGCType.ST_POLYGON;
            case "MultiPoint" -> OGCType.ST_MULTIPOINT;
            case "MultiLineString" -> OGCType.ST_MULTILINESTRING;
            case "MultiPolygon" -> OGCType.ST_MULTIPOLYGON;
            case null, default -> OGCType.UNKNOWN;
        };
        shapeHeader[4] = ogcType.getIndex();
        // write the serialized shape
        System.arraycopy(shape, 0, shapeHeader, 5, shape.length);

        // write the shape to the page
        VARBINARY.writeSlice(getBlockBuilderForWrite(pageBuilder, geometryColumn), Slices.wrappedBuffer(shapeHeader));
    }

    private void parseAttributes(JsonParser parser, PageBuilder pageBuilder)
            throws IOException
    {
        if (parser.currentToken() != START_OBJECT) {
            throw invalidJson("attributes is not an object");
        }
        while (nextObjectField(parser)) {
            String attributeName = parser.getText().toLowerCase(ENGLISH);
            parser.nextToken();
            Integer fieldIndex = columnIndex.get(attributeName);
            if (fieldIndex != null) {
                Column column = columns.get(fieldIndex);
                parseAttribute(parser, column.type(), column.name(), getBlockBuilderForWrite(pageBuilder, fieldIndex));
            }
            else {
                skipCurrentValue(parser);
            }
        }
    }

    private static void parseAttribute(JsonParser parser, Type columnType, String columnName, BlockBuilder builder)
    {
        if (VARBINARY.equals(columnType)) {
            throw new UnsupportedTypeException(columnType, columnName);
        }

        if (parser.getCurrentToken() == VALUE_NULL) {
            builder.appendNull();
            return;
        }

        try {
            if (BOOLEAN.equals(columnType)) {
                columnType.writeBoolean(builder, parser.getBooleanValue());
            }
            else if (BIGINT.equals(columnType)) {
                columnType.writeLong(builder, parser.getLongValue());
            }
            else if (INTEGER.equals(columnType)) {
                columnType.writeLong(builder, parser.getIntValue());
            }
            else if (SMALLINT.equals(columnType)) {
                columnType.writeLong(builder, parser.getShortValue());
            }
            else if (TINYINT.equals(columnType)) {
                columnType.writeLong(builder, parser.getByteValue());
            }
            else if (columnType instanceof DecimalType decimalType) {
                parseDecimal(parser.getText(), decimalType, builder);
            }
            else if (REAL.equals(columnType)) {
                columnType.writeLong(builder, floatToRawIntBits(parser.getFloatValue()));
            }
            else if (DOUBLE.equals(columnType)) {
                columnType.writeDouble(builder, parser.getDoubleValue());
            }
            else if (DATE.equals(columnType)) {
                columnType.writeLong(builder, toIntExact(parseDate(parser).getTime() / 86400000L));
            }
            else if (columnType instanceof TimestampType timestampType) {
                Timestamp timestamp = parseTimestamp(parser);
                DecodedTimestamp decodedTimestamp = createDecodedTimestamp(timestamp);
                createTimestampEncoder(timestampType, DateTimeZone.UTC).write(decodedTimestamp, builder);
            }
            else if (columnType instanceof VarcharType varcharType) {
                columnType.writeSlice(builder, truncateToLength(Slices.utf8Slice(parser.getText()), varcharType));
            }
            else if (columnType instanceof CharType charType) {
                columnType.writeSlice(builder, truncateToLengthAndTrimSpaces(Slices.utf8Slice(parser.getText()), charType));
            }
            else {
                // This should never happen due to prior validation
                throw new UnsupportedTypeException(columnType, columnName);
            }
        }
        catch (UnsupportedTypeException e) {
            throw e;
        }
        catch (Exception e) {
            // invalid columns are ignored
            builder.appendNull();
        }
    }

    private static void validateSupportedType(Type type, String columnName)
    {
        if (BOOLEAN.equals(type) ||
                BIGINT.equals(type) ||
                INTEGER.equals(type) ||
                SMALLINT.equals(type) ||
                TINYINT.equals(type) ||
                type instanceof DecimalType ||
                REAL.equals(type) ||
                DOUBLE.equals(type) ||
                DATE.equals(type) ||
                type instanceof TimestampType ||
                type instanceof VarcharType ||
                type instanceof CharType ||
                VARBINARY.equals(type)) {
            return;
        }
        throw new UnsupportedTypeException(type, columnName);
    }

    private static DecodedTimestamp createDecodedTimestamp(Timestamp timestamp)
    {
        long millis = timestamp.getTime();
        long epochSeconds = floorDiv(millis, (long) MILLISECONDS_PER_SECOND);
        long fractionalSecond = floorMod(millis, (long) MILLISECONDS_PER_SECOND);
        int nanosOfSecond = toIntExact(fractionalSecond * (long) NANOSECONDS_PER_MILLISECOND);

        return new DecodedTimestamp(epochSeconds, nanosOfSecond);
    }

    private static void parseDecimal(String value, DecimalType decimalType, BlockBuilder builder)
    {
        BigDecimal bigDecimal;
        try {
            bigDecimal = new BigDecimal(value).setScale(DecimalConversions.intScale(decimalType.getScale()), HALF_UP);
        }
        catch (NumberFormatException e) {
            throw new NumberFormatException(format("Cannot convert '%s' to %s. Value is not a number.", value, decimalType));
        }

        if (overflows(bigDecimal, decimalType.getPrecision())) {
            throw new IllegalArgumentException(format("Cannot convert '%s' to %s. Value too large.", value, decimalType));
        }

        if (decimalType.isShort()) {
            decimalType.writeLong(builder, bigDecimal.unscaledValue().longValueExact());
        }
        else {
            decimalType.writeObject(builder, Int128.valueOf(bigDecimal.unscaledValue()));
        }
    }

    // NOTE: It only supports UTC timezone.
    private static Date parseDate(JsonParser parser)
            throws IOException
    {
        if (VALUE_NUMBER_INT == parser.getCurrentToken()) {
            long epoch = parser.getLongValue();
            return new Date(epoch);
        }

        try {
            LocalDate localDate = LocalDate.parse(parser.getText(), DATE_FORMATTER);
            // Add 12 hours (43200000L milliseconds) to handle noon conversion
            return new Date(localDate.atStartOfDay(UTC)
                    .toInstant()
                    .toEpochMilli() + 43200000L);
        }
        catch (DateTimeParseException e) {
            throw new IllegalArgumentException(format("Value '%s' cannot be parsed to a Date. Expected format: yyyy-MM-dd", parser.getText()), e);
        }
    }

    private static Timestamp parseTimestamp(JsonParser parser)
            throws IOException
    {
        if (VALUE_NUMBER_INT == parser.getCurrentToken()) {
            long epoch = parser.getLongValue();
            return new Timestamp(epoch);
        }

        String value = parser.getText();
        int point = value.indexOf('.');
        String dateStr = point < 0 ? value : value.substring(0, Math.min(point + 4, value.length()));

        for (DateTimeFormatter formatter : TIMESTAMP_FORMATTERS) {
            try {
                Instant instant;
                if (formatter.equals(DATE_FORMATTER)) {
                    LocalDate localDate = LocalDate.parse(dateStr, formatter);
                    instant = localDate.atStartOfDay(UTC).toInstant();
                }
                else {
                    LocalDateTime dateTime = LocalDateTime.parse(dateStr, formatter);
                    instant = dateTime.atZone(UTC).toInstant();
                }
                return Timestamp.from(instant);
            }
            catch (DateTimeParseException e) {
                // Try next formatter
            }
        }

        throw new IllegalArgumentException(format("Value '%s' cannot be parsed to a Timestamp", dateStr));
    }

    static boolean nextObjectField(JsonParser parser)
            throws IOException
    {
        JsonToken token = nextTokenRequired(parser);
        if (token == FIELD_NAME) {
            return true;
        }
        if (token == END_OBJECT) {
            return false;
        }
        throw invalidJson("field name expected, but was " + token);
    }

    static JsonToken nextTokenRequired(JsonParser parser)
            throws IOException
    {
        JsonToken token = parser.nextToken();
        if (token == null) {
            throw invalidJson("object is truncated");
        }
        return token;
    }

    static void skipCurrentValue(JsonParser parser)
            throws IOException
    {
        JsonToken valueToken = parser.currentToken();
        if ((valueToken == START_ARRAY) || (valueToken == START_OBJECT)) {
            // if the current token is a beginning of an array or object, move the stream forward
            // skipping any child tokens till we're at the corresponding END_ARRAY or END_OBJECT token
            parser.skipChildren();
        }
        // At the end of this function, the stream should be pointing to the last token that
        // corresponds to the value being skipped. This way, the next call to nextToken
        // will advance it to the next field name.
    }

    static IOException invalidJson(String message)
    {
        return new IOException("Invalid JSON: " + message);
    }

    public static class UnsupportedTypeException
            extends RuntimeException
    {
        public UnsupportedTypeException(Type columnType, String columnName)
        {
            super("Column '" + columnName + "' with type: " + columnType.getDisplayName() + " is not supported");
        }
    }
}
