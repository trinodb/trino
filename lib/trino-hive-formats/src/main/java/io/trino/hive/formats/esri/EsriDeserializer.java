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
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.airlift.slice.Slices;
import io.trino.hive.formats.line.Column;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.List;
import java.util.TimeZone;

import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.createTimestampEncoder;
import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
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
import static java.util.Locale.ENGLISH;
import static org.joda.time.DateTimeZone.UTC;

public final class EsriDeserializer
{
    private static final TimeZone tz = TimeZone.getTimeZone(ZoneId.of("UTC"));
    private static final JsonFactory jsonFactory = jsonFactory();
    private static final String GEOMETRY_FIELD_NAME = "geometry";
    private static final String ATTRIBUTES_FIELD_NAME = "attributes";
    private static final String[] TIMESTAMP_FORMATS = new String[] {"yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd"};

    private final int numColumns;
    private int geometryColumn;
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    public EsriDeserializer(List<Column> columns)
    {
        numColumns = columns.size();
        columnNames = columns.stream().map(Column::name).toList();
        columnTypes = columns.stream().map(Column::type).toList();

        geometryColumn = -1;
        for (int i = 0; i < numColumns; i++) {
            if (columnTypes.get(i) == VARBINARY) {
                if (geometryColumn >= 0) {
                    throw new IllegalArgumentException("Multiple binary columns defined. Define only one binary column for geometries");
                }
                geometryColumn = i;
            }
        }
    }

    public void deserialize(PageBuilder pageBuilder, String jsonStr)
            throws IOException
    {
        if (jsonStr == null) {
            return;
        }

        try (JsonParser parser = jsonFactory.createParser(jsonStr)) {
            for (JsonToken token = parser.nextToken(); token != null; token = parser.nextToken()) {
                if (token != JsonToken.START_OBJECT) {
                    continue;
                }

                if (GEOMETRY_FIELD_NAME.equals(parser.currentName())) {
                    if (geometryColumn > -1) {
                        OGCGeometry ogcGeom = parseGeom(parser);
                        if (ogcGeom != null) {
                            byte[] esriShapeBytes = geometryToEsriShape(ogcGeom);
                            VARBINARY.writeSlice(pageBuilder.getBlockBuilder(geometryColumn), Slices.wrappedBuffer(esriShapeBytes));
                        }
                    }
                    else {
                        parser.skipChildren();
                    }
                }
                else if (ATTRIBUTES_FIELD_NAME.equals(parser.currentName())) {
                    for (JsonToken token2 = parser.nextToken(); token2 != JsonToken.END_OBJECT && token2 != null; token2 = parser.nextToken()) {
                        String name = parser.getText().toLowerCase(ENGLISH);
                        parser.nextToken();
                        int fieldIndex = columnNames.indexOf(name);
                        if (fieldIndex >= 0) {
                            Type columnType = columnTypes.get(fieldIndex);
                            String columnName = columnNames.get(fieldIndex);
                            serializeValue(parser, columnType, columnName, pageBuilder.getBlockBuilder(fieldIndex), true);
                        }
                    }
                }
            }

            pageBuilder.declarePosition();

            for (int i = 0; i < numColumns; i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                if (blockBuilder.getPositionCount() != pageBuilder.getPositionCount()) {
                    blockBuilder.appendNull();
                }
            }
        }
    }

    private OGCGeometry parseGeom(JsonParser parser)
    {
        MapGeometry mapGeom = GeometryEngine.jsonToGeometry(parser);
        return OGCGeometry.createFromEsriGeometry(mapGeom.getGeometry(), mapGeom.getSpatialReference());
    }

    private byte[] geometryToEsriShape(OGCGeometry ogcGeometry)
    {
        int wkid;
        try {
            wkid = ogcGeometry.SRID();
        }
        catch (NullPointerException e) {
            wkid = 0;
        }

        OGCType ogcType;
        try {
            String typeName = ogcGeometry.geometryType();
            if (typeName.equals("Point")) {
                ogcType = OGCType.ST_POINT;
            }
            else if (typeName.equals("LineString")) {
                ogcType = OGCType.ST_LINESTRING;
            }
            else if (typeName.equals("Polygon")) {
                ogcType = OGCType.ST_POLYGON;
            }
            else if (typeName.equals("MultiPoint")) {
                ogcType = OGCType.ST_MULTIPOINT;
            }
            else if (typeName.equals("MultiLineString")) {
                ogcType = OGCType.ST_MULTILINESTRING;
            }
            else if (typeName.equals("MultiPolygon")) {
                ogcType = OGCType.ST_MULTIPOLYGON;
            }
            else {
                ogcType = OGCType.UNKNOWN;
            }
        }
        catch (NullPointerException e) {
            ogcType = OGCType.UNKNOWN;
        }

        return serializeGeometry(ogcGeometry.getEsriGeometry(), wkid, ogcType);
    }

    private static byte[] serializeGeometry(Geometry geometry, int wkid, OGCType type)
    {
        if (geometry == null) {
            return null;
        }
        else {
            byte[] shape = GeometryEngine.geometryToEsriShape(geometry);
            if (shape == null) {
                return null;
            }
            else {
                byte[] shapeWithData = new byte[shape.length + 4 + 1];
                System.arraycopy(shape, 0, shapeWithData, 5, shape.length);

                setWKID(shapeWithData, wkid);
                setType(shapeWithData, type);

                return shapeWithData;
            }
        }
    }

    private static void setWKID(byte[] geomref, int wkid)
    {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(wkid);
        System.arraycopy(bb.array(), 0, geomref, 0, 4);
    }

    private static void setType(byte[] geomref, OGCType type)
    {
        geomref[4] = (byte) type.getIndex();
    }

    private void serializeValue(JsonParser parser, Type columnType, String columnName, BlockBuilder builder, boolean nullOnParseError)
    {
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
                serializeDecimal(parser.getText(), decimalType, builder);
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
                Timestamp timestamp = parseTime(parser);
                DecodedTimestamp decodedTimestamp = createDecodedTimestamp(timestamp);

                createTimestampEncoder(timestampType, UTC).write(decodedTimestamp, builder);
            }
            else if (columnType instanceof VarcharType varcharType) {
                if (parser.getCurrentToken() == JsonToken.VALUE_NULL) {
                    builder.appendNull();
                }
                else {
                    columnType.writeSlice(builder, truncateToLength(Slices.utf8Slice(parser.getText()), varcharType));
                }
            }
            else if (columnType instanceof CharType charType) {
                columnType.writeSlice(builder, truncateToLengthAndTrimSpaces(Slices.utf8Slice(parser.getText()), charType));
            }
            else {
                throw new RuntimeException("Column '" + columnName + "' with type: " + columnType.getDisplayName() + " is not supported");
            }
        }
        catch (Exception e) {
            // invalid columns are ignored
            if (nullOnParseError) {
                builder.appendNull();
            }
            else {
                throw new TrinoException(GENERIC_USER_ERROR, "Error Parsing a column in the table: " + e.getMessage(), e);
            }
        }
    }

    private static DecodedTimestamp createDecodedTimestamp(Timestamp timestamp)
    {
        long millis = timestamp.getTime();
        long epochSeconds = floorDiv(millis, (long) MILLISECONDS_PER_SECOND);
        long fractionalSecond = floorMod(millis, (long) MILLISECONDS_PER_SECOND);
        int nanosOfSecond = toIntExact(fractionalSecond * (long) NANOSECONDS_PER_MILLISECOND);

        return new DecodedTimestamp(epochSeconds, nanosOfSecond);
    }

    private static void serializeDecimal(String value, DecimalType decimalType, BlockBuilder builder)
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

    private Date parseDate(JsonParser parser)
            throws IOException
    {
        Date jsd = null;
        if (JsonToken.VALUE_NUMBER_INT.equals(parser.getCurrentToken())) {
            long epoch = parser.getLongValue();
            jsd = new Date(epoch - (long) tz.getOffset(epoch));
        }
        else {
            try {
                long epoch = parseTime(parser.getText(), "yyyy-MM-dd");
                jsd = new Date(epoch + 43200000L);
            }
            catch (ParseException e) {
            }
        }

        return jsd;
    }

    private Timestamp parseTime(JsonParser parser)
            throws IOException
    {
        Timestamp jst = null;
        if (JsonToken.VALUE_NUMBER_INT.equals(parser.getCurrentToken())) {
            long epoch = parser.getLongValue();
            jst = new Timestamp(epoch);
        }
        else {
            String value = parser.getText();
            int point = value.indexOf(46);
            String dateStr = point < 0 ? value : value.substring(0, point + 4);

            for (String format : TIMESTAMP_FORMATS) {
                try {
                    jst = new Timestamp(parseTime(dateStr, format));
                    break;
                }
                catch (ParseException e) {
                }
            }
        }

        return jst;
    }

    private long parseTime(String value, String format)
            throws ParseException
    {
        SimpleDateFormat dtFmt = new SimpleDateFormat(format);
        dtFmt.setTimeZone(tz);
        return dtFmt.parse(value).getTime();
    }
}
