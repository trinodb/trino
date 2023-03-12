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
package io.trino.hive.formats;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.StructObject;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.type.TrinoTimestampEncoderFactory.createTimestampEncoder;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.trimTrailingSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static java.lang.Math.max;
import static java.math.RoundingMode.HALF_UP;
import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public final class FormatTestUtils
{
    public static final List<Optional<CompressionKind>> COMPRESSION = ImmutableList.<Optional<CompressionKind>>builder()
            .add(Optional.of(CompressionKind.SNAPPY))
            .add(Optional.of(CompressionKind.LZ4))
            .add(Optional.of(CompressionKind.GZIP))
            .add(Optional.of(CompressionKind.DEFLATE))
            .add(Optional.of(CompressionKind.ZSTD))
            .add(Optional.of(CompressionKind.LZO))
            .add(Optional.of(CompressionKind.LZOP))
            .add(Optional.of(CompressionKind.BZIP2))
            .add(Optional.empty())
            .build();

    private FormatTestUtils() {}

    public static void configureCompressionCodecs(Configuration configuration)
    {
        checkArgument(configuration.get("io.compression.codecs") == null, "Compression codecs already configured");
        configuration.set("io.compression.codecs", CompressionKind.LZOP.getHadoopClassName() + "," + CompressionKind.LZO.getHadoopClassName());
    }

    public static ObjectInspector getJavaObjectInspector(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return javaBooleanObjectInspector;
        }
        if (type.equals(BIGINT)) {
            return javaLongObjectInspector;
        }
        if (type.equals(INTEGER)) {
            return javaIntObjectInspector;
        }
        if (type.equals(SMALLINT)) {
            return javaShortObjectInspector;
        }
        if (type.equals(TINYINT)) {
            return javaByteObjectInspector;
        }
        if (type.equals(REAL)) {
            return javaFloatObjectInspector;
        }
        if (type.equals(DOUBLE)) {
            return javaDoubleObjectInspector;
        }
        if (type instanceof VarcharType varcharType) {
            return varcharType.getLength()
                    .map(length -> getPrimitiveJavaObjectInspector(new VarcharTypeInfo(length)))
                    .orElse(javaStringObjectInspector);
        }
        if (type instanceof CharType charType) {
            return getPrimitiveJavaObjectInspector(new CharTypeInfo(charType.getLength()));
        }
        if (type.equals(VARBINARY)) {
            return javaByteArrayObjectInspector;
        }
        if (type.equals(DATE)) {
            return javaDateObjectInspector;
        }
        if (type instanceof TimestampType) {
            return javaTimestampObjectInspector;
        }
        if (type instanceof DecimalType decimalType) {
            return getPrimitiveJavaObjectInspector(new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()));
        }
        if (type instanceof ArrayType arrayType) {
            return ObjectInspectorFactory.getStandardListObjectInspector(getJavaObjectInspector(arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            ObjectInspector keyObjectInspector = getJavaObjectInspector(mapType.getKeyType());
            ObjectInspector valueObjectInspector = getJavaObjectInspector(mapType.getValueType());
            return ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
        }
        if (type instanceof RowType rowType) {
            return getStandardStructObjectInspector(
                    rowType.getFields().stream()
                            .map(Field::getName)
                            .map(Optional::orElseThrow)
                            .collect(toList()),
                    rowType.getFields().stream()
                            .map(Field::getType)
                            .map(FormatTestUtils::getJavaObjectInspector)
                            .collect(toList()));
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public static Object decodeRecordReaderValue(Type type, Object actualValue)
    {
        return decodeRecordReaderValue(type, actualValue, Optional.empty());
    }

    public static Object decodeRecordReaderValue(Type type, Object actualValue, Optional<DateTimeZone> hiveStorageTimeZone)
    {
        if (actualValue instanceof LazyObjectBase lazyObject) {
            actualValue = unwrapLazy(lazyObject);
        }
        if (actualValue instanceof Writable writable) {
            actualValue = unwrapWritable(writable);
        }

        if (actualValue instanceof HiveDecimal decimal) {
            DecimalType decimalType = (DecimalType) type;
            // writable messes with the scale so rescale the values to the Trino type
            BigDecimal bigDecimal = decimal.bigDecimalValue();
            bigDecimal = bigDecimal.setScale(decimalType.getScale(), HALF_UP);
            if (bigDecimal.precision() > decimalType.getPrecision()) {
                throw new IllegalArgumentException("decimal precision larger than column precision");
            }
            return new SqlDecimal(bigDecimal.unscaledValue(), decimalType.getPrecision(), decimalType.getScale());
        }

        if (actualValue instanceof Date date) {
            return new SqlDate(date.toEpochDay());
        }
        if (actualValue instanceof Timestamp timestamp) {
            return decodeRecordReaderTimestamp((TimestampType) type, hiveStorageTimeZone, timestamp);
        }

        if (actualValue instanceof HiveVarchar varchar) {
            return varchar.getValue();
        }
        if (actualValue instanceof HiveChar varchar) {
            return varchar.getValue();
        }
        if (actualValue instanceof Text) {
            return actualValue.toString();
        }
        if (actualValue instanceof byte[] bytes && type == VARBINARY) {
            return new SqlVarbinary(bytes);
        }

        if (actualValue instanceof StructObject structObject) {
            return decodeRecordReaderStruct(type, structObject.getFieldsAsList(), hiveStorageTimeZone);
        }
        if (actualValue instanceof List) {
            return decodeRecordReaderList(type, ((List<?>) actualValue), hiveStorageTimeZone);
        }
        if (actualValue instanceof Map) {
            return decodeRecordReaderMap(type, (Map<?, ?>) actualValue, hiveStorageTimeZone);
        }
        return actualValue;
    }

    private static SqlTimestamp decodeRecordReaderTimestamp(TimestampType timestampType, Optional<DateTimeZone> hiveStorageTimeZone, Timestamp timestamp)
    {
        if (hiveStorageTimeZone.isPresent()) {
            long millis = timestamp.toEpochMilli();
            millis = hiveStorageTimeZone.get().convertUTCToLocal(millis);
            return sqlTimestampOf(3, millis);
        }
        return SqlTimestamp.fromSeconds(timestampType.getPrecision(), timestamp.toEpochSecond(), timestamp.getNanos());
    }

    private static List<Object> decodeRecordReaderList(Type type, List<?> list, Optional<DateTimeZone> hiveStorageTimeZone)
    {
        Type elementType = type.getTypeParameters().get(0);
        return list.stream()
                .map(element -> decodeRecordReaderValue(elementType, element, hiveStorageTimeZone))
                .toList();
    }

    private static Object decodeRecordReaderMap(Type type, Map<?, ?> map, Optional<DateTimeZone> hiveStorageTimeZone)
    {
        Type keyType = type.getTypeParameters().get(0);
        Type valueType = type.getTypeParameters().get(1);
        Map<Object, Object> newMap = new HashMap<>();
        map.forEach((entryKey, entryValue) -> newMap.put(
                decodeRecordReaderValue(keyType, entryKey, hiveStorageTimeZone),
                decodeRecordReaderValue(valueType, entryValue, hiveStorageTimeZone)));
        return newMap;
    }

    private static List<Object> decodeRecordReaderStruct(Type type, List<?> fields, Optional<DateTimeZone> hiveStorageTimeZone)
    {
        List<Type> fieldTypes = type.getTypeParameters();
        List<Object> newFields = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Type fieldType = fieldTypes.get(i);
            Object field = fields.get(i);
            newFields.add(decodeRecordReaderValue(fieldType, field, hiveStorageTimeZone));
        }
        return newFields;
    }

    private static Object unwrapLazy(LazyObjectBase lazyObject)
    {
        if (lazyObject instanceof LazyPrimitive) {
            return ((LazyPrimitive<?, ?>) lazyObject).getWritableObject();
        }
        if (lazyObject instanceof LazyBinaryArray lazyBinaryArray) {
            return lazyBinaryArray.getList();
        }
        if (lazyObject instanceof LazyBinaryMap lazyBinaryMap) {
            return lazyBinaryMap.getMap();
        }
        if (lazyObject instanceof LazyBinaryStruct lazyBinaryStruct) {
            return lazyBinaryStruct;
        }
        if (lazyObject instanceof LazyArray lazyArray) {
            return lazyArray.getList();
        }
        if (lazyObject instanceof LazyMap lazyMap) {
            return lazyMap.getMap();
        }
        if (lazyObject instanceof LazyStruct lazyStruct) {
            return lazyStruct;
        }
        throw new IllegalArgumentException("Unsupported lazy type: " + lazyObject.getClass().getSimpleName());
    }

    private static Object unwrapWritable(Writable writable)
    {
        if (writable instanceof HiveDecimalWritable decimalWritable) {
            return decimalWritable.getHiveDecimal();
        }
        if (writable instanceof BooleanWritable booleanWritable) {
            return booleanWritable.get();
        }
        if (writable instanceof ByteWritable byteWritable) {
            return byteWritable.get();
        }
        if (writable instanceof BytesWritable bytesWritable) {
            return bytesWritable.copyBytes();
        }
        if (writable instanceof HiveCharWritable charWritable) {
            return charWritable.getHiveChar();
        }
        if (writable instanceof HiveVarcharWritable varcharWritable) {
            return varcharWritable.getHiveVarchar();
        }
        if (writable instanceof Text text) {
            return text;
        }
        if (writable instanceof DateWritableV2 dateWritable) {
            return dateWritable.get();
        }
        if (writable instanceof DoubleWritable doubleWritable) {
            return doubleWritable.get();
        }
        if (writable instanceof FloatWritable floatWritable) {
            return floatWritable.get();
        }
        if (writable instanceof IntWritable intWritable) {
            return intWritable.get();
        }
        if (writable instanceof LongWritable longWritable) {
            return longWritable.get();
        }
        if (writable instanceof ShortWritable shortWritable) {
            return shortWritable.get();
        }
        if (writable instanceof TimestampWritableV2 timestampWritable) {
            return timestampWritable.getTimestamp();
        }
        throw new IllegalArgumentException("Unsupported writable type: " + writable.getClass().getSimpleName());
    }

    public static void assertColumnValueEquals(Type type, Object actual, Object expected)
    {
        if (actual == null) {
            assertNull(expected);
            return;
        }
        if (type instanceof ArrayType) {
            List<?> actualArray = (List<?>) actual;
            List<?> expectedArray = (List<?>) expected;
            assertEquals(actualArray.size(), expectedArray.size());

            Type elementType = type.getTypeParameters().get(0);
            for (int i = 0; i < actualArray.size(); i++) {
                Object actualElement = actualArray.get(i);
                Object expectedElement = expectedArray.get(i);
                assertColumnValueEquals(elementType, actualElement, expectedElement);
            }
        }
        else if (type instanceof MapType) {
            Map<?, ?> actualMap = (Map<?, ?>) actual;
            Map<?, ?> expectedMap = (Map<?, ?>) expected;
            assertEquals(actualMap.size(), expectedMap.size());

            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);

            List<Entry<?, ?>> expectedEntries = new ArrayList<>(expectedMap.entrySet());
            actualMap.forEach((key, value) -> {
                Iterator<Entry<?, ?>> iterator = expectedEntries.iterator();
                while (iterator.hasNext()) {
                    Entry<?, ?> expectedEntry = iterator.next();
                    try {
                        assertColumnValueEquals(keyType, key, expectedEntry.getKey());
                        assertColumnValueEquals(valueType, value, expectedEntry.getValue());
                        iterator.remove();
                    }
                    catch (AssertionError ignored) {
                    }
                }
            });
            assertThat(expectedEntries).isEmpty();
        }
        else if (type instanceof RowType) {
            List<Type> fieldTypes = type.getTypeParameters();

            List<?> actualRow = (List<?>) actual;
            List<?> expectedRow = (List<?>) expected;
            assertEquals(actualRow.size(), fieldTypes.size());
            assertEquals(actualRow.size(), expectedRow.size());

            for (int fieldId = 0; fieldId < actualRow.size(); fieldId++) {
                Type fieldType = fieldTypes.get(fieldId);
                Object actualElement = actualRow.get(fieldId);
                Object expectedElement = expectedRow.get(fieldId);
                assertColumnValueEquals(fieldType, actualElement, expectedElement);
            }
        }
        else if (type.equals(DOUBLE)) {
            Double actualDouble = (Double) actual;
            Double expectedDouble = (Double) expected;
            assertEquals(actualDouble, expectedDouble, 0.001);
        }
        else if (!Objects.equals(actual, expected)) {
            assertEquals(actual, expected);
        }
    }

    public static List<Object> readTrinoValues(List<Column> columns, Page page, int position)
    {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            values.add(columns.get(i).type().getObjectValue(null, page.getBlock(i), position));
        }
        return values;
    }

    public static Page toSingleRowPage(List<Column> columns, List<?> expectedValues)
    {
        PageBuilder pageBuilder = new PageBuilder(columns.stream().map(Column::type).collect(toImmutableList()));
        pageBuilder.declarePosition();
        for (int index = 0; index < columns.size(); index++) {
            writeTrinoValue(columns.get(index).type(), pageBuilder.getBlockBuilder(index), expectedValues.get(index));
        }
        Page page = pageBuilder.build();
        return page;
    }

    public static void writeTrinoValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (BOOLEAN.equals(type)) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (TINYINT.equals(type)) {
            type.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (SMALLINT.equals(type)) {
            type.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (INTEGER.equals(type)) {
            type.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (BIGINT.equals(type)) {
            type.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (type instanceof DecimalType decimalType) {
            if (decimalType.isShort()) {
                type.writeLong(blockBuilder, ((SqlDecimal) value).toBigDecimal().unscaledValue().longValue());
            }
            else {
                type.writeObject(blockBuilder, Int128.valueOf(((SqlDecimal) value).toBigDecimal().unscaledValue()));
            }
        }
        else if (REAL.equals(type)) {
            type.writeLong(blockBuilder, Float.floatToIntBits((Float) value));
        }
        else if (DOUBLE.equals(type)) {
            type.writeDouble(blockBuilder, ((Number) value).doubleValue());
        }
        else if (type instanceof VarcharType) {
            type.writeSlice(blockBuilder, utf8Slice((String) value));
        }
        else if (type instanceof CharType) {
            type.writeSlice(blockBuilder, trimTrailingSpaces(utf8Slice((String) value)));
        }
        else if (VARBINARY.equals(type)) {
            type.writeSlice(blockBuilder, Slices.wrappedBuffer(((SqlVarbinary) value).getBytes()));
        }
        else if (DATE.equals(type)) {
            long days = ((SqlDate) value).getDays();
            type.writeLong(blockBuilder, days);
        }
        else if (type instanceof TimestampType timestampType) {
            SqlTimestamp sqlTimestamp = (SqlTimestamp) value;
            if (timestampType.isShort()) {
                type.writeLong(blockBuilder, sqlTimestamp.getEpochMicros());
            }
            else {
                type.writeObject(blockBuilder, new LongTimestamp(sqlTimestamp.getEpochMicros(), sqlTimestamp.getPicosOfMicros()));
            }
        }
        else if (type instanceof ArrayType) {
            List<?> array = (List<?>) value;
            Type elementType = type.getTypeParameters().get(0);
            BlockBuilder arrayBlockBuilder = blockBuilder.beginBlockEntry();
            for (Object elementValue : array) {
                writeTrinoValue(elementType, arrayBlockBuilder, elementValue);
            }
            blockBuilder.closeEntry();
        }
        else if (type instanceof MapType) {
            Map<?, ?> map = (Map<?, ?>) value;
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);
            BlockBuilder mapBlockBuilder = blockBuilder.beginBlockEntry();
            map.forEach((entryKey, entryValue) -> {
                writeTrinoValue(keyType, mapBlockBuilder, entryKey);
                writeTrinoValue(valueType, mapBlockBuilder, entryValue);
            });
            blockBuilder.closeEntry();
        }
        else if (type instanceof RowType) {
            List<?> array = (List<?>) value;
            List<Type> fieldTypes = type.getTypeParameters();
            BlockBuilder rowBlockBuilder = blockBuilder.beginBlockEntry();
            for (int fieldId = 0; fieldId < fieldTypes.size(); fieldId++) {
                Type fieldType = fieldTypes.get(fieldId);
                writeTrinoValue(fieldType, rowBlockBuilder, array.get(fieldId));
            }
            blockBuilder.closeEntry();
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public static Object toHiveWriteValue(Type type, Object value, Optional<DateTimeZone> storageTimeZone)
    {
        if (value == null) {
            return null;
        }

        if (type.equals(BOOLEAN)) {
            return value;
        }
        if (type.equals(TINYINT)) {
            return ((Number) value).byteValue();
        }
        if (type.equals(SMALLINT)) {
            return ((Number) value).shortValue();
        }
        if (type.equals(INTEGER)) {
            return ((Number) value).intValue();
        }
        if (type.equals(BIGINT)) {
            return ((Number) value).longValue();
        }
        if (type.equals(REAL)) {
            return ((Number) value).floatValue();
        }
        if (type.equals(DOUBLE)) {
            return ((Number) value).doubleValue();
        }
        if (type instanceof CharType) {
            return value;
        }
        if (type instanceof VarcharType) {
            return value;
        }
        if (type.equals(VARBINARY)) {
            return ((SqlVarbinary) value).getBytes();
        }
        if (type.equals(DATE)) {
            return Date.ofEpochDay(((SqlDate) value).getDays());
        }
        if (type instanceof TimestampType) {
            SqlTimestamp timestampValue = (SqlTimestamp) value;
            if (storageTimeZone.isPresent()) {
                long millis = timestampValue.getMillis();
                millis = storageTimeZone.get().convertLocalToUTC(millis, false);
                return Timestamp.ofEpochMilli(millis);
            }
            LocalDateTime localDateTime = timestampValue.toLocalDateTime();
            return Timestamp.ofEpochSecond(localDateTime.toEpochSecond(UTC), localDateTime.getNano());
        }
        if (type instanceof DecimalType) {
            return HiveDecimal.create(((SqlDecimal) value).toBigDecimal());
        }
        if (type instanceof ArrayType) {
            Type elementType = type.getTypeParameters().get(0);
            return ((List<?>) value).stream()
                    .map(element -> toHiveWriteValue(elementType, element, storageTimeZone))
                    .collect(toList());
        }
        if (type instanceof MapType) {
            Map<?, ?> map = (Map<?, ?>) value;
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);
            Map<Object, Object> newMap = new HashMap<>();
            map.forEach((entryKey, entryValue) -> newMap.put(
                    toHiveWriteValue(keyType, entryKey, storageTimeZone),
                    toHiveWriteValue(valueType, entryValue, storageTimeZone)));
            return newMap;
        }
        if (type instanceof RowType) {
            List<?> fieldValues = (List<?>) value;
            List<Type> fieldTypes = type.getTypeParameters();
            List<Object> newStruct = new ArrayList<>();
            for (int fieldId = 0; fieldId < fieldValues.size(); fieldId++) {
                newStruct.add(toHiveWriteValue(fieldTypes.get(fieldId), fieldValues.get(fieldId), storageTimeZone));
            }
            return newStruct;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public static SqlTimestamp toSqlTimestamp(TimestampType timestampType, LocalDateTime localDateTime)
    {
        if (localDateTime == null) {
            return null;
        }
        DecodedTimestamp decodedTimestamp = new DecodedTimestamp(localDateTime.toEpochSecond(UTC), localDateTime.getNano());
        if (timestampType.isShort()) {
            long micros = (Long) createTimestampEncoder(timestampType, DateTimeZone.UTC).getTimestamp(decodedTimestamp);
            return SqlTimestamp.newInstance(timestampType.getPrecision(), micros, 0);
        }
        LongTimestamp longTimestamp = (LongTimestamp) createTimestampEncoder(timestampType, DateTimeZone.UTC).getTimestamp(decodedTimestamp);
        return SqlTimestamp.newInstance(timestampType.getPrecision(), longTimestamp.getEpochMicros(), longTimestamp.getPicosOfMicro());
    }

    public static LineBuffer createLineBuffer(String value)
            throws IOException
    {
        return createLineBuffer(utf8Slice(value));
    }

    public static LineBuffer createLineBuffer(Slice value)
            throws IOException
    {
        int bufferSize = max(1, value.length());
        LineBuffer lineBuffer = new LineBuffer(bufferSize, bufferSize);
        lineBuffer.write(value.getInput(), value.length());
        return lineBuffer;
    }

    public static boolean isScalarType(Type type)
    {
        return !(type instanceof ArrayType) && !(type instanceof MapType) && !(type instanceof RowType);
    }
}
