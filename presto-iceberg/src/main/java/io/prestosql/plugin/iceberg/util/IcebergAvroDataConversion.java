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
package io.prestosql.plugin.iceberg.util;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.util.HiveUtil.checkCondition;
import static io.prestosql.plugin.iceberg.util.Timestamps.getTimestampTz;
import static io.prestosql.plugin.iceberg.util.Timestamps.timestampTzFromMicros;
import static io.prestosql.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.encodeUnscaledValue;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeType.TIME_MICROS;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.types.Type.TypeID.FIXED;
import static org.apache.iceberg.util.DateTimeUtil.microsFromTimestamp;
import static org.apache.iceberg.util.DateTimeUtil.microsFromTimestamptz;
import static org.apache.iceberg.util.DateTimeUtil.timeFromMicros;
import static org.apache.iceberg.util.DateTimeUtil.timestampFromMicros;
import static org.apache.iceberg.util.DateTimeUtil.timestamptzFromMicros;

public final class IcebergAvroDataConversion
{
    private IcebergAvroDataConversion()
    {
    }

    public static Iterable<Record> toIcebergRecords(Page page, List<Type> types, Schema icebergSchema)
    {
        return () -> new RecordIterator(page, types, icebergSchema);
    }

    private static class RecordIterator
            implements Iterator<Record>
    {
        private List<Block> columnBlocks;
        private List<Type> types;
        private List<org.apache.iceberg.types.Type> icebergTypes;
        private Schema icebergSchema;
        private int position;
        private int positionCount;

        public RecordIterator(Page page, List<Type> types, Schema icebergSchema)
        {
            requireNonNull(page, "page is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.icebergSchema = requireNonNull(icebergSchema, "icebergSchema is null");
            checkArgument(page.getChannelCount() == types.size(), "the page's channel count must be equal to the size of types");
            checkArgument(types.size() == icebergSchema.columns().size(), "the size of types must be equal to the number of columns in icebergSchema");
            icebergTypes = icebergSchema.columns().stream()
                    .map(Types.NestedField::type)
                    .collect(toImmutableList());
            columnBlocks = IntStream.range(0, types.size())
                    .mapToObj(page::getBlock)
                    .collect(toImmutableList());
            positionCount = page.getPositionCount();
        }

        @Override
        public boolean hasNext()
        {
            return position < positionCount;
        }

        @Override
        public Record next()
        {
            Record record = GenericRecord.create(icebergSchema);
            for (int channel = 0; channel < types.size(); channel++) {
                Object element = toIcebergAvroObject(types.get(channel), icebergTypes.get(channel), columnBlocks.get(channel), position);
                record.set(channel, element);
            }
            position++;
            return record;
        }
    }

    public static Object toIcebergAvroObject(Type type, org.apache.iceberg.types.Type icebergType, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type.equals(BOOLEAN)) {
            return type.getBoolean(block, position);
        }
        if (type.equals(BIGINT)) {
            return type.getLong(block, position);
        }
        if (type.equals(INTEGER)) {
            return toIntExact(type.getLong(block, position));
        }
        if (type.equals(REAL)) {
            return intBitsToFloat((int) type.getLong(block, position));
        }
        if (type.equals(DOUBLE)) {
            return type.getDouble(block, position);
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            BigInteger unscaledValue;
            if (decimalType.isShort()) {
                unscaledValue = BigInteger.valueOf(decimalType.getLong(block, position));
            }
            else {
                unscaledValue = Decimals.decodeUnscaledValue(decimalType.getSlice(block, position));
            }
            return new BigDecimal(unscaledValue, decimalType.getScale());
        }
        if (type.equals(VARCHAR)) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (type.equals(VARBINARY)) {
            if (icebergType.typeId().equals(FIXED)) {
                return type.getSlice(block, position).getBytes();
            }
            return ByteBuffer.wrap(type.getSlice(block, position).getBytes());
        }
        if (type.equals(DATE)) {
            long epochDays = type.getLong(block, position);
            return LocalDate.ofEpochDay(epochDays);
        }
        if (type.equals(TIME_MICROS)) {
            long microsOfDay = type.getLong(block, position) / PICOSECONDS_PER_MICROSECOND;
            return timeFromMicros(microsOfDay);
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            long epochMicros = type.getLong(block, position);
            return timestampFromMicros(epochMicros);
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            long epochUtcMicros = timestampTzToMicros(getTimestampTz(block, position));
            return timestamptzFromMicros(epochUtcMicros);
        }
        if (type instanceof ArrayType) {
            Type elementType = type.getTypeParameters().get(0);
            org.apache.iceberg.types.Type elementIcebergType = icebergType.asListType().elementType();

            Block arrayBlock = block.getObject(position, Block.class);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object element = toIcebergAvroObject(elementType, elementIcebergType, arrayBlock, i);
                list.add(element);
            }

            return Collections.unmodifiableList(list);
        }
        if (type instanceof MapType) {
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);
            org.apache.iceberg.types.Type keyIcebergtype = icebergType.asMapType().keyType();
            org.apache.iceberg.types.Type valueIcebergtype = icebergType.asMapType().valueType();

            Block mapBlock = block.getObject(position, Block.class);
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                Object key = toIcebergAvroObject(keyType, keyIcebergtype, mapBlock, i);
                Object value = toIcebergAvroObject(valueType, valueIcebergtype, mapBlock, i + 1);
                map.put(key, value);
            }

            return Collections.unmodifiableMap(map);
        }
        if (type instanceof RowType) {
            Block rowBlock = block.getObject(position, Block.class);

            List<Type> fieldTypes = type.getTypeParameters();
            checkCondition(fieldTypes.size() == rowBlock.getPositionCount(), GENERIC_INTERNAL_ERROR, "Expected row value field count does not match type field count");
            List<Types.NestedField> icebergFields = icebergType.asStructType().fields();

            Record record = GenericRecord.create(icebergType.asStructType());
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                Object element = toIcebergAvroObject(fieldTypes.get(i), icebergFields.get(i).type(), rowBlock, i);
                record.set(i, element);
            }

            return record;
        }
        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    public static void serializeToPrestoObject(Type type, org.apache.iceberg.types.Type icebergType, BlockBuilder builder, Object object, TimeZoneKey timeZoneKey)
    {
        if (object == null) {
            builder.appendNull();
            return;
        }
        if (type.equals(BOOLEAN)) {
            BOOLEAN.writeBoolean(builder, (boolean) object);
            return;
        }
        if (type.equals(BIGINT)) {
            BIGINT.writeLong(builder, (long) object);
            return;
        }
        if (type.equals(INTEGER)) {
            INTEGER.writeLong(builder, (int) object);
            return;
        }
        if (type.equals(REAL)) {
            REAL.writeLong(builder, floatToRawIntBits((float) object));
            return;
        }
        if (type.equals(DOUBLE)) {
            DOUBLE.writeDouble(builder, (double) object);
            return;
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            BigDecimal decimal = (BigDecimal) object;
            BigInteger unscaledValue = decimal.unscaledValue();
            if (decimalType.isShort()) {
                decimalType.writeLong(builder, unscaledValue.longValue());
            }
            else {
                decimalType.writeSlice(builder, encodeUnscaledValue(unscaledValue));
            }
            return;
        }
        if (type.equals(VARCHAR)) {
            type.writeSlice(builder, Slices.utf8Slice((String) object));
            return;
        }
        if (type.equals(VARBINARY)) {
            if (icebergType.typeId().equals(FIXED)) {
                VARBINARY.writeSlice(builder, Slices.wrappedBuffer((byte[]) object));
            }
            VARBINARY.writeSlice(builder, Slices.wrappedBuffer((ByteBuffer) object));
            return;
        }
        if (type.equals(DATE)) {
            DATE.writeLong(builder, ((LocalDate) object).toEpochDay());
            return;
        }
        if (type.equals(TIME_MICROS)) {
            type.writeLong(builder, ((LocalTime) object).toNanoOfDay() * PICOSECONDS_PER_NANOSECOND);
            return;
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            long epochMicros = microsFromTimestamp((LocalDateTime) object);
            type.writeLong(builder, epochMicros);
            return;
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            long epochUtcMicros = microsFromTimestamptz((OffsetDateTime) object);
            type.writeObject(builder, timestampTzFromMicros(epochUtcMicros, timeZoneKey));
            return;
        }
        if (type instanceof ArrayType) {
            Collection<?> array = (Collection<?>) object;
            Type elementType = ((ArrayType) type).getElementType();
            org.apache.iceberg.types.Type elementIcebergType = icebergType.asListType().elementType();
            BlockBuilder currentBuilder = builder.beginBlockEntry();
            for (Object element : array) {
                serializeToPrestoObject(elementType, elementIcebergType, currentBuilder, element, timeZoneKey);
            }
            builder.closeEntry();
            return;
        }
        if (type instanceof MapType) {
            Map<?, ?> map = (Map<?, ?>) object;
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();
            org.apache.iceberg.types.Type keyIcebergtype = icebergType.asMapType().keyType();
            org.apache.iceberg.types.Type valueIcebergtype = icebergType.asMapType().valueType();
            BlockBuilder currentBuilder = builder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                serializeToPrestoObject(keyType, keyIcebergtype, currentBuilder, entry.getKey(), timeZoneKey);
                serializeToPrestoObject(valueType, valueIcebergtype, currentBuilder, entry.getValue(), timeZoneKey);
            }
            builder.closeEntry();
            return;
        }
        if (type instanceof RowType) {
            Record record = (Record) object;
            List<Type> typeParameters = type.getTypeParameters();
            List<Types.NestedField> icebergFields = icebergType.asStructType().fields();
            BlockBuilder currentBuilder = builder.beginBlockEntry();
            for (int i = 0; i < typeParameters.size(); i++) {
                serializeToPrestoObject(typeParameters.get(i), icebergFields.get(1).type(), currentBuilder, record.get(i), timeZoneKey);
            }
            builder.closeEntry();
            return;
        }
        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }
}
