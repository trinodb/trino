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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

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
import java.util.UUID;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.util.Timestamps.getTimestampTz;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzFromMicros;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.types.Type.TypeID.FIXED;
import static org.apache.iceberg.util.DateTimeUtil.microsFromTimestamp;
import static org.apache.iceberg.util.DateTimeUtil.microsFromTimestamptz;
import static org.apache.iceberg.util.DateTimeUtil.timeFromMicros;
import static org.apache.iceberg.util.DateTimeUtil.timestampFromMicros;
import static org.apache.iceberg.util.DateTimeUtil.timestamptzFromMicros;

public final class IcebergAvroDataConversion
{
    private IcebergAvroDataConversion() {}

    public static Iterable<Record> toIcebergRecords(Page page, List<Type> types, Schema icebergSchema)
    {
        return () -> new RecordIterator(page, types, icebergSchema);
    }

    private static class RecordIterator
            implements Iterator<Record>
    {
        private final List<Block> columnBlocks;
        private final List<Type> types;
        private final List<org.apache.iceberg.types.Type> icebergTypes;
        private final Schema icebergSchema;
        private final int positionCount;
        private int position;

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

    @Nullable
    public static Object toIcebergAvroObject(Type type, org.apache.iceberg.types.Type icebergType, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type.equals(BOOLEAN)) {
            return BOOLEAN.getBoolean(block, position);
        }
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(block, position);
        }
        if (type.equals(BIGINT)) {
            return BIGINT.getLong(block, position);
        }
        if (type.equals(REAL)) {
            return REAL.getFloat(block, position);
        }
        if (type.equals(DOUBLE)) {
            return DOUBLE.getDouble(block, position);
        }
        if (type instanceof DecimalType decimalType) {
            return Decimals.readBigDecimal(decimalType, block, position);
        }
        if (type instanceof VarcharType varcharType) {
            return varcharType.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof VarbinaryType varbinaryType) {
            if (icebergType.typeId().equals(FIXED)) {
                return varbinaryType.getSlice(block, position).getBytes();
            }
            return ByteBuffer.wrap(varbinaryType.getSlice(block, position).getBytes());
        }
        if (type.equals(DATE)) {
            int epochDays = DATE.getInt(block, position);
            return LocalDate.ofEpochDay(epochDays);
        }
        if (type.equals(TIME_MICROS)) {
            long microsOfDay = TIME_MICROS.getLong(block, position) / PICOSECONDS_PER_MICROSECOND;
            return timeFromMicros(microsOfDay);
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            long epochMicros = TIMESTAMP_MICROS.getLong(block, position);
            return timestampFromMicros(epochMicros);
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            long epochUtcMicros = timestampTzToMicros(getTimestampTz(block, position));
            return timestamptzFromMicros(epochUtcMicros);
        }
        if (type.equals(UUID)) {
            return trinoUuidToJavaUuid(UUID.getSlice(block, position));
        }
        if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();
            org.apache.iceberg.types.Type elementIcebergType = icebergType.asListType().elementType();

            Block arrayBlock = block.getObject(position, Block.class);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object element = toIcebergAvroObject(elementType, elementIcebergType, arrayBlock, i);
                list.add(element);
            }

            return Collections.unmodifiableList(list);
        }
        if (type instanceof MapType mapType) {
            Type keyType = mapType.getKeyType();
            Type valueType = mapType.getValueType();
            org.apache.iceberg.types.Type keyIcebergType = icebergType.asMapType().keyType();
            org.apache.iceberg.types.Type valueIcebergType = icebergType.asMapType().valueType();

            Block mapBlock = block.getObject(position, Block.class);
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                Object key = toIcebergAvroObject(keyType, keyIcebergType, mapBlock, i);
                Object value = toIcebergAvroObject(valueType, valueIcebergType, mapBlock, i + 1);
                map.put(key, value);
            }

            return Collections.unmodifiableMap(map);
        }
        if (type instanceof RowType rowType) {
            Block rowBlock = block.getObject(position, Block.class);

            List<Type> fieldTypes = rowType.getTypeParameters();
            checkArgument(fieldTypes.size() == rowBlock.getPositionCount(), "Expected row value field count does not match type field count");
            List<Types.NestedField> icebergFields = icebergType.asStructType().fields();

            Record record = GenericRecord.create(icebergType.asStructType());
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                Object element = toIcebergAvroObject(fieldTypes.get(i), icebergFields.get(i).type(), rowBlock, i);
                record.set(i, element);
            }

            return record;
        }
        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    public static void serializeToTrinoBlock(Type type, org.apache.iceberg.types.Type icebergType, BlockBuilder builder, Object object)
    {
        if (object == null) {
            builder.appendNull();
            return;
        }
        if (type.equals(BOOLEAN)) {
            BOOLEAN.writeBoolean(builder, (boolean) object);
            return;
        }
        if (type.equals(INTEGER)) {
            INTEGER.writeLong(builder, (int) object);
            return;
        }
        if (type.equals(BIGINT)) {
            BIGINT.writeLong(builder, (long) object);
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
        if (type instanceof DecimalType decimalType) {
            BigDecimal decimal = (BigDecimal) object;
            BigInteger unscaledValue = decimal.unscaledValue();
            if (decimalType.isShort()) {
                decimalType.writeLong(builder, unscaledValue.longValue());
            }
            else {
                decimalType.writeObject(builder, Int128.valueOf(unscaledValue));
            }
            return;
        }
        if (type instanceof VarcharType) {
            type.writeSlice(builder, Slices.utf8Slice((String) object));
            return;
        }
        if (type instanceof VarbinaryType) {
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
            type.writeObject(builder, timestampTzFromMicros(epochUtcMicros));
            return;
        }
        if (type.equals(UUID)) {
            type.writeSlice(builder, javaUuidToTrinoUuid((UUID) object));
            return;
        }
        if (type instanceof ArrayType) {
            Collection<?> array = (Collection<?>) object;
            Type elementType = ((ArrayType) type).getElementType();
            org.apache.iceberg.types.Type elementIcebergType = icebergType.asListType().elementType();
            BlockBuilder currentBuilder = builder.beginBlockEntry();
            for (Object element : array) {
                serializeToTrinoBlock(elementType, elementIcebergType, currentBuilder, element);
            }
            builder.closeEntry();
            return;
        }
        if (type instanceof MapType) {
            Map<?, ?> map = (Map<?, ?>) object;
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();
            org.apache.iceberg.types.Type keyIcebergType = icebergType.asMapType().keyType();
            org.apache.iceberg.types.Type valueIcebergType = icebergType.asMapType().valueType();
            BlockBuilder currentBuilder = builder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                serializeToTrinoBlock(keyType, keyIcebergType, currentBuilder, entry.getKey());
                serializeToTrinoBlock(valueType, valueIcebergType, currentBuilder, entry.getValue());
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
                serializeToTrinoBlock(typeParameters.get(i), icebergFields.get(i).type(), currentBuilder, record.get(i));
            }
            builder.closeEntry();
            return;
        }
        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }
}
