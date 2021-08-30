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
package io.trino.plugin.pulsar.decoder.avro;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.avro.AvroColumnDecoder;
import io.trino.plugin.pulsar.PulsarConnectorUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericEnumSymbol;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericFixed;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Extended {@link io.trino.decoder.avro.AvroColumnDecoder} with support for additional types:
 * 1) support {@link TimestampType},{@link DateType}DATE, {@link TimeType}.
 * 2) support {@link RealType}.
 */
public class PulsarAvroColumnDecoder
            extends AvroColumnDecoder
{
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BooleanType.BOOLEAN,
            TinyintType.TINYINT,
            SmallintType.SMALLINT,
            IntegerType.INTEGER,
            BigintType.BIGINT,
            RealType.REAL,
            DoubleType.DOUBLE,
            TIMESTAMP_MILLIS,
            DateType.DATE,
            TIME_MILLIS,
            VarbinaryType.VARBINARY);

    private final Type pulsarColumnType;
    private final String pulsarColumnMapping;
    private final String pulsarColumnName;

    public PulsarAvroColumnDecoder(DecoderColumnHandle columnHandle)
    {
        super(columnHandle);
        try {
            requireNonNull(columnHandle, "columnHandle is null");
            this.pulsarColumnType = columnHandle.getType();
            this.pulsarColumnMapping = columnHandle.getMapping();
            this.pulsarColumnName = columnHandle.getName();
            checkArgument(!columnHandle.isInternal(), "unexpected internal column '%s'", pulsarColumnName);
            checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), pulsarColumnName);
            checkArgument(columnHandle.getDataFormat() == null, "unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), pulsarColumnName);
            checkArgument(columnHandle.getMapping() != null, "mapping not defined for column '%s'", pulsarColumnName);
            checkArgument(isSupportedType(pulsarColumnType), "Unsupported column type '%s' for column '%s'", pulsarColumnType, pulsarColumnName);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_USER_ERROR, e);
        }
    }

    protected boolean isSupportedPrimitive(Type type)
    {
        return type instanceof VarcharType || SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    public FieldValueProvider decodeField(GenericRecord avroRecord)
    {
        Object avroColumnValue = locateNode(avroRecord, pulsarColumnMapping);
        return new ObjectValueProvider(avroColumnValue, pulsarColumnType, pulsarColumnName);
    }

    private static Object locateNode(GenericRecord avroRecord, String columnMapping)
    {
        Object value = avroRecord;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(columnMapping)) {
            if (value == null) {
                return null;
            }
            value = ((GenericRecord) value).get(pathElement);
        }
        return value;
    }

    private static class ObjectValueProvider
            extends FieldValueProvider
    {
        private final Object value;
        private final Type columnType;
        private final String columnName;

        public ObjectValueProvider(Object value, Type columnType, String columnName)
        {
            this.value = value;
            this.columnType = columnType;
            this.columnName = columnName;
        }

        @Override
        public boolean isNull()
        {
            return value == null;
        }

        @Override
        public double getDouble()
        {
            if (value instanceof Double || value instanceof Float) {
                return ((Number) value).doubleValue();
            }
            throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), columnType, columnName));
        }

        @Override
        public boolean getBoolean()
        {
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), columnType, columnName));
        }

        @Override
        public long getLong()
        {
            if (value instanceof Long || value instanceof Integer) {
                return columnType == TimestampType.TIMESTAMP_MILLIS ?
                        PulsarConnectorUtils.roundToTrinoTime(((Number) value).longValue()) : ((Number) value).longValue();
            }

            if (columnType instanceof RealType) {
                return floatToIntBits((Float) value);
            }

            throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), columnType, columnName));
        }

        @Override
        public Slice getSlice()
        {
            return AvroColumnDecoder.getSlice(value, columnType, columnName);
        }

        @Override
        public Block getBlock()
        {
            return serializeObject(null, value, columnType, columnName);
        }
    }

    protected static Slice getSlice(Object value, Type type, String columnName)
    {
        if (type instanceof VarcharType && (value instanceof CharSequence || value instanceof GenericEnumSymbol)) {
            return truncateToLength(utf8Slice(value.toString()), type);
        }

        if (type instanceof VarbinaryType) {
            if (value instanceof ByteBuffer) {
                return Slices.wrappedBuffer((ByteBuffer) value);
            }
            else if (value instanceof GenericFixed) {
                return Slices.wrappedBuffer(((GenericFixed) value).bytes());
            }
        }

        throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), type, columnName));
    }

    protected static Block serializeObject(BlockBuilder builder, Object value, Type type, String columnName)
    {
        if (type instanceof ArrayType) {
            return serializeList(builder, value, type, columnName);
        }
        if (type instanceof MapType) {
            return serializeMap(builder, value, type, columnName);
        }
        if (type instanceof RowType) {
            return serializeRow(builder, value, type, columnName);
        }
        serializePrimitive(builder, value, type, columnName);
        return null;
    }

    protected static Block serializeList(BlockBuilder parentBlockBuilder, Object value, Type type, String columnName)
    {
        if (value == null) {
            checkState(parentBlockBuilder != null, "parentBlockBuilder is null");
            parentBlockBuilder.appendNull();
            return null;
        }
        List<?> list = (List<?>) value;
        List<Type> typeParameters = type.getTypeParameters();
        Type elementType = typeParameters.get(0);

        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, list.size());
        for (Object element : list) {
            serializeObject(blockBuilder, element, elementType, columnName);
        }
        if (parentBlockBuilder != null) {
            type.writeObject(parentBlockBuilder, blockBuilder.build());
            return null;
        }
        return blockBuilder.build();
    }

    protected static void serializePrimitive(BlockBuilder blockBuilder, Object value, Type type, String columnName)
    {
        requireNonNull(blockBuilder, "parent blockBuilder is null");

        if (value == null) {
            blockBuilder.appendNull();
            return;
        }

        if (type instanceof BooleanType) {
            type.writeBoolean(blockBuilder, (Boolean) value);
            return;
        }

        if ((value instanceof Integer || value instanceof Long)
                && (type instanceof BigintType || type instanceof IntegerType
                || type instanceof SmallintType || type instanceof TinyintType)) {
            type.writeLong(blockBuilder, ((Number) value).longValue());
            return;
        }

        if (type instanceof DoubleType) {
            type.writeDouble(blockBuilder, (Double) value);
            return;
        }

        if (type instanceof RealType) {
            type.writeLong(blockBuilder, floatToIntBits((Float) value));
            return;
        }

        if (type instanceof VarcharType || type instanceof VarbinaryType) {
            type.writeSlice(blockBuilder, getSlice(value, type, columnName));
            return;
        }

        if (type instanceof TimeType) {
            type.writeLong(blockBuilder, (Long) value);
            return;
        }

        if (type instanceof TimestampType) {
            type.writeLong(blockBuilder, (Long) value);
            return;
        }

        throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), type, columnName));
    }

    protected static Block serializeMap(BlockBuilder parentBlockBuilder, Object value, Type type, String columnName)
    {
        if (value == null) {
            checkState(parentBlockBuilder != null, "parentBlockBuilder is null");
            parentBlockBuilder.appendNull();
            return null;
        }

        Map<?, ?> map = (Map<?, ?>) value;
        List<Type> typeParameters = type.getTypeParameters();
        Type keyType = typeParameters.get(0);
        Type valueType = typeParameters.get(1);

        BlockBuilder blockBuilder;
        if (parentBlockBuilder != null) {
            blockBuilder = parentBlockBuilder;
        }
        else {
            blockBuilder = type.createBlockBuilder(null, 1);
        }

        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (entry.getKey() != null) {
                keyType.writeSlice(entryBuilder, truncateToLength(utf8Slice(entry.getKey().toString()), keyType));
                serializeObject(entryBuilder, entry.getValue(), valueType, columnName);
            }
        }
        blockBuilder.closeEntry();

        if (parentBlockBuilder == null) {
            return blockBuilder.getObject(0, Block.class);
        }
        return null;
    }

    protected static Block serializeRow(BlockBuilder parentBlockBuilder, Object value, Type type, String columnName)
    {
        if (value == null) {
            checkState(parentBlockBuilder != null, "parent block builder is null");
            parentBlockBuilder.appendNull();
            return null;
        }

        BlockBuilder blockBuilder;
        if (parentBlockBuilder != null) {
            blockBuilder = parentBlockBuilder;
        }
        else {
            blockBuilder = type.createBlockBuilder(null, 1);
        }
        BlockBuilder singleRowBuilder = blockBuilder.beginBlockEntry();
        GenericRecord record = (GenericRecord) value;
        List<Field> fields = ((RowType) type).getFields();
        for (Field field : fields) {
            checkState(field.getName().isPresent(), "field name not found");
            serializeObject(singleRowBuilder, record.get(field.getName().get()), field.getType(), columnName);
        }
        blockBuilder.closeEntry();
        if (parentBlockBuilder == null) {
            return blockBuilder.getObject(0, Block.class);
        }
        return null;
    }
}
