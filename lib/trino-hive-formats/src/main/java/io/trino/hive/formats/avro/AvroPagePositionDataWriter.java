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
package io.trino.hive.formats.avro;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.SingleRowBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.function.ToIntBiFunction;
import java.util.function.ToLongBiFunction;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.avro.AvroTypeUtils.SimpleUnionNullIndex;
import static io.trino.hive.formats.avro.AvroTypeUtils.getSimpleNullableUnionNullIndex;
import static io.trino.hive.formats.avro.AvroTypeUtils.isSimpleNullableUnion;
import static io.trino.hive.formats.avro.AvroTypeUtils.unwrapNullableUnion;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class AvroPagePositionDataWriter
        implements DatumWriter<Integer>
{
    private Page page;
    private final Schema schema;
    private final RecordBlockPositionEncoder pageBlockPositionEncoder;

    public AvroPagePositionDataWriter(Schema schema, AvroTypeManager avroTypeManager, List<String> channelNames, List<Type> channelTypes)
            throws AvroTypeException
    {
        this.schema = requireNonNull(schema, "schema is null");
        pageBlockPositionEncoder = new RecordBlockPositionEncoder(schema, avroTypeManager, channelNames, channelTypes);
        checkInvariants();
    }

    @Override
    public void setSchema(Schema schema)
    {
        verify(this.schema == requireNonNull(schema, "schema is null"), "Unable to change schema for this data writer");
    }

    public void setPage(Page page)
    {
        this.page = requireNonNull(page, "page is null");
        checkInvariants();
        pageBlockPositionEncoder.setChannelBlocksFromPage(page);
    }

    private void checkInvariants()
    {
        verify(schema.getType() == Schema.Type.RECORD, "Can only write pages to record schema");
        verify(page == null || page.getChannelCount() == schema.getFields().size(), "Page channel count must equal schema field count");
    }

    @Override
    public void write(Integer position, Encoder encoder)
            throws IOException
    {
        checkWritable();
        if (position >= page.getPositionCount()) {
            throw new IndexOutOfBoundsException("Position %s not within page with position count %s".formatted(position, page.getPositionCount()));
        }
        pageBlockPositionEncoder.encodePositionInEachChannel(position, encoder);
    }

    private void checkWritable()
    {
        checkState(page != null, "page must be set before beginning to write positions");
    }

    private abstract static class BlockPositionEncoder
    {
        protected Block block;
        private final Optional<SimpleUnionNullIndex> nullIndex;

        public BlockPositionEncoder(Optional<SimpleUnionNullIndex> nullIndex)
        {
            this.nullIndex = requireNonNull(nullIndex, "nullIdx is null");
        }

        abstract void encodeFromBlock(int position, Encoder encoder)
                throws IOException;

        void encode(int position, Encoder encoder)
                throws IOException
        {
            checkState(block != null, "block must be set before calling encode");
            boolean isNull = block.isNull(position);
            if (isNull && nullIndex.isEmpty()) {
                throw new IOException("Can not write null value for non-nullable schema");
            }
            if (nullIndex.isPresent()) {
                encoder.writeIndex(isNull ? nullIndex.get().getIndex() : 1 ^ nullIndex.get().getIndex());
            }
            if (isNull) {
                encoder.writeNull();
            }
            else {
                encodeFromBlock(position, encoder);
            }
        }

        void setBlock(Block block)
        {
            this.block = block;
        }
    }

    private static BlockPositionEncoder createBlockPositionEncoder(Schema schema, AvroTypeManager avroTypeManager, Type type)
            throws AvroTypeException
    {
        return createBlockPositionEncoder(schema, avroTypeManager, type, Optional.empty());
    }

    private static BlockPositionEncoder createBlockPositionEncoder(Schema schema, AvroTypeManager avroTypeManager, Type type, Optional<SimpleUnionNullIndex> nullIdx)
            throws AvroTypeException
    {
        Optional<BiFunction<Block, Integer, Object>> overrideToAvroGenericObject = avroTypeManager.overrideBlockToAvroObject(schema, type);
        if (overrideToAvroGenericObject.isPresent()) {
            return new UserDefinedBlockPositionEncoder(nullIdx, schema, overrideToAvroGenericObject.get());
        }
        switch (schema.getType()) {
            case NULL -> throw new AvroTypeException("No null support outside of union");
            case BOOLEAN -> {
                if (BOOLEAN.equals(type)) {
                    return new BooleanBlockPositionEncoder(nullIdx);
                }
            }
            case INT -> {
                if (TINYINT.equals(type)) {
                    return new IntBlockPositionEncoder(nullIdx, TINYINT::getByte);
                }
                if (SMALLINT.equals(type)) {
                    return new IntBlockPositionEncoder(nullIdx, SMALLINT::getShort);
                }
                if (INTEGER.equals(type)) {
                    return new IntBlockPositionEncoder(nullIdx, INTEGER::getInt);
                }
            }
            case LONG -> {
                if (TINYINT.equals(type)) {
                    return new LongBlockPositionEncoder(nullIdx, TINYINT::getByte);
                }
                if (SMALLINT.equals(type)) {
                    return new LongBlockPositionEncoder(nullIdx, SMALLINT::getShort);
                }
                if (INTEGER.equals(type)) {
                    return new LongBlockPositionEncoder(nullIdx, INTEGER::getInt);
                }
                if (BIGINT.equals(type)) {
                    return new LongBlockPositionEncoder(nullIdx, BIGINT::getLong);
                }
            }
            case FLOAT -> {
                if (REAL.equals(type)) {
                    return new FloatBlockPositionEncoder(nullIdx);
                }
            }
            case DOUBLE -> {
                if (DOUBLE.equals(type)) {
                    return new DoubleBlockPositionEncoder(nullIdx);
                }
            }
            case STRING -> {
                if (VARCHAR.equals(type)) {
                    return new StringOrBytesPositionEncoder(nullIdx);
                }
            }
            case BYTES -> {
                if (VarbinaryType.VARBINARY.equals(type)) {
                    return new StringOrBytesPositionEncoder(nullIdx);
                }
            }
            case FIXED -> {
                if (VarbinaryType.VARBINARY.equals(type)) {
                    return new FixedBlockPositionEncoder(nullIdx, schema.getFixedSize());
                }
            }
            case ENUM -> {
                if (VARCHAR.equals(type)) {
                    return new EnumBlockPositionEncoder(nullIdx, schema.getEnumSymbols());
                }
            }
            case ARRAY -> {
                if (type instanceof ArrayType arrayType) {
                    return new ArrayBlockPositionEncoder(nullIdx, schema, avroTypeManager, arrayType);
                }
            }
            case MAP -> {
                if (type instanceof MapType mapType) {
                    return new MapBlockPositionEncoder(nullIdx, schema, avroTypeManager, mapType);
                }
            }
            case RECORD -> {
                if (type instanceof RowType rowType) {
                    return new RecordBlockPositionEncoder(nullIdx, schema, avroTypeManager, rowType);
                }
            }
            case UNION -> {
                if (isSimpleNullableUnion(schema)) {
                    return createBlockPositionEncoder(unwrapNullableUnion(schema), avroTypeManager, type, Optional.of(getSimpleNullableUnionNullIndex(schema)));
                }
                else {
                    throw new AvroTypeException("Unable to make writer for schema with non simple nullable union %s".formatted(schema));
                }
            }
        }
        throw new AvroTypeException("Schema and Trino Type mismatch between %s and %s".formatted(schema, type));
    }

    private static class BooleanBlockPositionEncoder
            extends BlockPositionEncoder
    {
        public BooleanBlockPositionEncoder(Optional<SimpleUnionNullIndex> isNullWithIndex)
        {
            super(isNullWithIndex);
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            encoder.writeBoolean(BOOLEAN.getBoolean(block, position));
        }
    }

    private static class IntBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private final ToIntBiFunction<Block, Integer> getInt;

        public IntBlockPositionEncoder(Optional<SimpleUnionNullIndex> isNullWithIndex, ToIntBiFunction<Block, Integer> getInt)
        {
            super(isNullWithIndex);
            this.getInt = requireNonNull(getInt, "getInt is null");
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            encoder.writeInt(getInt.applyAsInt(block, position));
        }
    }

    private static class LongBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private final ToLongBiFunction<Block, Integer> getLong;

        public LongBlockPositionEncoder(Optional<SimpleUnionNullIndex> isNullWithIndex, ToLongBiFunction<Block, Integer> getLong)
        {
            super(isNullWithIndex);
            this.getLong = requireNonNull(getLong, "getLong is null");
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            encoder.writeLong(getLong.applyAsLong(block, position));
        }
    }

    private static class FloatBlockPositionEncoder
            extends BlockPositionEncoder
    {
        public FloatBlockPositionEncoder(Optional<SimpleUnionNullIndex> isNullWithIndex)
        {
            super(isNullWithIndex);
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            encoder.writeFloat(REAL.getFloat(block, position));
        }
    }

    private static class DoubleBlockPositionEncoder
            extends BlockPositionEncoder
    {
        public DoubleBlockPositionEncoder(Optional<SimpleUnionNullIndex> isNullWithIndex)
        {
            super(isNullWithIndex);
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            encoder.writeDouble(DOUBLE.getDouble(block, position));
        }
    }

    private static class StringOrBytesPositionEncoder
            extends BlockPositionEncoder
    {
        public StringOrBytesPositionEncoder(Optional<SimpleUnionNullIndex> isNullWithIndex)
        {
            super(isNullWithIndex);
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            int length = block.getSliceLength(position);
            encoder.writeLong(length);
            encoder.writeFixed(block.getSlice(position, 0, length).getBytes());
        }
    }

    private static class FixedBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private final int fixedSize;

        public FixedBlockPositionEncoder(Optional<SimpleUnionNullIndex> nullIdx, int fixedSize)
        {
            super(nullIdx);
            this.fixedSize = fixedSize;
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            int length = block.getSliceLength(position);
            if (length != fixedSize) {
                throw new IOException("Unable to write Avro fixed with size %s from slice of length %s".formatted(fixedSize, length));
            }
            encoder.writeFixed(block.getSlice(position, 0, length).getBytes());
        }
    }

    private static class EnumBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private final Map<Slice, Integer> symbolToIndex;

        public EnumBlockPositionEncoder(Optional<SimpleUnionNullIndex> nullIdx, List<String> symbols)
        {
            super(nullIdx);
            ImmutableMap.Builder<Slice, Integer> symbolToIndex = ImmutableMap.builder();
            for (int i = 0; i < symbols.size(); i++) {
                symbolToIndex.put(Slices.utf8Slice(symbols.get(i)), i);
            }
            this.symbolToIndex = symbolToIndex.buildOrThrow();
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            int length = block.getSliceLength(position);
            Integer symbolIndex = symbolToIndex.get(block.getSlice(position, 0, length));
            if (symbolIndex == null) {
                throw new IOException("Unable to write Avro Enum symbol %s. Not found in set %s".formatted(
                        block.getSlice(position, 0, length).toStringUtf8(),
                        symbolToIndex.keySet().stream().map(Slice::toStringUtf8).toList()));
            }
            encoder.writeEnum(symbolIndex);
        }
    }

    private static class ArrayBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private final BlockPositionEncoder elementBlockPositionEncoder;
        private final ArrayType type;

        public ArrayBlockPositionEncoder(Optional<SimpleUnionNullIndex> nullIdx, Schema schema, AvroTypeManager avroTypeManager, ArrayType type)
                throws AvroTypeException
        {
            super(nullIdx);
            verify(requireNonNull(schema, "schema is null").getType() == Schema.Type.ARRAY);
            this.type = requireNonNull(type, "type is null");
            elementBlockPositionEncoder = createBlockPositionEncoder(schema.getElementType(), avroTypeManager, type.getElementType());
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            Block elementBlock = type.getObject(block, position);
            elementBlockPositionEncoder.setBlock(elementBlock);
            int size = elementBlock.getPositionCount();
            encoder.writeArrayStart();
            encoder.setItemCount(size);
            for (int itemPos = 0; itemPos < size; itemPos++) {
                encoder.startItem();
                elementBlockPositionEncoder.encode(itemPos, encoder);
            }
            encoder.writeArrayEnd();
        }
    }

    private static class MapBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private final BlockPositionEncoder keyBlockPositionEncoder = new StringOrBytesPositionEncoder(Optional.empty());
        private final BlockPositionEncoder valueBlockPositionEncoder;
        private final MapType type;

        public MapBlockPositionEncoder(Optional<SimpleUnionNullIndex> nullIdx, Schema schema, AvroTypeManager avroTypeManager, MapType type)
                throws AvroTypeException
        {
            super(nullIdx);
            verify(requireNonNull(schema, "schema is null").getType() == Schema.Type.MAP);
            this.type = requireNonNull(type, "type is null");
            if (!VARCHAR.equals(this.type.getKeyType())) {
                throw new AvroTypeException("Avro Maps must have String keys, invalid type: %s".formatted(this.type.getKeyType()));
            }
            valueBlockPositionEncoder = createBlockPositionEncoder(schema.getValueType(), avroTypeManager, type.getValueType());
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            Block mapBlock = type.getObject(block, position);
            keyBlockPositionEncoder.setBlock(mapBlock);
            valueBlockPositionEncoder.setBlock(mapBlock);
            encoder.writeMapStart();
            encoder.setItemCount(mapBlock.getPositionCount() / 2);
            for (int mapIndex = 0; mapIndex < mapBlock.getPositionCount(); mapIndex += 2) {
                encoder.startItem();
                keyBlockPositionEncoder.encode(mapIndex, encoder);
                valueBlockPositionEncoder.encode(mapIndex + 1, encoder);
            }
            encoder.writeMapEnd();
        }
    }

    private static class RecordBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private final RowType type;
        private final BlockPositionEncoder[] channelEncoders;
        private final int[] fieldToChannel;

        // used only for nested row building
        public RecordBlockPositionEncoder(Optional<SimpleUnionNullIndex> nullIdx, Schema schema, AvroTypeManager avroTypeManager, RowType rowType)
                throws AvroTypeException
        {
            this(nullIdx,
                    schema,
                    avroTypeManager,
                    rowType.getFields().stream()
                            .map(RowType.Field::getName)
                            .map(optName -> optName.orElseThrow(() -> new IllegalArgumentException("Unable to use nested anonymous row type for avro writing")))
                            .collect(toImmutableList()),
                    rowType.getFields().stream()
                            .map(RowType.Field::getType)
                            .collect(toImmutableList()));
        }

        // used only for top level page building
        public RecordBlockPositionEncoder(Schema schema, AvroTypeManager avroTypeManager, List<String> channelNames, List<Type> channelTypes)
                throws AvroTypeException
        {
            this(Optional.empty(), schema, avroTypeManager, channelNames, channelTypes);
        }

        private RecordBlockPositionEncoder(Optional<SimpleUnionNullIndex> nullIdx, Schema schema, AvroTypeManager avroTypeManager, List<String> channelNames, List<Type> channelTypes)
                throws AvroTypeException
        {
            super(nullIdx);
            type = RowType.anonymous(requireNonNull(channelTypes, "channelTypes is null"));
            verify(requireNonNull(schema, "schema is null").getType() == Schema.Type.RECORD);
            verify(schema.getFields().size() == channelTypes.size(), "Must have channel for each record field");
            verify(requireNonNull(channelNames, "channelNames is null").size() == channelTypes.size(), "Must provide names for all channels");
            fieldToChannel = new int[schema.getFields().size()];
            channelEncoders = new BlockPositionEncoder[schema.getFields().size()];
            for (int i = 0; i < channelNames.size(); i++) {
                String fieldName = channelNames.get(i);
                Schema.Field avroField = requireNonNull(schema.getField(fieldName), "no field with name %s in schema %s".formatted(fieldName, schema));
                fieldToChannel[avroField.pos()] = i;
                channelEncoders[i] = createBlockPositionEncoder(avroField.schema(), avroTypeManager, channelTypes.get(i));
            }
            verify(IntStream.of(fieldToChannel).sum() == (schema.getFields().size() * (schema.getFields().size() - 1) / 2), "all channels must be accounted for");
        }

        // Used only for nested rows
        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            SingleRowBlock singleRowBlock = (SingleRowBlock) type.getObject(block, position);
            for (BlockPositionEncoder channelEncoder : channelEncoders) {
                channelEncoder.setBlock(singleRowBlock);
            }
            encodeInternal(i -> i, encoder);
        }

        public void setChannelBlocksFromPage(Page page)
        {
            verify(page.getChannelCount() == channelEncoders.length, "Page must have channels equal to provided type list");
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                channelEncoders[channel].setBlock(page.getBlock(channel));
            }
        }

        public void encodePositionInEachChannel(int position, Encoder encoder)
                throws IOException
        {
            encodeInternal(ignore -> position, encoder);
        }

        private void encodeInternal(IntFunction<Integer> channelToPosition, Encoder encoder)
                throws IOException
        {
            for (int channel : fieldToChannel) {
                BlockPositionEncoder channelEncoder = channelEncoders[channel];
                channelEncoder.encode(channelToPosition.apply(channel), encoder);
            }
        }
    }

    private static class UserDefinedBlockPositionEncoder
            extends BlockPositionEncoder
    {
        private final GenericDatumWriter<Object> datumWriter;
        private final BiFunction<Block, Integer, Object> toAvroGeneric;

        public UserDefinedBlockPositionEncoder(Optional<SimpleUnionNullIndex> nullIdx, Schema schema, BiFunction<Block, Integer, Object> toAvroGeneric)
        {
            super(nullIdx);
            datumWriter = new GenericDatumWriter<>(requireNonNull(schema, "schema is null"));
            this.toAvroGeneric = requireNonNull(toAvroGeneric, "toAvroGeneric is null");
        }

        @Override
        void encodeFromBlock(int position, Encoder encoder)
                throws IOException
        {
            datumWriter.write(toAvroGeneric.apply(block, position), encoder);
        }
    }
}
