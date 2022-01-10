package io.trino.operator.hash;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.hash.var.VariableOffsetGroupByHashTableEntries;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.AbstractIntType;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.IpAddressType;

import java.util.Optional;

public interface ColumnValueExtractor
{
    int INT96_BYTES = Long.BYTES + Integer.BYTES;
    int INT128_BYTES = Long.BYTES + Long.BYTES;

    static boolean isSupported(Type type)
    {
        return columnValueExtractor(type).isPresent();
    }

    void putValue(VariableOffsetGroupByHashTableEntries row, int rowPosition, int valueIndex, Block block, int position);

    void putValue(FastByteBuffer buffer, int offset, Block block, int position);

    int getSize();

    void appendValue(VariableOffsetGroupByHashTableEntries hashTable, int position, int valueIndex, BlockBuilder blockBuilder);

    void appendValue(FastByteBuffer buffer, int offset, BlockBuilder blockBuilder);

    boolean valueEquals(FastByteBuffer serialized, int offset, Block block, int position);

    int getSerializedValueLength(Block block, int position);

    boolean isFixedSize();

    static Optional<ColumnValueExtractor> columnValueExtractor(Type type)
    {
        if (type instanceof VarcharType) {
            return Optional.of(new SliceValueExtractor(((VarcharType) type).getLength()));
        }
        if (type instanceof VarbinaryType) {
            return Optional.of(new SliceValueExtractor(Optional.empty()));
        }
        if (type instanceof BooleanType || type instanceof TinyintType) {
            return Optional.of(new ByteValueExtractor());
        }
        if (type instanceof SmallintType) {
            return Optional.of(new ShortValueExtractor());
        }
        if (type instanceof AbstractIntType) {
            return Optional.of(new IntValueExtractor());
        }
        if (type instanceof AbstractLongType ||
                type instanceof DoubleType ||
                (type instanceof DecimalType && ((DecimalType) type).isShort()) ||
                (type instanceof TimestampType && ((TimestampType) type).isShort()) ||
                (type instanceof TimeWithTimeZoneType && ((TimeWithTimeZoneType) type).isShort()) ||
                (type instanceof TimestampWithTimeZoneType && ((TimestampWithTimeZoneType) type).isShort())) {
            return Optional.of(new LongValueExtractor());
        }

        if ((type instanceof TimestampType && !((TimestampType) type).isShort()) ||
                (type instanceof TimeWithTimeZoneType && !((TimeWithTimeZoneType) type).isShort()) ||
                (type instanceof TimestampWithTimeZoneType && !((TimestampWithTimeZoneType) type).isShort())) {
            return Optional.of(new Int96ValueExtractor());
        }
        if (type instanceof IpAddressType ||
                type instanceof UuidType ||
                (type instanceof DecimalType && !((DecimalType) type).isShort())) {
            return Optional.of(new Int128ValueExtractor());
        }
        return Optional.empty();
    }

    class SliceValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public SliceValueExtractor(Optional<Integer> sizeInBytes)
        {
            super(sizeInBytes.filter(size -> size < Integer.MAX_VALUE).map(size -> size + 1).orElse(Integer.MAX_VALUE));
        }

        @Override
        public void putValue(VariableOffsetGroupByHashTableEntries row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.putSliceValue(rowPosition, valueIndex, block.getSlice(position, 0, block.getSliceLength(position)));
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            int valueLength = block.getSliceLength(position);
            buffer.putByteUnsigned(offset, valueLength);
            Slice rawSlice = block.getRawSlice(position);
            buffer.putSlice(offset + 1, rawSlice, block.getPositionOffset(position), valueLength);
        }

        @Override
        public void appendValue(VariableOffsetGroupByHashTableEntries hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            Slice buffer = Slices.allocate(64);
            int valueLength = hashTable.getSliceValue(position, valueIndex, buffer);
            blockBuilder.writeBytes(buffer, 0, valueLength).closeEntry();
        }

        @Override
        public void appendValue(FastByteBuffer from, int offset, BlockBuilder blockBuilder)
        {
            int length = from.getByteUnsigned(offset);
            blockBuilder.writeBytes(from.asSlice(), offset + 1, length).closeEntry();
        }

        @Override
        public boolean valueEquals(FastByteBuffer serialized, int offset, Block block, int position)
        {
            int blockLength = block.getSliceLength(position);
            int length = serialized.getByteUnsigned(offset);
            if (blockLength != length) {
                return false;
            }
            Slice rawSlice = block.getRawSlice(position);
            int rawSlicePositionOffset = block.getPositionOffset(position);

//            serialized.getSlice(offset + 1, length, buffer, 0);
//            return rawSlice.equals(rawSlicePositionOffset, length, buffer, 0, length);
            return serialized.subArrayEquals(rawSlice, offset + 1, rawSlicePositionOffset, length);
        }

        @Override
        public int getSerializedValueLength(Block block, int position)
        {
            return block.getSliceLength(position) + Byte.BYTES /* length */;
        }

        @Override
        public boolean isFixedSize()
        {
            return false;
        }
    }

    class LongValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public LongValueExtractor()
        {
            super(Long.BYTES);
        }

        @Override
        public void putValue(VariableOffsetGroupByHashTableEntries row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.putLongValue(rowPosition, valueIndex, block.getLong(position, 0));
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putLong(offset, block.getLong(position, 0));
        }

        @Override
        public void appendValue(VariableOffsetGroupByHashTableEntries hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            blockBuilder.writeLong(hashTable.getLongValue(position, valueIndex)).closeEntry();
        }

        @Override
        public void appendValue(FastByteBuffer buffer, int offset, BlockBuilder blockBuilder)
        {
            blockBuilder.writeLong(buffer.getLong(offset)).closeEntry();
        }

        @Override
        public boolean valueEquals(FastByteBuffer serialized, int offset, Block block, int position)
        {
            return serialized.getLong(offset) == block.getLong(position, 0);
        }

        @Override
        public int getSerializedValueLength(Block block, int position)
        {
            return Long.BYTES;
        }

        @Override
        public boolean isFixedSize()
        {
            return true;
        }
    }

    class ByteValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public ByteValueExtractor()
        {
            super(Byte.BYTES);
        }

        @Override
        public void putValue(VariableOffsetGroupByHashTableEntries row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.putByteValue(rowPosition, valueIndex, block.getByte(position, 0));
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.put(offset, block.getByte(position, 0));
        }

        @Override
        public void appendValue(VariableOffsetGroupByHashTableEntries hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            blockBuilder.writeByte(hashTable.getByteValue(position, valueIndex)).closeEntry();
        }

        @Override
        public void appendValue(FastByteBuffer buffer, int offset, BlockBuilder blockBuilder)
        {
            blockBuilder.writeByte(buffer.get(offset)).closeEntry();
        }

        @Override
        public boolean valueEquals(FastByteBuffer serialized, int offset, Block block, int position)
        {
            return serialized.get(offset) == block.getByte(position, 0);
        }

        @Override
        public int getSerializedValueLength(Block block, int position)
        {
            return Byte.BYTES;
        }

        @Override
        public boolean isFixedSize()
        {
            return true;
        }
    }

    class ShortValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public ShortValueExtractor()
        {
            super(Short.BYTES);
        }

        @Override
        public void putValue(VariableOffsetGroupByHashTableEntries row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.putShortValue(rowPosition, valueIndex, block.getShort(position, 0));
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putShort(offset, block.getShort(position, 0));
        }

        @Override
        public void appendValue(VariableOffsetGroupByHashTableEntries hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            blockBuilder.writeShort(hashTable.getShortValue(position, valueIndex)).closeEntry();
        }

        @Override
        public void appendValue(FastByteBuffer buffer, int offset, BlockBuilder blockBuilder)
        {
            blockBuilder.writeShort(buffer.getShort(offset)).closeEntry();
        }

        @Override
        public boolean valueEquals(FastByteBuffer serialized, int offset, Block block, int position)
        {
            return serialized.getShort(offset) == block.getShort(position, 0);
        }

        @Override
        public int getSerializedValueLength(Block block, int position)
        {
            return Short.BYTES;
        }

        @Override
        public boolean isFixedSize()
        {
            return true;
        }
    }

    class IntValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public IntValueExtractor()
        {
            super(Integer.BYTES);
        }

        @Override
        public void putValue(VariableOffsetGroupByHashTableEntries row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.putIntValue(rowPosition, valueIndex, block.getInt(position, 0));
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putInt(offset, block.getInt(position, 0));
        }

        @Override
        public void appendValue(VariableOffsetGroupByHashTableEntries hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            blockBuilder.writeInt(hashTable.getIntValue(position, valueIndex)).closeEntry();
        }

        @Override
        public void appendValue(FastByteBuffer buffer, int offset, BlockBuilder blockBuilder)
        {
            blockBuilder.writeInt(buffer.getInt(offset)).closeEntry();
        }

        @Override
        public boolean valueEquals(FastByteBuffer serialized, int offset, Block block, int position)
        {
            return serialized.getInt(offset) == block.getInt(position, 0);
        }

        @Override
        public int getSerializedValueLength(Block block, int position)
        {
            return Integer.BYTES;
        }

        @Override
        public boolean isFixedSize()
        {
            return true;
        }
    }

    class Int128ValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public Int128ValueExtractor()
        {
            super(Long.BYTES + Long.BYTES);
        }

        @Override
        public void putValue(VariableOffsetGroupByHashTableEntries row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.put128BitValue(rowPosition, valueIndex, block.getLong(position, 0), block.getLong(position, Long.BYTES));
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putLong(offset, block.getLong(position, 0));
            buffer.putLong(offset + Long.BYTES, block.getLong(position, Long.BYTES));
        }

        private final Slice buffer = Slices.allocate(16);

        @Override
        public void appendValue(VariableOffsetGroupByHashTableEntries hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            hashTable.getInt128Value(position, valueIndex, buffer);
            blockBuilder.writeLong(buffer.getLong(0)).writeLong(buffer.getLong(Long.BYTES)).closeEntry();
        }

        @Override
        public void appendValue(FastByteBuffer buffer, int offset, BlockBuilder blockBuilder)
        {
            blockBuilder.writeLong(buffer.getLong(offset));
            blockBuilder.writeLong(buffer.getLong(offset + Long.BYTES));
            blockBuilder.closeEntry();
        }

        @Override
        public boolean valueEquals(FastByteBuffer serialized, int offset, Block block, int position)
        {
            return serialized.getLong(offset) == block.getLong(position, 0) &&
                    serialized.getLong(offset + Long.BYTES) == block.getLong(position, Long.BYTES);
        }

        @Override
        public int getSerializedValueLength(Block block, int position)
        {
            return INT128_BYTES;
        }

        @Override
        public boolean isFixedSize()
        {
            return true;
        }
    }

    class Int96ValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public Int96ValueExtractor()
        {
            super(Long.BYTES + Integer.BYTES);
        }

        @Override
        public void putValue(VariableOffsetGroupByHashTableEntries row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.put96BitValue(rowPosition, valueIndex, block.getLong(position, 0), block.getInt(position, Long.BYTES));
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putLong(offset, block.getLong(position, 0));
            buffer.putInt(offset + Long.BYTES, block.getInt(position, Long.BYTES));
        }

        private final Slice buffer = Slices.allocate(12);

        @Override
        public void appendValue(VariableOffsetGroupByHashTableEntries hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            hashTable.getInt96Value(position, valueIndex, buffer);
            blockBuilder.writeLong(buffer.getLong(0)).writeInt(buffer.getInt(Long.BYTES)).closeEntry();
        }

        @Override
        public void appendValue(FastByteBuffer buffer, int offset, BlockBuilder blockBuilder)
        {
            blockBuilder.writeLong(buffer.getLong(offset));
            blockBuilder.writeInt(buffer.getInt(offset + Long.BYTES));
            blockBuilder.closeEntry();
        }

        @Override
        public boolean valueEquals(FastByteBuffer serialized, int offset, Block block, int position)
        {
            return serialized.getLong(offset) == block.getLong(position, 0) &&
                    serialized.getInt(offset + Long.BYTES) == block.getInt(position, Long.BYTES);
        }

        @Override
        public int getSerializedValueLength(Block block, int position)
        {
            return INT96_BYTES;
        }

        @Override
        public boolean isFixedSize()
        {
            return true;
        }
    }

    abstract class AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        private final int sizeInBytes;

        protected AbstractColumnValueExtractor(int sizeInBytes)
        {
            this.sizeInBytes = sizeInBytes;
        }

        @Override
        public int getSize()
        {
            return sizeInBytes;
        }
    }
}
