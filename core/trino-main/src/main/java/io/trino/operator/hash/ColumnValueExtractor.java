package io.trino.operator.hash;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
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

import static io.trino.operator.hash.ColumnValueExtractor.AbstractColumnValueExtractor.MAX_SUMMARY_SIZE;

public interface ColumnValueExtractor
{
    static boolean isSupported(Type type)
    {
        return columnValueExtractor(type).isPresent();
    }

    void putValue(GroupByHashTableAccess row, int rowPosition, int valueIndex, Block block, int position);

    int getSummarySize();

    static Optional<ColumnValueExtractor> columnValueExtractor(Type type)
    {
        if (type instanceof VarcharType) {
            return Optional.of(new SliceValueExtractor(((VarcharType) type).getLength().orElse(MAX_SUMMARY_SIZE)));
        }
        if (type instanceof VarbinaryType) {
            return Optional.of(new SliceValueExtractor(MAX_SUMMARY_SIZE));
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

    void appendValue(GroupByHashTableAccess hashTable, int position, int valueIndex, BlockBuilder blockBuilder);

    class SliceValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public SliceValueExtractor(int summarySize)
        {
            super(summarySize);
        }

        @Override
        public void putValue(GroupByHashTableAccess row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.putSliceValue(rowPosition, valueIndex, block.getSlice(position, 0, block.getSliceLength(position)));
        }

        private Slice buffer = Slices.allocate(64);

        @Override
        public void appendValue(GroupByHashTableAccess hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            int valueLength = hashTable.getSliceValue(position, valueIndex, buffer);
            blockBuilder.writeBytes(buffer, 0, valueLength).closeEntry();
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
        public void putValue(GroupByHashTableAccess row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.putLongValue(rowPosition, valueIndex, block.getLong(position, 0));
        }

        @Override
        public void appendValue(GroupByHashTableAccess hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            blockBuilder.writeLong(hashTable.getLongValue(position, valueIndex)).closeEntry();
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
        public void putValue(GroupByHashTableAccess row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.putByteValue(rowPosition, valueIndex, block.getByte(position, 0));
        }

        @Override
        public void appendValue(GroupByHashTableAccess hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            blockBuilder.writeByte(hashTable.getByteValue(position, valueIndex)).closeEntry();
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
        public void putValue(GroupByHashTableAccess row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.putShortValue(rowPosition, valueIndex, block.getShort(position, 0));
        }

        @Override
        public void appendValue(GroupByHashTableAccess hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            blockBuilder.writeShort(hashTable.getShortValue(position, valueIndex)).closeEntry();
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
        public void putValue(GroupByHashTableAccess row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.putIntValue(rowPosition, valueIndex, block.getInt(position, 0));
        }

        @Override
        public void appendValue(GroupByHashTableAccess hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            blockBuilder.writeInt(hashTable.getIntValue(position, valueIndex)).closeEntry();
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
        public void putValue(GroupByHashTableAccess row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.put128BitValue(rowPosition, valueIndex, block.getLong(position, 0), block.getLong(position, Long.BYTES));
        }

        private final Slice buffer = Slices.allocate(16);

        @Override
        public void appendValue(GroupByHashTableAccess hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            hashTable.getInt128Value(position, valueIndex, buffer);
            blockBuilder.writeLong(buffer.getLong(0)).writeLong(buffer.getLong(Long.BYTES)).closeEntry();
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
        public void putValue(GroupByHashTableAccess row, int rowPosition, int valueIndex, Block block, int position)
        {
            row.put96BitValue(rowPosition, valueIndex, block.getLong(position, 0), block.getInt(position, Long.BYTES));
        }

        private final Slice buffer = Slices.allocate(12);

        @Override
        public void appendValue(GroupByHashTableAccess hashTable, int position, int valueIndex, BlockBuilder blockBuilder)
        {
            hashTable.getInt96Value(position, valueIndex, buffer);
            blockBuilder.writeLong(buffer.getLong(0)).writeInt(buffer.getInt(Long.BYTES)).closeEntry();
        }
    }

    abstract class AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        static final int MAX_SUMMARY_SIZE = 16;
        private final int summarySize;

        protected AbstractColumnValueExtractor(int summarySize)
        {
            this.summarySize = Math.min(summarySize, MAX_SUMMARY_SIZE);
        }

        @Override
        public int getSummarySize()
        {
            return summarySize;
        }
    }
}
