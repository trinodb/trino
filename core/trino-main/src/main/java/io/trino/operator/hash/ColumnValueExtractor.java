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
package io.trino.operator.hash;

import io.trino.operator.hash.fastbb.FastByteBuffer;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.AbstractIntType;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.type.IpAddressType;

import java.util.Optional;

import static java.lang.Float.intBitsToFloat;

/**
 * Block vs FastByteBuffer adapter.
 */
public interface ColumnValueExtractor
{
    int INT96_BYTES = Long.BYTES + Integer.BYTES;
    int INT128_BYTES = Long.BYTES + Long.BYTES;
    ByteValueExtractor BYTE_VALUE_EXTRACTOR = new ByteValueExtractor();
    ShortValueExtractor SHORT_VALUE_EXTRACTOR = new ShortValueExtractor();
    IntValueExtractor INT_VALUE_EXTRACTOR = new IntValueExtractor();
    FloatValueExtractor FLOAT_VALUE_EXTRACTOR = new FloatValueExtractor();
    LongValueExtractor LONG_VALUE_EXTRACTOR = new LongValueExtractor();
    DoubleValueExtractor DOUBLE_VALUE_EXTRACTOR = new DoubleValueExtractor();
    Int96ValueExtractor INT_96_VALUE_EXTRACTOR = new Int96ValueExtractor();
    Int128ValueExtractor INT_128_VALUE_EXTRACTOR = new Int128ValueExtractor();

    static boolean isSupported(Type type)
    {
        return columnValueExtractor(type).isPresent();
    }

    boolean isFixedSize();

    int getSize();

    void putValue(FastByteBuffer buffer, int offset, Block block, int position);

    void appendValue(FastByteBuffer buffer, int offset, BlockBuilder blockBuilder);

    boolean valueEquals(FastByteBuffer serialized, int offset, Block block, int position);

    int getSerializedValueLength(Block block, int position);

    static ColumnValueExtractor requiredColumnValueExtractor(Type type)
    {
        return columnValueExtractor(type).orElseThrow(() -> new IllegalArgumentException("Type " + type + " is not supported!"));
    }

    static Optional<ColumnValueExtractor> columnValueExtractor(Type type)
    {
        if (type instanceof BooleanType || type instanceof TinyintType) {
            return Optional.of(BYTE_VALUE_EXTRACTOR);
        }
        if (type instanceof SmallintType) {
            return Optional.of(SHORT_VALUE_EXTRACTOR);
        }
        if (type instanceof RealType) {
            // RealType is stored as int but has different equals semantics
            return Optional.of(FLOAT_VALUE_EXTRACTOR);
        }
        if (type instanceof AbstractIntType) {
            return Optional.of(INT_VALUE_EXTRACTOR);
        }
        if (type instanceof DoubleType) {
            return Optional.of(DOUBLE_VALUE_EXTRACTOR);
        }
        if (type instanceof AbstractLongType ||
                (type instanceof DecimalType && ((DecimalType) type).isShort()) ||
                (type instanceof TimestampType && ((TimestampType) type).isShort()) ||
                (type instanceof TimeWithTimeZoneType && ((TimeWithTimeZoneType) type).isShort()) ||
                (type instanceof TimestampWithTimeZoneType && ((TimestampWithTimeZoneType) type).isShort())) {
            return Optional.of(LONG_VALUE_EXTRACTOR);
        }

        if ((type instanceof TimestampType && !((TimestampType) type).isShort()) ||
                (type instanceof TimeWithTimeZoneType && !((TimeWithTimeZoneType) type).isShort()) ||
                (type instanceof TimestampWithTimeZoneType && !((TimestampWithTimeZoneType) type).isShort())) {
            return Optional.of(INT_96_VALUE_EXTRACTOR);
        }
        if (type instanceof IpAddressType ||
                type instanceof UuidType ||
                (type instanceof DecimalType && !((DecimalType) type).isShort())) {
            return Optional.of(INT_128_VALUE_EXTRACTOR);
        }
        return Optional.empty();
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
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.put(offset, block.getByte(position, 0));
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
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putShort(offset, block.getShort(position, 0));
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
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putInt(offset, block.getInt(position, 0));
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

    class FloatValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public FloatValueExtractor()
        {
            super(Integer.BYTES);
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putInt(offset, block.getInt(position, 0));
        }

        @Override
        public void appendValue(FastByteBuffer buffer, int offset, BlockBuilder blockBuilder)
        {
            blockBuilder.writeInt(buffer.getInt(offset)).closeEntry();
        }

        @Override
        public boolean valueEquals(FastByteBuffer serialized, int offset, Block block, int position)
        {
            float leftFloat = intBitsToFloat(serialized.getInt(offset));
            float rightFloat = intBitsToFloat(block.getInt(position, 0));
            if (Float.isNaN(leftFloat) && Float.isNaN(rightFloat)) {
                return true;
            }
            return leftFloat == rightFloat;
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

    class LongValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public LongValueExtractor()
        {
            super(Long.BYTES);
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putLong(offset, block.getLong(position, 0));
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

    class DoubleValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public DoubleValueExtractor()
        {
            super(Long.BYTES);
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putLong(offset, block.getLong(position, 0));
        }

        @Override
        public void appendValue(FastByteBuffer buffer, int offset, BlockBuilder blockBuilder)
        {
            blockBuilder.writeLong(buffer.getLong(offset)).closeEntry();
        }

        @Override
        public boolean valueEquals(FastByteBuffer serialized, int offset, Block block, int position)
        {
            double left = Double.longBitsToDouble(serialized.getLong(offset));
            double right = Double.longBitsToDouble(block.getLong(position, 0));
            if (Double.isNaN(left) && Double.isNaN(right)) {
                return true;
            }
            return left == right;
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

    class Int96ValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public Int96ValueExtractor()
        {
            super(Long.BYTES + Integer.BYTES);
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putLong(offset, block.getLong(position, 0));
            buffer.putInt(offset + Long.BYTES, block.getInt(position, Long.BYTES));
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

    class Int128ValueExtractor
            extends AbstractColumnValueExtractor
            implements ColumnValueExtractor
    {
        public Int128ValueExtractor()
        {
            super(Long.BYTES + Long.BYTES);
        }

        @Override
        public void putValue(FastByteBuffer buffer, int offset, Block block, int position)
        {
            buffer.putLong(offset, block.getLong(position, 0));
            buffer.putLong(offset + Long.BYTES, block.getLong(position, Long.BYTES));
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
