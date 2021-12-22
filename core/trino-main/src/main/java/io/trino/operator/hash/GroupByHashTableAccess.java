package io.trino.operator.hash;

import com.google.common.base.Verify;
import it.unimi.dsi.fastutil.HashCommon;

public class GroupByHashTableAccess
        implements AutoCloseable
{
    // Memory layout per entry
    //  0 - 4 : overflow length
    //  4 - 8 : overflow position
    //  8 - 16 : hash
    // 16 - 16 + channelCount : isNull
    // 16+channelCount - 16+2xchannelCount : values offsets
    // 16+2xchannelCount - 16+2xchannelCount+valuesLength : values prefix

    private final FastByteBuffer data;
    private final FastByteBuffer overflow;
    private boolean closeOverflow = true;
    private final int overflowLengthOffset;
    private final int overflowPositionOffset;
    private final int hashOffset;
    private final int isNullOffset;
    private final int keyLength;
    private final int hashValuesAndIsNullLength;
    private final int valuesOffsetsOffset;
    private final int valuesOffset;
    private final int valuesLength;
    private final int entrySize;
    private final int overflowHeaderLength;
    private final int keyOffset; // 4 for hash table (first 4 bytes are for group id), 0 for the row buffer

    private int overflowSize;

    public GroupByHashTableAccess(int entryCount, FastByteBuffer overflow, int channelCount, int keyOffset, int dataValuesLength)
    {
        this.overflow = overflow;
        this.keyOffset = keyOffset;
        overflowLengthOffset = keyOffset;
        overflowPositionOffset = overflowLengthOffset + Integer.BYTES;
        hashOffset = overflowPositionOffset + Integer.BYTES;
        isNullOffset = hashOffset + Long.BYTES;
        valuesOffset = isNullOffset + channelCount /* isNull */ + channelCount /* values offsets */;
        valuesLength = dataValuesLength;
        entrySize = valuesOffset + valuesLength;
        keyLength = valuesOffset + valuesLength - keyOffset;
        hashValuesAndIsNullLength = entrySize - hashOffset;
        valuesOffsetsOffset = isNullOffset + channelCount /* isNull */;
        overflowHeaderLength = channelCount * Integer.BYTES;
        this.data = FastByteBuffer.allocate(entryCount * entrySize);
        if (keyOffset > 0) {
            for (int i = 0; i <= data.capacity() - entrySize; i += entrySize) {
                this.data.putInt(i, -1);
            }
        }
    }

    public int getEntrySize()
    {
        return entrySize;
    }

    public long getHash(int position)
    {
        return data.getLong(position + hashOffset);
    }

    public void putHash(int position, long hash)
    {
        data.putLong(position + hashOffset, hash);
    }

    public void copyKeyFrom(int toPosition, GroupByHashTableAccess src, int srcPosition)
    {
        data.copyFrom(src.data, srcPosition + src.keyOffset, toPosition + keyOffset, src.keyLength());
        int overflowLength = src.getOverflowLength(srcPosition);
        data.putInt(toPosition + overflowLengthOffset, overflowLength);
        if (overflowLength > 0) {
            // TODO lysy: handle overflow resize
            overflow.copyFrom(src.overflow, src.getOverflowPosition(srcPosition), overflowSize, overflowLength);
            overflowSize += overflowLength;
        }
    }

    public void copyEntryFrom(GroupByHashTableAccess src, int srcPosition, int toPosition)
    {
        data.copyFrom(src.data, srcPosition, toPosition, src.getEntrySize());
        // overflow is shared
        Verify.verify(src.overflow == overflow);
    }

    private int requiredOverflowPosition(int position)
    {
        int overflowPosition = getOverflowPosition(position);
        if (overflowPosition == 0 && overflowSize > 0) {
            overflowPosition = overflowSize;
            putOverflowPosition(position, overflowPosition);
            overflowSize += overflowHeaderLength; // header is always necessary
        }
        return overflowPosition;
    }

    private void putOverflowPosition(int position, int overflowPosition)
    {
        data.putInt(position + overflowPositionOffset, overflowPosition);
    }

    private int getOverflowPosition(int position)
    {
        return data.getInt(position + overflowPositionOffset);
    }

    private int getOverflowLength(int position)
    {
        return data.getInt(position + overflowLengthOffset);
    }

    private int keyLength()
    {
        return keyLength;
    }

    public boolean keyEquals(int position, GroupByHashTableAccess other, int otherPosition)
    {
        if (!data.subArrayEquals(other.data, position + hashOffset, otherPosition + other.hashOffset, hashValuesAndIsNullLength)) {
            return false;
        }
        int overflowLength = getOverflowLength(position);
        int otherOverflowLength = other.getOverflowLength(otherPosition);
        if (overflowLength != otherOverflowLength) {
            return false;
        }
        if (overflowLength == 0) {
            return true;
        }

        return overflow.subArrayEquals(other.overflow, getOverflowPosition(position), other.getOverflowPosition(otherPosition), overflowLength);
    }

    @Override
    public void close()
    {
        try {
            data.close();
        }
        finally {
            if (closeOverflow) {
                overflow.close();
            }
        }
    }

    public FastByteBuffer takeOverflow()
    {
        closeOverflow = false;
        return overflow;
    }

    public long calculateValuesHash(int position)
    {
        long result = 1;
        int i = position + valuesOffset;
        for (; i <= position + entrySize - Long.BYTES; i += Long.BYTES) {
            long element = data.getLong(i);
            long elementHash = HashCommon.mix(element);
            result = 31 * result + elementHash;
        }
        for (; i < entrySize; i++) {
            byte element = data.get(i);
            result = 31 * result + element;
        }

        return result;
    }

    public byte isNull(int position, int i)
    {
        return data.get(position + isNullOffset + i);
    }

    public void putIsNull(int position, int channelIndex, byte isNull)
    {
        data.put(position + isNullOffset + channelIndex, isNull);
    }

    public void clear()
    {
        data.clear();
        if (overflowSize > 0) {
            overflow.clear(overflowSize);
            overflowSize = 0;
        }
    }

    public long getLongValue(int position, int index)
    {
        int valueOffset = getValueOffset(position, index);
        if (valueOffset + Long.BYTES < valuesLength) {
            // read from data
            return data.getLong(position + valuesOffset + valueOffset);
        }
        // calculate overflow offset
        int overflowOffset = valueOffset - valuesLength;
        if (overflowOffset < 0) {
            // do not split long
            overflowOffset = 0;
        }
        // read from overflow
        return overflow.getLong(getOverflowPosition(position) + overflowHeaderLength + overflowOffset);
    }

    public void putLongValue(int position, int valueIndex, long value)
    {
        int valueOffset = getValueOffset(position, valueIndex);

        if (valueOffset + Long.BYTES <= valuesLength) {
            // can put value in the data buffer
            data.putLong(position + valuesOffset + valueOffset, value);
            putValueOffset(position, valueIndex, valueOffset + Long.BYTES);
        }
        else {
            // put value in the overflow buffer
            int overflowOffset = valueOffset - valuesLength;
            if (overflowOffset < 0) {
                // do not split long
                // clear remaining bytes
                for (int i = position + valuesOffset + valueOffset; i < position + entrySize; i++) {
                    data.put(i, (byte) 0);
                }

                overflowOffset = 0;
            }

            overflow.putLong(requiredOverflowPosition(position) + overflowOffset + overflowHeaderLength, value);
            overflowSize += Long.BYTES;
            putValueOffset(position, valueIndex, valuesLength + overflowOffset + Long.BYTES);
        }
    }

    // byte unsigned
    private int getValueOffset(int position, int valueIndex)
    {
        if (valueIndex == 0) {
            return 0;
        }
        int valueOffset = data.getByteUnsigned(position + valuesOffsetsOffset + valueIndex - 1);
        if (valueOffset == 255) {
            // value offset larger than 254. correct offset is in the overflow buffer
            valueOffset = overflow.getInt(requiredOverflowPosition(position) + (valueIndex - 1) * Integer.BYTES);
        }

        return valueOffset;
    }

    // put offset for the next value at index valueIndex
    // (e.g. first value is always at offset 0, second values is at the offset offsets[0])
    private void putValueOffset(int position, int valueIndex, int valueOffset)
    {
        if (valueOffset < 255) {
            // offset fits in the byte so can but it in the data buffer
            data.putByteUnsigned(position + valuesOffsetsOffset + valueIndex, valueOffset);
        }
        else {
            // offset to large for data buffer, put 255 marker in the data and correct value in the overflow header
            data.putByteUnsigned(position + valuesOffsetsOffset + valueIndex, 255);
            // note that valueOffset includes valuesLength (i.e. it is counted from the valuesOffset)
            overflow.putInt(getOverflowPosition(position) + valueIndex * Integer.BYTES, valueOffset + overflowHeaderLength);
        }
    }

    public void putEntry(int hashPosition, int groupId, GroupByHashTableAccess key)
    {
        data.putInt(hashPosition, groupId);
        copyKeyFrom(hashPosition, key, 0);
    }

    public int getGroupId(int hashPosition)
    {
        return data.getInt(hashPosition);
    }

    public int capacity()
    {
        return data.capacity();
    }

    public long getEstimatedSize()
    {
        return data.capacity() + overflow.capacity();
    }

    public void putNullValue(int position, int channelIndex)
    {
        if (channelIndex > 0) {
            // TODO lysy: this can be optimized
            putValueOffset(position, channelIndex, getValueOffset(position, channelIndex - 1));
        }
    }
}
