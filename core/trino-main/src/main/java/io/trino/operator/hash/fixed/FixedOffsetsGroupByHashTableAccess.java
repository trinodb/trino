package io.trino.operator.hash.fixed;

import io.airlift.slice.Slice;
import io.trino.operator.hash.FastByteBuffer;
import it.unimi.dsi.fastutil.HashCommon;

import static com.google.common.base.Verify.verify;

public class FixedOffsetsGroupByHashTableAccess
        implements AutoCloseable
{
    private static final int INT96_BYTES = Long.BYTES + Integer.BYTES;
    private static final int INT128_BYTES = Long.BYTES + Long.BYTES;

    // Memory layout per entry
    //  0 - 4 : overflow length
    //  4 - 8 : overflow position
    //  8 - 16 : hash
    // 16 - 16 + channelCount : isNull
    // 16 + channelCount - xxx fixoffset values

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
    private final int channelCount;
    private final int keyOffset; // 4 for hash table (first 4 bytes are for group id), 0 for the row buffer

    private int overflowSize;

    public FixedOffsetsGroupByHashTableAccess(int entryCount, FastByteBuffer overflow, int channelCount, boolean includeGroupId, int dataValuesLength)
    {
        this.overflow = overflow;
        this.channelCount = channelCount;
        this.keyOffset = includeGroupId ? Integer.BYTES : 0;
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
        if (includeGroupId) {
            // set groupIds to -1
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

    public void copyKeyFrom(int toPosition, FixedOffsetsGroupByHashTableAccess src, int srcPosition)
    {
        data.copyFrom(src.data, srcPosition + src.keyOffset, toPosition + keyOffset, src.keyLength());
        int overflowLength = src.getOverflowLength(srcPosition);
        if (overflowLength > 0) {
            // TODO lysy: handle overflow resize
            overflow.copyFrom(src.overflow, src.getOverflowPosition(srcPosition), overflowSize, overflowLength);
            overflowSize += overflowLength;
        }
    }

    public void copyEntryFrom(FixedOffsetsGroupByHashTableAccess src, int srcPosition, int toPosition)
    {
        data.copyFrom(src.data, srcPosition, toPosition, src.getEntrySize());
        // overflow is shared
        verify(src.overflow == overflow);
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

    public boolean keyEquals(int position, FixedOffsetsGroupByHashTableAccess other, int otherPosition)
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
        if (valueOffset + Long.BYTES <= valuesLength) {
            // read from data
            return data.getLong(position + valuesOffset + valueOffset);
        }
        // calculate overflow offset
        int overflowOffset = valueOffset - valuesLength;
        if (overflowOffset < 0) {
            // do not split fixed width value
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
                // do not split fixed width value
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

    public void putByteValue(int position, int valueIndex, byte value)
    {
        int valueOffset = getValueOffset(position, valueIndex);

        if (valueOffset < valuesLength) {
            // can put value in the data buffer
            data.put(position + valuesOffset + valueOffset, value);
        }
        else {
            // put value in the overflow buffer
            int overflowOffset = valueOffset - valuesLength;

            overflow.put(requiredOverflowPosition(position) + overflowOffset + overflowHeaderLength, value);
            overflowSize += Byte.BYTES;
        }
        putValueOffset(position, valueIndex, valueOffset + Byte.BYTES);
    }

    public void putShortValue(int position, int valueIndex, short value)
    {
        int valueOffset = getValueOffset(position, valueIndex);

        if (valueOffset + Short.BYTES <= valuesLength) {
            // can put value in the data buffer
            data.putShort(position + valuesOffset + valueOffset, value);
            putValueOffset(position, valueIndex, valueOffset + Short.BYTES);
        }
        else {
            // put value in the overflow buffer
            int overflowOffset = valueOffset - valuesLength;
            if (overflowOffset < 0) {
                // do not split fixed width value
                // clear remaining byte
                data.put(position + entrySize - 1, (byte) 0);
                overflowOffset = 0;
            }

            overflow.putShort(requiredOverflowPosition(position) + overflowOffset + overflowHeaderLength, value);
            overflowSize += Short.BYTES;
            putValueOffset(position, valueIndex, valuesLength + overflowOffset + Short.BYTES);
        }
    }

    public void putIntValue(int position, int valueIndex, int value)
    {
        int valueOffset = getValueOffset(position, valueIndex);

        if (valueOffset + Integer.BYTES <= valuesLength) {
            // can put value in the data buffer
            data.putInt(position + valuesOffset + valueOffset, value);
            putValueOffset(position, valueIndex, valueOffset + Integer.BYTES);
        }
        else {
            // put value in the overflow buffer
            int overflowOffset = valueOffset - valuesLength;
            if (overflowOffset < 0) {
                // do not split fixed width value
                // clear remaining bytes
                for (int i = position + valuesOffset + valueOffset; i < position + entrySize; i++) {
                    data.put(i, (byte) 0);
                }

                overflowOffset = 0;
            }

            overflow.putInt(requiredOverflowPosition(position) + overflowOffset + overflowHeaderLength, value);
            overflowSize += Integer.BYTES;
            putValueOffset(position, valueIndex, valuesLength + overflowOffset + Integer.BYTES);
        }
    }

    public void put96BitValue(int position, int valueIndex, long high, int low)
    {
        int valueOffset = getValueOffset(position, valueIndex);

        if (valueOffset + INT96_BYTES <= valuesLength) {
            // can put value in the data buffer
            data.putLong(position + valuesOffset + valueOffset, high);
            data.putInt(position + valuesOffset + valueOffset + Long.BYTES, low);
            putValueOffset(position, valueIndex, valueOffset + INT96_BYTES);
        }
        else {
            // put value in the overflow buffer
            int overflowOffset = valueOffset - valuesLength;
            if (overflowOffset < 0) {
                // do not split fixed width value
                // clear remaining bytes
                for (int i = position + valuesOffset + valueOffset; i < position + entrySize; i++) {
                    data.put(i, (byte) 0);
                }

                overflowOffset = 0;
            }

            overflow.putLong(requiredOverflowPosition(position) + overflowOffset + overflowHeaderLength, high);
            overflow.putInt(requiredOverflowPosition(position) + overflowOffset + overflowHeaderLength + Long.BYTES, low);
            overflowSize += INT96_BYTES;
            putValueOffset(position, valueIndex, valuesLength + overflowOffset + INT96_BYTES);
        }
    }

    public void put128BitValue(int position, int valueIndex, long high, long low)
    {
        int valueOffset = getValueOffset(position, valueIndex);

        if (valueOffset + INT128_BYTES <= valuesLength) {
            // can put value in the data buffer
            data.putLong(position + valuesOffset + valueOffset, high);
            data.putLong(position + valuesOffset + valueOffset + Long.BYTES, low);
            putValueOffset(position, valueIndex, valueOffset + INT128_BYTES);
        }
        else {
            // put value in the overflow buffer
            int overflowOffset = valueOffset - valuesLength;
            if (overflowOffset < 0) {
                // do not split fixed width value
                // TODO lysy: it maybe wort splitting actually. especially if we dont read the value
                // clear remaining bytes
                for (int i = position + valuesOffset + valueOffset; i < position + entrySize; i++) {
                    data.put(i, (byte) 0);
                }

                overflowOffset = 0;
            }

            overflow.putLong(requiredOverflowPosition(position) + overflowOffset + overflowHeaderLength, high);
            overflow.putLong(requiredOverflowPosition(position) + overflowOffset + overflowHeaderLength + Long.BYTES, low);
            overflowSize += INT128_BYTES;
            putValueOffset(position, valueIndex, valuesLength + overflowOffset + INT128_BYTES);
        }
    }

    public void putSliceValue(int position, int valueIndex, Slice value)
    {
        int valueOffset = getValueOffset(position, valueIndex);
        int valueLength = value.length();
        if (valueOffset >= valuesLength) {
            // put everything in the overflow
            int overflowOffset = valueOffset - valuesLength;
            overflow.putSlice(requiredOverflowPosition(position) + overflowHeaderLength + overflowOffset, value, 0, valueLength);

            overflowSize += valueLength;
        }
        else {
            // some part in the data + optionally some part in the overflow
            int dataPartLength = Math.min(valueLength, valuesLength - valueOffset);
            // put as much as we can in the data buffer
            data.putSlice(position + valuesOffset + valueOffset, value, 0, dataPartLength);

            if (dataPartLength < valueLength) {
                // put rest of the value at the beginning of the overflow buffer
                int overflowPartLength = valueLength - dataPartLength;

                overflow.putSlice(requiredOverflowPosition(position) + overflowHeaderLength, value, dataPartLength, overflowPartLength);
                overflowSize += overflowPartLength;
            }
        }
        putValueOffset(position, valueIndex, valueOffset + valueLength);
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
            overflow.putInt(getOverflowPosition(position) + valueIndex * Integer.BYTES, valueOffset);
        }
    }

    public void putEntry(int hashPosition, int groupId, FixedOffsetsGroupByHashTableAccess key)
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
            // copy valueOffset
            int valueOffset = data.getByteUnsigned(position + valuesOffsetsOffset + channelIndex - 1);
            data.putByteUnsigned(position + valuesOffsetsOffset + channelIndex, valueOffset);

            if (valueOffset == 255) {
                // copy value offset in the overflow
                valueOffset = overflow.getInt(requiredOverflowPosition(position) + (channelIndex - 1) * Integer.BYTES);
                overflow.putInt(getOverflowPosition(position) + channelIndex * Integer.BYTES, valueOffset);
            }
        }
    }

    public int getSliceValue(int position, int valueIndex, Slice out)
    {
        int valueOffset = getValueOffset(position, valueIndex);
        int nextValueOffset = getValueOffset(position, valueIndex + 1);
        int valueLength = nextValueOffset - valueOffset;

        if (valueOffset >= valuesLength) {
            // read all from overflow
            int overflowOffset = valueOffset - valuesLength;
            overflow.getSlice(requiredOverflowPosition(position) + overflowHeaderLength + overflowOffset, valueLength, out, 0);
        }
        else {
            // read from data
            int dataPartLength = Math.min(valueLength, valuesLength - valueOffset);
            data.getSlice(position + valuesOffset + valueOffset, dataPartLength, out, 0);

            if (dataPartLength < valueLength) {
                // read rest from overflow
                int overflowPartLength = valueLength - dataPartLength;
                overflow.getSlice(requiredOverflowPosition(position) + overflowHeaderLength, overflowPartLength, out, dataPartLength);
            }
        }
        return valueLength;
    }

    public byte getByteValue(int position, int valueIndex)
    {
        int valueOffset = getValueOffset(position, valueIndex);

        if (valueOffset < valuesLength) {
            // can read value from the data buffer
            return data.get(position + valuesOffset + valueOffset);
        }
        else {
            // read value from the overflow buffer
            int overflowOffset = valueOffset - valuesLength;

            return overflow.get(requiredOverflowPosition(position) + overflowOffset + overflowHeaderLength);
        }
    }

    public short getShortValue(int position, int valueIndex)
    {
        int valueOffset = getValueOffset(position, valueIndex);

        if (valueOffset + Short.BYTES <= valuesLength) {
            // can read value from the data buffer
            return data.getShort(position + valuesOffset + valueOffset);
        }
        else {
            // read value from the overflow buffer
            int overflowOffset = valueOffset - valuesLength;

            return overflow.getShort(requiredOverflowPosition(position) + overflowOffset + overflowHeaderLength);
        }
    }

    public int getIntValue(int position, int valueIndex)
    {
        int valueOffset = getValueOffset(position, valueIndex);

        if (valueOffset + Integer.BYTES <= valuesLength) {
            // can read value from the data buffer
            return data.getInt(position + valuesOffset + valueOffset);
        }
        else {
            // read value from the overflow buffer
            int overflowOffset = valueOffset - valuesLength;

            return overflow.getInt(requiredOverflowPosition(position) + overflowOffset + overflowHeaderLength);
        }
    }

    public void getInt128Value(int position, int valueIndex, Slice out)
    {
        int valueOffset = getValueOffset(position, valueIndex);

        if (valueOffset + INT128_BYTES <= valuesLength) {
            // can read value from the data buffer
            out.setLong(0, data.getLong(position + valuesOffset + valueOffset));
            out.setLong(Long.BYTES, data.getLong(position + valuesOffset + valueOffset + Long.BYTES));
        }
        else {
            // read value from the overflow buffer
            int overflowOffset = valueOffset - valuesLength;

            int overflowPosition = requiredOverflowPosition(position);
            int overflowAbsoluteOffset = overflowPosition + overflowHeaderLength + overflowOffset;
            out.setLong(0, overflow.getLong(overflowAbsoluteOffset));
            out.setLong(Long.BYTES, overflow.getLong(overflowAbsoluteOffset + Long.BYTES));
        }
    }

    public void getInt96Value(int position, int valueIndex, Slice out)
    {
        int valueOffset = getValueOffset(position, valueIndex);

        if (valueOffset + INT96_BYTES <= valuesLength) {
            // can read value from the data buffer
            out.setLong(0, data.getLong(position + valuesOffset + valueOffset));
            out.setInt(Long.BYTES, data.getInt(position + valuesOffset + valueOffset + Long.BYTES));
        }
        else {
            // read value from the overflow buffer
            int overflowOffset = valueOffset - valuesLength;

            int overflowPosition = requiredOverflowPosition(position);
            int overflowAbsoluteOffset = overflowPosition + overflowHeaderLength + overflowOffset;
            out.setLong(0, overflow.getLong(overflowAbsoluteOffset));
            out.setInt(Long.BYTES, overflow.getInt(overflowAbsoluteOffset + Long.BYTES));
        }
    }

    public String toString(int position)
    {
        StringBuilder sb = new StringBuilder("[");
        if (keyOffset > 0) {
            sb.append("groupId=").append(getGroupId(position)).append("\n");
        }
        sb.append("overflow length=").append(getOverflowLength(position)).append("\n");
        sb.append("overflow position=").append(getOverflowPosition(position)).append("\n");
        sb.append("hash=").append(getHash(position)).append("\n");
        sb.append("isNull=[");
        for (int i = 0; i < channelCount; i++) {
            sb.append(isNull(position, i)).append(", ");
        }
        sb.append("]").append("\n");
        sb.append("values offsets=[");
        for (int i = 0; i < channelCount; i++) {
            sb.append(data.get(position + valuesOffsetsOffset + i)).append(", ");
        }
        sb.append("]").append("\n");

        return sb.toString();
    }
}
