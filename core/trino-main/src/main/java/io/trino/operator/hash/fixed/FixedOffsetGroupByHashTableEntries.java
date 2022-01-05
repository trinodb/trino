package io.trino.operator.hash.fixed;

import io.trino.operator.hash.FastByteBuffer;
import io.trino.operator.hash.GroupByHashTableEntries;
import it.unimi.dsi.fastutil.HashCommon;

import static com.google.common.base.Verify.verify;

public class FixedOffsetGroupByHashTableEntries
        implements AutoCloseable, GroupByHashTableEntries
{
    // Memory layout per entry
    // 0  - 4 : group id - this is optional, if absent other fields are moved by -4 bytes
    // 4  - 8 : overflow length
    // 8  - 12 : overflow position
    // 12 - 20 : hash
    // 20 - 20 + channelCount : isNull
    // 20 + channelCount - xxx values

    private final FastByteBuffer mainBuffer;
    private final FastByteBuffer overflowBuffer;
    private boolean closeOverflow = true;
    private final int overflowLengthOffset;
    private final int overflowPositionOffset;
    private final int hashOffset;
    private final int isNullOffset;
    private final int contentLength; // number of bytes from the hash offset to the end of the entry
    private final int keyLength;
    private final int valuesOffset;
    private final int valuesLength;
    private final int entrySize;
    private final int channelCount;
    private final int keyOffset; // 4 for hash table (first 4 bytes are for group id), 0 for the row buffer

    private int overflowSize;

    public FixedOffsetGroupByHashTableEntries(int entryCount, FastByteBuffer overflowBuffer, int channelCount, boolean includeGroupId, int dataValuesLength)
    {
        this.overflowBuffer = overflowBuffer;
        this.channelCount = channelCount;
        this.keyOffset = includeGroupId ? Integer.BYTES : 0;
        overflowLengthOffset = keyOffset;
        overflowPositionOffset = overflowLengthOffset + Integer.BYTES;
        hashOffset = overflowPositionOffset + Integer.BYTES;
        isNullOffset = hashOffset + Long.BYTES;
        valuesOffset = isNullOffset + channelCount /* isNull */;
        valuesLength = dataValuesLength;
        entrySize = valuesOffset + valuesLength;
        keyLength = valuesOffset + valuesLength - keyOffset;
        contentLength = entrySize - hashOffset;
        this.mainBuffer = FastByteBuffer.allocate(entryCount * entrySize);
        if (includeGroupId) {
            // set groupIds to -1
            for (int i = 0; i <= mainBuffer.capacity() - entrySize; i += entrySize) {
                this.mainBuffer.putInt(i, -1);
            }
        }
    }

    public int getEntrySize()
    {
        return entrySize;
    }

    public long getHash(int position)
    {
        return mainBuffer.getLong(position + hashOffset);
    }

    @Override
    public void putHash(int position, long hash)
    {
        mainBuffer.putLong(position + hashOffset, hash);
    }

    public void copyKeyFrom(int toPosition, GroupByHashTableEntries srcEntries, int srcPosition)
    {
        FixedOffsetGroupByHashTableEntries src = (FixedOffsetGroupByHashTableEntries) srcEntries;
        mainBuffer.copyFrom(src.mainBuffer, srcPosition + src.keyOffset, toPosition + keyOffset, src.keyLength());
        int overflowLength = src.getOverflowLength(srcPosition);
        if (overflowLength > 0) {
            // TODO lysy: handle overflow resize
            overflowBuffer.copyFrom(src.overflowBuffer, src.getOverflowPosition(srcPosition), overflowSize, overflowLength);
            overflowSize += overflowLength;
        }
    }

    public void copyEntryFrom(GroupByHashTableEntries srcEntries, int srcPosition, int toPosition)
    {
        FixedOffsetGroupByHashTableEntries src = (FixedOffsetGroupByHashTableEntries) srcEntries;
        mainBuffer.copyFrom(src.mainBuffer, srcPosition, toPosition, src.getEntrySize());
        // overflow is shared
        verify(src.overflowBuffer == overflowBuffer);
    }

    private int getOverflowPosition(int position)
    {
        return mainBuffer.getInt(position + overflowPositionOffset);
    }

    public int getOverflowLength(int position)
    {
        return mainBuffer.getInt(position + overflowLengthOffset);
    }

    private int keyLength()
    {
        return keyLength;
    }

    @Override
    public boolean keyEquals(int position, GroupByHashTableEntries entries, int otherPosition)
    {
        FixedOffsetGroupByHashTableEntries other = (FixedOffsetGroupByHashTableEntries) entries;
        if (!mainBuffer.subArrayEquals(other.mainBuffer, position + hashOffset, otherPosition + other.hashOffset, contentLength)) {
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

        return overflowBuffer.subArrayEquals(other.overflowBuffer, getOverflowPosition(position), other.getOverflowPosition(otherPosition), overflowLength);
    }

    @Override
    public void close()
    {
        try {
            mainBuffer.close();
        }
        finally {
            if (closeOverflow) {
                overflowBuffer.close();
            }
        }
    }

    public FastByteBuffer takeOverflow()
    {
        closeOverflow = false;
        return overflowBuffer;
    }

    public long calculateValuesHash(int position)
    {
        long result = 1;
        int i = position + valuesOffset;
        for (; i <= position + entrySize - Long.BYTES; i += Long.BYTES) {
            long element = mainBuffer.getLong(i);
            long elementHash = HashCommon.mix(element);
            result = 31 * result + elementHash;
        }
        for (; i < entrySize; i++) {
            byte element = mainBuffer.get(i);
            result = 31 * result + element;
        }

        return result;
    }

    public void markNoOverflow(int position)
    {
        mainBuffer.putInt(position + overflowLengthOffset, 0);
        mainBuffer.putInt(position + overflowPositionOffset, 0);
    }

    public void reserveOverflowLength(int position, int overflowLength)
    {
        mainBuffer.putInt(position + overflowLengthOffset, overflowLength);
        mainBuffer.putInt(position + overflowPositionOffset, overflowSize);
        overflowSize += overflowLength;
    }

    public int getValuesOffset(int position)
    {
        return position + valuesOffset;
    }

    public byte isNull(int position, int i)
    {
        return mainBuffer.get(position + isNullOffset + i);
    }

    public void putIsNull(int position, int channelIndex, byte isNull)
    {
        mainBuffer.put(position + isNullOffset + channelIndex, isNull);
    }

    public void putIsNull(int position, byte[] isNull)
    {
        for (int i = 0; i < isNull.length; i++) {
            mainBuffer.put(position + isNullOffset + i, isNull[i]);
        }
//        mainBuffer.put(position + isNullOffset, isNull, 0, isNull.length);
    }

    public void clear()
    {
        mainBuffer.clear();
        if (overflowSize > 0) {
            overflowBuffer.clear(overflowSize);
            overflowSize = 0;
        }
    }

    public void putEntry(int position, int groupId, GroupByHashTableEntries key)
    {
        mainBuffer.putInt(position, groupId);
        copyKeyFrom(position, key, 0);
    }

    public int getGroupId(int position)
    {
        return mainBuffer.getInt(position);
    }

    @Override
    public void putGroupId(int position, int groupId)
    {
        mainBuffer.putInt(position, groupId);
    }

    public int capacity()
    {
        return mainBuffer.capacity();
    }

    public long getEstimatedSize()
    {
        return mainBuffer.capacity() + overflowBuffer.capacity();
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
        sb.append("values=[");
        sb.append(mainBuffer.toString(position + valuesOffset, valuesLength));
        sb.append("]").append("\n");

        return sb.toString();
    }

    public FastByteBuffer getMainBuffer()
    {
        return mainBuffer;
    }
}
