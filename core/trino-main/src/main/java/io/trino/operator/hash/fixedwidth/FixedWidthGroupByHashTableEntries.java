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
package io.trino.operator.hash.fixedwidth;

import io.trino.operator.hash.GroupByHashTableEntries;
import io.trino.operator.hash.fastbb.FastByteBuffer;

public class FixedWidthGroupByHashTableEntries
        implements GroupByHashTableEntries
{
    // Memory layout per entry
    // 0  - 4 : group id
    // 4  - 12 : hash
    // 12  - 12 + channelCount : isNull
    // 12 + channelCount - 12 + channelCount + valuesLength : values

    private final FastByteBuffer buffer;
    private final int channelCount;
    private final int maxEntryCount;
    private final int entrySize;

    private final int hashOffset;
    private final int isNullOffset;
    private final int contentLength; // number of bytes from the hash offset to the end of the entry
    private final int valuesOffset;
    private final int valuesLength;

    public static FixedWidthGroupByHashTableEntries allocate(
            int maxEntryCount,
            int channelCount,
            int valuesLength)
    {
        // first 4 bytes are groupId
        int hashOffset = Integer.BYTES;
        int isNullOffset = hashOffset + Long.BYTES;
        int valuesOffset = isNullOffset + channelCount /* isNull */;
        int entrySize = valuesOffset + valuesLength;
        int contentLength = entrySize - hashOffset;
        FastByteBuffer mainBuffer = createBuffer(maxEntryCount, entrySize);
        return new FixedWidthGroupByHashTableEntries(
                mainBuffer,
                channelCount,
                maxEntryCount,
                entrySize,
                hashOffset,
                isNullOffset,
                contentLength,
                valuesOffset,
                valuesLength);
    }

    private FixedWidthGroupByHashTableEntries(
            FastByteBuffer buffer,
            int channelCount,
            int maxEntryCount,
            int entrySize,
            int hashOffset,
            int isNullOffset,
            int contentLength,
            int valuesOffset,
            int valuesLength)
    {
        this.buffer = buffer;
        this.channelCount = channelCount;
        this.maxEntryCount = maxEntryCount;
        this.entrySize = entrySize;
        this.hashOffset = hashOffset;
        this.isNullOffset = isNullOffset;
        this.contentLength = contentLength;
        this.valuesOffset = valuesOffset;
        this.valuesLength = valuesLength;
    }

    private static FastByteBuffer createBuffer(int entryCount, int entrySize)
    {
        final FastByteBuffer mainBuffer = FastByteBuffer.allocate(entryCount * entrySize);
        // set groupIds to -1
        for (int i = 0; i <= mainBuffer.capacity() - entrySize; i += entrySize) {
            mainBuffer.putInt(i, -1);
        }

        return mainBuffer;
    }

    @Override
    public int getEntrySize()
    {
        return entrySize;
    }

    @Override
    public long getHash(int position)
    {
        return buffer.getLong(position + hashOffset);
    }

    @Override
    public void putHash(int position, long hash)
    {
        buffer.putLong(position + hashOffset, hash);
    }

    public void copyEntryFrom(FixedWidthGroupByHashTableEntries src, int srcPosition, int toPosition)
    {
        buffer.copyFrom(src.buffer, srcPosition, toPosition, src.getEntrySize());
    }

    public int getValuesOffset(int position)
    {
        return position + valuesOffset;
    }

    @Override
    public byte isNull(int position, int i)
    {
        return buffer.get(position + isNullOffset + i);
    }

    public void putIsNull(int position, int channelIndex, byte isNull)
    {
        buffer.put(position + isNullOffset + channelIndex, isNull);
    }

    public void putIsNull(int position, byte[] isNull)
    {
        for (int i = 0; i < isNull.length; i++) {
            buffer.put(position + isNullOffset + i, isNull[i]);
        }
    }

    @Override
    public int getGroupId(int position)
    {
        return buffer.getInt(position);
    }

    @Override
    public void putGroupId(int position, int groupId)
    {
        buffer.putInt(position, groupId);
    }

    @Override
    public int capacity()
    {
        return buffer.capacity();
    }

    @Override
    public long getEstimatedSize()
    {
        return buffer.capacity();
    }

    @Override
    public String toString(int position)
    {
        StringBuilder sb = new StringBuilder("[");

        sb.append("groupId=").append(getGroupId(position)).append("\n");

        sb.append("hash=").append(getHash(position)).append("\n");
        sb.append("isNull=[");
        for (int i = 0; i < channelCount; i++) {
            sb.append(isNull(position, i)).append(", ");
        }
        sb.append("]").append("\n");
        sb.append("values=[");
        sb.append(buffer.toString(position + valuesOffset, valuesLength));
        sb.append("]").append("\n");

        return sb.toString();
    }

    public FastByteBuffer getBuffer()
    {
        return buffer;
    }
}
