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
package io.trino.parquet.writer.valuewriter;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Encoding;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.parquet.column.Encoding.PLAIN;

public final class TrinoBooleanPlainValuesWriter
        extends ValuesWriter
{
    private static final int INITIAL_SLAB_SIZE = 64;

    private final CapacityByteArrayOutputStream output;
    private int bitsInByte;
    private int currentByte;
    private boolean finished;

    public TrinoBooleanPlainValuesWriter(int maxPageSize)
    {
        output = new CapacityByteArrayOutputStream(INITIAL_SLAB_SIZE, maxPageSize, new HeapByteBufferAllocator());
    }

    @Override
    public void writeBoolean(boolean value)
    {
        checkState(!finished, "writer is finished");
        if (value) {
            currentByte |= 1 << bitsInByte;
        }
        bitsInByte++;
        if (bitsInByte == Byte.SIZE) {
            flushByte();
        }
    }

    public void writeBits(long bits, int bitCount)
    {
        checkState(!finished, "writer is finished");
        while (bitsInByte != 0 && bitCount > 0) {
            writeBoolean((bits & 1) != 0);
            bits >>>= 1;
            bitCount--;
        }
        while (bitCount >= Byte.SIZE) {
            output.write((byte) bits);
            bits >>>= Byte.SIZE;
            bitCount -= Byte.SIZE;
        }
        while (bitCount > 0) {
            writeBoolean((bits & 1) != 0);
            bits >>>= 1;
            bitCount--;
        }
    }

    private void flushByte()
    {
        output.write((byte) currentByte);
        bitsInByte = 0;
        currentByte = 0;
    }

    @Override
    public long getBufferedSize()
    {
        return output.size() + (bitsInByte == 0 ? 0 : 1);
    }

    @Override
    public BytesInput getBytes()
    {
        if (!finished) {
            if (bitsInByte != 0) {
                flushByte();
            }
            finished = true;
        }
        return BytesInput.from(output);
    }

    @Override
    public void reset()
    {
        output.reset();
        bitsInByte = 0;
        currentByte = 0;
        finished = false;
    }

    @Override
    public void close()
    {
        output.close();
    }

    @Override
    public long getAllocatedSize()
    {
        return output.getCapacity();
    }

    @Override
    public Encoding getEncoding()
    {
        return PLAIN;
    }

    @Override
    public String memUsageString(String prefix)
    {
        return output.memUsageString(prefix);
    }
}
