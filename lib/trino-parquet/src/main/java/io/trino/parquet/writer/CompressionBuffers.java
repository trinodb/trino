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
package io.trino.parquet.writer;

import org.apache.parquet.bytes.ByteBufferAllocator;

import java.nio.ByteBuffer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;

/**
 * Input and output buffers for compressing one page at a time, shared by the column writers of a file.
 */
final class CompressionBuffers
        implements ByteBufferAllocator
{
    private static final int INSTANCE_SIZE = instanceSize(CompressionBuffers.class);
    private static final byte[] EMPTY = new byte[0];

    private byte[] input = EMPTY;
    private byte[] output = EMPTY;

    /**
     * Returns the input buffer, positioned at zero with the given limit. {@link org.apache.parquet.bytes.BytesInput}
     * uses this allocator when it has to materialize an input that is not backed by a single buffer.
     */
    @Override
    public ByteBuffer allocate(int size)
    {
        if (input.length < size) {
            input = new byte[size];
        }
        return ByteBuffer.wrap(input, 0, size);
    }

    @Override
    public void release(ByteBuffer buffer) {}

    @Override
    public boolean isDirect()
    {
        return false;
    }

    /**
     * Returns the output buffer, at least {@code minimumSize} long; valid until the next call.
     */
    public byte[] output(int minimumSize)
    {
        if (output.length < minimumSize) {
            output = new byte[minimumSize];
        }
        return output;
    }

    public long getRetainedSize()
    {
        return INSTANCE_SIZE + sizeOf(input) + sizeOf(output);
    }

    public void release()
    {
        input = EMPTY;
        output = EMPTY;
    }
}
