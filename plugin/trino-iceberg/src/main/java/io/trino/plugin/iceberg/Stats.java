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
package io.trino.plugin.iceberg;

import io.airlift.slice.SizeOf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.google.common.base.Preconditions.checkArgument;

public final class Stats
{
    private Stats() {}

    public static final String NDV_STATS = "ndv-long-little-endian";

    public static ByteBuffer longToBytesLittleEndian(long value)
    {
        ByteBuffer byteBuffer = ByteBuffer.allocate(SizeOf.SIZE_OF_LONG);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putLong(value);
        byteBuffer.flip();
        return byteBuffer;
    }

    public static long bytesLittleEndianToLong(ByteBuffer byteBuffer)
    {
        checkArgument(byteBuffer.remaining() == SizeOf.SIZE_OF_LONG, "Incorrect number of bytes in the buffer: %s", byteBuffer.remaining());
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return byteBuffer.getLong();
    }
}
