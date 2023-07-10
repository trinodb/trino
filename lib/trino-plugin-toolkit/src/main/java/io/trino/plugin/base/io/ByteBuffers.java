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
package io.trino.plugin.base.io;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

public final class ByteBuffers
{
    private ByteBuffers() {}

    /**
     * Gets the bytes the provided {@link ByteBuffer} wraps, without advancing buffer position.
     * Throws when provided buffer does not directly wrap bytes.
     */
    public static byte[] getWrappedBytes(ByteBuffer byteBuffer)
    {
        checkArgument(byteBuffer.hasArray(), "buffer does not have array");
        checkArgument(byteBuffer.arrayOffset() == 0, "buffer has non-zero array offset: %s", byteBuffer.arrayOffset());
        checkArgument(byteBuffer.position() == 0, "buffer has been repositioned to %s", byteBuffer.position());
        byte[] array = byteBuffer.array();
        checkArgument(byteBuffer.remaining() == array.length, "buffer has %s remaining bytes while array length is %s", byteBuffer.remaining(), array.length);
        return array;
    }

    /**
     * Gets the bytes the provided {@link ByteBuffer} represents, without advancing buffer position.
     * The returned byte array may be shared with the buffer.
     */
    public static byte[] getBytes(ByteBuffer byteBuffer)
    {
        if (byteBuffer.hasArray() && byteBuffer.arrayOffset() == 0 && byteBuffer.position() == 0) {
            byte[] array = byteBuffer.array();
            if (byteBuffer.remaining() == array.length) {
                return array;
            }
        }

        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.asReadOnlyBuffer().get(bytes);
        return bytes;
    }
}
