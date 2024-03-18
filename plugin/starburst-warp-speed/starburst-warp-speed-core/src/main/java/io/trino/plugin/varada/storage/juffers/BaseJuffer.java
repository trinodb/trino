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
package io.trino.plugin.varada.storage.juffers;

import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

public abstract class BaseJuffer
{
    protected final BufferAllocator bufferAllocator;
    private final JuffersType bufferType;
    protected Buffer wrappedBuffer;  // buffer of this object, can be of any type (Int/Long/Byte/...), null only if not used
    protected ByteBuffer baseBuffer; // byte buffer duplicate of the wrappedBuffer, used only where applicable, can be null when wrappedBuffer is not null

    protected BaseJuffer(BufferAllocator bufferAllocator, JuffersType bufferType)
    {
        this.bufferAllocator = bufferAllocator;
        this.bufferType = bufferType;
    }

    public Buffer getWrappedBuffer()
    {
        return wrappedBuffer;
    }

    public JuffersType getJufferType()
    {
        return bufferType;
    }

    public ByteBuffer createGenericBuffer(java.nio.ByteBuffer byteBuffer)
    {
        ByteBuffer byteBuf = byteBuffer.slice().order(ByteOrder.LITTLE_ENDIAN);
        byteBuf.position(0);
        return byteBuf;
    }

    protected Buffer createWrapperBuffer(ByteBuffer buf, RecTypeCode recTypeCode, int recTypeLength, boolean forWrite, boolean useDictionary)
    {
        if (useDictionary) {
            ShortBuffer shortBuffer = buf.asShortBuffer();
            shortBuffer.position(0);
            return shortBuffer;
        }
        if (!TypeUtils.isStr(recTypeCode)) {
            // long decimal is treated as long buffer in read flow and as byte buffer in write flow
            if ((recTypeLength == Long.BYTES) || (!forWrite && (recTypeCode == RecTypeCode.REC_TYPE_DECIMAL_LONG))) {
                LongBuffer longBuf = buf.asLongBuffer();
                longBuf.position(0);
                return longBuf;
            }
            if (recTypeLength == Integer.BYTES) {
                IntBuffer intBuf = buf.asIntBuffer();
                intBuf.position(0);
                return intBuf;
            }
            if (recTypeLength == Short.BYTES) {
                ShortBuffer shortBuffer = buf.asShortBuffer();
                shortBuffer.position(0);
                return shortBuffer;
            }
        }
        // we have to duplicate the buffer since we want the original buffer to keep its properties (such as position)
        return createGenericBuffer(buf);
    }
}
