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
import io.trino.plugin.varada.storage.lucene.LuceneFileType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * buffer for marking extended strings
 */
public class LuceneReadJuffer
        extends BaseReadJuffer
{
    private final ByteBuffer[] luceneBuffers;

    public LuceneReadJuffer(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator, JuffersType.LUCENE);
        this.luceneBuffers = new ByteBuffer[LuceneFileType.values().length - 1];
    }

    @Override
    public JuffersType getJufferType()
    {
        return JuffersType.LUCENE;
    }

    @Override
    public void createBuffer(RecTypeCode recTypeCode, int recTypeLength, long[] buffIds, boolean hasDictionary)
    {
        ByteBuffer[] buffers = bufferAllocator.ids2LuceneBuffers(buffIds);
        for (int i = 0; i < buffers.length; i++) {
            this.luceneBuffers[i] = buffers[i].slice().order(ByteOrder.BIG_ENDIAN);
            this.luceneBuffers[i].position(0);
        }
    }

    public ByteBuffer getLuceneByteBuffer(int nativeId)
    {
        return getLuceneWEBuffer()[nativeId];
    }

    public ByteBuffer[] getLuceneWEBuffer()
    {
        return luceneBuffers;
    }
}
