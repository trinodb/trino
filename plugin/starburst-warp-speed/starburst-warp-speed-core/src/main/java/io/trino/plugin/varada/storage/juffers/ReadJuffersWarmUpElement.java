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

public class ReadJuffersWarmUpElement
        extends JuffersWarmUpElementBase
{
    public ReadJuffersWarmUpElement(BufferAllocator bufferAllocator, boolean withCollect, boolean withLucene)
    {
        super();

        if (withCollect) {
            RecordReadJuffer recordJuffers = new RecordReadJuffer(bufferAllocator);
            juffers.put(recordJuffers.getJufferType(), recordJuffers);

            NullReadJuffer nullJuffers = new NullReadJuffer(bufferAllocator);
            juffers.put(nullJuffers.getJufferType(), nullJuffers);
        }

        if (withLucene) {
            LuceneReadJuffer luceneJuffers = new LuceneReadJuffer(bufferAllocator);
            juffers.put(luceneJuffers.getJufferType(), luceneJuffers);
            LuceneBMResultJuffer luceneBMResultJuffers = new LuceneBMResultJuffer(bufferAllocator);
            juffers.put(luceneBMResultJuffers.getJufferType(), luceneBMResultJuffers);
        }
    }

    public void createBuffers(RecTypeCode recTypeCode, int recTypeLength, boolean hasDictionary, long[] buffIds)
    {
        for (BaseJuffer juffer : juffers.values()) {
            BaseReadJuffer readJuffer = (BaseReadJuffer) juffer;
            readJuffer.createBuffer(recTypeCode, recTypeLength, buffIds, hasDictionary);
        }
    }

    public ByteBuffer getLuceneFileBuffer(LuceneFileType luceneFileType)
    {
        return getLuceneJuffer().getLuceneByteBuffer(luceneFileType.getNativeId());
    }

    public LuceneReadJuffer getLuceneJuffer()
    {
        return (LuceneReadJuffer) getJufferByType(JuffersType.LUCENE);
    }

    public ByteBuffer getLuceneBMResultBuffer()
    {
        return (ByteBuffer) getBufferByType(JuffersType.LuceneBMResult);
    }
}
