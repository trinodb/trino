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
package io.trino.plugin.varada.storage.write.appenders;

import io.trino.plugin.varada.dictionary.WriteDictionary;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.storage.juffers.ChunkMap;
import io.trino.plugin.varada.storage.juffers.CrcJuffer;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.warmup.exceptions.WarmupException;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.List;
import java.util.Optional;

public abstract class BlockAppender
{
    protected static final byte NULL_VALUE_BYTE_SIGNAL = -1;
    protected static final byte ZERO_BYTE_SIGNAL = 0;
    private static final byte[] padding = new byte[8192];   // PageSize
    protected final WriteJuffersWarmUpElement juffersWE;
    protected final ByteBuffer nullBuff;
    protected final CrcJuffer crcJuffers;
    private final int copySize;

    public BlockAppender(WriteJuffersWarmUpElement juffersWE, int copySize)
    {
        this.juffersWE = juffersWE;
        this.nullBuff = juffersWE.getNullBuffer();
        this.crcJuffers = juffersWE.getCrcJuffer();
        this.copySize = copySize;
    }

    public final AppendResult append(
            int jufferPos,
            BlockPosHolder blockPos,
            boolean stopAfterOneFlush,
            Optional<WriteDictionary> writeDictionary,
            WarmUpElement warmUpElement,
            WarmupElementStats warmupElementStats)
    {
        AppendResult result;
        if (writeDictionary.isEmpty()) {
            result = appendWithoutDictionary(jufferPos, blockPos, stopAfterOneFlush, warmUpElement, warmupElementStats);
        }
        else {
            result = appendWithDictionary(blockPos, stopAfterOneFlush, writeDictionary.get(), warmupElementStats);
        }
        warmupElementStats.incNullCount(result.nullsCount());
        return result;
    }

    abstract AppendResult appendWithoutDictionary(int jufferPos, BlockPosHolder blockPos, boolean stopAfterOneFlush, WarmUpElement warmUpElement, WarmupElementStats warmupElementStats);

    public void writeChunkMapValuesIntoChunkMapJuffer(List<ChunkMap> chunkMapList)
    {
        ByteBuffer chunkBuffer = juffersWE.getChunkMapBuffer();
        chunkBuffer.position(0); // to be on the safe side
        for (ChunkMap chunkMap : chunkMapList) {
            chunkBuffer.put(chunkMap.chunkValues(), 0, copySize);
        }
    }

    protected void advanceNullBuff()
    {
        nullBuff.position(nullBuff.position() + 1);
    }

    protected void padBuffer(ByteBuffer buff, int len)
    {
        if (len > padding.length) {
            while (len > 0) {
                int size = Math.min(len, padding.length);
                buff.put(padding, 0, size);
                len -= size;
            }
        }
        else {
            buff.put(padding, 0, len);
        }
    }

    AppendResult appendWithDictionary(BlockPosHolder blockPos, boolean stopAfterOneFlush, WriteDictionary writeDictionary, WarmupElementStats warmupElementStats)
    {
        throw new UnsupportedOperationException();
    }

    void writeValue(short key, ShortBuffer buffer)
    {
        juffersWE.updateRecordBufferProps(key);
        buffer.put(key);
        advanceNullBuff();
    }

    protected boolean commitWEIfNeeded(BlockPosHolder blockPos, ByteBuffer buff, int jufferPos, int addedNv, int recBuffSize)
    {
        boolean committed = false;
        if (buff.position() >= recBuffSize) {
            if (buff.position() > recBuffSize) {
                throw new WarmupException(
                        "appendStringBlock reached " + buff.position() + " beyond recBuffSize " + recBuffSize,
                        WarmUpElementState.State.FAILED_PERMANENTLY);
            }
            juffersWE.commitAndResetWE(jufferPos + blockPos.getPos(), addedNv, buff.position(), 0);
            committed = true;
        }
        return committed;
    }

    protected AppendResult appendFromMapBlock(BlockPosHolder blockPos, int jufferPos, Object key)
    {
        throw new UnsupportedOperationException();
    }
}
