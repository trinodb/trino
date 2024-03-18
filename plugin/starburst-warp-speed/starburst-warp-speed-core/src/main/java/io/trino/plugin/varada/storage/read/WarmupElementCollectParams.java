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
package io.trino.plugin.varada.storage.read;

import io.trino.plugin.varada.dictionary.ReadDictionary;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.DataWarmEvents;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.block.Block;

import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.warp.gen.constants.WECollectJparams.WE_COLLECT_JPARAMS_FILE_OFFSET;
import static io.trino.plugin.warp.gen.constants.WECollectJparams.WE_COLLECT_JPARAMS_FILE_READ_SIZE;
import static io.trino.plugin.warp.gen.constants.WECollectJparams.WE_COLLECT_JPARAMS_IS_COLLECT_NULLS;
import static io.trino.plugin.warp.gen.constants.WECollectJparams.WE_COLLECT_JPARAMS_MATCH_COLLECT_ID;
import static io.trino.plugin.warp.gen.constants.WECollectJparams.WE_COLLECT_JPARAMS_MATCH_COLLECT_INDEX;
import static io.trino.plugin.warp.gen.constants.WECollectJparams.WE_COLLECT_JPARAMS_REC_TYPE_CODE;
import static io.trino.plugin.warp.gen.constants.WECollectJparams.WE_COLLECT_JPARAMS_REC_TYPE_LENGTH;
import static io.trino.plugin.warp.gen.constants.WECollectJparams.WE_COLLECT_JPARAMS_WARM_UP_TYPE;

class WarmupElementCollectParams
{
    private final int fileOffset;
    private final int fileReadSize;
    private final WarmUpType warmUpType;
    // the record type code and length as passed to the storage reader and should be kept in native
    private final RecTypeCode recTypeCode;
    private final int recTypeLength;
    // the record type code and length for the page block returned to trino
    private final RecTypeCode blockRecTypeCode;
    private final int blockRecTypeLength;
    private final int warmEvents;
    private final boolean isImported;
    private final Optional<WarmupElementDictionaryParams> dictionaryParams;
    private final int blockIndex;
    private final int matchCollectIndex;
    private final int matchCollectId;
    private final boolean isCollectNulls;
    private final Optional<Block> valuesDictBlock;
    private Optional<ReadDictionary> dictionary;

    public WarmupElementCollectParams(int fileOffset,
            int fileReadSize,
            WarmUpType warmUpType,
            RecTypeCode recTypeCode,
            int recTypeLength,
            RecTypeCode blockRecTypeCode,
            int blockRecTypeLength,
            int warmEvents,
            boolean isImported,
            Optional<WarmupElementDictionaryParams> dictionaryParams,
            int blockIndex,
            int matchCollectIndex,
            int matchCollectId,
            boolean isCollectNulls,
            Optional<Block> valuesDictBlock)
    {
        this.fileOffset = fileOffset;
        this.fileReadSize = fileReadSize;
        this.warmUpType = warmUpType;
        this.recTypeCode = recTypeCode;
        this.recTypeLength = recTypeLength;
        this.blockRecTypeCode = blockRecTypeCode;
        this.blockRecTypeLength = blockRecTypeLength;
        this.warmEvents = warmEvents;
        this.isImported = isImported;
        this.dictionaryParams = dictionaryParams;
        this.dictionary = Optional.empty();
        this.blockIndex = blockIndex;
        this.matchCollectIndex = matchCollectIndex;
        this.matchCollectId = matchCollectId;
        this.isCollectNulls = isCollectNulls;
        this.valuesDictBlock = valuesDictBlock;
    }

    public RecTypeCode getRecTypeCode()
    {
        return recTypeCode;
    }

    public int getRecTypeLength()
    {
        return recTypeLength;
    }

    public RecTypeCode getBlockRecTypeCode()
    {
        return blockRecTypeCode;
    }

    public int getBlockRecTypeLength()
    {
        return blockRecTypeLength;
    }

    public boolean hasDictionaryParams()
    {
        return dictionaryParams.isPresent();
    }

    public DictionaryKey getDictionaryKey()
    {
        return dictionaryParams.get().dictionaryKey();
    }

    public int getUsedDictionarySize()
    {
        return dictionaryParams.get().usedDictionarySize();
    }

    public RecTypeCode getDataValuesRecTypeCode()
    {
        return dictionaryParams.get().dataValuesRecTypeCode();
    }

    public int getDataValuesRecTypeLength()
    {
        return dictionaryParams.get().dataValuesRecTypeLength();
    }

    public int getDictionaryOffset()
    {
        return dictionaryParams.get().dictionaryOffset();
    }

    public boolean hasDictionary()
    {
        return dictionary.isPresent();
    }

    public ReadDictionary getDictionary()
    {
        return dictionary.get();
    }

    public boolean mappedMatchCollect()
    {
        return valuesDictBlock.isPresent();
    }

    public Optional<Block> getValuesDictBlock()
    {
        return valuesDictBlock;
    }

    public void setDictionary(ReadDictionary dictionary)
    {
        this.dictionary = Optional.of(dictionary);
    }

    public int getBlockIndex()
    {
        return blockIndex;
    }

    public boolean isCollectNulls()
    {
        return isCollectNulls;
    }

    public boolean hasMatchCollect()
    {
        return (matchCollectIndex != -1);
    }

    public void dump(int[] output, int offset)
    {
        output[offset + WE_COLLECT_JPARAMS_FILE_OFFSET.ordinal()] = fileOffset;
        output[offset + WE_COLLECT_JPARAMS_FILE_READ_SIZE.ordinal()] = fileReadSize;
        output[offset + WE_COLLECT_JPARAMS_WARM_UP_TYPE.ordinal()] = warmUpType.ordinal();
        output[offset + WE_COLLECT_JPARAMS_REC_TYPE_CODE.ordinal()] = TypeUtils.nativeRecTypeCode(recTypeCode);
        output[offset + WE_COLLECT_JPARAMS_REC_TYPE_LENGTH.ordinal()] = recTypeLength;
        output[offset + WE_COLLECT_JPARAMS_MATCH_COLLECT_INDEX.ordinal()] = matchCollectIndex;
        output[offset + WE_COLLECT_JPARAMS_MATCH_COLLECT_ID.ordinal()] = matchCollectId;
        output[offset + WE_COLLECT_JPARAMS_IS_COLLECT_NULLS.ordinal()] = isCollectNulls ? 1 : 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fileOffset, fileReadSize, warmUpType, recTypeCode, recTypeLength, isCollectNulls);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof WarmupElementCollectParams o)) {
            return false;
        }

        return fileOffset == o.fileOffset &&
                fileReadSize == o.fileReadSize &&
                warmUpType == o.warmUpType &&
                recTypeCode == o.recTypeCode &&
                recTypeLength == o.recTypeLength &&
                isCollectNulls == o.isCollectNulls;
    }

    @Override
    public String toString()
    {
        return "WarmupElementCollectParams{" +
                "fileOffset=" + fileOffset +
                ", fileReadSize=" + fileReadSize +
                ", warmUpType=" + warmUpType +
                ", recTypeCode=" + recTypeCode +
                ", recTypeLength=" + recTypeLength +
                ", blockRecTypeCode=" + recTypeCode +
                ", blockRecTypeLength=" + recTypeLength +
                ", warmEvents=" + warmEventsToString() +
                ", isImported=" + isImported +
                ", dictionaryParams=" + (dictionaryParams.isPresent() ? dictionaryParams : "none") +
                ", blockIndex=" + blockIndex +
                ", matchCollectIndex=" + matchCollectIndex +
                ", matchCollectId=" + matchCollectId +
                ", isCollectNulls=" + isCollectNulls +
                '}';
    }

    private String warmEventsToString()
    {
        if (warmUpType == WarmUpType.WARM_UP_TYPE_DATA) {
            return "data" +
                    (eventOccurred(DataWarmEvents.DATA_WARM_EVENTS_COMPRESSION_LZ.ordinal()) ? ":compression_lz" : "") +
                    (eventOccurred(DataWarmEvents.DATA_WARM_EVENTS_COMPRESSION_LZ_HIGH.ordinal()) ? ":compression_lz_high" : "") +
                    (eventOccurred(DataWarmEvents.DATA_WARM_EVENTS_ENCODE_BIT_PACKING.ordinal()) ? ":encode_bit_packing" : "") +
                    (eventOccurred(DataWarmEvents.DATA_WARM_EVENTS_ENCODE_BIT_PACKING_DELTA.ordinal()) ? ":encode_bit_packing_delta" : "") +
                    (eventOccurred(DataWarmEvents.DATA_WARM_EVENTS_SINGLE_CHUNK.ordinal()) ? ":single_chunk" : "") +
                    (eventOccurred(DataWarmEvents.DATA_WARM_EVENTS_PACKED_CHUNK.ordinal()) ? ":packed_chunk" : "") +
                    (eventOccurred(DataWarmEvents.DATA_WARM_EVENTS_SINGLE_CHUNK_ROLLBACK.ordinal()) ? ":single_chunk_rollback" : "") +
                    (eventOccurred(DataWarmEvents.DATA_WARM_EVENTS_EXT_RECS.ordinal()) ? ":ext_recs" : "");
        }
        return "no events";
    }

    private boolean eventOccurred(int eventNum)
    {
        return (warmEvents & (1 << eventNum)) != 0;
    }
}
