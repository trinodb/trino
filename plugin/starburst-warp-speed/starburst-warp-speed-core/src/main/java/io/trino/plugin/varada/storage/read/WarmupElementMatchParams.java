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

import io.trino.plugin.varada.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.BasicWarmEvents;
import io.trino.plugin.warp.gen.constants.MatchCollectOp;
import io.trino.plugin.warp.gen.constants.MatchNodeType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_FILE_OFFSET;
import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_FILE_READ_SIZE;
import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_IS_COLLECT_NULLS;
import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_IS_TIGHTNESS_REQUIRED;
import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_MATCH_COLLECT_INDEX;
import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_MATCH_COLLECT_OP;
import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_NUM_OF;
import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_PRED_BUF_ADDRESS_HIGH;
import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_PRED_BUF_ADDRESS_LOW;
import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_REC_TYPE_CODE;
import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_REC_TYPE_LENGTH;
import static io.trino.plugin.warp.gen.constants.WEMatchJparams.WE_MATCH_JPARAMS_WARM_UP_TYPE;

class WarmupElementMatchParams
        implements MatchNode
{
    private static final long PRED_BUF_ADDRESS_LOW_MASK = 0x00000000ffffffffL;
    private static final int PRED_BUF_ADDRESS_HIGH_SHIFT = 32;

    private final int fileOffset;
    private final int fileReadSize;
    private final WarmUpType warmUpType;
    private final RecTypeCode recTypeCode;
    private final int recTypeLength;
    private final int warmEvents;
    private final boolean isImported;
    private final MemorySegment predBuf;
    private final boolean isCollectNulls;
    private final boolean isTightnessRequired;
    private final int matchCollectIndex;
    private final MatchCollectOp matchCollectOp;
    private final Optional<WarmupElementLuceneParams> luceneParams;

    public WarmupElementMatchParams(int fileOffset,
            int fileReadSize,
            WarmUpType warmUpType,
            RecTypeCode recTypeCode,
            int recTypeLength,
            int warmEvents,
            boolean isImported,
            MemorySegment predBuf,
            boolean isCollectNulls,
            boolean isTightnessRequired,
            int matchCollectIndex,
            MatchCollectOp matchCollectOp,
            Optional<WarmupElementLuceneParams> luceneParams)
    {
        this.fileOffset = fileOffset;
        this.fileReadSize = fileReadSize;
        this.warmUpType = warmUpType;
        this.recTypeCode = recTypeCode;
        this.recTypeLength = recTypeLength;
        this.warmEvents = warmEvents;
        this.isImported = isImported;
        this.predBuf = predBuf;
        this.isCollectNulls = isCollectNulls;
        this.isTightnessRequired = isTightnessRequired;
        this.matchCollectIndex = matchCollectIndex;
        this.matchCollectOp = matchCollectOp;
        this.luceneParams = luceneParams;
    }

    public RecTypeCode getRecTypeCode()
    {
        return recTypeCode;
    }

    public int getRecTypeLength()
    {
        return recTypeLength;
    }

    public boolean isCollectNulls()
    {
        return isCollectNulls;
    }

    public boolean hasLuceneParams()
    {
        return luceneParams.isPresent();
    }

    public LuceneQueryMatchData getLuceneQueryMatchData()
    {
        return luceneParams.get().luceneQueryMatchData();
    }

    public int getLuceneIx()
    {
        return luceneParams.get().luceneIx();
    }

    @Override
    public MatchNodeType getNodeType()
    {
        return MatchNodeType.MATCH_NODE_TYPE_LEAF;
    }

    @Override
    public List<MatchNode> getChildren()
    {
        return Collections.emptyList();
    }

    @Override
    public int getDumpSize()
    {
        return 1 + WE_MATCH_JPARAMS_NUM_OF.ordinal();
    }

    @Override
    public int dump(int[] output, int offset)
    {
        long predBufAddress = predBuf.address();

        output[offset++] = getNodeType().ordinal();
        output[offset + WE_MATCH_JPARAMS_FILE_OFFSET.ordinal()] = fileOffset;
        output[offset + WE_MATCH_JPARAMS_FILE_READ_SIZE.ordinal()] = fileReadSize;
        output[offset + WE_MATCH_JPARAMS_WARM_UP_TYPE.ordinal()] = warmUpType.ordinal();
        output[offset + WE_MATCH_JPARAMS_REC_TYPE_CODE.ordinal()] = TypeUtils.nativeRecTypeCode(recTypeCode);
        output[offset + WE_MATCH_JPARAMS_REC_TYPE_LENGTH.ordinal()] = recTypeLength;
        output[offset + WE_MATCH_JPARAMS_PRED_BUF_ADDRESS_HIGH.ordinal()] = (int) (predBufAddress >> PRED_BUF_ADDRESS_HIGH_SHIFT);
        output[offset + WE_MATCH_JPARAMS_PRED_BUF_ADDRESS_LOW.ordinal()] = (int) (predBufAddress & PRED_BUF_ADDRESS_LOW_MASK);
        output[offset + WE_MATCH_JPARAMS_IS_COLLECT_NULLS.ordinal()] = isCollectNulls ? 1 : 0;
        output[offset + WE_MATCH_JPARAMS_IS_TIGHTNESS_REQUIRED.ordinal()] = isTightnessRequired ? 1 : 0;
        output[offset + WE_MATCH_JPARAMS_MATCH_COLLECT_INDEX.ordinal()] = matchCollectIndex;
        output[offset + WE_MATCH_JPARAMS_MATCH_COLLECT_OP.ordinal()] = matchCollectOp.ordinal();
        return offset + WE_MATCH_JPARAMS_NUM_OF.ordinal();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fileOffset, fileReadSize, warmUpType, recTypeCode, recTypeLength, isCollectNulls, matchCollectOp, matchCollectIndex);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof WarmupElementMatchParams o)) {
            return false;
        }

        return fileOffset == o.fileOffset &&
                fileReadSize == o.fileReadSize &&
                warmUpType == o.warmUpType &&
                recTypeCode == o.recTypeCode &&
                recTypeLength == o.recTypeLength &&
                matchCollectOp == o.matchCollectOp &&
                matchCollectIndex == o.matchCollectIndex &&
                isCollectNulls == o.isCollectNulls;
    }

    @Override
    public String toString()
    {
        return "WarmupElementMatchParams{" +
                "fileOffset=" + fileOffset +
                ", fileReadSize=" + fileReadSize +
                ", warmUpType=" + warmUpType +
                ", recTypeCode=" + recTypeCode +
                ", recTypeLength=" + recTypeLength +
                ", warmEvents=" + warmEventsToString() +
                ", isImported=" + isImported +
                ", predBuf.address=" + predBuf.address() +
                ", isCollectNulls=" + isCollectNulls +
                ", isTightnessRequired=" + isTightnessRequired +
                ", matchCollectOp=" + matchCollectOp +
                ", matchCollectIndex=" + matchCollectIndex +
                ", luceneParams=" + luceneParams +
                '}';
    }

    private String warmEventsToString()
    {
        if (warmUpType == WarmUpType.WARM_UP_TYPE_BASIC) {
            return "basic" +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_COMPRESSION_LZ.ordinal()) ? ":compression_lz" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_DIRECT_ROOT.ordinal()) ? ":direct_root" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_RAW.ordinal()) ? ":entry_raw" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_RANGE.ordinal()) ? ":entry_range" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_DELTA.ordinal()) ? ":entry_delta" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_BM.ordinal()) ? ":entry_bm" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_DELTA_BIT_PACKING.ordinal()) ? ":entry_delta_bit_packing" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_ENTRY_SINGLES.ordinal()) ? ":entry_singles" : "") +
                    (eventOccurred(BasicWarmEvents.BASIC_WARM_EVENTS_SINGLE_CHUNK.ordinal()) ? ":single_chunk" : "");
        }
        return "no events";
    }

    private boolean eventOccurred(int eventNum)
    {
        return (warmEvents & (1 << eventNum)) != 0;
    }
}
