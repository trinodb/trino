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
package io.trino.plugin.varada.dispatcher.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

@JsonDeserialize(builder = WarmUpElement.Builder.class)
public class WarmUpElement
{
    public static final String VARADA_COLUMN = "varadaColumn";
    public static final String WARM_UP_TYPE = "warmUpType";
    public static final String STORE_ID = "storeId";

    public static final String REC_TYPE_CODE = "recTypeCode";
    public static final String REC_TYPE_LENGTH = "recTypeLength";
    public static final String STATS = "stats";
    public static final String USED_DICTIONARY_SIZE = "usedDictionarySize";
    public static final String DICTIONARY_INFO = "dictionaryInfo";
    public static final String START_OFFSET = "startOffset";
    public static final String QUERY_OFFSET = "queryOffset";
    public static final String QUERY_READ_SIZE = "queryReadSize";
    public static final String WARM_EVENTS = "warmEvents";
    public static final String END_OFFSET = "endOffset";
    public static final String STATE = "state";
    public static final String EXPORT_STATE = "exportState";
    public static final String IS_IMPORTED = "isImported";
    public static final String WARM_STATE = "warmState";

    public static final String TOTAL_RECORDS = "total_records";

    private final VaradaColumn varadaColumn;
    private final WarmUpType warmUpType;
    private final RecTypeCode recTypeCode;
    private final int recTypeLength;
    private final WarmupElementStats warmupElementStats;
    private final int usedDictionarySize;
    private final DictionaryInfo dictionaryInfo;
    private final int startOffset;
    private final int queryOffset;
    private final int queryReadSize;
    private final int warmEvents;
    private final int endOffset;
    private final WarmUpElementState state;
    private transient long lastUsedTimestamp;
    private final ExportState exportState;
    private final boolean isImported;
    private final WarmState warmState;
    private final int totalRecords;

    /**
     * unique id for cacheManager, all WE that warmed in @CacheManager::storePages will have the same storeId
     */
    private final UUID storeId;

    private WarmUpElement(
            VaradaColumn varadaColumn,
            WarmUpType warmUpType,
            RecTypeCode recTypeCode,
            int recTypeLength,
            WarmupElementStats warmupElementStats,
            int usedDictionarySize,
            DictionaryInfo dictionaryInfo,
            int startOffset,
            int queryOffset,
            int queryReadSize,
            int warmEvents,
            int endOffset,
            WarmUpElementState state,
            ExportState exportState,
            boolean isImported,
            WarmState warmState,
            int totalRecords,
            UUID storeId)
    {
        this.varadaColumn = requireNonNull(varadaColumn);
        this.warmUpType = requireNonNull(warmUpType);
        this.recTypeCode = recTypeCode;
        this.recTypeLength = recTypeLength;
        this.warmupElementStats = requireNonNull(warmupElementStats);
        this.usedDictionarySize = usedDictionarySize;
        this.dictionaryInfo = dictionaryInfo;
        this.startOffset = startOffset;
        this.queryOffset = queryOffset;
        this.queryReadSize = queryReadSize;
        this.warmEvents = warmEvents;
        this.endOffset = endOffset;
        this.state = state;
        this.exportState = exportState;
        this.isImported = isImported;
        this.warmState = warmState;
        this.totalRecords = totalRecords;
        this.storeId = storeId;
        this.lastUsedTimestamp = System.currentTimeMillis(); // when loading from DB sets to loading time
    }

    public static Builder builder(WarmUpElement warmUpElement)
    {
        return new Builder()
                .varadaColumn(warmUpElement.getVaradaColumn())
                .warmUpType(warmUpElement.getWarmUpType())
                .recTypeCode(warmUpElement.getRecTypeCode())
                .recTypeLength(warmUpElement.getRecTypeLength())
                .warmupElementStats(warmUpElement.getWarmupElementStats())
                .usedDictionarySize(warmUpElement.getUsedDictionarySize())
                .dictionaryInfo(warmUpElement.getDictionaryInfo())
                .startOffset(warmUpElement.getStartOffset())
                .queryOffset(warmUpElement.getQueryOffset())
                .queryReadSize(warmUpElement.getQueryReadSize())
                .warmEvents(warmUpElement.getWarmEvents())
                .endOffset(warmUpElement.getEndOffset())
                .state(warmUpElement.getState())
                .storeId(warmUpElement.getStoreId())
                .exportState(warmUpElement.getExportState())
                .isImported(warmUpElement.isImported())
                .totalRecords(warmUpElement.getTotalRecords())
                .warmState(warmUpElement.getWarmState())
                .lastUsedTimestamp(warmUpElement.getLastUsedTimestamp());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @JsonProperty(VARADA_COLUMN)
    public VaradaColumn getVaradaColumn()
    {
        return varadaColumn;
    }

    @JsonProperty(WARM_UP_TYPE)
    public WarmUpType getWarmUpType()
    {
        return warmUpType;
    }

    @JsonProperty(STORE_ID)
    public UUID getStoreId()
    {
        return storeId;
    }

    @JsonProperty(REC_TYPE_CODE)
    public RecTypeCode getRecTypeCode()
    {
        return recTypeCode;
    }

    @JsonProperty(REC_TYPE_LENGTH)
    public int getRecTypeLength()
    {
        return recTypeLength;
    }

    @JsonProperty(EXPORT_STATE)
    public ExportState getExportState()
    {
        return exportState;
    }

    @JsonProperty(STATS)
    public WarmupElementStats getWarmupElementStats()
    {
        return warmupElementStats;
    }

    @JsonProperty(DICTIONARY_INFO)
    public DictionaryInfo getDictionaryInfo()
    {
        return dictionaryInfo;
    }

    @JsonProperty(USED_DICTIONARY_SIZE)
    public int getUsedDictionarySize()
    {
        return usedDictionarySize;
    }

    @JsonProperty(STATE)
    public WarmUpElementState getState()
    {
        return state;
    }

    @JsonProperty(IS_IMPORTED)
    public boolean isImported()
    {
        return isImported;
    }

    @JsonProperty(TOTAL_RECORDS)
    public int getTotalRecords()
    {
        return totalRecords;
    }

    @JsonIgnore
    public boolean isValid()
    {
        return WarmUpElementState.State.VALID.equals(state.state());
    }

    @JsonIgnore
    public boolean isSameColNameAndWarmUpType(WarmUpElement other)
    {
        return varadaColumn.equals(other.getVaradaColumn()) && warmUpType.equals(other.getWarmUpType());
    }

    @JsonIgnore
    public long getLastUsedTimestamp()
    {
        return lastUsedTimestamp;
    }

    @JsonIgnore
    public void setLastUsedTimestamp(long lastUsedTimestamp)
    {
        this.lastUsedTimestamp = lastUsedTimestamp;
    }

    @JsonIgnore
    public boolean isDictionaryUsed()
    {
        return usedDictionarySize > 0;
    }

    @JsonProperty(START_OFFSET)
    public int getStartOffset()
    {
        return startOffset;
    }

    @JsonProperty(QUERY_OFFSET)
    public int getQueryOffset()
    {
        return queryOffset;
    }

    @JsonProperty(QUERY_READ_SIZE)
    public int getQueryReadSize()
    {
        return queryReadSize;
    }

    @JsonProperty(WARM_EVENTS)
    public int getWarmEvents()
    {
        return warmEvents;
    }

    @JsonProperty(END_OFFSET)
    public int getEndOffset()
    {
        return endOffset;
    }

    @JsonProperty(WARM_STATE)
    public WarmState getWarmState()
    {
        return warmState;
    }

    @JsonIgnore
    public boolean isHot()
    {
        return WarmState.HOT.equals(warmState);
    }

    @Override
    public String toString()
    {
        return "WarmUpElement{" +
                "varadaColumn='" + varadaColumn + '\'' +
                ", warmUpType=" + warmUpType +
                ", recTypeCode=" + recTypeCode +
                ", recTypeLength=" + recTypeLength +
                ", exportState=" + exportState +
                ", dictionaryInfo=" + dictionaryInfo +
                ", usedDictionarySize=" + usedDictionarySize +
                ", warmupElementStats=" + warmupElementStats +
                ", state=" + state +
                ", lastUsedTimestamp=" + lastUsedTimestamp +
                ", startOffset=" + startOffset +
                ", queryOffset=" + queryOffset +
                ", queryReadSize=" + queryReadSize +
                ", warmEvents=" + warmEvents +
                ", endOffset=" + endOffset +
                ", warmState=" + warmState +
                ", totalRecords=" + totalRecords +
                ", isImported=" + isImported +
                ", storeId=" + storeId +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WarmUpElement warmUpElement = (WarmUpElement) o;
        return Objects.equals(varadaColumn, warmUpElement.varadaColumn) &&
                (warmUpType == warmUpElement.warmUpType) &&
                (recTypeCode == warmUpElement.recTypeCode) &&
                (recTypeLength == warmUpElement.recTypeLength) &&
                Objects.equals(warmupElementStats, warmUpElement.warmupElementStats) &&
                (usedDictionarySize == warmUpElement.usedDictionarySize) &&
                Objects.equals(dictionaryInfo, warmUpElement.dictionaryInfo) &&
                (startOffset == warmUpElement.startOffset) &&
                (queryOffset == warmUpElement.queryOffset) &&
                (queryReadSize == warmUpElement.queryReadSize) &&
                (warmEvents == warmUpElement.warmEvents) &&
                Objects.equals(storeId, warmUpElement.storeId) &&
                (totalRecords == warmUpElement.totalRecords) &&
                (endOffset == warmUpElement.endOffset) &&
                Objects.equals(state, warmUpElement.state) &&
                Objects.equals(exportState, warmUpElement.exportState) &&
                (isImported == warmUpElement.isImported) &&
                (warmState == warmUpElement.warmState);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(varadaColumn, warmUpType, recTypeCode, recTypeLength, warmupElementStats, usedDictionarySize, dictionaryInfo, startOffset, queryOffset, queryReadSize, warmEvents, endOffset, state, exportState, isImported, warmState, storeId, totalRecords);
    }

    @JsonPOJOBuilder
    public static class Builder
    {
        VaradaColumn varadaColumn;
        WarmUpType warmUpType;
        RecTypeCode recTypeCode;
        int recTypeLength;
        long lastUsedTimestamp = Instant.now().toEpochMilli();
        ExportState exportState = ExportState.NOT_EXPORTED;
        WarmupElementStats warmupElementStats;
        int usedDictionarySize;
        DictionaryInfo dictionaryInfo;
        WarmUpElementState state = WarmUpElementState.VALID;
        private int startOffset;
        private int queryOffset;
        private int queryReadSize;
        private int warmEvents;
        private int endOffset;

        private int totalRecords;
        private boolean isImported;
        private WarmState warmState = WarmState.HOT;
        private UUID storeId;

        @JsonProperty(WARM_UP_TYPE)
        public Builder warmUpType(WarmUpType warmUpType)
        {
            this.warmUpType = warmUpType;
            return this;
        }

        @JsonProperty(REC_TYPE_CODE)
        public Builder recTypeCode(RecTypeCode recTypeCode)
        {
            this.recTypeCode = recTypeCode;
            return this;
        }

        @JsonProperty(REC_TYPE_LENGTH)
        public Builder recTypeLength(int recTypeLength)
        {
            this.recTypeLength = recTypeLength;
            return this;
        }

        @JsonIgnore
        public Builder colName(String colName)
        {
            return varadaColumn(new RegularColumn(colName));
        }

        @JsonProperty(VARADA_COLUMN)
        public Builder varadaColumn(VaradaColumn varadaColumn)
        {
            this.varadaColumn = varadaColumn;
            return this;
        }

        @JsonIgnore
        public Builder lastUsedTimestamp(long lastUsedTimestamp)
        {
            this.lastUsedTimestamp = lastUsedTimestamp;
            return this;
        }

        @JsonProperty(EXPORT_STATE)
        public Builder exportState(ExportState exportState)
        {
            this.exportState = exportState;
            return this;
        }

        @JsonProperty(DICTIONARY_INFO)
        public Builder dictionaryInfo(DictionaryInfo dictionaryInfo)
        {
            this.dictionaryInfo = dictionaryInfo;
            return this;
        }

        @JsonProperty(USED_DICTIONARY_SIZE)
        public Builder usedDictionarySize(int usedDictionarySize)
        {
            this.usedDictionarySize = usedDictionarySize;
            return this;
        }

        @JsonProperty(STATE)
        public Builder state(WarmUpElementState state)
        {
            this.state = state;
            return this;
        }

        @JsonProperty(STATS)
        public Builder warmupElementStats(WarmupElementStats warmupElementStats)
        {
            this.warmupElementStats = warmupElementStats;
            return this;
        }

        @JsonProperty(START_OFFSET)
        public Builder startOffset(int startOffset)
        {
            this.startOffset = startOffset;
            return this;
        }

        @JsonProperty(QUERY_OFFSET)
        public Builder queryOffset(int queryOffset)
        {
            this.queryOffset = queryOffset;
            return this;
        }

        @JsonProperty(QUERY_READ_SIZE)
        public Builder queryReadSize(int queryReadSize)
        {
            this.queryReadSize = queryReadSize;
            return this;
        }

        @JsonProperty(WARM_EVENTS)
        public Builder warmEvents(int warmEvents)
        {
            this.warmEvents = warmEvents;
            return this;
        }

        @JsonProperty(END_OFFSET)
        public Builder endOffset(int endOffset)
        {
            this.endOffset = endOffset;
            return this;
        }

        @JsonProperty(STORE_ID)
        public Builder storeId(UUID storeId)
        {
            this.storeId = storeId;
            return this;
        }

        @JsonProperty(IS_IMPORTED)
        public Builder isImported(boolean isImported)
        {
            this.isImported = isImported;
            return this;
        }

        @JsonProperty(WARM_STATE)
        public Builder warmState(WarmState warmState)
        {
            this.warmState = warmState;
            return this;
        }

        @JsonProperty(TOTAL_RECORDS)
        public Builder totalRecords(int totalRecords)
        {
            this.totalRecords = totalRecords;
            return this;
        }

        public WarmUpElement build()
        {
            WarmUpElement warmUpElement = new WarmUpElement(varadaColumn,
                    warmUpType,
                    recTypeCode,
                    recTypeLength,
                    warmupElementStats,
                    usedDictionarySize,
                    dictionaryInfo,
                    startOffset,
                    queryOffset,
                    queryReadSize,
                    warmEvents,
                    endOffset,
                    state,
                    exportState,
                    isImported,
                    warmState,
                    totalRecords,
                    storeId);
            warmUpElement.lastUsedTimestamp = lastUsedTimestamp;
            return warmUpElement;
        }
    }
}
