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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.plugin.varada.util.json.VaradaColumnJsonMapKeyDeserializer;
import io.trino.plugin.varada.util.json.VaradaColumnJsonSerializer;
import io.varada.tools.util.VaradaReadWriteLock;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@JsonDeserialize(builder = RowGroupData.Builder.class)
public class RowGroupData
{
    public static final String KEY = "key";
    public static final String WARMUP_ELEMENTS = "warmup_elements";
    public static final String PARTITION_KEYS = "partition_keys";
    public static final String IS_EMPTY = "is_empty";
    public static final String NODE_IDENTIFIER = "node_identifier";
    public static final String NEXT_OFFSET = "next_offset";
    public static final String NEXT_EXPORT_OFFSET = "next_export_offset";
    public static final String IS_SPARSE_FILE = "is_sparse_file";
    public static final String FAST_WARMING_STATE = "export_state";
    public static final String DATA_VALIDATION = "data_validation";

    private final RowGroupKey rowGroupKey;
    private final Collection<WarmUpElement> warmUpElements;
    private final Map<VaradaColumn, String> partitionKeys;
    private final boolean isEmpty;
    private final String nodeIdentifier;
    private final int nextOffset;
    private final int nextExportOffset;
    private final boolean isSparseFile;
    private final FastWarmingState fastWarmingState;
    private final RowGroupDataValidation dataValidation;

    @JsonIgnore
    private final VaradaReadWriteLock lock;
    @JsonIgnore
    private List<WarmUpElement> validWarmUpElements;

    protected RowGroupData(
            RowGroupKey rowGroupKey,
            Collection<WarmUpElement> warmUpElements,
            Map<VaradaColumn, String> partitionKeys,
            boolean isEmpty,
            String nodeIdentifier,
            int nextOffset)
    {
        this(rowGroupKey,
                warmUpElements,
                partitionKeys,
                isEmpty,
                nodeIdentifier,
                nextOffset,
                // when loading set the following values:
                nextOffset,                 // nextExportOffset = nextOffset
                false,                      // isSparseFile = false
                FastWarmingState.EXPORTED,  // nothing to export
                new RowGroupDataValidation(0, 0),
                new VaradaReadWriteLock());
    }

    private RowGroupData(
            RowGroupKey rowGroupKey,
            Collection<WarmUpElement> warmUpElements,
            Map<VaradaColumn, String> partitionKeys,
            boolean isEmpty,
            String nodeIdentifier,
            int nextOffset,
            int nextExportOffset,
            boolean isSparseFile,
            FastWarmingState fastWarmingState,
            RowGroupDataValidation dataValidation,
            VaradaReadWriteLock lock)
    {
        this.rowGroupKey = requireNonNull(rowGroupKey);
        this.warmUpElements = requireNonNull(warmUpElements);
        this.isEmpty = isEmpty;
        this.partitionKeys = requireNonNull(partitionKeys);
        this.nodeIdentifier = nodeIdentifier;
        this.nextOffset = nextOffset;
        this.nextExportOffset = nextExportOffset;
        this.isSparseFile = isSparseFile;
        this.fastWarmingState = fastWarmingState;
        this.dataValidation = requireNonNull(dataValidation);
        this.lock = requireNonNull(lock);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(RowGroupData rowGroupData)
    {
        return new Builder()
                .rowGroupKey(rowGroupData.getRowGroupKey())
                .warmUpElements(rowGroupData.getWarmUpElements())
                .partitionKeys(rowGroupData.getPartitionKeys())
                .isEmpty(rowGroupData.isEmpty())
                .nodeIdentifier(rowGroupData.getNodeIdentifier())
                .nextOffset(rowGroupData.getNextOffset())
                .nextExportOffset(rowGroupData.getNextExportOffset())
                .sparseFile(rowGroupData.isSparseFile())
                .fastWarmingState(rowGroupData.getFastWarmingState())
                .dataValidation(rowGroupData.getDataValidation())
                .lock(rowGroupData.getLock());
    }

    @JsonProperty(KEY)
    public RowGroupKey getRowGroupKey()
    {
        return rowGroupKey;
    }

    @JsonProperty(WARMUP_ELEMENTS)
    public Collection<WarmUpElement> getWarmUpElements()
    {
        // tODO make sure to order by offset
        return warmUpElements;
    }

    @JsonSerialize(keyUsing = VaradaColumnJsonSerializer.class)
    @JsonDeserialize(keyUsing = VaradaColumnJsonMapKeyDeserializer.class)
    @JsonProperty(PARTITION_KEYS)
    public Map<VaradaColumn, String> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty(IS_EMPTY)
    public boolean isEmpty()
    {
        return isEmpty;
    }

    @JsonProperty(NODE_IDENTIFIER)
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @JsonProperty(NEXT_OFFSET)
    public int getNextOffset()
    {
        return nextOffset;
    }

    @JsonProperty(NEXT_EXPORT_OFFSET)
    public int getNextExportOffset()
    {
        return nextExportOffset;
    }

    @JsonProperty(IS_SPARSE_FILE)
    public boolean isSparseFile()
    {
        return isSparseFile;
    }

    @JsonProperty(FAST_WARMING_STATE)
    public FastWarmingState getFastWarmingState()
    {
        return fastWarmingState;
    }

    @JsonProperty(DATA_VALIDATION)
    public RowGroupDataValidation getDataValidation()
    {
        return dataValidation;
    }

    @JsonIgnore
    public long getFileModifiedTime()
    {
        return dataValidation.fileModifiedTime();
    }

    @JsonIgnore
    public long getFileContentLength()
    {
        return dataValidation.fileContentLength();
    }

    @JsonIgnore
    public List<WarmUpElement> getValidWarmUpElements()
    {
        if (validWarmUpElements == null) {
            validWarmUpElements = warmUpElements.stream()
                    .filter(WarmUpElement::isValid)
                    .filter(WarmUpElement::isHot)
                    .collect(Collectors.toList());
        }
        return validWarmUpElements;
    }

    @JsonIgnore
    public VaradaReadWriteLock getLock()
    {
        return lock;
    }

    @Override
    public String toString()
    {
        return "RowGroupData{" +
                "rowGroupKey=" + rowGroupKey +
                ", warmUpElements=" + warmUpElements +
                ", isEmpty=" + isEmpty +
                ", nextOffset=" + nextOffset +
                ", partitionKeys=" + partitionKeys +
                ", nodeIdentifier='" + nodeIdentifier + '\'' +
                ", nextExportOffset=" + nextExportOffset +
                ", isSparseFile=" + isSparseFile +
                ", fastWarmingState=" + fastWarmingState +
                ", dataValidation=" + dataValidation +
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
        RowGroupData rowGroupData = (RowGroupData) o;
        return Objects.equals(rowGroupKey, rowGroupData.rowGroupKey) &&
                Iterables.elementsEqual(warmUpElements, rowGroupData.warmUpElements) &&
                Objects.equals(partitionKeys, rowGroupData.partitionKeys) &&
                (isEmpty == rowGroupData.isEmpty) &&
                Objects.equals(nodeIdentifier, rowGroupData.nodeIdentifier) &&
                (nextOffset == rowGroupData.nextOffset) &&
                (nextExportOffset == rowGroupData.nextExportOffset) &&
                (isSparseFile == rowGroupData.isSparseFile) &&
                Objects.equals(fastWarmingState, rowGroupData.fastWarmingState) &&
                Objects.equals(dataValidation, rowGroupData.dataValidation);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowGroupKey, warmUpElements, partitionKeys, isEmpty, nodeIdentifier, nextOffset, nextExportOffset, isSparseFile, fastWarmingState, dataValidation);
    }

    @JsonPOJOBuilder
    public static class Builder
    {
        private RowGroupKey rowGroupKey;
        private Collection<WarmUpElement> warmUpElements;
        private Map<VaradaColumn, String> partitionKeys = Map.of();
        private boolean isEmpty;
        private String nodeIdentifier;
        private int nextOffset;
        private int nextExportOffset;
        private boolean isSparseFile;
        private FastWarmingState fastWarmingState = FastWarmingState.EXPORTED;
        private RowGroupDataValidation dataValidation = new RowGroupDataValidation(0, 0);
        private VaradaReadWriteLock lock;

        @JsonProperty(KEY)
        public Builder rowGroupKey(RowGroupKey rowGroupKey)
        {
            this.rowGroupKey = rowGroupKey;
            return this;
        }

        @JsonProperty(WARMUP_ELEMENTS)
        public Builder warmUpElements(Collection<WarmUpElement> warmUpElements)
        {
            this.warmUpElements = ImmutableList.copyOf(warmUpElements);
            return this;
        }

        @JsonSerialize(keyUsing = VaradaColumnJsonSerializer.class)
        @JsonDeserialize(keyUsing = VaradaColumnJsonMapKeyDeserializer.class)
        @JsonProperty(PARTITION_KEYS)
        public Builder partitionKeys(Map<VaradaColumn, String> partitionKeys)
        {
            this.partitionKeys = Map.copyOf(partitionKeys);
            return this;
        }

        @JsonProperty(NODE_IDENTIFIER)
        public Builder nodeIdentifier(String nodeIdentifier)
        {
            this.nodeIdentifier = nodeIdentifier;
            return this;
        }

        @JsonProperty(NEXT_OFFSET)
        public Builder nextOffset(int nextOffset)
        {
            this.nextOffset = nextOffset;
            return this;
        }

        @JsonProperty(NEXT_EXPORT_OFFSET)
        public Builder nextExportOffset(int nextExportOffset)
        {
            this.nextExportOffset = nextExportOffset;
            return this;
        }

        @JsonProperty(IS_SPARSE_FILE)
        public Builder sparseFile(boolean isSparseFile)
        {
            this.isSparseFile = isSparseFile;
            return this;
        }

        @JsonProperty(FAST_WARMING_STATE)
        public Builder fastWarmingState(FastWarmingState fastWarmingState)
        {
            this.fastWarmingState = fastWarmingState;
            return this;
        }

        @JsonProperty(DATA_VALIDATION)
        public Builder dataValidation(RowGroupDataValidation dataValidation)
        {
            this.dataValidation = dataValidation;
            return this;
        }

        @JsonIgnore
        public Builder lock(VaradaReadWriteLock lock)
        {
            this.lock = lock;
            return this;
        }

        @JsonProperty(IS_EMPTY)
        public Builder isEmpty(boolean isEmpty)
        {
            this.isEmpty = isEmpty;
            return this;
        }

        public RowGroupData build()
        {
            return new RowGroupData(rowGroupKey,
                    warmUpElements,
                    partitionKeys,
                    isEmpty,
                    nodeIdentifier,
                    nextOffset,
                    nextExportOffset,
                    isSparseFile,
                    fastWarmingState,
                    dataValidation,
                    Objects.nonNull(lock) ? lock : new VaradaReadWriteLock());
        }
    }
}
