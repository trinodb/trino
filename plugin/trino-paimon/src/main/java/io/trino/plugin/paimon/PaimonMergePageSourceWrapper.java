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
package io.trino.plugin.paimon;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.BigintType;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.util.Objects.requireNonNull;

public class PaimonMergePageSourceWrapper
        implements ConnectorPageSource
{
    static final String METADATA_DELETE_ROW_ID_FIELD = "_metadata_delete";

    private final ConnectorPageSource pageSource;
    private final List<String> rowIdFields;
    private final Map<String, Integer> fieldToIndex;
    private final Optional<int[]> outputChannels;

    public PaimonMergePageSourceWrapper(
            ConnectorPageSource pageSource,
            List<String> rowIdFields,
            Map<String, Integer> fieldToIndex)
    {
        this(pageSource, rowIdFields, fieldToIndex, Optional.empty());
    }

    private PaimonMergePageSourceWrapper(
            ConnectorPageSource pageSource,
            List<String> rowIdFields,
            Map<String, Integer> fieldToIndex,
            Optional<int[]> outputChannels)
    {
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
        this.rowIdFields = copyRowIdFields(rowIdFields);
        this.fieldToIndex = copyFieldToIndex(fieldToIndex, this.rowIdFields);
        this.outputChannels = copyOutputChannels(outputChannels);
    }

    public static PaimonMergePageSourceWrapper wrap(
            ConnectorPageSource pageSource,
            List<String> rowIdFields,
            Map<String, Integer> fieldToIndex)
    {
        return new PaimonMergePageSourceWrapper(pageSource, rowIdFields, fieldToIndex);
    }

    public static PaimonMergePageSourceWrapper wrap(
            ConnectorPageSource pageSource,
            List<String> rowIdFields,
            Map<String, Integer> fieldToIndex,
            int[] outputChannels)
    {
        return new PaimonMergePageSourceWrapper(
                pageSource,
                rowIdFields,
                fieldToIndex,
                Optional.of(requireNonNull(outputChannels, "outputChannels is null")));
    }

    private static List<String> copyRowIdFields(List<String> rowIdFields)
    {
        requireNonNull(rowIdFields, "rowIdFields is null");
        if (rowIdFields.isEmpty()) {
            throw new IllegalArgumentException("rowIdFields is empty");
        }
        Set<String> seenFields = new HashSet<>();
        for (String rowIdField : rowIdFields) {
            requireNonNull(rowIdField, "rowIdFields contains null field");
            if (rowIdField.isBlank()) {
                throw new IllegalArgumentException("rowIdFields contains blank field");
            }
            if (!seenFields.add(rowIdField)) {
                throw new IllegalArgumentException("rowIdFields contains duplicate field: " + rowIdField);
            }
        }
        return List.copyOf(rowIdFields);
    }

    private static Map<String, Integer> copyFieldToIndex(
            Map<String, Integer> fieldToIndex,
            List<String> rowIdFields)
    {
        requireNonNull(fieldToIndex, "fieldToIndex is null");
        fieldToIndex.forEach((field, index) -> {
            requireNonNull(field, "fieldToIndex contains null field");
            if (field.isBlank()) {
                throw new IllegalArgumentException("fieldToIndex contains blank field");
            }
            requireNonNull(index, "fieldToIndex contains null index for field '" + field + "'");
            if (index < 0) {
                throw new IllegalArgumentException(
                        "fieldToIndex contains negative index for field '%s': %s".formatted(field, index));
            }
        });
        for (String rowIdField : rowIdFields) {
            if (METADATA_DELETE_ROW_ID_FIELD.equals(rowIdField)) {
                continue;
            }
            if (!fieldToIndex.containsKey(rowIdField)) {
                throw new IllegalArgumentException("Missing row id field: " + rowIdField);
            }
        }
        return new HashMap<>(fieldToIndex);
    }

    private static Optional<int[]> copyOutputChannels(Optional<int[]> outputChannels)
    {
        requireNonNull(outputChannels, "outputChannels is null");
        return outputChannels.map(channels -> {
            int[] copy = requireNonNull(channels, "outputChannels contains null channels").clone();
            for (int channel : copy) {
                if (channel < 0) {
                    throw new IllegalArgumentException("outputChannels contains negative channel: " + channel);
                }
            }
            return copy;
        });
    }

    @Override
    public long getCompletedBytes()
    {
        return pageSource.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return pageSource.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return pageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return pageSource.isFinished();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        try {
            SourcePage sourcePage = pageSource.getNextSourcePage();
            if (sourcePage == null) {
                return null;
            }
            Page nextPage = sourcePage.getPage();
            int rowCount = nextPage.getPositionCount();

            int outputChannelCount = outputChannels.map(channels -> channels.length)
                    .orElse(nextPage.getChannelCount());
            Block[] newBlocks = new Block[outputChannelCount + 1];
            if (outputChannels.isPresent()) {
                int[] channels = outputChannels.get();
                for (int output = 0; output < channels.length; output++) {
                    int channel = channels[output];
                    if (channel >= nextPage.getChannelCount()) {
                        throw new IllegalStateException(
                                "Output channel %s is not present in page with %s channels"
                                        .formatted(channel, nextPage.getChannelCount()));
                    }
                    newBlocks[output] = nextPage.getBlock(channel);
                }
            }
            else {
                for (int i = 0; i < nextPage.getChannelCount(); i++) {
                    newBlocks[i] = nextPage.getBlock(i);
                }
            }

            Block[] rowIdBlocks = new Block[rowIdFields.size()];
            for (int i = 0; i < rowIdFields.size(); i++) {
                String fieldName = rowIdFields.get(i);
                if (METADATA_DELETE_ROW_ID_FIELD.equals(fieldName)) {
                    rowIdBlocks[i] = RunLengthEncodedBlock.create(BigintType.BIGINT, 0L, rowCount);
                    continue;
                }
                int channelIndex = fieldToIndex.get(fieldName);
                if (channelIndex >= nextPage.getChannelCount()) {
                    throw new IllegalStateException(
                            "Row id field '%s' maps to channel %s, but page has %s channels"
                                    .formatted(fieldName, channelIndex, nextPage.getChannelCount()));
                }
                rowIdBlocks[i] = nextPage.getBlock(channelIndex);
            }

            newBlocks[outputChannelCount] = RowBlock.fromNotNullSuppressedFieldBlocks(
                    rowCount,
                    Optional.empty(),
                    rowIdBlocks);

            return SourcePage.create(new Page(rowCount, newBlocks));
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, this);
            throw PaimonPageSourceProvider.wrapPaimonReadException(e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return pageSource.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        pageSource.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return pageSource.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return pageSource.getMetrics();
    }
}
