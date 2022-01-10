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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import io.trino.Session;
import io.trino.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.trino.operator.hash.ColumnValueExtractor;
import io.trino.operator.hash.IsolatedRowExtractorFactory;
import io.trino.operator.hash.MultiChannelBigintGroupByHashInlineBatch;
import io.trino.operator.hash.MultiChannelGroupByHashBatch;
import io.trino.operator.hash.MultiChannelGroupByHashInlineFastBBAllTypes;
import io.trino.operator.hash.bigint.BigintGroupByHashInlineGID;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;

import static io.trino.SystemSessionProperties.getEnhancedGroupByMaxVarWidthBufferSize;
import static io.trino.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.trino.SystemSessionProperties.isUseEnhancedGroupByEnabled;
import static io.trino.spi.type.BigintType.BIGINT;

public interface GroupByHash
        extends AutoCloseable
{
    IsolatedRowExtractorFactory ISOLATED_ROW_EXTRACTOR_FACTORY = new IsolatedRowExtractorFactory();

    static GroupByHash createGroupByHash(
            Session session,
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            UpdateMemory updateMemory)
    {
        return createGroupByHash(
                hashTypes,
                hashChannels,
                inputHashChannel,
                expectedSize,
                isDictionaryAggregationEnabled(session),
                isUseEnhancedGroupByEnabled(session),
                getEnhancedGroupByMaxVarWidthBufferSize(session),
                joinCompiler,
                blockTypeOperators,
                updateMemory);
    }

    static GroupByHash createGroupByHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            UpdateMemory updateMemory)
    {
        return createGroupByHash(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, false, 16, joinCompiler, blockTypeOperators, updateMemory);
    }

    static GroupByHash createGroupByHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            boolean useEnhancedGroupBy,
            int maxVarWidthBufferSize, JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            UpdateMemory updateMemory)
    {
        if (useEnhancedGroupBy && hashTypes.stream().allMatch(ColumnValueExtractor::isSupported)) {
            return new MultiChannelGroupByHashInlineFastBBAllTypes(ISOLATED_ROW_EXTRACTOR_FACTORY, hashTypes, hashChannels, inputHashChannel, expectedSize, updateMemory, blockTypeOperators, maxVarWidthBufferSize);
        }
        if (useEnhancedGroupBy && hashTypes.stream().allMatch(type -> type.equals(BIGINT))) {
            return new MultiChannelBigintGroupByHashInlineBatch(hashChannels, inputHashChannel, expectedSize, updateMemory);
        }
        if (hashTypes.size() == 1 && hashTypes.get(0).equals(BIGINT) && hashChannels.length == 1) {
            if (useEnhancedGroupBy) {
                return new BigintGroupByHashInlineGID(hashChannels[0], inputHashChannel.isPresent(), expectedSize, updateMemory);
            }
            return new BigintGroupByHash(hashChannels[0], inputHashChannel.isPresent(), expectedSize, updateMemory);
        }

        return new MultiChannelGroupByHash(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, joinCompiler, blockTypeOperators, updateMemory);
    }

    long getEstimatedSize();

    long getHashCollisions();

    double getExpectedHashCollisions();

    List<Type> getTypes();

    int getGroupCount();

    Work<?> addPage(Page page);

    Work<GroupByIdBlock> getGroupIds(Page page);

    boolean contains(int position, Page page, int[] hashChannels);

    default boolean contains(int position, Page page, int[] hashChannels, long rawHash)
    {
        return contains(position, page, hashChannels);
    }

    @VisibleForTesting
    int getCapacity();

    default GroupCursor hashSortedGroups()
    {
        throw new UnsupportedOperationException(getClass().getName() + " does not support groups");
    }

    default GroupCursor consecutiveGroups()
    {
        throw new UnsupportedOperationException(getClass().getName() + " does not support groups");
    }

    interface GroupCursor
    {
        boolean hasNext();

        void next();

        void appendValuesTo(PageBuilder pageBuilder, int outputChannelOffset);

        int getGroupId();

        void evaluatePage(PageBuilder pageBuilder, List<InMemoryHashAggregationBuilder.Aggregator> aggregators);
    }

    @Override
    default void close()
    {
    }
}
