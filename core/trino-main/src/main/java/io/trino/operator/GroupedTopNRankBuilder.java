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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.trino.array.LongBigArray;
import io.trino.operator.RowReferencePageManager.LoadCursor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

/**
 * This class finds the top N rows by rank value defined by {@param comparator} and {@param equalsAndHash} for each
 * group specified by {@param groupByHash}.
 */
public class GroupedTopNRankBuilder
        implements GroupedTopNBuilder
{
    private static final long INSTANCE_SIZE = instanceSize(GroupedTopNRankBuilder.class);

    private final List<Type> sourceTypes;
    private final boolean produceRanking;
    private final int[] groupByChannels;
    @Nullable
    private GroupByHash groupByHash; // null after output starts
    private final PageWithPositionComparator comparator;
    private final RowReferencePageManager pageManager = new RowReferencePageManager();
    private final GroupedTopNRankAccumulator groupedTopNRankAccumulator;

    public GroupedTopNRankBuilder(
            List<Type> sourceTypes,
            PageWithPositionComparator comparator,
            PageWithPositionEqualsAndHash equalsAndHash,
            int topN,
            boolean produceRanking,
            int[] groupByChannels,
            GroupByHash groupByHash)
    {
        this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
        checkArgument(topN > 0, "topN must be > 0");
        this.produceRanking = produceRanking;
        this.groupByChannels = requireNonNull(groupByChannels, "groupByChannels is null");
        this.groupByHash = requireNonNull(groupByHash, "groupByHash is null");

        this.comparator = requireNonNull(comparator, "comparator is null");
        requireNonNull(equalsAndHash, "equalsAndHash is null");
        groupedTopNRankAccumulator = new GroupedTopNRankAccumulator(
                new RowIdComparisonHashStrategy()
                {
                    @Override
                    public int compare(long leftRowId, long rightRowId)
                    {
                        Page leftPage = pageManager.getPage(leftRowId);
                        int leftPosition = pageManager.getPosition(leftRowId);
                        Page rightPage = pageManager.getPage(rightRowId);
                        int rightPosition = pageManager.getPosition(rightRowId);
                        return comparator.compareTo(leftPage, leftPosition, rightPage, rightPosition);
                    }

                    @Override
                    public boolean equals(long leftRowId, long rightRowId)
                    {
                        Page leftPage = pageManager.getPage(leftRowId);
                        int leftPosition = pageManager.getPosition(leftRowId);
                        Page rightPage = pageManager.getPage(rightRowId);
                        int rightPosition = pageManager.getPosition(rightRowId);
                        return equalsAndHash.equals(leftPage, leftPosition, rightPage, rightPosition);
                    }

                    @Override
                    public long hashCode(long rowId)
                    {
                        Page page = pageManager.getPage(rowId);
                        int position = pageManager.getPosition(rowId);
                        return equalsAndHash.hashCode(page, position);
                    }
                },
                topN,
                pageManager::dereference);
    }

    @Override
    public Work<?> processPage(Page page)
    {
        if (groupByHash == null) {
            throw new IllegalStateException("already producing results");
        }
        return new TransformWork<>(
                groupByHash.getGroupIds(page.getColumns(groupByChannels)),
                groupIds -> {
                    processPage(page, groupByHash.getGroupCount(), groupIds);
                    return null;
                });
    }

    @Override
    public Iterator<Page> buildResult()
    {
        if (groupByHash == null) {
            throw new IllegalStateException("already producing results");
        }
        int groupIdCount = groupByHash.getGroupCount();
        groupByHash = null;
        return new ResultIterator(groupIdCount);
    }

    @Override
    public long getEstimatedSizeInBytes()
    {
        return INSTANCE_SIZE
                + (groupByHash == null ? 0L : groupByHash.getEstimatedSize())
                + pageManager.sizeOf()
                + groupedTopNRankAccumulator.sizeOf();
    }

    private void processPage(Page newPage, int groupCount, int[] groupIds)
    {
        int firstPositionToAdd = groupedTopNRankAccumulator.findFirstPositionToAdd(newPage, groupCount, groupIds, comparator, pageManager);
        if (firstPositionToAdd < 0) {
            return;
        }

        try (LoadCursor loadCursor = pageManager.add(newPage, firstPositionToAdd)) {
            for (int position = firstPositionToAdd; position < newPage.getPositionCount(); position++) {
                int groupId = groupIds[position];
                loadCursor.advance();
                groupedTopNRankAccumulator.add(groupId, loadCursor);
            }
            verify(!loadCursor.advance());
        }

        pageManager.compactIfNeeded();
    }

    private class ResultIterator
            extends AbstractIterator<Page>
    {
        private final PageBuilder pageBuilder;
        private final int groupIdCount;
        private int currentGroupId = -1;
        private final LongBigArray rowIdOutput = new LongBigArray();
        private final LongBigArray rankingOutput = new LongBigArray();
        private long currentGroupSize;
        private int currentIndexInGroup;

        private ResultIterator(int groupIdCount)
        {
            this.groupIdCount = groupIdCount;
            ImmutableList.Builder<Type> sourceTypesBuilders = ImmutableList.<Type>builder().addAll(sourceTypes);
            if (produceRanking) {
                sourceTypesBuilders.add(BIGINT);
            }
            pageBuilder = new PageBuilder(sourceTypesBuilders.build());
        }

        @Override
        protected Page computeNext()
        {
            pageBuilder.reset();
            while (!pageBuilder.isFull()) {
                while (currentIndexInGroup >= currentGroupSize) {
                    if (currentGroupId + 1 >= groupIdCount) {
                        if (pageBuilder.isEmpty()) {
                            return endOfData();
                        }
                        return pageBuilder.build();
                    }
                    currentGroupId++;
                    currentGroupSize = produceRanking
                            ? groupedTopNRankAccumulator.drainTo(currentGroupId, rowIdOutput, rankingOutput)
                            : groupedTopNRankAccumulator.drainTo(currentGroupId, rowIdOutput);
                    currentIndexInGroup = 0;
                }

                long rowId = rowIdOutput.get(currentIndexInGroup);
                Page page = pageManager.getPage(rowId);
                int position = pageManager.getPosition(rowId);
                for (int i = 0; i < sourceTypes.size(); i++) {
                    sourceTypes.get(i).appendTo(page.getBlock(i), position, pageBuilder.getBlockBuilder(i));
                }
                if (produceRanking) {
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(sourceTypes.size()), rankingOutput.get(currentIndexInGroup));
                }
                pageBuilder.declarePosition();
                currentIndexInGroup++;

                // Deference the row for hygiene, but no need to compact them at this point
                pageManager.dereference(rowId);
            }

            if (pageBuilder.isEmpty()) {
                return endOfData();
            }
            return pageBuilder.build();
        }
    }
}
