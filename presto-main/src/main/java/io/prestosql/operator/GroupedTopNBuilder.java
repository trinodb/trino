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
package io.prestosql.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.operator.RankingBuilder.RowWithRanking;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.openjdk.jol.info.ClassLayout;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.operator.RankingBuilders.createRankingBuilder;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

/**
 * This class finds the top N rows defined by {@param comparator} for each group specified by {@param groupByHash}.
 */
public class GroupedTopNBuilder
{
    enum RankingFunction
    {
        ROW_NUMBER, RANK, DENSE_RANK
    }

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(GroupedTopNBuilder.class).instanceSize();
    // compact a page when 50% of its positions are unreferenced
    public static final int COMPACT_THRESHOLD = 2;

    private final List<Type> sourceTypes;
    private final int topN;

    private final RankingFunction rankingFunction;
    private final boolean produceRanking;
    private final GroupByHash groupByHash;

    // a map of heaps, each of which records the top N rows

    private final ObjectBigArray<RankingBuilder> groupedRows = new ObjectBigArray<>();

    // a list of input pages, each of which has information of which row in which heap references which position
    private final ObjectBigArray<PageReference> pageReferences = new ObjectBigArray<>();
    // for heap element comparison
    private final Comparator<Row> comparator;
    // when there is no row referenced in a page, it will be removed instead of compacted; use a list to record those empty slots to reuse them
    private final IntFIFOQueue emptyPageReferenceSlots;

    // keeps track sizes of input pages and heaps
    private long memorySizeInBytes;
    private int currentPageCount;

    public GroupedTopNBuilder(
            List<Type> sourceTypes,
            PageWithPositionComparator comparator,
            int topN,
            RankingFunction rankingFunction,
            boolean produceRanking,
            GroupByHash groupByHash)
    {
        this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
        checkArgument(topN > 0, "topN must be > 0");
        this.topN = topN;
        this.rankingFunction = rankingFunction;
        this.produceRanking = produceRanking;
        this.groupByHash = requireNonNull(groupByHash, "groupByHash is not null");

        requireNonNull(comparator, "comparator is null");
        this.comparator = (left, right) -> comparator.compareTo(
                pageReferences.get(left.getPageId()).getPage(),
                left.getPosition(),
                pageReferences.get(right.getPageId()).getPage(),
                right.getPosition());
        this.emptyPageReferenceSlots = new IntFIFOQueue();
    }

    public Work<?> processPage(Page page)
    {
        return new TransformWork<>(
                groupByHash.getGroupIds(page),
                groupIds -> {
                    processPage(page, groupIds);
                    return null;
                });
    }

    public Iterator<Page> buildResult()
    {
        return new ResultIterator();
    }

    public long getEstimatedSizeInBytes()
    {
        return INSTANCE_SIZE +
                memorySizeInBytes +
                groupByHash.getEstimatedSize() +
                groupedRows.sizeOf() +
                pageReferences.sizeOf() +
                emptyPageReferenceSlots.getEstimatedSizeInBytes();
    }

    @VisibleForTesting
    List<Page> getBufferedPages()
    {
        return IntStream.range(0, currentPageCount)
                .filter(i -> pageReferences.get(i) != null)
                .mapToObj(i -> pageReferences.get(i).getPage())
                .collect(toImmutableList());
    }

    private void processPage(Page newPage, GroupByIdBlock groupIds)
    {
        checkArgument(newPage != null);
        checkArgument(groupIds != null);

        // save the new page
        PageReference newPageReference = new PageReference(newPage);
        memorySizeInBytes += newPageReference.getEstimatedSizeInBytes();
        int newPageId;
        if (emptyPageReferenceSlots.isEmpty()) {
            // all the previous slots are full; create a new one
            pageReferences.ensureCapacity(currentPageCount + 1);
            newPageId = currentPageCount;
            currentPageCount++;
        }
        else {
            // reuse a previously removed page's slot
            newPageId = emptyPageReferenceSlots.dequeueInt();
        }
        verify(pageReferences.get(newPageId) == null, "should not overwrite a non-empty slot");
        pageReferences.set(newPageId, newPageReference);

        // update the affected heaps and record candidate pages that need compaction
        IntSet pagesToCompact = new IntOpenHashSet();
        for (int position = 0; position < newPage.getPositionCount(); position++) {
            long groupId = groupIds.getGroupId(position);
            groupedRows.ensureCapacity(groupId + 1);

            RankingBuilder builder = groupedRows.get(groupId);
            if (builder == null) {
                // a new group
                builder = createRankingBuilder(rankingFunction, comparator, pageReferences, topN);
                groupedRows.set(groupId, builder);
            }
            else {
                // update an existing group;
                // remove the memory usage for this group for now; add it back after update
                memorySizeInBytes -= builder.getEstimatedSizeInBytes();
            }

            Row newRow = new Row(newPageId, position);
            builder.addRow(newRow, pagesToCompact);

            memorySizeInBytes += builder.getEstimatedSizeInBytes();
        }

        // may compact the new page as well
        if (newPageReference.getUsedPositionCount() * COMPACT_THRESHOLD < newPage.getPositionCount()) {
            verify(!pagesToCompact.contains(newPageId));
            pagesToCompact.add(newPageId);
        }

        // compact pages
        IntIterator iterator = pagesToCompact.iterator();
        while (iterator.hasNext()) {
            int pageId = iterator.nextInt();
            PageReference pageReference = pageReferences.get(pageId);
            if (pageReference.getUsedPositionCount() == 0) {
                pageReferences.set(pageId, null);
                emptyPageReferenceSlots.enqueue(pageId);
                memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
            }
            else {
                memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                pageReference.compact();
                memorySizeInBytes += pageReference.getEstimatedSizeInBytes();
            }
        }
    }

    // this class is for precise memory tracking
    private static class IntFIFOQueue
            extends IntArrayFIFOQueue
    {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(IntFIFOQueue.class).instanceSize();

        private long getEstimatedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOf(array);
        }
    }

    private class ResultIterator
            extends AbstractIterator<Page>
    {
        private final PageBuilder pageBuilder;

        private int currentGroupNumber;

        RankingBuilder builder;
        private Iterator<RowWithRanking> currentRows = emptyIterator();

        ResultIterator()
        {
            if (produceRanking) {
                pageBuilder = new PageBuilder(new ImmutableList.Builder<Type>().addAll(sourceTypes).add(BIGINT).build());
            }
            else {
                pageBuilder = new PageBuilder(sourceTypes);
            }
        }

        @Override
        protected Page computeNext()
        {
            pageBuilder.reset();
            while (!pageBuilder.isFull()) {
                if (!currentRows.hasNext()) {
                    // Remove memory usage of last group
                    memorySizeInBytes -= builder != null ? builder.getEstimatedSizeInBytes() : 0;
                    builder = groupedRows.get(currentGroupNumber);
                    if (builder == null) {
                        break;
                    }
                    currentRows = builder.nextGroupedRows();
                    groupedRows.set(currentGroupNumber, null);
                    currentGroupNumber++;
                    continue;
                }
                RowWithRanking rowWithRanking = currentRows.next();
                outputRow(rowWithRanking.getRow(), rowWithRanking.getRanking());
            }
            if (pageBuilder.isEmpty()) {
                return endOfData();
            }
            return pageBuilder.build();
        }

        private void outputRow(Row row, int ranking)
        {
            for (int i = 0; i < sourceTypes.size(); i++) {
                sourceTypes.get(i).appendTo(pageReferences.get(row.getPageId()).getPage().getBlock(i), row.getPosition(), pageBuilder.getBlockBuilder(i));
            }
            if (produceRanking) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(sourceTypes.size()), ranking + 1);
            }
            pageBuilder.declarePosition();
            // deference the row; no need to compact the pages but remove them if completely unused
            PageReference pageReference = pageReferences.get(row.getPageId());
            pageReference.dereference(row.getPosition());
            if (pageReference.getUsedPositionCount() == 0) {
                pageReferences.set(row.getPageId(), null);
                memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
            }
        }
    }
}
