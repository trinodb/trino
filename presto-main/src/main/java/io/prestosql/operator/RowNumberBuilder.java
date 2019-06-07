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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.Page;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Comparator;
import java.util.Iterator;

import static com.google.common.base.Verify.verify;
import static io.prestosql.operator.GroupedTopNBuilder.COMPACT_THRESHOLD;

public class RowNumberBuilder
        implements RankingBuilder
{
    private final Comparator<Row> comparator;
    private final ObjectBigArray<PageReference> pageReferences;
    private final int topN;
    private final RowHeap heap;
    long memorySizeInBytes;

    RowNumberBuilder(Comparator<Row> comparator, ObjectBigArray<PageReference> pageReferences, int topN)
    {
        this.comparator = comparator;
        this.pageReferences = pageReferences;
        this.topN = topN;

        this.heap = new RowHeap(Ordering.from(comparator).reversed());
    }

    public long getEstimatedSizeInBytes()
    {
        // this stores the size of the heap before it's dumped so that the original size can be subtracted
        return memorySizeInBytes;
    }

    public void addRow(Row row, IntSet pagesToCompact)
    {
        PageReference newPageReference = pageReferences.get(row.getPageId());
        Page newPage = newPageReference.getPage();
        if (heap.size() < topN) {
            // still have space for the current group
            heap.enqueue(row);
            newPageReference.reference(row);
        }
        else {
            // may compare with the topN-th element with in the heap to decide if update is necessary
            Row previousRow = heap.first();
            if (comparator.compare(row, previousRow) < 0) {
                // update reference and the heap
                heap.dequeue();
                PageReference previousPageReference = pageReferences.get(previousRow.getPageId());
                previousPageReference.dereference(previousRow.getPosition());
                newPageReference.reference(row);
                heap.enqueue(row);

                // compact a page if it is not the current input page and the reference count is below the threshold
                if (previousPageReference.getPage() != newPage &&
                        previousPageReference.getUsedPositionCount() * COMPACT_THRESHOLD < previousPageReference.getPage().getPositionCount()) {
                    pagesToCompact.add(previousRow.getPageId());
                }
            }
        }
        memorySizeInBytes = heap.getEstimatedSizeInBytes();
    }

    public Iterator<RowWithRanking> nextGroupedRows()
    {
        verify(heap != null && !heap.isEmpty(), "impossible to have inserted a group without a witness row");
        // number of rows in the group
        int currentGroupSize = heap.size();

        int index = currentGroupSize - 1;
        ObjectBigArray<Row> sortedRows = new ObjectBigArray<>();
        sortedRows.ensureCapacity(currentGroupSize);

        while (!heap.isEmpty()) {
            sortedRows.set(index, heap.dequeue());

            index--;
        }
        Preconditions.checkArgument(index == -1);

        ImmutableList.Builder<RowWithRanking> currentRows = ImmutableList.builder();
        index = 0;
        while (index < currentGroupSize) {
            currentRows.add(new RowWithRanking(sortedRows.get(index), index));
            index++;
        }
        return currentRows.build().iterator();
    }
}
