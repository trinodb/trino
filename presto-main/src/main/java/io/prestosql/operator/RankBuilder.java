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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.Page;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import static com.google.common.base.Verify.verify;
import static io.prestosql.operator.GroupedTopNBuilder.COMPACT_THRESHOLD;

public class RankBuilder
        implements RankingBuilder
{
    private final Comparator<Row> comparator;
    private final ObjectBigArray<PageReference> pageReferences;
    private final int topN;
    // the top N distinct rows
    private final RowHeap heap;
    // a map from row ranks to a list of corresponding rows with the same rank
    TreeMap<Row, List<Row>> map;

    // store the  memory of the heap while the heap is changing
    long memorySizeInBytes;
    int mapSize;

    RankBuilder(Comparator<Row> comparator, ObjectBigArray<PageReference> pageReferences, int topN)
    {
        this.comparator = comparator;
        this.pageReferences = pageReferences;
        this.topN = topN;

        this.heap = new RowHeap(Ordering.from(comparator).reversed());
        this.map = new TreeMap<>(Ordering.from(comparator).reversed());
        this.mapSize = 0;
    }

    public long getEstimatedSizeInBytes()
    {
        return memorySizeInBytes;
    }

    public void addRow(Row row, IntSet pagesToCompact)
    {
        PageReference newPageReference = pageReferences.get(row.getPageId());
        Page newPage = newPageReference.getPage();
        if (map.containsKey(row)) {
            map.get(row).add(row);
            mapSize++;
            newPageReference.reference(row);
        }
        else if (heap.size() < topN) {
            heap.enqueue(row);
            newPageReference.reference(row);
            map.put(row, new LinkedList<>());
        }
        else {
            Row previousRow = heap.first();
            if (comparator.compare(row, previousRow) < 0) {
                heap.dequeue();
                PageReference previousPageReference = pageReferences.get(previousRow.getPageId());
                previousPageReference.dereference(previousRow.getPosition());
                for (Row r : map.get(previousRow)) {
                    mapSize--;
                    PageReference tempPageReference = pageReferences.get(r.getPageId());
                    tempPageReference.dereference(r.getPosition());
                }
                map.remove(previousRow);
                heap.enqueue(row);
                map.put(row, new LinkedList<>());
                newPageReference.reference(row);

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
        int currentGroupSize = heap.size() + mapSize;

        // sort output rows in a big array in case there are too many rows
        ObjectBigArray<Row> sortedRows = new ObjectBigArray<>();
        sortedRows.ensureCapacity(currentGroupSize);
        int index = currentGroupSize - 1;
        while (!heap.isEmpty()) {
            Row row = heap.dequeue();
            for (Row r : map.get(row)) {
                sortedRows.set(index, r);
                index--;
            }
            sortedRows.set(index, row);
            index--;
        }

        ImmutableList.Builder<RowWithRanking> currentRows = ImmutableList.builder();
        index = 0;
        int currentRank = 0;
        Row prevRow = null;
        while (index < currentGroupSize) {
            Row row = sortedRows.get(index);
            if (index == 0 || comparator.compare(row, prevRow) > 0) {
                currentRank = index;
                map.remove(row); // need to remove b/c TreeMap uses BST comparator to getEntry, so if not removed, map could try to access this row again to compare with future rows even tho it's dereferenced already
            }
            currentRows.add(new RowWithRanking(row, currentRank));
            index++;
            prevRow = row;
        }
        return currentRows.build().iterator();
    }
}
