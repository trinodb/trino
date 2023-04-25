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
package io.trino.operator.join.smj;

import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.function.Supplier;

public class SortMergeInnerJoinScanner
        extends SortMergeJoinScanner
{
    public SortMergeInnerJoinScanner(String taskId, int numRowsInMemoryBufferThreshold, Supplier<Spiller> spillerSupplier, PageIterator streamedIter, PageIterator bufferedIter, BlockTypeOperators blockTypeOperators, List<Type> probeTypes, List<Integer> probeEquiJoinClauseChannels, List<Integer> probeOutputChannels, List<Type> buildTypes, List<Integer> buildEquiJoinClauseChannels, List<Integer> buildOutputChannels)
    {
        super(taskId, numRowsInMemoryBufferThreshold, spillerSupplier, streamedIter, bufferedIter, blockTypeOperators, probeTypes, probeEquiJoinClauseChannels, probeOutputChannels, buildTypes, buildEquiJoinClauseChannels, buildOutputChannels);
    }

    @Override
    protected void doJoin()
            throws Exception
    {
        advancedBufferedToRowWithNullFreeJoinKey();
        while (findNextInnerJoinRows()) {
            consumeMatchingRows();
        }
    }

    protected boolean findNextInnerJoinRows()
    {
        while (advancedStreamed() && isRowKeyAnyNull(streamedRow)) {
            // Advance the streamed side of the join until we find the next row whose join key contains
            // no nulls or we hit the end of the streamed iterator.
        }
        if (!streamedRow.isExist()) {
            // We have consumed the entire streamed iterator, so there can be no more matches.
            bufferedMatches.clear();
            return false;
        }
        else if (!bufferedMatches.isEmpty() && compare(streamedRow.getPage(), streamedRow.getPosition(),
                bufferedMatches.getFirstMatchRow().getPage(), bufferedMatches.getFirstMatchRow().getPosition()) == 0) {
            return true;
        }
        else if (!bufferedRow.isExist()) {
            // The streamed row's join key does not match the current batch of buffered rows and there are
            // no more rows to read from the buffered iterator, so there can be no more matches.
            bufferedMatches.clear();
            return false;
        }

        long comp = compare(streamedRow.getPage(), streamedRow.getPosition(), bufferedRow.getPage(), bufferedRow.getPosition());
        do {
            if (isRowKeyAnyNull(streamedRow)) {
                advancedStreamed();
            }
            else {
                comp = compare(streamedRow.getPage(), streamedRow.getPosition(), bufferedRow.getPage(), bufferedRow.getPosition());
                if (comp > 0) {
                    advancedBufferedToRowWithNullFreeJoinKey();
                }
                else if (comp < 0) {
                    advancedStreamed();
                }
            }
        }
        while (streamedRow.isExist() && bufferedRow.isExist() && comp != 0);
        if (!streamedRow.isExist() || !bufferedRow.isExist()) {
            // We have either hit the end of one of the iterators, so there can be no more matches.
            bufferedMatches.clear();
            return false;
        }
        else {
            // The streamed row's join key matches the current buffered row's join, so walk through the
            // buffered iterator to buffer the rest of the matching rows.
            bufferMatchingRows();
            return true;
        }
    }
}
