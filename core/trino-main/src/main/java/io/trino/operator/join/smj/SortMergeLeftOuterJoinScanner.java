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

public class SortMergeLeftOuterJoinScanner
        extends SortMergeJoinScanner
{
    public SortMergeLeftOuterJoinScanner(String taskId, int numRowsInMemoryBufferThreshold, Supplier<Spiller> spillerSupplier, PageIterator streamedIter, PageIterator bufferedIter, BlockTypeOperators blockTypeOperators, List<Type> probeTypes, List<Integer> probeEquiJoinClauseChannels, List<Integer> probeOutputChannels, List<Type> buildTypes, List<Integer> buildEquiJoinClauseChannels, List<Integer> buildOutputChannels)
    {
        super(taskId, numRowsInMemoryBufferThreshold, spillerSupplier, streamedIter, bufferedIter, blockTypeOperators, probeTypes, probeEquiJoinClauseChannels, probeOutputChannels, buildTypes, buildEquiJoinClauseChannels, buildOutputChannels);
    }

    @Override
    protected void doJoin()
            throws Exception
    {
        advancedBufferedToRowWithNullFreeJoinKey();
        while (findNextOuterJoinRows()) {
            if (bufferedMatches.isEmpty()) {
                appendRow(streamedRow.getPage(), streamedRow.getPosition(), null, 0);
            }
            else {
                consumeMatchingRows();
            }
        }
    }

    protected boolean findNextOuterJoinRows()
    {
        if (!advancedStreamed()) {
            bufferedMatches.clear();
            return false;
        }

        if (!bufferedMatches.isEmpty() && compare(streamedRow.getPage(), streamedRow.getPosition(),
                bufferedMatches.getFirstMatchRow().getPage(), bufferedMatches.getFirstMatchRow().getPosition()) == 0) {
            // Matches the current group, so do nothing.
        }
        else {
            // The streamed row does not match the current group.
            bufferedMatches.clear();
            if (bufferedRow.isExist() && !isRowKeyAnyNull(streamedRow)) {
                // The buffered iterator could still contain matching rows, so we'll need to walk through
                // it until we either find matches or pass where they would be found.
                long comp = 1;
                do {
                    comp = compare(streamedRow.getPage(), streamedRow.getPosition(), bufferedRow.getPage(), bufferedRow.getPosition());
                }
                while (comp > 0 && advancedBufferedToRowWithNullFreeJoinKey());
                if (comp == 0) {
                    // We have found matches, so buffer them (this updates matchJoinKey)
                    bufferMatchingRows();
                }
                else {
                    // We have overshot the position where the row would be found, hence no matches.
                }
            }
        }
        // If there is a streamed input then we always return true
        return true;
    }
}
