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

public class SortMergeFullOuterJoinScanner
        extends SortMergeJoinScanner
{
    public SortMergeFullOuterJoinScanner(String taskId, int numRowsInMemoryBufferThreshold, Supplier<Spiller> spillerSupplier, PageIterator streamedIter, PageIterator bufferedIter, BlockTypeOperators blockTypeOperators, List<Type> probeTypes, List<Integer> probeEquiJoinClauseChannels, List<Integer> probeOutputChannels, List<Type> buildTypes, List<Integer> buildEquiJoinClauseChannels, List<Integer> buildOutputChannels)
    {
        super(taskId, numRowsInMemoryBufferThreshold, spillerSupplier, streamedIter, bufferedIter, blockTypeOperators, probeTypes, probeEquiJoinClauseChannels, probeOutputChannels, buildTypes, buildEquiJoinClauseChannels, buildOutputChannels);
    }

    @Override
    protected void doJoin()
            throws Exception
    {
        advancedStreamed();
        advancedBuffered();
        while (true) {
            if (!bufferedMatches.isEmpty() && streamedRow.isExist() && compare(streamedRow.getPage(), streamedRow.getPosition(),
                    bufferedMatches.getFirstMatchRow().getPage(), bufferedMatches.getFirstMatchRow().getPosition()) == 0) {
                consumeMatchingRows();
                advancedStreamed();
            }
            else if (streamedRow.isExist() && (isRowKeyAnyNull(streamedRow) || !bufferedRow.isExist())) {
                appendRow(streamedRow.getPage(), streamedRow.getPosition(), null, 0);
                advancedStreamed();
            }
            else if (bufferedRow.isExist() && (isRowKeyAnyNull(bufferedRow) || !streamedRow.isExist())) {
                appendRow(null, 0, bufferedRow.getPage(), bufferedRow.getPosition());
                advancedBuffered();
            }
            else if (streamedRow.isExist() && bufferedRow.isExist()) {
                // Both rows are present and neither have null values,
                // so we populate the buffers with rows matching the next key
                long comp = compare(streamedRow.getPage(), streamedRow.getPosition(), bufferedRow.getPage(), bufferedRow.getPosition());
                if (comp == 0) {
                    bufferMatchingRows();
                    consumeMatchingRows();
                    advancedStreamed();
                }
                else if (comp > 0) {
                    appendRow(null, 0, bufferedRow.getPage(), bufferedRow.getPosition());
                    advancedBuffered();
                    if (!bufferedMatches.isEmpty()) {
                        bufferedMatches.clear();
                    }
                }
                else {
                    appendRow(streamedRow.getPage(), streamedRow.getPosition(), null, 0);
                    advancedStreamed();
                }
            }
            else {
                // Both iterators have been consumed
                break;
            }
        }
    }
}
