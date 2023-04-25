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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class SortMergeRightOuterJoinScanner
        extends SortMergeLeftOuterJoinScanner
{
    public SortMergeRightOuterJoinScanner(String taskId, int numRowsInMemoryBufferThreshold, Supplier<Spiller> spillerSupplier, PageIterator streamedIter,
            PageIterator bufferedIter, BlockTypeOperators blockTypeOperators,
            List<Type> probeTypes, List<Integer> probeEquiJoinClauseChannels, List<Integer> probeOutputChannels,
            List<Type> buildTypes, List<Integer> buildEquiJoinClauseChannels, List<Integer> buildOutputChannels)
    {
        super(taskId, numRowsInMemoryBufferThreshold, spillerSupplier, bufferedIter, streamedIter, blockTypeOperators, buildTypes, buildEquiJoinClauseChannels, buildOutputChannels,
                probeTypes, probeEquiJoinClauseChannels, probeOutputChannels);
    }

    @Override
    protected PageBuilder createJoinPageBuilder()
    {
        return new PageBuilder(ImmutableList.<Type>builder()
                .addAll(buildOutputChannels.stream()
                        .map(buildTypes::get)
                        .collect(toImmutableList()))
                .addAll(probeOutputChannels.stream()
                        .map(probeTypes::get)
                        .collect(toImmutableList()))
                .build());
    }

    @Override
    protected Page appendRowToPageBuilder(Page probePage, int probePosition, List<Type> probeTypes, List<Integer> probeOutputChannels, Page buildPage, int buildPosition, List<Type> buildTypes, List<Integer> buildOutputChannels)
    {
        return super.appendRowToPageBuilder(buildPage, buildPosition, buildTypes, buildOutputChannels, probePage, probePosition, probeTypes, probeOutputChannels);
    }
}
