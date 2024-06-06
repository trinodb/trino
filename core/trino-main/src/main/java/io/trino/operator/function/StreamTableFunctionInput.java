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
package io.trino.operator.function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.operator.PagesIndex;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.spi.Page;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.operator.WorkProcessorUtils.yieldingIteratorFrom;
import static java.util.Objects.requireNonNull;

public class StreamTableFunctionInput
        implements WorkProcessor.Process<WorkProcessor<Page>>
{
    private final TableFunctionDataProcessor tableFunction;
    private final int properChannelsCount;
    private final int passThroughSourcesCount;
    private final List<List<Integer>> requiredChannels;
    private final Optional<Map<Integer, Integer>> markerChannels;
    private final List<RegularTableFunctionPartition.PassThroughColumnSpecification> passThroughSpecifications;

    private final Iterator<Optional<Page>> inputPages;
    private final PagesIndex pagesIndex;
    private boolean finished;

    public StreamTableFunctionInput(
            TableFunctionDataProcessor tableFunction,
            int properChannelsCount,
            int passThroughSourcesCount,
            List<List<Integer>> requiredChannels,
            Optional<Map<Integer, Integer>> markerChannels,
            List<RegularTableFunctionPartition.PassThroughColumnSpecification> passThroughSpecifications,
            WorkProcessor<Page> inputPages,
            PagesIndex pagesIndex)
    {
        this.tableFunction = requireNonNull(tableFunction, "tableFunction is null");
        this.properChannelsCount = properChannelsCount;
        this.passThroughSourcesCount = passThroughSourcesCount;
        this.requiredChannels = requiredChannels.stream()
                .map(ImmutableList::copyOf)
                .collect(toImmutableList());
        this.markerChannels = markerChannels.map(ImmutableMap::copyOf);
        this.passThroughSpecifications = ImmutableList.copyOf(requireNonNull(passThroughSpecifications, "passThroughSpecifications is null"));
        this.inputPages = yieldingIteratorFrom(requireNonNull(inputPages, "inputPages is null"));
        this.pagesIndex = requireNonNull(pagesIndex, "pagesIndex is null");
    }

    @Override
    public ProcessState<WorkProcessor<Page>> process()
    {
        if (finished) {
            return ProcessState.finished();
        }

        if (inputPages.hasNext()) {
            Optional<Page> next = inputPages.next();
            if (next.isEmpty()) {
                return ProcessState.yielded();
            }
            Page currentInputPage = next.get();
            pagesIndex.clear();
            pagesIndex.addPage(currentInputPage);
            return ProcessState.ofResult(new RegularTableFunctionPartition(
                    pagesIndex,
                    0,
                    pagesIndex.getPositionCount(),
                    new TableFunctionDataProcessor()
                    {
                        @Override
                        public TableFunctionProcessorState process(@org.jetbrains.annotations.Nullable List<Optional<Page>> input)
                        {
                            if (input == null) {
                                // end of page
                                return TableFunctionProcessorState.Finished.FINISHED;
                            }
                            TableFunctionProcessorState process = tableFunction.process(input);
                            if (process instanceof TableFunctionProcessorState.Finished) {
                                finished = true;
                            }
                            return process;
                        }
                    },
                    properChannelsCount,
                    passThroughSourcesCount,
                    requiredChannels,
                    markerChannels,
                    passThroughSpecifications)
                    .toOutputPages());
        }

        // finish
        finished = true;
        return ProcessState.ofResult(WorkProcessor.create(() -> {
            TableFunctionProcessorState state = tableFunction.process(null);
            return switch (state) {
                case TableFunctionProcessorState.Processed processed -> ProcessState.ofResult(processed.getResult());
                case TableFunctionProcessorState.Blocked blocked -> ProcessState.blocked(toListenableFuture(blocked.getFuture()));
                case TableFunctionProcessorState.Finished __ -> ProcessState.finished();
            };
        }));
    }
}
