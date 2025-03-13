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
import io.trino.operator.WorkProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.List;
import java.util.Optional;

public class StreamingWorkProcessor
{
    private boolean processEmptyInput;
    private final int[] channelsForSource;

    public StreamingWorkProcessor(boolean processEmptyInput, List<Integer> requiredChannels)
    {
        this.processEmptyInput = processEmptyInput;
        channelsForSource = requiredChannels.stream().mapToInt(i -> i).toArray();
    }

    public WorkProcessor.Transformation<Page, List<Optional<Page>>> toTableFunctionInput()
    {
        return input -> {
            if (input == null) {
                if (processEmptyInput) {
                    // it can only happen at the first call to process(), which implies that there is no input. Empty PagesIndex can be passed on only once.
                    processEmptyInput = false;
                    return WorkProcessor.TransformationState.ofResult(null, false);
                }
                return WorkProcessor.TransformationState.finished();
            }

            // there is input, so we are not interested in processing empty input
            processEmptyInput = false;
            Page outputPage;
            if (channelsForSource.length == 0) {
                outputPage = new Page(input.getPositionCount());
            }
            else {
                Block[] sourceBlocks = new Block[channelsForSource.length];
                for (int i = 0; i < channelsForSource.length; i++) {
                    int inputChannel = channelsForSource[i];
                    sourceBlocks[i] = input.getBlock(inputChannel);
                }
                outputPage = new Page(sourceBlocks);
            }
            return WorkProcessor.TransformationState.ofResult(ImmutableList.of(Optional.of(outputPage)));
        };
    }
}
