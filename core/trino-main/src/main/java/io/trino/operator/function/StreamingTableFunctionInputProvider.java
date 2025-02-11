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
import com.google.common.primitives.Ints;
import io.trino.operator.WorkProcessor;
import io.trino.spi.Page;

import java.util.List;
import java.util.Optional;

public class StreamingTableFunctionInputProvider
        implements WorkProcessor.Transformation<Page, List<Optional<Page>>>
{
    private boolean processEmptyInput;
    private final int[] requiredChannels;

    public StreamingTableFunctionInputProvider(boolean processEmptyInput, List<Integer> requiredChannels)
    {
        this.processEmptyInput = processEmptyInput;
        this.requiredChannels = Ints.toArray(requiredChannels);
    }

    @Override
    public WorkProcessor.TransformationState<List<Optional<Page>>> process(Page input)
    {
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

        return WorkProcessor.TransformationState.ofResult(ImmutableList.of(Optional.of(input.getColumns(requiredChannels))));
    }
}
