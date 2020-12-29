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
package io.prestosql.operator.window;

import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.ValueWindowFunction;
import io.prestosql.spi.function.WindowFunctionSignature;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;

@WindowFunctionSignature(name = "first_value", typeVariable = "T", returnType = "T", argumentTypes = "T")
public class FirstValueFunction
        extends ValueWindowFunction
{
    private final int argumentChannel;
    private final boolean ignoreNulls;
    private int lastNonNull;

    @Override
    public void reset()
    {
        lastNonNull = -1;
    }

    public FirstValueFunction(List<Integer> argumentChannels, boolean ignoreNulls)
    {
        this.argumentChannel = getOnlyElement(argumentChannels);
        this.ignoreNulls = ignoreNulls;
        if (this.ignoreNulls) {
            this.lastNonNull = -1;
        }
    }

    @Override
    public void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition)
    {
        if (frameStart < 0) {
            output.appendNull();
            return;
        }
        int returnInd = frameStart;

        if (ignoreNulls) {
            returnInd = searchWindowAndUpdateIndices(frameStart, frameEnd);
        }
        windowIndex.appendTo(argumentChannel, returnInd, output);
    }

    private int searchWindowAndUpdateIndices(int frameStart, int frameEnd)
    {
        //This assumes that frameStart never decreases.
        if (lastNonNull < frameStart) {
            lastNonNull = frameStart;
        }
        while (windowIndex.isNull(argumentChannel, lastNonNull) && lastNonNull < frameEnd) {
            lastNonNull++;
        }
        return lastNonNull;
    }
}
