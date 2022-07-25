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
package io.trino.operator.window;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.WindowFunction;
import io.trino.spi.function.WindowIndex;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class MappedWindowFunction
        implements WindowFunction
{
    private final WindowFunction function;
    private final MappedWindowIndex mappedWindowIndex;

    public MappedWindowFunction(WindowFunction windowFunction, List<Integer> argumentChannels)
    {
        this.function = requireNonNull(windowFunction, "windowFunction is null");
        this.mappedWindowIndex = new MappedWindowIndex(argumentChannels);
    }

    @Override
    public void reset(WindowIndex windowIndex)
    {
        mappedWindowIndex.setDelegate((InternalWindowIndex) windowIndex);
        function.reset(mappedWindowIndex);
    }

    @Override
    public void processRow(BlockBuilder output, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd)
    {
        function.processRow(output, peerGroupStart, peerGroupEnd, frameStart, frameEnd);
    }
}
