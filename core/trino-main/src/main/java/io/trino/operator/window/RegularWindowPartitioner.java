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

import com.google.common.collect.Streams;
import io.trino.operator.PagesHashStrategy;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexComparator;
import io.trino.operator.WindowOperator.FrameBoundKey;
import io.trino.spi.function.WindowFunction;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class RegularWindowPartitioner
        implements Partitioner
{
    @Override
    public WindowPartition createPartition(
            PagesIndex pagesIndex,
            int partitionStart,
            int partitionEnd,
            int[] outputChannels,
            List<WindowFunction> windowFunctions,
            List<FrameInfo> frames,
            PagesHashStrategy peerGroupHashStrategy,
            Map<FrameBoundKey, PagesIndexComparator> frameBoundComparators)
    {
        List<FramedWindowFunction> functions = Streams.zip(windowFunctions.stream(), frames.stream(), FramedWindowFunction::new)
                .collect(toImmutableList());
        return new RegularWindowPartition(pagesIndex, partitionStart, partitionEnd, outputChannels, functions, peerGroupHashStrategy, frameBoundComparators);
    }
}
