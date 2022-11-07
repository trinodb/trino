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
package io.trino.operator.project;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.spi.Page;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class InputChannels
{
    private final int[] inputChannels;
    private final int[] eagerlyLoadedChannels;

    public InputChannels(int... inputChannels)
    {
        this.inputChannels = inputChannels.clone();
        this.eagerlyLoadedChannels = new int[0];
    }

    public InputChannels(List<Integer> inputChannels)
    {
        this(inputChannels, ImmutableList.of());
    }

    public InputChannels(List<Integer> inputChannels, List<Integer> eagerlyLoadedChannels)
    {
        this.inputChannels = inputChannels.stream().mapToInt(Integer::intValue).toArray();
        this.eagerlyLoadedChannels = eagerlyLoadedChannels.stream().mapToInt(Integer::intValue).toArray();
    }

    public int size()
    {
        return inputChannels.length;
    }

    public List<Integer> getInputChannels()
    {
        return Collections.unmodifiableList(Ints.asList(inputChannels));
    }

    public Page getInputChannels(Page page)
    {
        return page.getLoadedPage(inputChannels, eagerlyLoadedChannels);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(Arrays.toString(inputChannels))
                .toString();
    }
}
