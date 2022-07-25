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

import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class MappedWindowIndex
        implements InternalWindowIndex
{
    private final int[] channelMap;
    private InternalWindowIndex delegate;

    public MappedWindowIndex(List<Integer> argumentChannels)
    {
        this.channelMap = Ints.toArray(requireNonNull(argumentChannels, "argumentChannels is null"));
    }

    public void setDelegate(InternalWindowIndex delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public int size()
    {
        return delegate.size();
    }

    @Override
    public boolean isNull(int channel, int position)
    {
        return delegate.isNull(toDelegateChannel(channel), position);
    }

    @Override
    public boolean getBoolean(int channel, int position)
    {
        return delegate.getBoolean(toDelegateChannel(channel), position);
    }

    @Override
    public long getLong(int channel, int position)
    {
        return delegate.getLong(toDelegateChannel(channel), position);
    }

    @Override
    public double getDouble(int channel, int position)
    {
        return delegate.getDouble(toDelegateChannel(channel), position);
    }

    @Override
    public Slice getSlice(int channel, int position)
    {
        return delegate.getSlice(toDelegateChannel(channel), position);
    }

    @Override
    public Block getSingleValueBlock(int channel, int position)
    {
        return delegate.getSingleValueBlock(toDelegateChannel(channel), position);
    }

    @Override
    public Object getObject(int channel, int position)
    {
        return delegate.getObject(toDelegateChannel(channel), position);
    }

    @Override
    public void appendTo(int channel, int position, BlockBuilder output)
    {
        delegate.appendTo(toDelegateChannel(channel), position, output);
    }

    @Override
    @UsedByGeneratedCode
    public Block getRawBlock(int channel, int position)
    {
        return delegate.getRawBlock(toDelegateChannel(channel), position);
    }

    @Override
    @UsedByGeneratedCode
    public int getRawBlockPosition(int position)
    {
        return delegate.getRawBlockPosition(position);
    }

    private int toDelegateChannel(int channel)
    {
        return channelMap[channel];
    }
}
