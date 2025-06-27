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
package io.trino.spi.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;

public class P4HyperLogLogType
        extends AbstractVariableWidthType
{
    public static final P4HyperLogLogType P4_HYPER_LOG_LOG = new P4HyperLogLogType();

    @JsonCreator
    public P4HyperLogLogType()
    {
        super(new TypeSignature(StandardTypes.P4_HYPER_LOG_LOG), Slice.class);
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return HYPER_LOG_LOG.getSlice(block, position);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        HYPER_LOG_LOG.writeSlice(blockBuilder, value);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        HYPER_LOG_LOG.writeSlice(blockBuilder, value, offset, length);
    }

    @Override
    public Object getObjectValue(Block block, int position)
    {
        return HYPER_LOG_LOG.getObjectValue(block, position);
    }
}
