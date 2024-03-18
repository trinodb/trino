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
package io.trino.plugin.varada.storage.write.appenders;

import io.trino.plugin.varada.dispatcher.model.WarmUpElementState;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.warmup.exceptions.WarmupException;

import java.util.function.Function;

public class TransformedCrcDateBlockAppender
        extends CrcIntBlockAppender
{
    private final Function<BlockPosHolder, Integer> transformColumn;

    public TransformedCrcDateBlockAppender(WriteJuffersWarmUpElement juffersWE,
            Function<BlockPosHolder, Integer> transformColumnFunction)
    {
        super(juffersWE);
        this.transformColumn = transformColumnFunction;
    }

    @Override
    protected int getIntValue(BlockPosHolder blockPos)
    {
        try {
            return transformColumn.apply(blockPos);
        }
        catch (Exception e) {
            String invalidSlice = blockPos.getSlice().toStringUtf8();
            throw new WarmupException(
                    String.format("failed to transform %s into date format", invalidSlice),
                    WarmUpElementState.State.FAILED_PERMANENTLY);
        }
    }
}
