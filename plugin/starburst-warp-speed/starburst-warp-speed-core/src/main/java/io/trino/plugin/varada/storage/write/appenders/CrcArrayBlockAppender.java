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

import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.warmup.transform.BlockTransformer;
import io.trino.plugin.varada.dispatcher.warmup.transform.BlockTransformerFactory;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class CrcArrayBlockAppender
        extends CrcBlockAppender
{
    private final BlockTransformerFactory blockTransformerFactory;
    private final CrcStringBlockAppender stringBlockAppender;
    private final Type filterType;

    public CrcArrayBlockAppender(BlockTransformerFactory blockTransformerFactory,
            WriteJuffersWarmUpElement juffersWE,
            CrcStringBlockAppender stringBlockAppender,
            Type filterType)
    {
        super(juffersWE);
        this.blockTransformerFactory = blockTransformerFactory;
        this.stringBlockAppender = stringBlockAppender;
        this.filterType = filterType;
    }

    @Override
    public AppendResult appendWithoutDictionary(int jufferPos, BlockPosHolder blockPos, boolean stopAfterOneFlush, WarmUpElement warmUpElement, WarmupElementStats warmupElementStats)
    {
        Optional<BlockTransformer> blockTransformer = blockTransformerFactory.getBlockTransformer(WarmUpType.WARM_UP_TYPE_DATA, warmUpElement.getRecTypeCode());
        checkArgument(blockTransformer.isPresent());
        BlockPosHolder blockAsVarchar = blockTransformer.get().transformBlock(blockPos, filterType);
        return stringBlockAppender.appendWithoutDictionary(jufferPos, blockAsVarchar, stopAfterOneFlush, warmUpElement, warmupElementStats);
    }
}
