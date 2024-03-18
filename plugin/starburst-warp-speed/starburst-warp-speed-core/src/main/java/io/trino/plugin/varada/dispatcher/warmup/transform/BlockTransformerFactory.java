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
package io.trino.plugin.varada.dispatcher.warmup.transform;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;

import java.util.Map;
import java.util.Optional;

@Singleton
public class BlockTransformerFactory
{
    private final Map<BlockTransformerKey, BlockTransformer> typeBlockTransformerMap;

    @Inject
    public BlockTransformerFactory()
    {
        ArrayBlockToVarcharBlockTransformer arrayBlockToVarcharBlockTransformer = new ArrayBlockToVarcharBlockTransformer();
        this.typeBlockTransformerMap = Map.of(
                new BlockTransformerKey(WarmUpType.WARM_UP_TYPE_DATA, RecTypeCode.REC_TYPE_ARRAY_INT),
                arrayBlockToVarcharBlockTransformer,
                new BlockTransformerKey(WarmUpType.WARM_UP_TYPE_DATA, RecTypeCode.REC_TYPE_ARRAY_BIGINT),
                arrayBlockToVarcharBlockTransformer,
                new BlockTransformerKey(WarmUpType.WARM_UP_TYPE_DATA, RecTypeCode.REC_TYPE_ARRAY_VARCHAR),
                arrayBlockToVarcharBlockTransformer,
                new BlockTransformerKey(WarmUpType.WARM_UP_TYPE_DATA, RecTypeCode.REC_TYPE_ARRAY_CHAR),
                arrayBlockToVarcharBlockTransformer,
                new BlockTransformerKey(WarmUpType.WARM_UP_TYPE_DATA, RecTypeCode.REC_TYPE_ARRAY_BOOLEAN),
                arrayBlockToVarcharBlockTransformer,
                new BlockTransformerKey(WarmUpType.WARM_UP_TYPE_DATA, RecTypeCode.REC_TYPE_ARRAY_TIMESTAMP),
                arrayBlockToVarcharBlockTransformer,
                new BlockTransformerKey(WarmUpType.WARM_UP_TYPE_DATA, RecTypeCode.REC_TYPE_ARRAY_DATE),
                arrayBlockToVarcharBlockTransformer,
                new BlockTransformerKey(WarmUpType.WARM_UP_TYPE_DATA, RecTypeCode.REC_TYPE_ARRAY_DOUBLE),
                arrayBlockToVarcharBlockTransformer);
    }

    public Optional<BlockTransformer> getBlockTransformer(WarmUpType sourceWarmupType, RecTypeCode recTypeCode)
    {
        BlockTransformerKey key = new BlockTransformerKey(sourceWarmupType, recTypeCode);
        BlockTransformer blockTransformer = typeBlockTransformerMap.get(key);
        return Optional.ofNullable(blockTransformer);
    }

    private record BlockTransformerKey(WarmUpType sourceWarmupType, RecTypeCode recTypeCode) {}
}
