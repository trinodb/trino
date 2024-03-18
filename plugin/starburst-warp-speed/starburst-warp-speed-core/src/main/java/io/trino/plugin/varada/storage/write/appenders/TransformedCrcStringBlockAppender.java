package io.trino.plugin.varada.storage.write.appenders;
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

import io.airlift.slice.Slice;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.spi.type.Type;

import java.util.function.Function;

public class TransformedCrcStringBlockAppender
        extends CrcStringBlockAppender
{
    private final Function<Slice, Slice> transformColumn;

    public TransformedCrcStringBlockAppender(WriteJuffersWarmUpElement juffersWE,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            Type filterType,
            boolean isFixedLength,
            Function<Slice, Slice> transformColumnFunction)
    {
        super(juffersWE, storageEngineConstants, bufferAllocator, filterType, isFixedLength);
        this.transformColumn = transformColumnFunction;
    }

    //todo: what to do with non utf8 characters?
    @Override
    protected Slice getSlice(Slice slice)
    {
        return transformColumn.apply(slice);
    }
}
