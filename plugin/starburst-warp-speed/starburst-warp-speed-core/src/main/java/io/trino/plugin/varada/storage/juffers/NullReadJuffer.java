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
package io.trino.plugin.varada.storage.juffers;

import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.warp.gen.constants.RecTypeCode;

/**
 * buffer for marking null values
 */
public class NullReadJuffer
        extends BaseReadJuffer
{
    public NullReadJuffer(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator, JuffersType.NULL);
    }

    @Override
    public void createBuffer(RecTypeCode recTypeCode, int recTypeLength, long[] buffIds, boolean hasDictionary)
    {
        this.baseBuffer = createGenericBuffer(bufferAllocator.ids2NullBuff(buffIds));
        this.wrappedBuffer = this.baseBuffer;
    }
}
