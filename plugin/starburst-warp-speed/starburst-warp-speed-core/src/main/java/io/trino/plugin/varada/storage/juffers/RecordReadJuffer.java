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

public class RecordReadJuffer
        extends BaseReadJuffer
{
    public RecordReadJuffer(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator, JuffersType.RECORD);
    }

    @Override
    public void createBuffer(RecTypeCode recTypeCode, int recTypeLength, long[] buffIds, boolean hasDictionary)
    {
        this.baseBuffer = bufferAllocator.ids2RecBuff(buffIds);
        this.wrappedBuffer = createWrapperBuffer(this.baseBuffer, recTypeCode, recTypeLength, false, hasDictionary);
    }
}
