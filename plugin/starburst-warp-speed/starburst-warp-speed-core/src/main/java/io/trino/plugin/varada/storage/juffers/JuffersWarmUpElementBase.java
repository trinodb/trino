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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public abstract class JuffersWarmUpElementBase
{
    protected final Map<JuffersType, BaseJuffer> juffers = new HashMap<>();

    protected BaseJuffer getJufferByType(JuffersType juffersType)
    {
        return juffers.get(juffersType);
    }

    protected Buffer getBufferByType(JuffersType juffersType)
    {
        return getJufferByType(juffersType).getWrappedBuffer();
    }

    public Buffer getRecordBuffer()
    {
        return getBufferByType(JuffersType.RECORD);
    }

    public ByteBuffer getNullBuffer()
    {
        return (ByteBuffer) getBufferByType(JuffersType.NULL);
    }
}
