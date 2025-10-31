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
package io.trino.lance.file.v2.reader;

import java.util.List;

import static java.lang.Math.toIntExact;

public abstract class ArrayBufferAdapter<T>
        implements BufferAdapter<T>
{
    protected abstract int getLength(T buffer);

    @Override
    public T merge(List<T> buffers)
    {
        long totalSize = 0;
        for (T buffer : buffers) {
            totalSize += getLength(buffer);
        }
        T result = createBuffer(toIntExact(totalSize));

        int offset = 0;
        for (T buffer : buffers) {
            copy(buffer, 0, result, offset, getLength(buffer));
            offset += getLength(buffer);
        }
        return result;
    }
}
