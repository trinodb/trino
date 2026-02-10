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
package io.trino.lance.file.v2.encoding;

import io.trino.lance.file.v2.reader.BufferAdapter;

public class FixedSizeListEncoding
        implements LanceEncoding
{
    @Override
    public BufferAdapter getBufferAdapter()
    {
        throw new UnsupportedOperationException("getBufferAdapter is not supported for " + getClass().getSimpleName());
    }

    @Override
    public <T> MiniBlockDecoder<T> getMiniBlockDecoder()
    {
        throw new UnsupportedOperationException("getMiniBlockDecoder is not supported for " + getClass().getSimpleName());
    }
}
