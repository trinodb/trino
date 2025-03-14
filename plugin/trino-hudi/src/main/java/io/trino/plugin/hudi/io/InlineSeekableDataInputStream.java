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
package io.trino.plugin.hudi.io;

import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;

public class InlineSeekableDataInputStream
        extends TrinoSeekableDataInputStream
{
    private final long startOffset;
    private final long length;

    public InlineSeekableDataInputStream(TrinoInputStream stream, long startOffset, long length)
    {
        super(stream);
        this.startOffset = startOffset;
        this.length = length;
    }

    @Override
    public long getPos()
            throws IOException
    {
        return super.getPos() - startOffset;
    }

    @Override
    public void seek(long pos)
            throws IOException
    {
        if (pos > length) {
            throw new IOException(String.format(
                    "Attempting to seek past inline content: position to seek to is %s but the length is %s",
                    pos, length));
        }
        super.seek(startOffset + pos);
    }
}
