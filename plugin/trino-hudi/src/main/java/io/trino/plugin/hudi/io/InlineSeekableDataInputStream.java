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

/**
 * A seekable data input stream that reads inline content from an outer file at a specific offset and length.
 * <p>
 * This class is used when reading inline files stored within larger files (e.g., log blocks embedded in Hudi log files).
 * It provides a view of a segment of the underlying stream as if it were an independent file.
 * <p>
 * Example InlineFS URL:
 * <pre>
 * inlinefs://tests_7af7f087-c807-4f5e-a759-65fd9c21063b/hudi_multi_fg_pt_v8_mor/.hoodie/metadata/column_stats/
 * .col-stats-0001-0_20250429145946675.log.1_1-120-382/local/?start_offset=8036&amp;length=6959
 * </pre>
 * <p>
 * Key behaviors:
 * <ul>
 *   <li>Upon initialization, the underlying stream is immediately seeked to the start offset to ensure correct positioning</li>
 *   <li>{@link #getPos()} returns positions relative to the start offset (0-based from the inline content start)</li>
 *   <li>{@link #seek(long)} accepts positions relative to the start offset and translates them to absolute positions</li>
 *   <li>Attempting to seek beyond the length throws an {@link IOException}</li>
 * </ul>
 */
public class InlineSeekableDataInputStream
        extends TrinoSeekableDataInputStream
{
    private final long startOffset;
    private final long length;

    public InlineSeekableDataInputStream(TrinoInputStream stream, long startOffset, long length)
            throws IOException
    {
        super(stream);
        this.startOffset = startOffset;
        this.length = length;
        stream.seek(startOffset);
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
