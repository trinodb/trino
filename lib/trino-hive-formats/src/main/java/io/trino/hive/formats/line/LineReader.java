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
package io.trino.hive.formats.line;

import java.io.Closeable;
import java.io.IOException;

public interface LineReader
        extends Closeable
{
    boolean isClosed();

    long getRetainedSize();

    long getBytesRead();

    long getReadTimeNanos();

    /**
     * Read a line into the buffer. If there are no more lines in the steam, this reader is closed.
     *
     * @return true if a line was read; otherwise, there no more lines and false is returned
     */
    boolean readLine(LineBuffer lineBuffer)
            throws IOException;
}
