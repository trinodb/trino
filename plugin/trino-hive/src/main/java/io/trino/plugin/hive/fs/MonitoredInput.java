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
package io.trino.plugin.hive.fs;

import io.trino.filesystem.TrinoInput;
import io.trino.plugin.hive.FileFormatDataSourceStats;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

final class MonitoredInput
        implements TrinoInput
{
    private final FileFormatDataSourceStats stats;
    private final TrinoInput delegate;

    public MonitoredInput(FileFormatDataSourceStats stats, TrinoInput delegate)
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long readStart = System.nanoTime();
        delegate.readFully(position, buffer, bufferOffset, bufferLength);
        stats.readDataBytesPerSecond(bufferLength, System.nanoTime() - readStart);
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long readStart = System.nanoTime();
        int size = delegate.readTail(buffer, bufferOffset, bufferLength);
        stats.readDataBytesPerSecond(size, System.nanoTime() - readStart);
        return size;
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }
}
