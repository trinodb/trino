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

import io.trino.filesystem.TrinoInputStream;
import io.trino.plugin.hive.FileFormatDataSourceStats;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

final class MonitoredInputStream
        extends TrinoInputStream
{
    private final FileFormatDataSourceStats stats;
    private final TrinoInputStream delegate;

    public MonitoredInputStream(FileFormatDataSourceStats stats, TrinoInputStream delegate)
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public long getPosition()
            throws IOException
    {
        return delegate.getPosition();
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        delegate.seek(position);
    }

    @Override
    public int read()
            throws IOException
    {
        long readStart = System.nanoTime();
        int value = delegate.read();
        stats.readDataBytesPerSecond(1, System.nanoTime() - readStart);
        return value;
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException
    {
        long readStart = System.nanoTime();
        int size = delegate.read(b, off, len);
        stats.readDataBytesPerSecond(size, System.nanoTime() - readStart);
        return size;
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        long readStart = System.nanoTime();
        long size = delegate.skip(n);
        stats.readDataBytesPerSecond(size, System.nanoTime() - readStart);
        return size;
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }
}
