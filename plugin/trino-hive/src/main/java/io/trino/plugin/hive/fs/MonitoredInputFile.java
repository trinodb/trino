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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.plugin.hive.FileFormatDataSourceStats;

import java.io.IOException;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

public final class MonitoredInputFile
        implements TrinoInputFile
{
    private final FileFormatDataSourceStats stats;
    private final TrinoInputFile delegate;

    public MonitoredInputFile(FileFormatDataSourceStats stats, TrinoInputFile delegate)
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        return new MonitoredInput(stats, delegate.newInput());
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        return new MonitoredInputStream(stats, delegate.newStream());
    }

    @Override
    public long length()
            throws IOException
    {
        return delegate.length();
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        return delegate.lastModified();
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return delegate.exists();
    }

    @Override
    public Location location()
    {
        return delegate.location();
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }
}
