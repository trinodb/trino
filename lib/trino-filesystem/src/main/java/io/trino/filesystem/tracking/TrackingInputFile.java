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
package io.trino.filesystem.tracking;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

public class TrackingInputFile
        implements TrinoInputFile
{
    private final TrinoInputFile delegate;
    private final Cleaner cleaner;

    public TrackingInputFile(TrinoInputFile delegate, Cleaner cleaner)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cleaner = requireNonNull(cleaner, "cleaner is null");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        return new TrackingInput(delegate.newInput(), delegate.location(), cleaner);
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        return new TrackingInputStream(delegate.newStream(), delegate.location(), cleaner);
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
}
