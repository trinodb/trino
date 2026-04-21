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
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.Cleaner;

import static java.util.Objects.requireNonNull;

public class TrackingOutputFile
        implements TrinoOutputFile
{
    private final TrinoOutputFile delegate;
    private final Cleaner cleaner;

    public TrackingOutputFile(TrinoOutputFile delegate, Cleaner cleaner)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cleaner = requireNonNull(cleaner, "cleaner is null");
    }

    @Override
    public OutputStream create()
            throws IOException
    {
        return new TrackingOutputStream(delegate.create(), delegate.location(), cleaner);
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        delegate.createOrOverwrite(data);
    }

    @Override
    public void createExclusive(byte[] data)
            throws IOException
    {
        delegate.createExclusive(data);
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return new TrackingOutputStream(delegate.create(memoryContext), delegate.location(), cleaner);
    }

    @Override
    public Location location()
    {
        return delegate.location();
    }
}
