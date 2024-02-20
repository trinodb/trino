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
package io.trino.filesystem.memory;

import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class MemoryInputFile
        implements TrinoInputFile
{
    private final Location location;
    private final Supplier<MemoryBlob> dataSupplier;
    private OptionalLong length;
    private Optional<Instant> lastModified = Optional.empty();

    public MemoryInputFile(Location location, Slice data)
    {
        this(location, () -> new MemoryBlob(data), OptionalLong.of(data.length()));
    }

    public MemoryInputFile(Location location, Supplier<MemoryBlob> dataSupplier, OptionalLong length)
    {
        this.location = requireNonNull(location, "location is null");
        this.dataSupplier = requireNonNull(dataSupplier, "dataSupplier is null");
        this.length = requireNonNull(length, "length is null");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        return new MemoryInput(location, getBlobRequired().data());
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        return new MemoryInputStream(location, getBlobRequired().data());
    }

    @Override
    public long length()
            throws IOException
    {
        if (length.isEmpty()) {
            length = OptionalLong.of(getBlobRequired().data().length());
        }
        return length.getAsLong();
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        if (lastModified.isEmpty()) {
            lastModified = Optional.of(getBlobRequired().lastModified());
        }
        return lastModified.get();
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return dataSupplier.get() != null;
    }

    @Override
    public Location location()
    {
        return location;
    }

    @Override
    public String toString()
    {
        return location.toString();
    }

    private MemoryBlob getBlobRequired()
            throws FileNotFoundException
    {
        MemoryBlob data = dataSupplier.get();
        if (data == null) {
            throw new FileNotFoundException(toString());
        }
        return data;
    }
}
