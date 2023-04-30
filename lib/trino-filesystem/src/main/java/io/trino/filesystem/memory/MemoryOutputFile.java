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
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;

import static java.util.Objects.requireNonNull;

class MemoryOutputFile
        implements TrinoOutputFile
{
    public interface OutputBlob
    {
        boolean exists();

        void createBlob(Slice data)
                throws FileAlreadyExistsException;

        void overwriteBlob(Slice data);
    }

    private final Location location;
    private final OutputBlob outputBlob;

    public MemoryOutputFile(Location location, OutputBlob outputBlob)
    {
        this.location = requireNonNull(location, "location is null");
        this.outputBlob = requireNonNull(outputBlob, "outputBlob is null");
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        if (outputBlob.exists()) {
            throw new FileAlreadyExistsException(toString());
        }
        return new MemoryOutputStream(location, outputBlob::createBlob);
    }

    @Override
    public OutputStream createOrOverwrite(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return new MemoryOutputStream(location, outputBlob::overwriteBlob);
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
}
