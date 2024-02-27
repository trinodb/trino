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
package io.trino.filesystem.ozone;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import org.apache.hadoop.ozone.client.ObjectStore;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

class OzoneOutputFile
        implements TrinoOutputFile
{
    private final OzoneLocation location;
    private final ObjectStore store;

    public OzoneOutputFile(OzoneLocation location, ObjectStore store)
    {
        this.location = requireNonNull(location, "location is null");
        this.store = requireNonNull(store, "blobClient is null");
        location.location().verifyValidFileLocation();
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        // Ozone doesn't support exclusive file creation. Fallback to createOrOverwrite().
        return createOutputStream(memoryContext);
    }

    @Override
    public OutputStream createOrOverwrite(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return createOutputStream(memoryContext);
    }

    private OzoneTrinoOutputStream createOutputStream(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return new OzoneTrinoOutputStream(location, store, memoryContext);
    }

    @Override
    public Location location()
    {
        return location.location();
    }
}
