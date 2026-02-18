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
package io.trino.plugin.iceberg.fileio;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

public class EncryptedTrinoOutputFile
        implements TrinoOutputFile
{
    private final Location location;
    private final OutputFile encryptingOutputFile;

    public EncryptedTrinoOutputFile(Location location, OutputFile encryptingOutputFile)
    {
        this.location = requireNonNull(location, "location is null");
        this.encryptingOutputFile = requireNonNull(encryptingOutputFile, "encryptingOutputFile is null");
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        PositionOutputStream outputStream = encryptingOutputFile.create();
        return outputStream;
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        try (OutputStream outputStream = encryptingOutputFile.createOrOverwrite()) {
            outputStream.write(data);
        }
    }

    @Override
    public Location location()
    {
        return location;
    }
}
