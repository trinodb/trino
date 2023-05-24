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
package io.trino.filesystem.local;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.trino.filesystem.local.LocalUtils.handleException;
import static java.util.Objects.requireNonNull;

public class LocalOutputFile
        implements TrinoOutputFile
{
    private final Location location;
    private final Path path;

    public LocalOutputFile(Location location, Path path)
    {
        this.location = requireNonNull(location, "location is null");
        this.path = requireNonNull(path, "path is null");
    }

    public LocalOutputFile(File file)
    {
        this(Location.of(file.toURI().toString()), file.toPath());
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        try {
            Files.createDirectories(path.getParent());
            OutputStream stream = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            return new LocalOutputStream(location, stream);
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public OutputStream createOrOverwrite(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        try {
            Files.createDirectories(path.getParent());
            OutputStream stream = Files.newOutputStream(path);
            return new LocalOutputStream(location, stream);
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
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
