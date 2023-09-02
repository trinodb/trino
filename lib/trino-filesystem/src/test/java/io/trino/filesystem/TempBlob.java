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
package io.trino.filesystem;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import static io.trino.filesystem.AbstractTrinoFileSystemTestingEnvironment.readLocation;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TempBlob
        implements Closeable
{
    private final Location location;
    private final TrinoFileSystem fileSystem;

    public TempBlob(Location location, TrinoFileSystem fileSystem)
    {
        this.location = requireNonNull(location, "location is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
    }

    public Location location()
    {
        return location;
    }

    public boolean exists()
    {
        try {
            return inputFile().exists();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TrinoInputFile inputFile()
    {
        return fileSystem.newInputFile(location);
    }

    public TrinoOutputFile outputFile()
    {
        return fileSystem.newOutputFile(location);
    }

    public void createOrOverwrite(String data)
    {
        try (OutputStream outputStream = outputFile().createOrOverwrite()) {
            outputStream.write(data.getBytes(UTF_8));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        assertThat(exists()).isTrue();
    }

    public String read()
    {
        return readLocation(location, fileSystem);
    }

    @Override
    public void close()
    {
        try {
            fileSystem.deleteFile(location);
        }
        catch (IOException ignored) {
        }
    }
}
