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
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.local.LocalUtils.handleException;
import static java.util.Objects.requireNonNull;

public class LocalInputFile
        implements TrinoInputFile
{
    private final Location location;
    private final Path path;
    private OptionalLong length = OptionalLong.empty();
    private Optional<Instant> lastModified = Optional.empty();

    public LocalInputFile(Location location, Path path)
    {
        this.location = requireNonNull(location, "location is null");
        this.path = requireNonNull(path, "path is null");
    }

    public LocalInputFile(Location location, Path path, long length)
    {
        this.location = requireNonNull(location, "location is null");
        this.path = requireNonNull(path, "path is null");
        checkArgument(length >= 0, "length is negative");
        this.length = OptionalLong.of(length);
    }

    public LocalInputFile(File file)
    {
        this(Location.of(file.toURI().toString()), file.toPath());
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        try {
            return new LocalInput(location, path.toFile());
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        try {
            return new LocalInputStream(location, path.toFile());
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public long length()
            throws IOException
    {
        if (length.isEmpty()) {
            try {
                length = OptionalLong.of(Files.size(path));
            }
            catch (IOException e) {
                throw handleException(location, e);
            }
        }
        return length.getAsLong();
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        if (lastModified.isEmpty()) {
            try {
                lastModified = Optional.of(Files.getLastModifiedTime(path).toInstant());
            }
            catch (IOException e) {
                throw handleException(location, e);
            }
        }
        return lastModified.get();
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return Files.exists(path);
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
