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

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.filesystem.local.LocalUtils.handleException;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

class LocalFileIterator
        implements FileIterator
{
    private final Path rootPath;
    private final Iterator<Path> iterator;

    public LocalFileIterator(Location location, Path rootPath, Path path)
            throws IOException
    {
        this.rootPath = requireNonNull(rootPath, "rootPath is null");
        if (Files.isRegularFile(path)) {
            throw new IOException("Location is a file: " + location);
        }
        if (!Files.isDirectory(path)) {
            this.iterator = emptyIterator();
        }
        else {
            try (Stream<Path> stream = Files.walk(path)) {
                this.iterator = stream
                        .filter(Files::isRegularFile)
                        // materialize full list so stream can be closed
                        .collect(toImmutableList())
                        .iterator();
            }
            catch (IOException e) {
                throw handleException(location, e);
            }
        }
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        return iterator.hasNext();
    }

    @Override
    public FileEntry next()
            throws IOException
    {
        Path path = iterator.next();
        if (!path.startsWith(rootPath)) {
            throw new IOException("entry is not inside of filesystem root");
        }

        return new FileEntry(
                Location.of("local:///" + rootPath.relativize(path)),
                Files.size(path),
                Files.getLastModifiedTime(path).toInstant(),
                Optional.empty());
    }
}
