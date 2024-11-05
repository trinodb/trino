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

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.Optional;

import static java.util.Collections.emptyIterator;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

class LocalFileIterator
        implements FileIterator
{
    private final Iterator<FileEntry> iterator;

    public LocalFileIterator(Location location, Path rootPath, Path path)
            throws IOException
    {
        requireNonNull(rootPath, "rootPath is null");
        if (Files.isRegularFile(path)) {
            throw new IOException("Location is a file: " + location);
        }
        if (!Files.isDirectory(path)) {
            this.iterator = emptyIterator();
        }
        else {
            ImmutableList.Builder<FileEntry> files = ImmutableList.builder();
            Files.walkFileTree(path, new SimpleFileVisitor<>()
            {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                        throws IOException
                {
                    if (Files.isRegularFile(file)) {
                        if (!file.startsWith(rootPath)) {
                            throw new IOException("entry is not inside of filesystem root");
                        }

                        files.add(new FileEntry(
                                Location.of("local:///" + rootPath.relativize(file)),
                                attributes.size(),
                                attributes.lastModifiedTime().toInstant(),
                                Optional.empty()));
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException e)
                        throws IOException
                {
                    if (e instanceof NoSuchFileException) {
                        // File disappeared during listing operation
                        return FileVisitResult.CONTINUE;
                    }
                    throw e;
                }
            });
            this.iterator = files.build()
                    .stream()
                    .sorted(comparing(entry -> entry.location().fileName()))
                    .iterator();
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
        return iterator.next();
    }
}
