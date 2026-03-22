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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TestEmulatedListFilesStartingFromIterator
{
    private static final Instant MODIFIED = Instant.ofEpochSecond(1234567890);

    @Test
    void testListFilesStartingFromUsesFullPathRemainder()
            throws IOException
    {
        assertThat(listFilesStartingFrom(
                Location.of("file:///prefix/"),
                "a/file-2",
                List.of(
                        entry("file:///prefix/a/file-1"),
                        entry("file:///prefix/a/file-2"),
                        entry("file:///prefix/a/sub/file-3"),
                        entry("file:///prefix/b/file-4"))))
                .containsExactly(
                        Location.of("file:///prefix/a/file-2"),
                        Location.of("file:///prefix/a/sub/file-3"),
                        Location.of("file:///prefix/b/file-4"));
    }

    @Test
    void testListFilesStartingFromNormalizesDirectoryLocationWithoutTrailingSlash()
            throws IOException
    {
        assertThat(listFilesStartingFrom(
                Location.of("file:///prefix"),
                "b",
                List.of(
                        entry("file:///prefix/a/file-1"),
                        entry("file:///prefix/b/file-2"),
                        entry("file:///prefix/c/file-3"))))
                .containsExactly(
                        Location.of("file:///prefix/b/file-2"),
                        Location.of("file:///prefix/c/file-3"));
    }

    @Test
    void testListFilesStartingFromSupportsRootListing()
            throws IOException
    {
        assertThat(listFilesStartingFrom(
                Location.of("file:///"),
                "b",
                List.of(
                        entry("file:///a/file-1"),
                        entry("file:///b/file-2"),
                        entry("file:///c/file-3"))))
                .containsExactly(
                        Location.of("file:///b/file-2"),
                        Location.of("file:///c/file-3"));
    }

    @Test
    void testListFilesStartingFromSupportsAuthorityRootListing()
            throws IOException
    {
        assertThat(listFilesStartingFrom(
                Location.of("memory://"),
                "b/file-2",
                List.of(
                        entry("memory:///a/file-1"),
                        entry("memory:///b/file-2"),
                        entry("memory:///c/file-3"))))
                .containsExactly(
                        Location.of("memory:///b/file-2"),
                        Location.of("memory:///c/file-3"));
    }

    @Test
    void testListFilesStartingFromSupportsBarePaths()
            throws IOException
    {
        assertThat(listFilesStartingFrom(
                Location.of("/prefix/"),
                "b",
                List.of(
                        entry("/prefix/a/file-1"),
                        entry("/prefix/b/file-2"),
                        entry("/prefix/c/file-3"))))
                .containsExactly(
                        Location.of("/prefix/b/file-2"),
                        Location.of("/prefix/c/file-3"));
    }

    @Test
    void testListFilesStartingFromEmptyStartingFromMatchesListFiles()
            throws IOException
    {
        List<FileEntry> entries = List.of(
                entry("file:///prefix/a/file-1"),
                entry("file:///prefix/b/file-2"));

        assertThat(listLocations(new EmulatedListFilesStartingFromIterator(
                new ListFileIterator(entries),
                Location.of("file:///prefix/"),
                "")))
                .isEqualTo(entries.stream()
                        .map(FileEntry::location)
                        .toList());
    }

    @Test
    void testListFilesStartingFromAllowsSlashInStartingFrom()
            throws IOException
    {
        assertThat(listFilesStartingFrom(
                Location.of("file:///prefix/"),
                "a/file-1",
                List.of(
                        entry("file:///prefix/a/file-1"),
                        entry("file:///prefix/a/file-2"),
                        entry("file:///prefix/b/file-3"))))
                .containsExactly(
                        Location.of("file:///prefix/a/file-1"),
                        Location.of("file:///prefix/a/file-2"),
                        Location.of("file:///prefix/b/file-3"));
    }

    private static List<Location> listFilesStartingFrom(Location location, String startingFrom, List<FileEntry> entries)
            throws IOException
    {
        return listLocations(new EmulatedListFilesStartingFromIterator(new ListFileIterator(entries), location, startingFrom));
    }

    private static FileEntry entry(String location)
    {
        return new FileEntry(Location.of(location), 1, MODIFIED, Optional.empty());
    }

    private static List<Location> listLocations(FileIterator iterator)
            throws IOException
    {
        List<Location> locations = new ArrayList<>();
        while (iterator.hasNext()) {
            locations.add(iterator.next().location());
        }
        return List.copyOf(locations);
    }

    private static final class ListFileIterator
            implements FileIterator
    {
        private final Iterator<FileEntry> iterator;

        private ListFileIterator(List<FileEntry> entries)
        {
            iterator = entries.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public FileEntry next()
        {
            return iterator.next();
        }
    }
}
