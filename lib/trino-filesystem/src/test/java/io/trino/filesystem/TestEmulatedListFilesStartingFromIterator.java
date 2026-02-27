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

    @Test
    void testListFilesStartingFromLeadingSlashInStartingFromMatchesAllFiles()
            throws IOException
    {
        // '/' (0x2F) is less than any printable filename-start character, so a startingFrom
        // value beginning with '/' matches every file whose path remainder does not itself
        // begin with '/' - i.e., every regular file under the directory.
        assertThat(listFilesStartingFrom(
                Location.of("file:///prefix/"),
                "/bar",
                List.of(
                        entry("file:///prefix/abc"),
                        entry("file:///prefix/bar"),
                        entry("file:///prefix/foo"))))
                .containsExactly(
                        Location.of("file:///prefix/abc"),
                        Location.of("file:///prefix/bar"),
                        Location.of("file:///prefix/foo"));
    }

    @Test
    void testListFilesStartingFromDoubleSlashPathComponent()
            throws IOException
    {
        // Blob stores allow keys with empty-name components (e.g. "double//slash"). The
        // path remainder after the listing directory preserves the extra slash, so the
        // slash participates in the comparison against startingFrom at its literal position.
        assertThat(listFilesStartingFrom(
                Location.of("file:///double/"),
                ".",
                List.of(
                        entry("file:///double//slash"),
                        entry("file:///double/a"))))
                .containsExactly(
                        Location.of("file:///double//slash"),
                        Location.of("file:///double/a"));

        assertThat(listFilesStartingFrom(
                Location.of("file:///double/"),
                "a",
                List.of(
                        entry("file:///double//slash"),
                        entry("file:///double/a"))))
                .containsExactly(
                        Location.of("file:///double/a"));
    }

    @Test
    void testListFilesStartingFromIncludesAllNonAsciiFilenames()
            throws IOException
    {
        // Any non-ASCII code point is lexicographically greater than every ASCII character
        // under UTF-8 byte, Java char, and Unicode code point comparisons, so an ASCII
        // startingFrom must never exclude a file with a non-ASCII path remainder.
        assertThat(listFilesStartingFrom(
                Location.of("file:///prefix/"),
                "a",
                List.of(
                        entry("file:///prefix/a-ascii"),
                        entry("file:///prefix/é-latin1"),
                        entry("file:///prefix/α-greek"),
                        entry("file:///prefix/💡-supplementary"),
                        entry("file:///prefix/Ａ-fullwidth"))))
                .containsExactlyInAnyOrder(
                        Location.of("file:///prefix/a-ascii"),
                        Location.of("file:///prefix/é-latin1"),
                        Location.of("file:///prefix/α-greek"),
                        Location.of("file:///prefix/💡-supplementary"),
                        Location.of("file:///prefix/Ａ-fullwidth"));
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
