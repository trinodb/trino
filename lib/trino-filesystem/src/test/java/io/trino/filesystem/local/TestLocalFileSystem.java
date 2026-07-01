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
import com.google.common.io.Closer;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLocalFileSystem
        extends AbstractTestTrinoFileSystem
{
    private LocalFileSystem fileSystem;
    private Path tempDirectory;

    @BeforeAll
    void beforeAll()
            throws IOException
    {
        tempDirectory = Files.createTempDirectory("test");
        fileSystem = new LocalFileSystem(tempDirectory);
    }

    @AfterEach
    void afterEach()
            throws IOException
    {
        cleanupFiles();
    }

    @AfterAll
    void afterAll()
            throws IOException
    {
        Files.delete(tempDirectory);
    }

    private void cleanupFiles()
            throws IOException
    {
        // tests will leave directories
        try (Stream<Path> walk = Files.walk(tempDirectory)) {
            Iterator<Path> iterator = walk.sorted(Comparator.reverseOrder()).iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                if (!path.equals(tempDirectory)) {
                    Files.delete(path);
                }
            }
        }
    }

    @Override
    protected boolean isHierarchical()
    {
        return true;
    }

    @Override
    protected boolean supportsIncompleteWriteNoClobber()
    {
        return false;
    }

    @Override
    protected TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected Location getRootLocation()
    {
        return Location.of("local://");
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        try {
            try (Stream<Path> entries = Files.list(tempDirectory)) {
                assertThat(entries.filter(not(tempDirectory::equals)).findFirst()).isEmpty();
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void testListFilesStartingFromConsecutiveSlashesInLocation()
            throws IOException
    {
        // LocalFileSystem (Java NIO) canonicalizes runs of slashes; verifies
        // EmulatedListFilesStartingFromIterator falls back to the slash-collapsed prefix.
        try (Closer closer = Closer.create()) {
            Location file1 = createBlob(closer, "level0/level1-file1");
            Location file2 = createBlob(closer, "level0/level1-file2");

            Location doubledSlash = createLocation("level0").appendSuffix("//");
            ImmutableList.Builder<Location> builder = ImmutableList.builder();
            FileIterator iterator = getFileSystem().listFilesStartingFrom(doubledSlash, "level1-file1");
            while (iterator.hasNext()) {
                builder.add(iterator.next().location());
            }
            assertThat(builder.build()).containsExactlyInAnyOrder(file1, file2);
        }
    }

    @Test
    void testFileSchemeIsLiteralAbsolutePath()
            throws IOException
    {
        Location location = Location.of("file://" + tempDirectory + "/sub/dir/file");
        try (Closer closer = Closer.create()) {
            closer.register(() -> fileSystem.deleteFile(location));
            fileSystem.newOutputFile(location).createOrOverwrite("hello".getBytes(UTF_8));

            assertThat(Files.readString(tempDirectory.resolve("sub/dir/file"))).isEqualTo("hello");
        }
    }

    @Test
    void testFileSchemeOutsideRootRejected()
            throws IOException
    {
        Path outsideDirectory = Files.createTempDirectory("outside");
        try {
            Location location = Location.of("file://" + outsideDirectory + "/file");
            assertThatThrownBy(() -> fileSystem.newOutputFile(location).createOrOverwrite("hello".getBytes(UTF_8)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("outside of the root");
        }
        finally {
            Files.delete(outsideDirectory);
        }
    }

    @Test
    void testFileSchemeTraversalOutsideRootRejected()
    {
        Location location = Location.of("file://" + tempDirectory + "/sub/../../escape");
        assertThatThrownBy(() -> fileSystem.newOutputFile(location).createOrOverwrite("hello".getBytes(UTF_8)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside of the root");
    }

    @Test
    void testSymlinkEscapingRootRejected()
            throws IOException
    {
        Path outsideDirectory = Files.createTempDirectory("outside");
        try {
            Path outsideFile = Files.writeString(outsideDirectory.resolve("secret"), "secret");
            Path link = tempDirectory.resolve("link-to-outside");
            Files.createSymbolicLink(link, outsideFile);
            try {
                Location location = Location.of("local:///link-to-outside");
                assertThatThrownBy(() -> fileSystem.newInputFile(location).newStream())
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("outside of the root");
            }
            finally {
                Files.delete(link);
            }
        }
        finally {
            Files.walk(outsideDirectory)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }

    @Test
    void testLegacyPrefixRebasesUnderNewRoot()
            throws IOException
    {
        Path oldMount = Files.createTempDirectory("old-mount");
        Path newRoot = Files.createTempDirectory("new-root");
        try {
            LocalFileSystem migrated = new LocalFileSystem(newRoot, Optional.of(oldMount));
            Location location = Location.of("file://" + oldMount + "/schema/table/data.parquet");

            migrated.newOutputFile(location).createOrOverwrite("hello".getBytes(UTF_8));

            assertThat(Files.readString(newRoot.resolve("schema/table/data.parquet"))).isEqualTo("hello");
            assertThat(Files.exists(oldMount.resolve("schema/table/data.parquet"))).isFalse();
        }
        finally {
            deleteRecursively(newRoot);
            Files.delete(oldMount);
        }
    }

    @Test
    void testFileSchemeNotMatchingLegacyPrefixIsLiteral()
            throws IOException
    {
        Path oldMount = Files.createTempDirectory("old-mount");
        Path newRoot = Files.createTempDirectory("new-root");
        try {
            LocalFileSystem migrated = new LocalFileSystem(newRoot, Optional.of(oldMount));
            Location location = Location.of("file://" + newRoot + "/schema/table/data.parquet");

            migrated.newOutputFile(location).createOrOverwrite("hello".getBytes(UTF_8));

            assertThat(Files.readString(newRoot.resolve("schema/table/data.parquet"))).isEqualTo("hello");
        }
        finally {
            deleteRecursively(newRoot);
            Files.delete(oldMount);
        }
    }

    private static void deleteRecursively(Path root)
            throws IOException
    {
        try (Stream<Path> walk = Files.walk(root)) {
            Iterator<Path> iterator = walk.sorted(Comparator.reverseOrder()).iterator();
            while (iterator.hasNext()) {
                Files.delete(iterator.next());
            }
        }
    }

    @Test
    void testPathsOutOfBounds()
    {
        assertThatThrownBy(() -> getFileSystem().newInputFile(createLocation("../file")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().newInputFile(createLocation("../file"), 22))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().newOutputFile(createLocation("../file")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().deleteFile(createLocation("../file")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().listFiles(createLocation("../file")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().renameFile(createLocation("../file"), createLocation("target")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
        assertThatThrownBy(() -> getFileSystem().renameFile(createLocation("source"), createLocation("../file")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(createLocation("../file").toString());
    }
}
