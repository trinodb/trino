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

import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests a filesystem with a root location that has a non-empty path instead of a bare
 * top-level directory. A filesystem may be nested under some other root.
 */
public class TestLocalFileSystemWithPathPrefix
        extends AbstractTestTrinoFileSystem
{
    private LocalFileSystem fileSystem;
    private Path tempDirectory;
    private Path rootDirectory;

    @BeforeAll
    void beforeAll()
            throws IOException
    {
        tempDirectory = Files.createTempDirectory("test");
        rootDirectory = tempDirectory.resolve("test-root");
        Files.createDirectory(rootDirectory);
        fileSystem = new LocalFileSystem(tempDirectory);
    }

    @Override
    protected boolean supportsCreateExclusive()
    {
        return false;
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
        Files.delete(rootDirectory);
        Files.delete(tempDirectory);
    }

    private void cleanupFiles()
            throws IOException
    {
        try (Stream<Path> walk = Files.walk(tempDirectory)) {
            Iterator<Path> iterator = walk.sorted(Comparator.reverseOrder()).iterator();
            while (iterator.hasNext()) {
                Path path = iterator.next();
                if (!path.equals(tempDirectory)) {
                    Files.delete(path);
                }
            }
        }
        Files.createDirectory(rootDirectory);
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
        return Location.of("local:///test-root/");
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        try (Stream<Path> entries = Files.list(rootDirectory)) {
            assertThat(entries.findFirst()).isEmpty();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void testPathHierarchical()
            throws IOException
    {
        // The root is a subdirectory, so `..` resolves to a valid path.
        try (TempBlob absolute = new TempBlob(createLocation("b"))) {
            try (TempBlob alias = new TempBlob(createLocation("a/../b"))) {
                absolute.createOrOverwrite(TEST_BLOB_CONTENT_PREFIX + absolute.location().toString());
                assertThat(alias.exists()).isTrue();
                assertThat(absolute.exists()).isTrue();

                assertThat(alias.read()).isEqualTo(TEST_BLOB_CONTENT_PREFIX + absolute.location().toString());

                assertThat(listPath("")).containsExactly(absolute.location());

                getFileSystem().deleteFile(alias.location());
                assertThat(alias.exists()).isFalse();
                assertThat(absolute.exists()).isFalse();
            }
        }
    }
}
