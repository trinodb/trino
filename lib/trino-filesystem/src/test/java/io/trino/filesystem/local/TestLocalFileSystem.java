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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

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
