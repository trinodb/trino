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
package io.trino.filesystem.hdfs;

import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.hdfs.DynamicConfigurationProvider;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Comparator.reverseOrder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsFileSystemLocal
        extends AbstractTestTrinoFileSystem
{
    private TrinoFileSystem fileSystem;
    private Path tempDirectory;

    @BeforeAll
    void beforeAll()
            throws IOException
    {
        RawLocalFileSystem.useStatIfAvailable();
        DynamicConfigurationProvider viewFs = (config, context, uri) ->
                config.set("fs.viewfs.mounttable.abc.linkFallback", tempDirectory.toAbsolutePath().toUri().toString());

        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), Set.of(viewFs));
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        HdfsContext hdfsContext = new HdfsContext(ConnectorIdentity.ofUser("test"));
        TrinoHdfsFileSystemStats stats = new TrinoHdfsFileSystemStats();

        tempDirectory = Files.createTempDirectory("test");
        fileSystem = new HdfsFileSystem(hdfsEnvironment, hdfsContext, stats);
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
            Iterator<Path> iterator = walk.sorted(reverseOrder()).iterator();
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
        return Location.of("viewfs://abc/");
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        try (Stream<Path> entries = Files.list(tempDirectory)) {
            assertThat(entries.toList()).isEmpty();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Disabled("ViewFs allows traversal outside root")
    @Test
    @Override
    public void testPaths() {}
}
