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
package io.trino.plugin.iceberg.catalog.file;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import org.apache.iceberg.io.InputFile;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;

public class TestAbstractIcebergTableOperations
{
    @Test
    public void testS3ErrorReporting()
            throws IOException
    {
        Path tempDir = Files.createTempDirectory("test_s3_error_reporting");
        File metastoreDir = tempDir.resolve("iceberg_data").toFile();
        metastoreDir.mkdirs();
        TrinoFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(metastoreDir.toPath());
        HiveMetastore metastore = createTestingFileHiveMetastore(fileSystemFactory, Location.of("local:///"));

        FileMetastoreTableOperations fileMetastoreTableOperations = new FileMetastoreTableOperations(
                new ForwardingFileIo(fileSystemFactory.create(SESSION))
                {
                    @Override
                    public InputFile newInputFile(String path)
                    {
                        // Mimic ForwardingInputFile.newStream() behavior when there's an S3 error.
                        throw new UncheckedIOException(new IOException());
                    }
                },
                createPerTransactionCache(metastore, 1000),
                SESSION,
                "test-database",
                "test-table",
                Optional.of("test-owner"),
                Optional.empty())
        {
            // Without this, we'd have to create a table that's never accessed anyway, because we're simulating S3 errors.
            @Override
            protected String getRefreshedLocation(boolean invalidateCaches)
            {
                return "local:///0.metadata.json";
            }
        };

        assertTrinoExceptionThrownBy(fileMetastoreTableOperations::refresh).hasErrorCode(ICEBERG_INVALID_METADATA);
    }
}
