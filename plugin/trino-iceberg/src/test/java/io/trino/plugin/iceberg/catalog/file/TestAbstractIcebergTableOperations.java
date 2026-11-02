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

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.plugin.iceberg.fileio.ForwardingInputFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_MISSING_METADATA;
import static io.trino.plugin.iceberg.IcebergTestUtils.ENCRYPTION_MANAGER_FACTORY;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAbstractIcebergTableOperations
{
    private static final String TABLE_LOCATION = "local:///test-table";
    private static final String EXPECTED_LOCATION = TABLE_LOCATION + "/metadata/00000-expected.metadata.json";
    private static final String NEW_LOCATION = TABLE_LOCATION + "/metadata/00001-new.metadata.json";
    private static final String CONCURRENT_LOCATION = TABLE_LOCATION + "/metadata/00001-concurrent.metadata.json";
    private static final String LATER_LOCATION = TABLE_LOCATION + "/metadata/00002-later.metadata.json";

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
                new ForwardingFileIo(fileSystemFactory.create(SESSION), true)
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
                Optional.empty(),
                ENCRYPTION_MANAGER_FACTORY)
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

    @Test
    public void testMissingMetadataFileReporting()
            throws IOException
    {
        Path tempDir = Files.createTempDirectory("test_missing_metadata_file_reporting");
        File metastoreDir = tempDir.resolve("iceberg_data").toFile();
        metastoreDir.mkdirs();
        TrinoFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(metastoreDir.toPath());
        HiveMetastore metastore = createTestingFileHiveMetastore(fileSystemFactory, Location.of("local:///"));

        FileMetastoreTableOperations fileMetastoreTableOperations = new FileMetastoreTableOperations(
                new ForwardingFileIo(fileSystemFactory.create(SESSION), true)
                {
                    @Override
                    public InputFile newInputFile(String path)
                    {
                        // Reproduce the S3 shape : opening the stream
                        // succeeds and the missing object only surfaces on read, so the real
                        // TableMetadataParser is what wraps FileNotFoundException into
                        // RuntimeIOException (an UncheckedIOException).
                        return new ForwardingInputFile(lazyMissingFile(path));
                    }
                },
                createPerTransactionCache(metastore, 1000),
                SESSION,
                "test-database",
                "test-table",
                Optional.of("test-owner"),
                Optional.empty(),
                ENCRYPTION_MANAGER_FACTORY)
        {
            @Override
            protected String getRefreshedLocation(boolean invalidateCaches)
            {
                return "local:///0.metadata.json";
            }
        };

        assertTrinoExceptionThrownBy(fileMetastoreTableOperations::refresh)
                .hasErrorCode(ICEBERG_MISSING_METADATA)
                .hasMessageContaining("Metadata not found in metadata location for table");
    }

    /**
     * An input file that behaves like {@code S3InputFile} for a missing object: opening a stream
     * succeeds, and the absence is only reported once the stream is read.
     */
    private static TrinoInputFile lazyMissingFile(String path)
    {
        return new TrinoInputFile()
        {
            @Override
            public TrinoInputStream newStream()
            {
                return new TrinoInputStream()
                {
                    @Override
                    public int read()
                            throws IOException
                    {
                        throw new FileNotFoundException(path);
                    }

                    @Override
                    public long getPosition()
                    {
                        return 0;
                    }

                    @Override
                    public void seek(long position) {}
                };
            }

            @Override
            public TrinoInput newInput()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long length()
                    throws IOException
            {
                throw new FileNotFoundException(path);
            }

            @Override
            public Instant lastModified()
                    throws IOException
            {
                throw new FileNotFoundException(path);
            }

            @Override
            public boolean exists()
            {
                return false;
            }

            @Override
            public Location location()
            {
                return Location.of(path);
            }
        };
    }

    @Test
    public void testUpdateCommitSucceedsWhenLaterCommitBuiltOnIt()
            throws IOException
    {
        TestingFileMetastoreTableOperations tableOperations = tableOperationsPointingAt(LATER_LOCATION, metadataWithLog(EXPECTED_LOCATION, NEW_LOCATION));

        assertThatCode(() -> tableOperations.checkExistingTableCommit(EXPECTED_LOCATION, NEW_LOCATION, false, new RuntimeException("simulated metastore failure")))
                .doesNotThrowAnyException();
    }

    @Test
    public void testUpdateCommitFailsWhenRejectedAndExpectedStillInMetadataLog()
            throws IOException
    {
        TestingFileMetastoreTableOperations tableOperations = tableOperationsPointingAt(CONCURRENT_LOCATION, metadataWithLog(EXPECTED_LOCATION));

        assertThatThrownBy(() -> tableOperations.checkExistingTableCommit(EXPECTED_LOCATION, NEW_LOCATION, true, new RuntimeException("simulated rejection")))
                .isInstanceOf(CommitFailedException.class);
    }

    @Test
    public void testUpdateCommitFailsWhenRejectedAndCatalogStillPointsAtExpected()
            throws IOException
    {
        TestingFileMetastoreTableOperations tableOperations = tableOperationsPointingAt(EXPECTED_LOCATION, metadataWithLog());

        assertThatThrownBy(() -> tableOperations.checkExistingTableCommit(EXPECTED_LOCATION, NEW_LOCATION, true, new RuntimeException("simulated rejection")))
                .isInstanceOf(CommitFailedException.class);
    }

    @Test
    public void testUpdateCommitUnknownWhenNotRejected()
            throws IOException
    {
        TestingFileMetastoreTableOperations tableOperations = tableOperationsPointingAt(CONCURRENT_LOCATION, metadataWithLog(EXPECTED_LOCATION));

        assertThatThrownBy(() -> tableOperations.checkExistingTableCommit(EXPECTED_LOCATION, NEW_LOCATION, false, new RuntimeException("simulated metastore failure")))
                .isInstanceOf(CommitStateUnknownException.class);
    }

    @Test
    public void testUpdateCommitUnknownWhenExpectedEvictedFromMetadataLog()
            throws IOException
    {
        TestingFileMetastoreTableOperations tableOperations = tableOperationsPointingAt(LATER_LOCATION, metadataWithLog(CONCURRENT_LOCATION));

        assertThatThrownBy(() -> tableOperations.checkExistingTableCommit(EXPECTED_LOCATION, NEW_LOCATION, true, new RuntimeException("simulated rejection")))
                .isInstanceOf(CommitStateUnknownException.class);
    }

    private static TestingFileMetastoreTableOperations tableOperationsPointingAt(String committedMetadataLocation, TableMetadata committedMetadata)
            throws IOException
    {
        Path tempDir = Files.createTempDirectory("test_update_commit_status");
        TrinoFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(tempDir);
        FileIO fileIo = new ForwardingFileIo(fileSystemFactory.create(SESSION), true);
        TableMetadataParser.write(committedMetadata, fileIo.newOutputFile(committedMetadataLocation));
        return new TestingFileMetastoreTableOperations(fileIo, createTestingFileHiveMetastore(fileSystemFactory, Location.of("local:///")), committedMetadataLocation);
    }

    private static TableMetadata metadataWithLog(String... previousLocations)
    {
        TableMetadata metadata = TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                TABLE_LOCATION,
                ImmutableMap.of());
        for (String previousLocation : previousLocations) {
            // setPreviousFileLocation alone is not a change, and build() records the previous file only when there is one
            metadata = TableMetadata.buildFrom(metadata)
                    .setPreviousFileLocation(previousLocation)
                    .setProperties(ImmutableMap.of("touched", previousLocation))
                    .build();
        }
        return metadata;
    }

    private static class TestingFileMetastoreTableOperations
            extends FileMetastoreTableOperations
    {
        private final String committedMetadataLocation;

        public TestingFileMetastoreTableOperations(FileIO fileIo, HiveMetastore metastore, String committedMetadataLocation)
        {
            super(fileIo, createPerTransactionCache(metastore, 1000), SESSION, "test-database", "test-table", Optional.of("test-owner"), Optional.empty(), ENCRYPTION_MANAGER_FACTORY);
            this.committedMetadataLocation = committedMetadataLocation;
        }

        @Override
        protected String getRefreshedLocation(boolean invalidateCaches)
        {
            return committedMetadataLocation;
        }

        @Override
        public void checkExistingTableCommit(String expectedMetadataLocation, String newMetadataLocation, boolean rejected, Exception failure)
        {
            super.checkExistingTableCommit(expectedMetadataLocation, newMetadataLocation, rejected, failure);
        }
    }
}
