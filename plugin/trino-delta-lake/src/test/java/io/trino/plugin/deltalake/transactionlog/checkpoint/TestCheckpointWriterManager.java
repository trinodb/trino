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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.filesystem.tracking.TrackingFileSystem;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeTableCredentials;
import io.trino.plugin.deltalake.metastore.FileSystemCredentials;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.TransactionLogCleanup;
import io.trino.plugin.deltalake.transactionlog.reader.FileSystemTransactionLogReaderFactory;
import io.trino.plugin.deltalake.transactionlog.reader.TransactionLogReader;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.deltalake.DeltaLakeConfig.DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

final class TestCheckpointWriterManager
{
    private static final SchemaTableName TABLE = new SchemaTableName("schema", "table");
    private static final long CHECKPOINT_VERSION = 2;

    @TempDir
    Path temporaryDirectory;

    @Test
    void testCleanupUsesCredentialFileSystemAndCustomRetentionFromMetadata()
            throws Exception
    {
        Fixture fixture = createFixture(
                "custom-retention",
                ImmutableMap.of("delta.logRetentionDuration", "interval 1 day"),
                FailureMode.NONE);

        fixture.writeCheckpoint();

        assertThat(fixture.fileSystemFactory.requestedCredentials()).contains(fixture.tableCredentials);
        assertThat(fixture.fileSystem.listCalls()).isOne();
        assertThat(fixture.fileSystem.deletedFiles())
                .extracting(Location::fileName)
                .containsExactlyInAnyOrder(jsonName(0), jsonName(1), checkpointName(0));
        assertThat(fixture.transactionLogDirectory.resolve(jsonName(0))).doesNotExist();
        assertThat(fixture.transactionLogDirectory.resolve(jsonName(1))).doesNotExist();
        assertCheckpointPublished(fixture.transactionLogDirectory);
    }

    @Test
    void testCleanupDisabledThroughMetadata()
            throws Exception
    {
        Fixture fixture = createFixture(
                "disabled",
                ImmutableMap.of(
                        "delta.enableExpiredLogCleanup", "false",
                        "delta.logRetentionDuration", "interval 0 seconds"),
                FailureMode.NONE);

        fixture.writeCheckpoint();

        assertThat(fixture.fileSystem.listCalls()).isZero();
        assertThat(fixture.fileSystem.deletedFiles()).isEmpty();
        assertThat(fixture.transactionLogDirectory.resolve(jsonName(0))).exists();
        assertThat(fixture.transactionLogDirectory.resolve(checkpointName(0))).exists();
        assertCheckpointPublished(fixture.transactionLogDirectory);
    }

    @Test
    void testCleanupFailureDoesNotFailCheckpointPublication()
            throws Exception
    {
        for (FailureMode failureMode : List.of(FailureMode.LIST, FailureMode.DELETE)) {
            Fixture fixture = createFixture(
                    failureMode.name().toLowerCase(Locale.ENGLISH),
                    ImmutableMap.of("delta.logRetentionDuration", "interval 1 day"),
                    failureMode);

            assertThatCode(fixture::writeCheckpoint).doesNotThrowAnyException();

            assertThat(fixture.fileSystem.listCalls()).isOne();
            assertThat(fixture.fileSystem.failureTriggered()).isTrue();
            assertCheckpointPublished(fixture.transactionLogDirectory);
        }
    }

    private Fixture createFixture(String name, Map<String, String> configuration, FailureMode failureMode)
            throws Exception
    {
        String tableLocation = "local:///" + name;
        Path transactionLogDirectory = temporaryDirectory.resolve(name).resolve("_delta_log");
        Files.createDirectories(transactionLogDirectory);

        Files.writeString(transactionLogDirectory.resolve(jsonName(0)), transactionLog(configuration));
        Files.writeString(transactionLogDirectory.resolve(jsonName(1)), transactionEntry(1));
        Files.writeString(transactionLogDirectory.resolve(jsonName(2)), transactionEntry(2));
        Files.writeString(transactionLogDirectory.resolve(checkpointName(0)), "old checkpoint");

        FileTime expired = FileTime.from(Instant.now().minus(Duration.ofDays(2)));
        for (String fileName : List.of(jsonName(0), jsonName(1), jsonName(2), checkpointName(0))) {
            Files.setLastModifiedTime(transactionLogDirectory.resolve(fileName), expired);
        }

        TrinoFileSystem localFileSystem = new LocalFileSystem(temporaryDirectory);
        RecordingFileSystem recordingFileSystem = new RecordingFileSystem(localFileSystem, failureMode);
        DeltaLakeTableCredentials tableCredentials = new DeltaLakeTableCredentials(
                VendedCredentialsHandle.empty(tableLocation),
                new TestingFileSystemCredentials());
        TestingDeltaLakeFileSystemFactory fileSystemFactory = new TestingDeltaLakeFileSystemFactory(recordingFileSystem);

        TransactionLogReader transactionLogReader = (_, startVersion, endVersion, maxCachedFileSize) ->
                TransactionLogTail.loadNewTail(localFileSystem, tableLocation, startVersion, endVersion, maxCachedFileSize);
        TableSnapshot snapshot = TableSnapshot.load(
                SESSION,
                transactionLogReader,
                TABLE,
                Optional.empty(),
                tableLocation,
                ParquetReaderOptions.defaultOptions(),
                true,
                32,
                DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE,
                Optional.empty());

        DeltaLakeConfig config = new DeltaLakeConfig();
        CheckpointSchemaManager checkpointSchemaManager = new CheckpointSchemaManager(TESTING_TYPE_MANAGER);
        FileFormatDataSourceStats fileFormatDataSourceStats = new FileFormatDataSourceStats();
        TransactionLogAccess transactionLogAccess = new TransactionLogAccess(
                TESTING_TYPE_MANAGER,
                checkpointSchemaManager,
                config,
                fileFormatDataSourceStats,
                fileSystemFactory,
                new ParquetReaderConfig(),
                newDirectExecutorService(),
                new FileSystemTransactionLogReaderFactory(fileSystemFactory));
        CheckpointWriterManager checkpointWriterManager = new CheckpointWriterManager(
                TESTING_TYPE_MANAGER,
                checkpointSchemaManager,
                fileSystemFactory,
                new NodeVersion("test"),
                transactionLogAccess,
                new TransactionLogCleanup(),
                fileFormatDataSourceStats,
                jsonCodec(LastCheckpoint.class),
                config,
                newDirectExecutorService());
        return new Fixture(transactionLogDirectory, tableCredentials, fileSystemFactory, recordingFileSystem, checkpointWriterManager, snapshot);
    }

    private static void assertCheckpointPublished(Path transactionLogDirectory)
            throws IOException
    {
        assertThat(transactionLogDirectory.resolve(checkpointName(CHECKPOINT_VERSION))).isNotEmptyFile();
        assertThat(transactionLogDirectory.resolve("_last_checkpoint")).isNotEmptyFile();
        LastCheckpoint lastCheckpoint = jsonCodec(LastCheckpoint.class)
                .fromJson(Files.readString(transactionLogDirectory.resolve("_last_checkpoint")));
        assertThat(lastCheckpoint.version()).isEqualTo(CHECKPOINT_VERSION);
    }

    private static String transactionLog(Map<String, String> configuration)
    {
        String configurationJson = configuration.entrySet().stream()
                .map(entry -> "\"%s\":\"%s\"".formatted(entry.getKey(), entry.getValue()))
                .collect(joining(",", "{", "}"));
        return """
               {"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
               {"metaData":{"id":"test","format":{"provider":"parquet","options":{}},"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[{\\"name\\":\\"value\\",\\"type\\":\\"integer\\",\\"nullable\\":true,\\"metadata\\":{}}]}","partitionColumns":[],"configuration":%s,"createdTime":0}}
               """.formatted(configurationJson);
    }

    private static String transactionEntry(long version)
    {
        return "{\"txn\":{\"appId\":\"test\",\"version\":%s,\"lastUpdated\":0}}\n".formatted(version);
    }

    private static String jsonName(long version)
    {
        return "%020d.json".formatted(version);
    }

    private static String checkpointName(long version)
    {
        return "%020d.checkpoint.parquet".formatted(version);
    }

    private record Fixture(
            Path transactionLogDirectory,
            DeltaLakeTableCredentials tableCredentials,
            TestingDeltaLakeFileSystemFactory fileSystemFactory,
            RecordingFileSystem fileSystem,
            CheckpointWriterManager checkpointWriterManager,
            TableSnapshot snapshot)
    {
        void writeCheckpoint()
        {
            checkpointWriterManager.writeCheckpoint(SESSION, snapshot, Optional.of(tableCredentials));
        }
    }

    private static final class TestingDeltaLakeFileSystemFactory
            implements DeltaLakeFileSystemFactory
    {
        private final TrinoFileSystem fileSystem;
        private Optional<DeltaLakeTableCredentials> requestedCredentials = Optional.empty();

        private TestingDeltaLakeFileSystemFactory(TrinoFileSystem fileSystem)
        {
            this.fileSystem = fileSystem;
        }

        @Override
        public TrinoFileSystem create(ConnectorSession session, Optional<DeltaLakeTableCredentials> tableCredentials)
        {
            requestedCredentials = tableCredentials;
            return fileSystem;
        }

        @Override
        public TrinoFileSystem create(ConnectorSession session, String tableLocation)
        {
            throw new AssertionError("Unexpected location-based filesystem creation");
        }

        private Optional<DeltaLakeTableCredentials> requestedCredentials()
        {
            return requestedCredentials;
        }
    }

    private static final class RecordingFileSystem
            extends TrackingFileSystem
    {
        private static final Cleaner CLEANER = Cleaner.create();

        private final FailureMode failureMode;
        private final ImmutableList.Builder<Location> deletedFiles = ImmutableList.builder();
        private int listCalls;
        private boolean failureTriggered;

        private RecordingFileSystem(TrinoFileSystem delegate, FailureMode failureMode)
        {
            super(delegate, CLEANER);
            this.failureMode = failureMode;
        }

        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            listCalls++;
            if (failureMode == FailureMode.LIST) {
                failureTriggered = true;
                throw new IOException("list failed");
            }
            return super.listFiles(location);
        }

        @Override
        public void deleteFiles(Collection<Location> locations)
                throws IOException
        {
            deletedFiles.addAll(locations);
            if (failureMode == FailureMode.DELETE) {
                failureTriggered = true;
                throw new IOException("delete failed");
            }
            super.deleteFiles(locations);
        }

        private int listCalls()
        {
            return listCalls;
        }

        private List<Location> deletedFiles()
        {
            return deletedFiles.build();
        }

        private boolean failureTriggered()
        {
            return failureTriggered;
        }
    }

    private static final class TestingFileSystemCredentials
            implements FileSystemCredentials
    {
        @Override
        public Map<String, String> asExtraCredentials()
        {
            return ImmutableMap.of();
        }

        @Override
        public boolean isValid()
        {
            return true;
        }
    }

    private enum FailureMode
    {
        NONE,
        LIST,
        DELETE,
    }
}
