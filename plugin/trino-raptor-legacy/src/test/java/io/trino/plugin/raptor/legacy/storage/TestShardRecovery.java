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
package io.trino.plugin.raptor.legacy.storage;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.airlift.units.Duration;
import io.trino.plugin.raptor.legacy.backup.BackupStore;
import io.trino.plugin.raptor.legacy.backup.FileBackupStore;
import io.trino.plugin.raptor.legacy.metadata.ShardManager;
import io.trino.spi.TrinoException;
import io.trino.testing.TestingNodeManager;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.raptor.legacy.DatabaseTesting.createTestingJdbi;
import static io.trino.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_BACKUP_CORRUPTION;
import static io.trino.plugin.raptor.legacy.metadata.SchemaDaoUtil.createTablesWithRetry;
import static io.trino.plugin.raptor.legacy.metadata.TestDatabaseShardManager.createShardManager;
import static io.trino.plugin.raptor.legacy.storage.RaptorStorageManager.xxhash64;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.writeString;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestShardRecovery
{
    private StorageService storageService;
    private ShardRecoveryManager recoveryManager;
    private Handle dummyHandle;
    private Path temporary;
    private FileBackupStore backupStore;

    @BeforeEach
    public void setup()
            throws IOException
    {
        temporary = createTempDirectory(null);
        File directory = temporary.resolve("data").toFile();
        File backupDirectory = temporary.resolve("backup").toFile();
        backupStore = new FileBackupStore(backupDirectory);
        backupStore.start();
        storageService = new FileStorageService(directory);
        storageService.start();

        Jdbi dbi = createTestingJdbi();
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);
        ShardManager shardManager = createShardManager(dbi);
        recoveryManager = createShardRecoveryManager(storageService, Optional.of(backupStore), shardManager);
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        if (dummyHandle != null) {
            dummyHandle.close();
            dummyHandle = null;
        }
        deleteRecursively(temporary, ALLOW_INSECURE);
    }

    @Test
    public void testShardRecovery()
            throws Exception
    {
        UUID shardUuid = UUID.randomUUID();
        File file = storageService.getStorageFile(shardUuid);
        File tempFile = createTempFile(temporary, "tmp", null).toFile();

        writeString(tempFile.toPath(), "test data");

        backupStore.backupShard(shardUuid, tempFile);
        assertThat(backupStore.shardExists(shardUuid)).isTrue();
        File backupFile = backupStore.getBackupFile(shardUuid);
        assertThat(backupFile.exists()).isTrue();
        assertThat(backupFile.length()).isEqualTo(tempFile.length());

        assertThat(file.exists()).isFalse();
        recoveryManager.restoreFromBackup(shardUuid, tempFile.length(), OptionalLong.empty());
        assertThat(file.exists()).isTrue();
        assertThat(file.length()).isEqualTo(tempFile.length());
    }

    @Test
    public void testShardRecoveryExistingFileSizeMismatch()
            throws Exception
    {
        UUID shardUuid = UUID.randomUUID();

        // write data and backup
        File tempFile = createTempFile(temporary, "tmp", null).toFile();
        writeString(tempFile.toPath(), "test data");

        backupStore.backupShard(shardUuid, tempFile);
        assertThat(backupStore.shardExists(shardUuid)).isTrue();

        File backupFile = backupStore.getBackupFile(shardUuid);
        assertThat(Files.equal(tempFile, backupFile)).isTrue();

        // write corrupt storage file with wrong length
        File storageFile = storageService.getStorageFile(shardUuid);
        storageService.createParents(storageFile);

        writeString(storageFile.toPath(), "bad data");

        assertThat(storageFile.exists()).isTrue();
        assertThat(storageFile.length())
                .isNotEqualTo(tempFile.length());
        assertThat(Files.equal(storageFile, tempFile)).isFalse();

        // restore from backup and verify
        recoveryManager.restoreFromBackup(shardUuid, tempFile.length(), OptionalLong.empty());

        assertThat(storageFile.exists()).isTrue();
        assertThat(Files.equal(storageFile, tempFile)).isTrue();

        // verify quarantine exists
        List<String> quarantined = listFiles(storageService.getQuarantineFile(shardUuid).getParentFile());
        assertThat(quarantined.size()).isEqualTo(1);
        assertThat(getOnlyElement(quarantined).startsWith(shardUuid + ".orc.corrupt")).isTrue();
    }

    @Test
    public void testShardRecoveryExistingFileChecksumMismatch()
            throws Exception
    {
        UUID shardUuid = UUID.randomUUID();

        // write data and backup
        File tempFile = createTempFile(temporary, "tmp", null).toFile();
        writeString(tempFile.toPath(), "test data");

        backupStore.backupShard(shardUuid, tempFile);
        assertThat(backupStore.shardExists(shardUuid)).isTrue();

        File backupFile = backupStore.getBackupFile(shardUuid);
        assertThat(Files.equal(tempFile, backupFile)).isTrue();

        // write corrupt storage file with wrong data
        File storageFile = storageService.getStorageFile(shardUuid);
        storageService.createParents(storageFile);

        writeString(storageFile.toPath(), "test xata");

        assertThat(storageFile.exists()).isTrue();
        assertThat(storageFile.length()).isEqualTo(tempFile.length());
        assertThat(Files.equal(storageFile, tempFile)).isFalse();

        // restore from backup and verify
        recoveryManager.restoreFromBackup(shardUuid, tempFile.length(), OptionalLong.of(xxhash64(tempFile)));

        assertThat(storageFile.exists()).isTrue();
        assertThat(Files.equal(storageFile, tempFile)).isTrue();

        // verify quarantine exists
        List<String> quarantined = listFiles(storageService.getQuarantineFile(shardUuid).getParentFile());
        assertThat(quarantined.size()).isEqualTo(1);
        assertThat(getOnlyElement(quarantined).startsWith(shardUuid + ".orc.corrupt")).isTrue();
    }

    @Test
    public void testShardRecoveryBackupChecksumMismatch()
            throws Exception
    {
        UUID shardUuid = UUID.randomUUID();

        // write storage file
        File storageFile = storageService.getStorageFile(shardUuid);
        storageService.createParents(storageFile);

        writeString(storageFile.toPath(), "test data");

        long size = storageFile.length();
        long xxhash64 = xxhash64(storageFile);

        // backup and verify
        backupStore.backupShard(shardUuid, storageFile);

        assertThat(backupStore.shardExists(shardUuid)).isTrue();
        File backupFile = backupStore.getBackupFile(shardUuid);
        assertThat(Files.equal(storageFile, backupFile)).isTrue();

        // corrupt backup file
        writeString(backupFile.toPath(), "test xata");

        assertThat(backupFile.exists()).isTrue();
        assertThat(storageFile.length()).isEqualTo(backupFile.length());
        assertThat(Files.equal(storageFile, backupFile)).isFalse();

        // delete local file to force restore
        assertThat(storageFile.delete()).isTrue();
        assertThat(storageFile.exists()).isFalse();

        // restore should fail
        assertTrinoExceptionThrownBy(() -> recoveryManager.restoreFromBackup(shardUuid, size, OptionalLong.of(xxhash64)))
                .hasErrorCode(RAPTOR_BACKUP_CORRUPTION)
                .hasMessage("Backup is corrupt after read: %s", shardUuid);

        // verify quarantine exists
        List<String> quarantined = listFiles(storageService.getQuarantineFile(shardUuid).getParentFile());
        assertThat(quarantined.size()).isEqualTo(1);
        assertThat(getOnlyElement(quarantined).startsWith(shardUuid + ".orc.corrupt")).isTrue();
    }

    @Test
    public void testNoBackupException()
    {
        assertThatThrownBy(() -> {
            recoveryManager.restoreFromBackup(UUID.randomUUID(), 0, OptionalLong.empty());
        })
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("No backup file found for shard: .*");
    }

    public static ShardRecoveryManager createShardRecoveryManager(
            StorageService storageService,
            Optional<BackupStore> backupStore,
            ShardManager shardManager)
    {
        return new ShardRecoveryManager(
                storageService,
                backupStore,
                new TestingNodeManager(),
                shardManager,
                new Duration(5, MINUTES),
                10);
    }

    private static List<String> listFiles(File path)
    {
        String[] files = path.list();
        assertThat(files).isNotNull();
        return ImmutableList.copyOf(files);
    }
}
