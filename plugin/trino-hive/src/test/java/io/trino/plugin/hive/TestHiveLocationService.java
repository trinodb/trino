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
package io.trino.plugin.hive;

import io.trino.filesystem.Location;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.metastore.Partition;
import io.trino.metastore.Table;
import io.trino.plugin.hive.LocationService.WriteInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_FILES_TO_TARGET_DIRECTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;

public class TestHiveLocationService
{
    @Test
    public void testGetTableWriteInfoAppend()
    {
        assertThat(locationHandle(STAGE_AND_MOVE_TO_TARGET_DIRECTORY), false)
                .producesWriteInfo(writeInfo(
                        "/target",
                        "/write",
                        STAGE_AND_MOVE_TO_TARGET_DIRECTORY));

        assertThat(locationHandle(DIRECT_TO_TARGET_EXISTING_DIRECTORY, "/target", "/target"), false)
                .producesWriteInfo(writeInfo(
                        "/target",
                        "/target",
                        DIRECT_TO_TARGET_EXISTING_DIRECTORY));

        assertThat(locationHandle(DIRECT_TO_TARGET_NEW_DIRECTORY, "/target", "/target"), false)
                .producesWriteInfo(writeInfo(
                        "/target",
                        "/target",
                        DIRECT_TO_TARGET_NEW_DIRECTORY));
    }

    @Test
    public void testGetTableWriteInfoOverwriteSuccess()
    {
        assertThat(locationHandle(STAGE_AND_MOVE_TO_TARGET_DIRECTORY), true)
                .producesWriteInfo(writeInfo("/target", "/write", STAGE_AND_MOVE_TO_TARGET_DIRECTORY));
    }

    @Test
    public void testGetTableWriteInfoOverwriteFailDirectNew()
    {
        assertTrinoExceptionThrownBy(() -> assertThat(locationHandle(DIRECT_TO_TARGET_NEW_DIRECTORY, "/target", "/target"), true))
                .hasMessage("Overwriting unpartitioned table not supported when writing directly to target directory");
    }

    @Test
    public void testGetTableWriteInfoOverwriteFailDirectExisting()
    {
        assertTrinoExceptionThrownBy(() -> assertThat(locationHandle(DIRECT_TO_TARGET_EXISTING_DIRECTORY, "/target", "/target"), true))
                .hasMessage("Overwriting unpartitioned table not supported when writing directly to target directory");
    }

    @Test
    public void testTableLocalStaging(@TempDir Path directory)
    {
        HiveLocationService service = new HiveLocationService(new LocalFileSystemFactory(directory), new HiveConfig().setTableLocalStagingDirectoryEnabled(true).setTemporaryStagingDirectoryPath("legacy-spill"));
        LocationHandle handle = service.forExistingTable(null, SESSION, table("local:///table", Map.of()));
        WriteInfo tableWrite = service.getTableWriteInfo(handle, false);
        Assertions.assertThat(tableWrite.writeMode()).isEqualTo(STAGE_AND_MOVE_FILES_TO_TARGET_DIRECTORY);
        Assertions.assertThat(tableWrite.writePath()).isEqualTo(Location.of("local:///table/.hive-staging/" + handle.getStagingOperationId().orElseThrow()));

        Partition partition = Partition.builder()
                .setDatabaseName("schema")
                .setTableName("table")
                .setValues(List.of("1"))
                .setColumns(List.of())
                .setParameters(Map.of())
                .withStorage(storage -> storage.setStorageFormat(ORC.toStorageFormat()).setLocation("local:///custom/nested/partition"))
                .build();
        WriteInfo partitionWrite = service.getPartitionWriteInfo(handle, Optional.of(partition), "p=1", false);
        Assertions.assertThat(partitionWrite.targetPath()).isEqualTo(Location.of("local:///custom/nested/partition"));
        Assertions.assertThat(partitionWrite.writePath()).isEqualTo(partitionWrite.targetPath().appendPath(".hive-staging").appendPath(handle.getStagingOperationId().orElseThrow()));

        Partition ordinary = Partition.builder(partition)
                .withStorage(storage -> storage.setLocation("local:///table/p=1"))
                .build();
        Assertions.assertThat(service.getPartitionWriteInfo(handle, Optional.of(ordinary), "p=1", false).writePath())
                .isEqualTo(tableWrite.writePath().appendPath("p=1"));

        WriteInfo newPartition = service.getPartitionWriteInfo(handle, Optional.empty(), "p=2", false);
        Assertions.assertThat(newPartition.writePath()).isEqualTo(Location.of("local:///table/.hive-staging/" + handle.getStagingOperationId().orElseThrow() + "/p=2"));
        Assertions.assertThat(newPartition.writeMode()).isEqualTo(STAGE_AND_MOVE_FILES_TO_TARGET_DIRECTORY);
        assertTrinoExceptionThrownBy(() -> service.getTableWriteInfo(handle, true))
                .hasMessage("Overwrite is not supported with table-local staging");

        assertTrinoExceptionThrownBy(() -> service.getPartitionWriteInfo(handle, Optional.empty(), "p=1", true))
                .hasMessage("Overwrite is not supported with table-local staging");

        Partition otherFileSystem = Partition.builder(partition)
                .withStorage(storage -> storage.setLocation("hdfs://other/partition"))
                .build();
        Assertions.assertThat(service.getPartitionWriteInfo(handle, Optional.of(otherFileSystem), "p=1", false).writePath())
                .isEqualTo(Location.of("hdfs://other/partition/.hive-staging/" + handle.getStagingOperationId().orElseThrow()));

        LocationHandle roundTrip = jsonCodec(LocationHandle.class).fromJson(jsonCodec(LocationHandle.class).toJson(handle));
        Assertions.assertThat(service.getPartitionWriteInfo(roundTrip, Optional.of(partition), "p=1", false)).isEqualTo(partitionWrite);
    }

    @Test
    public void testTableLocalExclusions(@TempDir Path directory)
    {
        LocalFileSystemFactory factory = new LocalFileSystemFactory(directory);
        HiveLocationService disabled = new HiveLocationService(factory, new HiveConfig().setTableLocalStagingDirectoryEnabled(true).setTemporaryStagingDirectoryEnabled(false));
        Assertions.assertThat(disabled.forExistingTable(null, SESSION, table("local:///table", Map.of())).getWriteMode()).isEqualTo(DIRECT_TO_TARGET_EXISTING_DIRECTORY);
        HiveLocationService enabled = new HiveLocationService(factory, new HiveConfig().setTableLocalStagingDirectoryEnabled(true));
        Assertions.assertThat(enabled.forExistingTable(null, SESSION, table("local:///table", Map.of("transactional", "true"))).getWriteMode()).isEqualTo(DIRECT_TO_TARGET_EXISTING_DIRECTORY);
        Assertions.assertThat(enabled.forOptimize(null, SESSION, table("local:///table", Map.of())).getWriteMode()).isEqualTo(DIRECT_TO_TARGET_EXISTING_DIRECTORY);
        HiveLocationService unsupported = new HiveLocationService(new MemoryFileSystemFactory(), new HiveConfig().setTableLocalStagingDirectoryEnabled(true));
        Assertions.assertThat(unsupported.forExistingTable(null, SESSION, table("memory:///table", Map.of())).getWriteMode()).isEqualTo(DIRECT_TO_TARGET_EXISTING_DIRECTORY);
    }

    @Test
    public void testRelativeLegacyStaging(@TempDir Path directory)
    {
        HiveLocationService service = new HiveLocationService(new LocalFileSystemFactory(directory), new HiveConfig().setTemporaryStagingDirectoryPath("relative-staging"));
        LocationHandle handle = service.forExistingTable(null, SESSION, table("local:///table", Map.of()));
        Assertions.assertThat(handle.getWriteMode()).isEqualTo(STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        Assertions.assertThat(handle.getWritePath().toString()).startsWith("local:///table/relative-staging/");
        Assertions.assertThat(handle.getStagingOperationId()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHdfsEncryptionZoneExclusion(boolean encrypted)
            throws Exception
    {
        List<org.apache.hadoop.fs.Path> createdDirectories = new ArrayList<>();
        DistributedFileSystem hdfs = new DistributedFileSystem()
        {
            @Override
            public EncryptionZone getEZForPath(org.apache.hadoop.fs.Path path)
            {
                return encrypted ? new EncryptionZone(1, "/table", CipherSuite.AES_CTR_NOPADDING, CryptoProtocolVersion.ENCRYPTION_ZONES, "key") : null;
            }

            @Override
            public boolean mkdirs(org.apache.hadoop.fs.Path path, FsPermission permission)
            {
                createdDirectories.add(path);
                return true;
            }

            @Override
            public void setPermission(org.apache.hadoop.fs.Path path, FsPermission permission) {}
        };
        HdfsEnvironment environment = new HdfsEnvironment((_, _) -> new Configuration(false), new HdfsConfig(), new NoHdfsAuthentication())
        {
            @Override
            public FileSystem getFileSystem(HdfsContext context, org.apache.hadoop.fs.Path path)
            {
                return hdfs;
            }
        };
        HdfsFileSystemFactory factory = new HdfsFileSystemFactory(environment, new TrinoHdfsFileSystemStats());
        HiveLocationService service = new HiveLocationService(factory, new HiveConfig().setTableLocalStagingDirectoryEnabled(true));
        LocationHandle handle = service.forExistingTable(null, SESSION, table("hdfs://namenode/table", Map.of()));
        Assertions.assertThat(handle.getWriteMode()).isEqualTo(encrypted ? DIRECT_TO_TARGET_EXISTING_DIRECTORY : STAGE_AND_MOVE_FILES_TO_TARGET_DIRECTORY);
        if (encrypted) {
            Assertions.assertThat(createdDirectories).isEmpty();
        }
        else {
            Assertions.assertThat(handle.getWritePath()).isEqualTo(Location.of("hdfs://namenode/table/.hive-staging/" + handle.getStagingOperationId().orElseThrow()));
        }
    }

    @Test
    public void testHadoopRelativePlacement(@TempDir Path directory)
    {
        HiveLocationService service = new HiveLocationService(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig().setTableLocalStagingDirectoryEnabled(true));
        LocationHandle handle = service.forExistingTable(null, SESSION, table(directory.toUri().toString(), Map.of()));
        Assertions.assertThat(handle.getWriteMode()).isEqualTo(STAGE_AND_MOVE_FILES_TO_TARGET_DIRECTORY);
        Assertions.assertThat(Path.of(URI.create(handle.getWritePath().toString())))
                .isEqualTo(directory.resolve(".hive-staging").resolve(handle.getStagingOperationId().orElseThrow()));
    }

    private static Table table(String location, Map<String, String> parameters)
    {
        return Table.builder()
                .setDatabaseName("schema")
                .setTableName("table")
                .setTableType("EXTERNAL_TABLE")
                .setOwner(Optional.empty())
                .setDataColumns(List.of())
                .setPartitionColumns(List.of())
                .setParameters(parameters)
                .withStorage(storage -> storage.setStorageFormat(ORC.toStorageFormat()).setLocation(location))
                .build();
    }

    private static Assertion assertThat(LocationHandle locationHandle, boolean overwrite)
    {
        return new Assertion(locationHandle, overwrite);
    }

    public static class Assertion
    {
        private final WriteInfo actual;

        public Assertion(LocationHandle locationHandle, boolean overwrite)
        {
            LocationService service = new HiveLocationService(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig());
            this.actual = service.getTableWriteInfo(locationHandle, overwrite);
        }

        public void producesWriteInfo(WriteInfo expected)
        {
            Assertions.assertThat(actual.writePath()).isEqualTo(expected.writePath());
            Assertions.assertThat(actual.targetPath()).isEqualTo(expected.targetPath());
            Assertions.assertThat(actual.writeMode()).isEqualTo(expected.writeMode());
        }
    }

    private static LocationHandle locationHandle(LocationHandle.WriteMode writeMode)
    {
        return locationHandle(writeMode, "/target", "/write");
    }

    private static LocationHandle locationHandle(LocationHandle.WriteMode writeMode, String targetPath, String writePath)
    {
        return new LocationHandle(Location.of(targetPath), Location.of(writePath), writeMode);
    }

    private static WriteInfo writeInfo(String targetPath, String writePath, LocationHandle.WriteMode writeMode)
    {
        return new WriteInfo(Location.of(targetPath), Location.of(writePath), writeMode);
    }
}
