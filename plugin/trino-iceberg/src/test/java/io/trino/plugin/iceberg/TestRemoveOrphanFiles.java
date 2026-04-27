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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.spi.security.ConnectorIdentity.ofUser;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.FileFormat.PUFFIN;
import static org.apache.iceberg.Files.localInput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestRemoveOrphanFiles
{
    private static final HadoopTables HADOOP_TABLES = new HadoopTables(new Configuration(false));
    private static final Schema TABLE_SCHEMA = new Schema(
            Types.NestedField.optional(1, "col1", Types.IntegerType.get()),
            Types.NestedField.optional(2, "col2", Types.StringType.get()));
    private static final SchemaTableName SCHEMA_TABLE_NAME = new SchemaTableName("test_schema", "test_table");

    @TempDir
    private Path tempDir;

    private TrinoFileSystem fileSystem;
    private ExecutorService executor;

    @BeforeEach
    void setUp()
    {
        fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(ofUser("test"));
        executor = newDirectExecutorService();
    }

    @Test
    void testOrphanDataFileIsRemoved()
            throws Exception
    {
        Table table = createTable("test_orphan_removal");
        addDataFile(table, "data/valid-file.orc");

        String orphanLocation = table.location() + "/data/orphan-file.orc";
        createFileOnDisk(orphanLocation);

        setAllFileTimestamps(table, Instant.EPOCH);

        Map<String, Long> result = callRemoveOrphanFiles(table, Instant.now());

        assertThat(result.get("deleted_files_count")).isEqualTo(1L);
        assertThat(result.get("processed_manifests_count")).isGreaterThanOrEqualTo(1L);
        assertThat(result.get("active_files_count")).isGreaterThan(0L);
        assertThat(fileExists(orphanLocation)).isFalse();

        dropTable(table);
    }

    @Test
    void testNoOrphanFiles()
            throws Exception
    {
        Table table = createTable("test_no_orphans");
        addDataFile(table, "data/file1.orc");
        addDataFile(table, "data/file2.orc");

        setAllFileTimestamps(table, Instant.EPOCH);

        long fileCountBefore = countFiles(table);
        Map<String, Long> result = callRemoveOrphanFiles(table, Instant.now());

        assertThat(result.get("deleted_files_count")).isEqualTo(0L);
        assertThat(result.get("scanned_files_count")).isGreaterThan(0L);
        assertThat(countFiles(table)).isEqualTo(fileCountBefore);

        dropTable(table);
    }

    @Test
    void testRetentionThresholdPreventsRemoval()
            throws Exception
    {
        Table table = createTable("test_retention");
        addDataFile(table, "data/valid-file.orc");

        String orphanLocation = table.location() + "/data/orphan-file.orc";
        createFileOnDisk(orphanLocation);
        // Orphan file has a recent mtime (just created), so using an old expiration should keep it

        Map<String, Long> result = callRemoveOrphanFiles(table, Instant.EPOCH);

        assertThat(result.get("deleted_files_count")).isEqualTo(0L);
        assertThat(fileExists(orphanLocation)).isTrue();

        dropTable(table);
    }

    @Test
    void testMixedOldAndNewOrphanFiles()
            throws Exception
    {
        Table table = createTable("test_mixed_orphans");
        addDataFile(table, "data/valid-file.orc");

        String oldOrphan = table.location() + "/data/old-orphan.orc";
        String newOrphan = table.location() + "/data/new-orphan.orc";
        createFileOnDisk(oldOrphan);
        createFileOnDisk(newOrphan);

        // Set the old orphan to the far past
        setFileTimestamp(oldOrphan, Instant.EPOCH);
        // Set all table metadata and valid files to the far past too
        setAllFileTimestamps(table, Instant.EPOCH);
        // But reset the new orphan to now so it survives
        setFileTimestamp(newOrphan, Instant.now());

        Instant expiration = Instant.now().minusSeconds(3600);
        Map<String, Long> result = callRemoveOrphanFiles(table, expiration);

        assertThat(result.get("deleted_files_count")).isEqualTo(1L);
        assertThat(fileExists(oldOrphan)).isFalse();
        assertThat(fileExists(newOrphan)).isTrue();

        dropTable(table);
    }

    @Test
    void testOrphanMetadataFileIsRemoved()
            throws Exception
    {
        Table table = createTable("test_orphan_metadata");
        addDataFile(table, "data/valid-file.orc");

        String orphanMetadata = table.location() + "/metadata/orphan-metadata.json";
        createFileOnDisk(orphanMetadata);

        setAllFileTimestamps(table, Instant.EPOCH);

        Map<String, Long> result = callRemoveOrphanFiles(table, Instant.now());

        assertThat(result.get("deleted_files_count")).isEqualTo(1L);
        assertThat(fileExists(orphanMetadata)).isFalse();

        dropTable(table);
    }

    @Test
    void testMissingManifestThrows()
            throws Exception
    {
        Table table = createTable("test_missing_manifest");
        addDataFile(table, "data/valid-file.orc");

        List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
        assertThat(manifests).isNotEmpty();
        String manifestPath = manifests.get(0).path();

        fileSystem.deleteFile(Location.of(manifestPath));

        assertThatThrownBy(() -> callRemoveOrphanFiles(table, Instant.now()))
                .hasMessageContaining("Manifest file does not exist: " + manifestPath);

        dropTable(table);
    }

    @Test
    void testMultipleSnapshotsRetainAllFiles()
            throws Exception
    {
        Table table = createTable("test_multiple_snapshots");
        addDataFile(table, "data/file-snapshot1.orc");
        addDataFile(table, "data/file-snapshot2.orc");

        String orphanLocation = table.location() + "/data/orphan.orc";
        createFileOnDisk(orphanLocation);

        setAllFileTimestamps(table, Instant.EPOCH);

        Map<String, Long> result = callRemoveOrphanFiles(table, Instant.now());

        assertThat(result.get("deleted_files_count")).isEqualTo(1L);
        assertThat(result.get("processed_manifests_count")).isGreaterThanOrEqualTo(1L);
        assertThat(fileExists(orphanLocation)).isFalse();
        // Verify both data files are still present
        assertThat(fileExists(table.location() + "/data/file-snapshot1.orc")).isTrue();
        assertThat(fileExists(table.location() + "/data/file-snapshot2.orc")).isTrue();

        dropTable(table);
    }

    @Test
    void testTableWithNoSnapshots()
            throws Exception
    {
        Table table = createTable("test_no_snapshots");

        String orphanLocation = table.location() + "/data/orphan.orc";
        createFileOnDisk(orphanLocation);
        setFileTimestamp(orphanLocation, Instant.EPOCH);

        Map<String, Long> result = callRemoveOrphanFiles(table, Instant.now());

        assertThat(result.get("processed_manifests_count")).isEqualTo(0L);
        assertThat(result.get("deleted_files_count")).isEqualTo(1L);
        assertThat(fileExists(orphanLocation)).isFalse();

        dropTable(table);
    }

    @Test
    void testPositionDeleteFileRetained()
            throws Exception
    {
        // Unpartitioned table (v2 only — v3 requires DVs for position deletes)
        Table unpartitioned = createTable("test_pos_delete_unpart", 2, PartitionSpec.unpartitioned());
        String dataFilePath = addDataFile(unpartitioned, "data/data-file.orc");
        String posDeletePath = addPositionDeleteFile(unpartitioned, dataFilePath, PartitionSpec.unpartitioned(), null);
        String orphanLocation = unpartitioned.location() + "/data/orphan.orc";
        createFileOnDisk(orphanLocation);
        setAllFileTimestamps(unpartitioned, Instant.EPOCH);

        Map<String, Long> result = callRemoveOrphanFiles(unpartitioned, Instant.now());

        assertThat(result.get("deleted_files_count")).isEqualTo(1L);
        assertThat(fileExists(posDeletePath)).isTrue();
        assertThat(fileExists(dataFilePath)).isTrue();
        assertThat(fileExists(orphanLocation)).isFalse();
        dropTable(unpartitioned);

        // Partitioned table
        PartitionSpec partSpec = PartitionSpec.builderFor(TABLE_SCHEMA).identity("col2").build();
        Table partitioned = createTable("test_pos_delete_part", 2, partSpec);
        PartitionData partitionData = new PartitionData(new Object[] {"partval"});
        String partDataPath = addDataFile(partitioned, "data/col2=partval/data-file.orc", partitionData);
        String partPosDeletePath = addPositionDeleteFile(partitioned, partDataPath, partSpec, partitionData);
        String partOrphanLocation = partitioned.location() + "/data/col2=partval/orphan.orc";
        createFileOnDisk(partOrphanLocation);
        setAllFileTimestamps(partitioned, Instant.EPOCH);

        Map<String, Long> partResult = callRemoveOrphanFiles(partitioned, Instant.now());

        assertThat(partResult.get("deleted_files_count")).isEqualTo(1L);
        assertThat(fileExists(partPosDeletePath)).isTrue();
        assertThat(fileExists(partDataPath)).isTrue();
        assertThat(fileExists(partOrphanLocation)).isFalse();
        dropTable(partitioned);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testEqualityDeleteFileRetained(int formatVersion)
            throws Exception
    {
        // Unpartitioned table
        Table unpartitioned = createTable("test_eq_delete_unpart", formatVersion, PartitionSpec.unpartitioned());
        String dataFilePath = addDataFile(unpartitioned, "data/data-file.orc");
        String eqDeletePath = addEqualityDeleteFile(unpartitioned, PartitionSpec.unpartitioned(), null);
        String orphanLocation = unpartitioned.location() + "/data/orphan.orc";
        createFileOnDisk(orphanLocation);
        setAllFileTimestamps(unpartitioned, Instant.EPOCH);

        Map<String, Long> result = callRemoveOrphanFiles(unpartitioned, Instant.now());

        assertThat(result.get("deleted_files_count")).isEqualTo(1L);
        assertThat(fileExists(eqDeletePath)).isTrue();
        assertThat(fileExists(dataFilePath)).isTrue();
        assertThat(fileExists(orphanLocation)).isFalse();
        dropTable(unpartitioned);

        // Partitioned table
        PartitionSpec partSpec = PartitionSpec.builderFor(TABLE_SCHEMA).identity("col2").build();
        Table partitioned = createTable("test_eq_delete_part", formatVersion, partSpec);
        PartitionData partitionData = new PartitionData(new Object[] {"partval"});
        String partDataPath = addDataFile(partitioned, "data/col2=partval/data-file.orc", partitionData);
        String partEqDeletePath = addEqualityDeleteFile(partitioned, partSpec, partitionData);
        String partOrphanLocation = partitioned.location() + "/data/col2=partval/orphan.orc";
        createFileOnDisk(partOrphanLocation);
        setAllFileTimestamps(partitioned, Instant.EPOCH);

        Map<String, Long> partResult = callRemoveOrphanFiles(partitioned, Instant.now());

        assertThat(partResult.get("deleted_files_count")).isEqualTo(1L);
        assertThat(fileExists(partEqDeletePath)).isTrue();
        assertThat(fileExists(partDataPath)).isTrue();
        assertThat(fileExists(partOrphanLocation)).isFalse();
        dropTable(partitioned);
    }

    @Test
    void testDeletionVectorRetained()
            throws Exception
    {
        // Unpartitioned table
        Table unpartitioned = createTable("test_dv_unpart", 3, PartitionSpec.unpartitioned());
        String dataFilePath = addDataFile(unpartitioned, "data/data-file.orc");
        String dvPath = addDeletionVector(unpartitioned, dataFilePath, null);
        String orphanLocation = unpartitioned.location() + "/data/orphan.orc";
        createFileOnDisk(orphanLocation);
        setAllFileTimestamps(unpartitioned, Instant.EPOCH);

        Map<String, Long> result = callRemoveOrphanFiles(unpartitioned, Instant.now());

        assertThat(result.get("deleted_files_count")).isEqualTo(1L);
        assertThat(fileExists(dvPath)).isTrue();
        assertThat(fileExists(dataFilePath)).isTrue();
        assertThat(fileExists(orphanLocation)).isFalse();
        dropTable(unpartitioned);

        // Partitioned table
        PartitionSpec partSpec = PartitionSpec.builderFor(TABLE_SCHEMA).identity("col2").build();
        Table partitioned = createTable("test_dv_part", 3, partSpec);
        PartitionData partitionData = new PartitionData(new Object[] {"partval"});
        String partDataPath = addDataFile(partitioned, "data/col2=partval/data-file.orc", partitionData);
        String partDvPath = addDeletionVector(partitioned, partDataPath, partitionData);
        String partOrphanLocation = partitioned.location() + "/data/col2=partval/orphan.orc";
        createFileOnDisk(partOrphanLocation);
        setAllFileTimestamps(partitioned, Instant.EPOCH);

        Map<String, Long> partResult = callRemoveOrphanFiles(partitioned, Instant.now());

        assertThat(partResult.get("deleted_files_count")).isEqualTo(1L);
        assertThat(fileExists(partDvPath)).isTrue();
        assertThat(fileExists(partDataPath)).isTrue();
        assertThat(fileExists(partOrphanLocation)).isFalse();
        dropTable(partitioned);
    }

    @Test
    void testVersionHintFileRetained()
            throws Exception
    {
        Table table = createTable("test_version_hint");
        addDataFile(table, "data/valid-file.orc");

        // HadoopTables already creates version-hint.text, but ensure it exists
        String versionHint = table.location() + "/metadata/version-hint.text";
        if (!fileExists(versionHint)) {
            createFileOnDisk(versionHint);
        }

        setAllFileTimestamps(table, Instant.EPOCH);

        Map<String, Long> result = callRemoveOrphanFiles(table, Instant.now());

        assertThat(result.get("deleted_files_count")).isEqualTo(0L);
        assertThat(fileExists(versionHint)).isTrue();

        dropTable(table);
    }

    private Table createTable(String name)
    {
        return createTable(name, 2, PartitionSpec.unpartitioned());
    }

    private Table createTable(String name, int formatVersion, PartitionSpec partitionSpec)
    {
        String location = tempDir.resolve(name + "_" + randomNameSuffix()).toUri().toString();
        return HADOOP_TABLES.create(
                TABLE_SCHEMA,
                partitionSpec,
                SortOrder.unsorted(),
                ImmutableMap.of(
                        "format-version", String.valueOf(formatVersion),
                        "write.format.default", "ORC"),
                location);
    }

    private String addDataFile(Table table, String relativePath)
            throws Exception
    {
        return addDataFile(table, relativePath, null);
    }

    private String addDataFile(Table table, String relativePath, PartitionData partitionData)
            throws Exception
    {
        String fileLocation = table.location() + "/" + relativePath;
        Path localPath = Path.of(new URI(fileLocation));
        Files.createDirectories(localPath.getParent());
        Files.createFile(localPath);

        DataFiles.Builder builder = DataFiles.builder(table.spec())
                .withInputFile(localInput(localPath.toFile()))
                .withPath(fileLocation)
                .withFormat(FileFormat.ORC)
                .withRecordCount(1)
                .withFileSizeInBytes(0);
        if (partitionData != null) {
            builder.withPartition(partitionData);
        }
        table.newFastAppend()
                .appendFile(builder.build())
                .commit();
        return fileLocation;
    }

    private String addPositionDeleteFile(Table table, String dataFilePath, PartitionSpec spec, PartitionData partitionData)
            throws IOException
    {
        String deletePath = table.location() + "/data/pos-delete-" + UUID.randomUUID() + ".parquet";
        Parquet.DeleteWriteBuilder writerBuilder = Parquet.writeDeletes(table.io().newOutputFile(deletePath))
                .createWriterFunc(GenericParquetWriter::create)
                .forTable(table)
                .overwrite()
                .rowSchema(null)
                .withSpec(spec);
        if (partitionData != null) {
            writerBuilder.withPartition(partitionData);
        }
        PositionDeleteWriter<Void> writer = writerBuilder.buildPositionWriter();

        PositionDelete<Void> positionDelete = PositionDelete.create();
        positionDelete.set(dataFilePath, 0);
        try (Closeable ignored = writer) {
            writer.write(positionDelete);
        }

        table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
        return deletePath;
    }

    private String addEqualityDeleteFile(Table table, PartitionSpec spec, PartitionData partitionData)
            throws IOException
    {
        String deletePath = table.location() + "/data/eq-delete-" + UUID.randomUUID() + ".parquet";
        Schema deleteRowSchema = table.schema().select("col1");
        List<Integer> equalityFieldIds = deleteRowSchema.columns().stream()
                .map(Types.NestedField::fieldId)
                .collect(toImmutableList());

        Parquet.DeleteWriteBuilder writerBuilder = Parquet.writeDeletes(table.io().newOutputFile(deletePath))
                .forTable(table)
                .rowSchema(deleteRowSchema)
                .createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(equalityFieldIds)
                .overwrite()
                .withSpec(spec);
        if (partitionData != null) {
            writerBuilder.withPartition(partitionData);
        }
        EqualityDeleteWriter<Record> writer = writerBuilder.buildEqualityWriter();

        Record dataDelete = GenericRecord.create(deleteRowSchema);
        try (Closeable ignored = writer) {
            writer.write(dataDelete.copy(ImmutableMap.of("col1", 1)));
        }

        table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
        return deletePath;
    }

    private String addDeletionVector(Table table, String dataFilePath, PartitionData partitionData)
            throws IOException
    {
        try (DVFileWriter dvWriter = new BaseDVFileWriter(
                OutputFileFactory.builderFor(table, 1, 1).format(PUFFIN).build(),
                _ -> PositionDeleteIndex.empty())) {
            dvWriter.delete(dataFilePath, 0, table.spec(), partitionData);
            dvWriter.close();

            DeleteFile deleteFile = getOnlyElement(dvWriter.result().deleteFiles());
            table.newRowDelta().addDeletes(deleteFile).commit();
            return deleteFile.location();
        }
    }

    private void createFileOnDisk(String location)
            throws Exception
    {
        Path localPath = Path.of(new URI(location));
        Files.createDirectories(localPath.getParent());
        Files.createFile(localPath);
    }

    private void setAllFileTimestamps(Table table, Instant time)
            throws Exception
    {
        FileIterator iterator = fileSystem.listFiles(Location.of(table.location()));
        ImmutableList.Builder<String> locationsBuilder = ImmutableList.builder();
        while (iterator.hasNext()) {
            locationsBuilder.add(iterator.next().location().toString());
        }
        for (String location : locationsBuilder.build()) {
            setFileTimestamp(location, time);
        }
    }

    private void setFileTimestamp(String location, Instant time)
            throws Exception
    {
        Path localPath = Path.of(new URI(location));
        Files.setLastModifiedTime(localPath, FileTime.from(time));
    }

    private Map<String, Long> callRemoveOrphanFiles(Table table, Instant expiration)
    {
        return IcebergMetadata.removeOrphanFiles(table, fileSystem, executor, executor, SCHEMA_TABLE_NAME, expiration);
    }

    private static void dropTable(Table table)
    {
        HADOOP_TABLES.dropTable(table.location());
    }

    private boolean fileExists(String location)
            throws IOException
    {
        return fileSystem.newInputFile(Location.of(location)).exists();
    }

    private long countFiles(Table table)
            throws IOException
    {
        long count = 0;
        FileIterator iterator = fileSystem.listFiles(Location.of(table.location()));
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }
}
