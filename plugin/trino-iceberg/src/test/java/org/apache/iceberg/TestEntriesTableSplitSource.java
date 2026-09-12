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
package org.apache.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.plugin.iceberg.system.entries.EntriesTablePageSource;
import io.trino.plugin.iceberg.system.entries.EntriesTableSplit;
import io.trino.plugin.iceberg.system.entries.EntriesTableSplitSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilterSnapshot;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.iceberg.MetadataTableType.ALL_ENTRIES;
import static org.apache.iceberg.MetadataTableType.ENTRIES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestEntriesTableSplitSource
{
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
    private static final DynamicFilterSnapshot FILTER = DynamicFilterSnapshot.EMPTY;

    @Test
    void testBatchesAndSnapshotCache()
            throws IOException
    {
        TestingFileIO fileIO = new TestingFileIO();
        ManifestFile first = manifest(fileIO, 1);
        ManifestFile second = manifest(fileIO, 3);
        Table table = table(fileIO, ImmutableList.of(
                snapshot(fileIO, 1, ImmutableList.of(first)),
                snapshot(fileIO, 2, ImmutableList.of(first)),
                snapshot(fileIO, 3, ImmutableList.of(first, second)),
                snapshot(fileIO, 4, ImmutableList.of(first, second))));
        try (EntriesTableSplitSource source = source(table, ALL_ENTRIES)) {
            assertThat(source.getNextBatch(1, FILTER).join()).extracting(TestEntriesTableSplitSource::path).containsExactly(first.path());
            assertThat(fileIO.reads).isEqualTo(1);
            assertThat(source.getNextBatch(1, FILTER).join()).extracting(TestEntriesTableSplitSource::path).containsExactly(second.path());
            assertThat(fileIO.reads).isEqualTo(3);
            assertThat(source.getNextBatch(1, FILTER).join()).isEmpty();
            assertThat(source.isFinished()).isTrue();
        }
        assertThat(fileIO.closes).isEqualTo(1);
        for (Snapshot snapshot : table.snapshots()) {
            snapshot.allManifests(fileIO);
        }
        assertThat(fileIO.reads).isEqualTo(8);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEarlyClose(boolean start)
            throws IOException
    {
        TestingFileIO fileIO = new TestingFileIO();
        ManifestFile manifest = manifest(fileIO, 1);
        Table table = table(fileIO, ImmutableList.of(
                snapshot(fileIO, 1, ImmutableList.of(manifest)),
                snapshot(fileIO, 2, ImmutableList.of(manifest))));
        EntriesTableSplitSource source = source(table, ALL_ENTRIES);
        if (start) {
            assertThat(source.getNextBatch(1, FILTER).join()).hasSize(1);
        }
        source.close();
        source.close();
        assertThat(source.isFinished()).isTrue();
        assertThat(source.getNextBatch(1, FILTER).join()).isEmpty();
        assertThat(fileIO.reads).isEqualTo(start ? 1 : 0);
        assertThat(fileIO.closes).isEqualTo(1);
    }

    @Test
    void testReadFailureClosesSource()
            throws IOException
    {
        TestingFileIO fileIO = new TestingFileIO();
        Table table = table(fileIO, ImmutableList.of(snapshot(fileIO, 1, ImmutableList.of(manifest(fileIO, 1)))));
        EntriesTableSplitSource source = source(table, ALL_ENTRIES);
        fileIO.fail = true;
        assertThatThrownBy(() -> source.getNextBatch(1, FILTER))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("read failed")
                .hasSuppressedException(new IllegalStateException("close failed"));
        source.close();
        assertThat(source.isFinished()).isTrue();
        assertThat(source.getNextBatch(1, FILTER).join()).isEmpty();
        assertThat(fileIO.closes).isEqualTo(1);
    }

    @Test
    void testLegacySnapshot()
            throws IOException
    {
        TestingFileIO fileIO = new TestingFileIO();
        ManifestFile manifest = manifest(fileIO, 1);
        Snapshot snapshot = new BaseSnapshot(0, 1, null, 1, "append", ImmutableMap.of(), SCHEMA.schemaId(), new String[] {manifest.path()});
        try (EntriesTableSplitSource source = source(table(fileIO, ImmutableList.of(snapshot)), ALL_ENTRIES)) {
            assertThat(source.getNextBatch(10, FILTER).join()).extracting(TestEntriesTableSplitSource::path).containsExactly(manifest.path());
            assertThat(source.isFinished()).isTrue();
        }
    }

    @Test
    void testPageSourceEarlyCloseAndInitializationFailure()
            throws IOException
    {
        TestingFileIO fileIO = new TestingFileIO();
        Table table = table(fileIO, ImmutableList.of(snapshot(fileIO, 1, ImmutableList.of(manifest(fileIO, 1)))));
        try (EntriesTableSplitSource source = source(table, ENTRIES)) {
            EntriesTableSplit split = (EntriesTableSplit) source.getNextBatch(1, FILTER).join().getFirst();
            EntriesTablePageSource pageSource = new EntriesTablePageSource(TESTING_TYPE_MANAGER, fileIO, ImmutableList.of(), split);
            pageSource.close();
            pageSource.close();
            assertThat(pageSource.isFinished()).isTrue();
            assertThat(pageSource.getNextSourcePage()).isNull();
            assertThat(fileIO.closes).isEqualTo(1);

            assertThatThrownBy(() -> new EntriesTablePageSource(TESTING_TYPE_MANAGER, fileIO, ImmutableList.of("invalid"), split))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Unexpected column: invalid");
            assertThat(fileIO.closes).isEqualTo(2);
        }
        assertThat(fileIO.closes).isEqualTo(3);
    }

    private static String path(ConnectorSplit split)
    {
        return ((EntriesTableSplit) split).manifestFile().path();
    }

    private static EntriesTableSplitSource source(Table table, MetadataTableType type)
    {
        return new EntriesTableSplitSource(
                table,
                type,
                SchemaParser.toJson(table.schema()),
                SchemaParser.toJson(MetadataTableUtils.createMetadataTableInstance(table, type).schema()),
                table.specs().entrySet().stream().collect(toImmutableMap(
                        entry -> entry.getKey(),
                        entry -> PartitionSpecParser.toJson(entry.getValue()))));
    }

    private static Table table(FileIO fileIO, List<Snapshot> snapshots)
    {
        TableMetadata.Builder metadata = TableMetadata.buildFrom(TableMetadata.newTableMetadata(SCHEMA, SPEC, "memory:///table", ImmutableMap.of("format-version", "2")));
        for (Snapshot snapshot : snapshots) {
            metadata.setBranchSnapshot(snapshot, "main");
        }
        return new BaseTable(new StaticTableOperations(metadata.build(), fileIO), "test");
    }

    private static ManifestFile manifest(FileIO fileIO, long id)
            throws IOException
    {
        ManifestFile manifest;
        try (ManifestWriter<DataFile> writer = ManifestFiles.write(1, SPEC, fileIO.newOutputFile("memory:///manifest-" + id + ".avro"), id)) {
            writer.add(dataFile(id));
            writer.close();
            manifest = writer.toManifestFile();
        }
        String location = "memory:///committed-list-" + id + ".avro";
        try (ManifestListWriter writer = ManifestLists.write(2, fileIO.newOutputFile(location), PlaintextEncryptionManager.instance(), id, null, id, null)) {
            writer.add(manifest);
        }
        return ManifestLists.read(fileIO.newInputFile(location)).getFirst();
    }

    private static DataFile dataFile(long id)
    {
        return DataFiles.builder(SPEC).withPath("memory:///data-" + id + ".parquet").withFileSizeInBytes(1).withRecordCount(1).build();
    }

    private static Snapshot snapshot(FileIO fileIO, long id, List<ManifestFile> manifests)
            throws IOException
    {
        String location = "memory:///list-" + id + ".avro";
        try (ManifestListWriter writer = ManifestLists.write(2, fileIO.newOutputFile(location), PlaintextEncryptionManager.instance(), id, null, id, null)) {
            writer.addAll(manifests);
        }
        return new BaseSnapshot(id, id, null, id, "append", ImmutableMap.of(), SCHEMA.schemaId(), location, null, null, null);
    }

    private static class TestingFileIO
            extends ForwardingFileIo
    {
        private int reads;
        private int closes;
        private boolean fail;

        private TestingFileIO()
        {
            super(new MemoryFileSystem(), false);
        }

        @Override
        public InputFile newInputFile(ManifestListFile file)
        {
            reads++;
            if (fail) {
                throw new IllegalStateException("read failed");
            }
            return super.newInputFile(file);
        }

        @Override
        public void close()
        {
            closes++;
            if (fail) {
                throw new IllegalStateException("close failed");
            }
        }
    }
}
