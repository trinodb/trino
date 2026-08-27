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
package io.trino.plugin.paimon;

import io.airlift.slice.Slices;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeDescriptor;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FullTextSearchTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.VectorSearchTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.AbstractList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_WRITER_CLOSE_ERROR;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_WRITER_DATA_ERROR;
import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_SNAPSHOT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingPageSinkId.TESTING_PAGE_SINK_ID;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonPageSinkProviderTest
{
    private static final String UNSUPPORTED_FORMAT_WRITE_MESSAGE = "Trino Paimon file format does not support Paimon BLOB, VARIANT, VECTOR, or MULTISET writes";
    private static final String ORC_TIME_WRITE_MESSAGE = "Trino Paimon ORC writer does not support Paimon TIME columns; use Parquet or Paimon's native writer for ORC TIME data";
    private static final RowType ID_ROW_TYPE = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));

    @Test
    public void testSupportedWriteBucketModes()
    {
        assertThatCode(() -> PaimonPageSinkProvider.validateWriteBucketMode(fileStoreTable(BucketMode.HASH_FIXED)))
                .doesNotThrowAnyException();
        assertThatCode(() -> PaimonPageSinkProvider.validateWriteBucketMode(fileStoreTable(BucketMode.HASH_DYNAMIC)))
                .doesNotThrowAnyException();
        assertThatCode(() -> PaimonPageSinkProvider.validateWriteBucketMode(fileStoreTable(BucketMode.KEY_DYNAMIC)))
                .doesNotThrowAnyException();
        assertThatCode(() -> PaimonPageSinkProvider.validateWriteBucketMode(fileStoreTable(BucketMode.BUCKET_UNAWARE)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testKeyDynamicMemoryUsageReservesIndexState()
    {
        FileStoreTable table = fileStoreTable(BucketMode.KEY_DYNAMIC);
        long expected = table.coreOptions().lookupCacheMaxMemory().getBytes() + table.coreOptions().writeBufferSize();

        assertThat(PaimonPageSinkProvider.keyDynamicMemoryUsage(table)).isEqualTo(expected);
    }

    @Test
    public void testUnsupportedWriteBucketModesFailFast()
    {
        assertUnsupportedWriteBucketMode(BucketMode.POSTPONE_MODE);
    }

    @Test
    public void testMergeSupportsFixedAndDynamicBucketModes()
    {
        assertThatCode(() -> PaimonPageSinkProvider.validateMergeBucketMode(fileStoreTable(BucketMode.HASH_FIXED)))
                .doesNotThrowAnyException();
        assertThatCode(() -> PaimonPageSinkProvider.validateMergeBucketMode(fileStoreTable(BucketMode.HASH_DYNAMIC)))
                .doesNotThrowAnyException();
        assertThatCode(() -> PaimonPageSinkProvider.validateMergeBucketMode(fileStoreTable(BucketMode.KEY_DYNAMIC)))
                .doesNotThrowAnyException();

        assertUnsupportedMergeBucketMode(BucketMode.BUCKET_UNAWARE);
        assertUnsupportedMergeBucketMode(BucketMode.POSTPONE_MODE);
    }

    @Test
    public void testNonFileStoreTableFailsFast()
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteBucketMode(table()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining("Paimon writes requires FileStoreTable, but got:");
                });

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeBucketMode(table()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining("Paimon merge writes requires FileStoreTable, but got:");
                });
    }

    @Test
    public void testSearchWrapperTablesFailFast()
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteBucketMode(VectorSearchTable.create(
                innerTable(),
                new VectorSearch(new float[] {1.0f}, 1, "embedding"))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon vector search tables are not supported by the Trino connector");
                });

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeBucketMode(FullTextSearchTable.create(
                innerTable(),
                new FullTextSearch("content", "paimon", 1))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon full-text search tables are not supported by the Trino connector");
                });
    }

    @Test
    public void testPageSinkUsesLatestFileStoreTableSchema()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, copiedWithLatestSchema);

        assertThat(PaimonPageSinkProvider.latestFileStoreTable(table, "writes"))
                .isSameAs(table);
        assertThat(copiedWithLatestSchema).isTrue();
    }

    @Test
    public void testWriteColumnsAreRequired()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> PaimonPageSinkProvider.getWriteColumns(handle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page sink requires explicit write columns");
    }

    @Test
    public void testGetWriteColumnsRejectsNullTableHandle()
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.getWriteColumns(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");
    }

    @Test
    public void testPageSinkProviderRejectsNullSessionBeforeCatalogInitialization()
    {
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(failingInitMetadataFactory());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));

        assertThatThrownBy(() -> provider.createPageSink(null, null, (ConnectorOutputTableHandle) tableHandle, Optional.empty(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> provider.createPageSink(null, null, (ConnectorInsertTableHandle) tableHandle, Optional.empty(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> provider.createMergeSink(null, null, new PaimonMergeTableHandle(tableHandle), Optional.empty(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
    }

    @Test
    public void testPageSinkProviderRejectsMissingWriteColumnsBeforeCatalogInitialization()
    {
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(failingInitMetadataFactory());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> provider.createPageSink(null, SESSION, (ConnectorOutputTableHandle) tableHandle, Optional.empty(), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page sink requires explicit write columns");
        assertThatThrownBy(() -> provider.createPageSink(null, SESSION, (ConnectorInsertTableHandle) tableHandle, Optional.empty(), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page sink requires explicit write columns");
        assertThatThrownBy(() -> provider.createMergeSink(null, SESSION, new PaimonMergeTableHandle(tableHandle), Optional.empty(), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page sink requires explicit write columns");
    }

    @Test
    public void testCreatePageSinkIgnoresSessionScanSnapshotAndHandleStartupSelections()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions = new AtomicReference<>();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyFileStoreTable(copiedWithLatestSchema, copyWithoutTimeTravelOptions)));
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(
                        "custom.option", "value",
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2",
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta",
                        CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true"))
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_SNAPSHOT, 9L))
                .build();

        ConnectorPageSink pageSink = provider.createPageSink(
                null,
                session,
                (ConnectorInsertTableHandle) tableHandle,
                Optional.empty(),
                null);

        assertThat(pageSink).isNotNull();
        assertThat(copyWithoutTimeTravelOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
        assertThat(copiedWithLatestSchema).isTrue();
    }

    @Test
    public void testInsertOverwriteAppliesToInsertPageSinkOnly()
    {
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyFileStoreTable(new AtomicBoolean(), new AtomicReference<>(), overwriteEnabled)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        ConnectorPageSink pageSink = provider.createPageSink(
                null,
                overwriteSession,
                (ConnectorInsertTableHandle) tableHandle,
                Optional.empty(),
                TESTING_PAGE_SINK_ID);

        assertThat(pageSink).isNotNull();
        assertThat(overwriteEnabled).isTrue();
    }

    @Test
    public void testPageSinkProviderSharesWriteBufferMemoryPoolWithSink()
    {
        AtomicReference<MemoryPoolFactory> writeBufferPool = new AtomicReference<>();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyFileStoreTable(
                        new AtomicBoolean(),
                        new AtomicReference<>(),
                        new AtomicBoolean(),
                        writeBufferPool)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));

        ConnectorPageSink pageSink = provider.createPageSink(
                null,
                SESSION,
                (ConnectorOutputTableHandle) tableHandle,
                Optional.empty(),
                null);

        assertThat(writeBufferPool.get()).isNotNull();
        writeBufferPool.get().addOwners(List.of(memoryOwner(1234)));
        assertThat(pageSink.getMemoryUsage()).isEqualTo(1234);
    }

    @Test
    public void testPageSinkProviderSharesIoManagerWithWriteAndSink()
    {
        AtomicReference<IOManager> writeIoManager = new AtomicReference<>();
        TestingIoManager ioManager = new TestingIoManager();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyFileStoreTable(
                        new AtomicBoolean(),
                        new AtomicReference<>(),
                        new AtomicBoolean(),
                        new AtomicReference<>(),
                        writeIoManager)), () -> ioManager);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));

        ConnectorPageSink pageSink = provider.createPageSink(
                null,
                SESSION,
                (ConnectorOutputTableHandle) tableHandle,
                Optional.empty(),
                null);

        assertThat(writeIoManager.get()).isSameAs(ioManager);
        assertThat(ioManager.isClosed()).isFalse();

        pageSink.finish().join();

        assertThat(ioManager.isClosed()).isTrue();
    }

    @Test
    public void testCreateIoManagerRejectsEmptySpillPath()
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.createIoManager(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("write.spill-path must contain at least one path");
        assertThatThrownBy(() -> PaimonPageSinkProvider.createIoManager("/tmp,,/var/tmp"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("write.spill-path must not contain empty path entries");
        assertThatThrownBy(() -> PaimonPageSinkProvider.createIoManager("/tmp,"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("write.spill-path must not contain empty path entries");
    }

    @Test
    public void testDynamicBucketInsertOverwriteUsesOverwriteAssigner()
    {
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyFileStoreTable(
                        new AtomicBoolean(),
                        new AtomicReference<>(),
                        overwriteEnabled,
                        List.of(),
                        Map.of(CoreOptions.BUCKET.key(), "-1"),
                        List.of("id"),
                        BucketMode.HASH_DYNAMIC)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        ConnectorPageSink pageSink = provider.createPageSink(
                null,
                overwriteSession,
                (ConnectorInsertTableHandle) tableHandle,
                Optional.empty(),
                TESTING_PAGE_SINK_ID);

        assertThat(pageSink).isNotNull();
        assertThat(overwriteEnabled).isTrue();
    }

    @Test
    public void testInsertOverwriteDoesNotApplyToMergePageSink()
    {
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyFileStoreTable(new AtomicBoolean(), new AtomicReference<>(), overwriteEnabled)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        ConnectorMergeSink pageSink = provider.createMergeSink(null, overwriteSession, new PaimonMergeTableHandle(tableHandle), Optional.empty(), null);

        assertThat(pageSink).isNotNull();
        assertThat(overwriteEnabled).isFalse();
    }

    @Test
    public void testDynamicBucketMergePageSinkCreatesDynamicBucketWriter()
    {
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyFileStoreTable(
                        new AtomicBoolean(),
                        new AtomicReference<>(),
                        overwriteEnabled,
                        List.of(),
                        Map.of(
                                CoreOptions.BUCKET.key(), "-1",
                                CoreOptions.DYNAMIC_BUCKET_ASSIGNER_PARALLELISM.key(), "4"),
                        List.of("id"),
                        BucketMode.HASH_DYNAMIC)), TestingIoManager::new, () -> 4, new PaimonConnectorStats());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())))
                .withDynamicBucketAssignerParallelism(OptionalInt.of(4));

        ConnectorMergeSink pageSink = provider.createMergeSink(
                null,
                SESSION,
                new PaimonMergeTableHandle(tableHandle),
                Optional.empty(),
                pageSinkId(2));

        assertThat(pageSink).isInstanceOf(PaimonMergeSink.class);
        assertThat(overwriteEnabled).isFalse();
    }

    @Test
    public void testMergePageSinkRequiresPrimaryKeys()
    {
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyFileStoreTable(
                        new AtomicBoolean(),
                        new AtomicReference<>(),
                        new AtomicBoolean(),
                        List.of(),
                        Map.of(),
                        List.of())));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));

        assertThatThrownBy(() -> provider.createMergeSink(null, SESSION, new PaimonMergeTableHandle(tableHandle), Optional.empty(), null))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon merge writes requires primary keys");
                });
    }

    @Test
    public void testMergePageSinkUsesMetadataDeleteFallbackSinkWithoutCatalogInitialization()
    {
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(failingInitMetadataFactory());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorMergeSink sink = provider.createMergeSink(
                null,
                SESSION,
                PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                Optional.empty(),
                null);

        assertThat(sink).isInstanceOf(PaimonMetadataDeleteMergeSink.class);
    }

    @Test
    public void testInsertOverwriteRejectsPartitionedTableWithoutDynamicPartitionOverwrite()
    {
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writeReadyPartitionedFileStoreTable(
                        new AtomicBoolean(),
                        new AtomicReference<>(),
                        overwriteEnabled,
                        false)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("id", DataTypes.INT())));
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        assertThatThrownBy(() -> provider.createPageSink(null, overwriteSession, (ConnectorInsertTableHandle) tableHandle, Optional.empty(), null))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage(
                            "Paimon insert overwrite requires dynamic-partition-overwrite=true for partitioned tables");
                });
        assertThat(overwriteEnabled).isFalse();
    }

    @Test
    public void testMergeSinkRejectsMalformedHandleBeforeCatalogInitialization()
    {
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(failingInitMetadataFactory());

        assertThatThrownBy(() -> provider.createMergeSink(null, SESSION, null, Optional.empty(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeHandle is null");
        assertThatThrownBy(() -> provider.createMergeSink(null, SESSION, mergeTableHandle(null), Optional.empty(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeHandle tableHandle is null");

        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        assertThatThrownBy(() -> provider.createMergeSink(null, SESSION, mergeTableHandle(wrongTableHandle), Optional.empty(), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon merge sink requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testPageSinkCreateTableRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonPageSinkProvider.getOutputTableHandle(tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonPageSinkProvider.getOutputTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("outputTableHandle is null");

        ConnectorOutputTableHandle wrongTableHandle = new ConnectorOutputTableHandle() {};
        assertThatThrownBy(() -> PaimonPageSinkProvider.getOutputTableHandle(wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon create table page sink requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testPageSinkInsertRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonPageSinkProvider.getInsertTableHandle(tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonPageSinkProvider.getInsertTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("insertTableHandle is null");

        ConnectorInsertTableHandle wrongTableHandle = new ConnectorInsertTableHandle() {};
        assertThatThrownBy(() -> PaimonPageSinkProvider.getInsertTableHandle(wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon insert page sink requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testEmptyExplicitWriteColumnsFailFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page sink requires non-empty write columns");
    }

    @Test
    public void testValidateWriteColumnsRejectsNullInputs()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(
                null,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT()))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("table is null");

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("writeColumns is null");

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(
                table,
                Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("writeColumns contains null column");
    }

    @Test
    public void testValidateLatestTableFieldsRejectsNulls()
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.validateNoCaseInsensitiveDuplicateFieldNames(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fields is null");

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateNoCaseInsensitiveDuplicateFieldNames(
                Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fields contains null field");
    }

    @Test
    public void testLatestFieldIndexesScansLatestFieldsOnce()
    {
        int fieldCount = 200;
        AtomicInteger fieldReads = new AtomicInteger();
        List<DataField> fields = new AbstractList<>()
        {
            @Override
            public DataField get(int index)
            {
                fieldReads.incrementAndGet();
                return DataTypes.FIELD(index, "field_" + index, DataTypes.INT());
            }

            @Override
            public int size()
            {
                return fieldCount;
            }
        };

        Map<String, Integer> fieldIndexes = PaimonPageSinkProvider.latestFieldIndexes(fields);

        assertThat(fieldIndexes).hasSize(fieldCount);
        assertThat(fieldIndexes).containsEntry("field_0", 0);
        assertThat(fieldIndexes).containsEntry("field_199", 199);
        assertThat(fieldReads).hasValue(fieldCount);
    }

    @Test
    public void testWriteColumnsPreserveExplicitOrder()
    {
        List<ColumnHandle> writeColumns = List.of(
                PaimonColumnHandle.of("new_column", DataTypes.STRING()),
                PaimonColumnHandle.of("id", DataTypes.INT()));
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withWriteColumns(writeColumns);

        assertThat(PaimonPageSinkProvider.getWriteColumns(handle))
                .extracting(PaimonColumnHandle::getColumnName)
                .containsExactly("new_column", "id");
    }

    @Test
    public void testWriteColumnsAreValidatedAgainstLatestTableSchema()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING())));
        List<PaimonColumnHandle> writeColumns = List.of(
                PaimonColumnHandle.of("name", DataTypes.STRING()),
                PaimonColumnHandle.of("id", DataTypes.INT()));

        assertThatCode(() -> PaimonPageSinkProvider.validateWriteColumns(table, writeColumns))
                .doesNotThrowAnyException();
    }

    @Test
    public void testWriteLayoutUsesLatestTableSchemaOrder()
    {
        DataField defaultZipField = new DataField(2, "zip", DataTypes.STRING().notNull()).newDefaultValue("'00000'");
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()),
                defaultZipField));
        List<PaimonColumnHandle> writeColumns = List.of(
                PaimonColumnHandle.of("name", DataTypes.STRING()),
                PaimonColumnHandle.of("id", DataTypes.INT()));

        PaimonPageSinkProvider.WriteLayout layout = PaimonPageSinkProvider.writeLayout(
                table,
                writeColumns,
                TESTING_TYPE_MANAGER);

        assertThat(layout.columnTypes()).containsExactly(INTEGER, VARCHAR, VARCHAR);
        assertThat(layout.logicalTypes()).containsExactly(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING().notNull());
        assertThat(layout.inputChannels()).containsExactly(1, 0, -1);
        assertThat(layout.defaultValues()).containsExactly(null, null, BinaryString.fromString("00000"));

        int[] inputChannels = layout.inputChannels();
        inputChannels[0] = 99;
        assertThat(layout.inputChannels()).containsExactly(1, 0, -1);

        Object[] defaultValues = layout.defaultValues();
        defaultValues[2] = null;
        assertThat(layout.defaultValues()).containsExactly(null, null, BinaryString.fromString("00000"));
    }

    @Test
    public void testWriteLayoutRejectsMissingNotNullColumnWithoutDefault()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING().notNull())));
        List<PaimonColumnHandle> writeColumns = List.of(PaimonColumnHandle.of("id", DataTypes.INT()));

        assertThatThrownBy(() -> PaimonPageSinkProvider.writeLayout(table, writeColumns, TESTING_TYPE_MANAGER))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception)
                            .hasMessage("Write column 'name' is missing, has no default value, and latest Paimon table schema type STRING NOT NULL is not nullable");
                });
    }

    @Test
    public void testWriteLayoutAllowsMissingNullableColumnWithoutDefault()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING())));
        List<PaimonColumnHandle> writeColumns = List.of(PaimonColumnHandle.of("id", DataTypes.INT()));

        PaimonPageSinkProvider.WriteLayout layout = PaimonPageSinkProvider.writeLayout(
                table,
                writeColumns,
                TESTING_TYPE_MANAGER);

        assertThat(layout.inputChannels()).containsExactly(0, -1);
        assertThat(layout.defaultValues()).containsExactly(null, null);
    }

    @Test
    public void testWriteLayoutWrapsInvalidPaimonDefaultValue()
    {
        DataField badDefaultField = new DataField(1, "retry_count", DataTypes.INT()).newDefaultValue("'not-an-int'");
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                badDefaultField));
        List<PaimonColumnHandle> writeColumns = List.of(PaimonColumnHandle.of("id", DataTypes.INT()));

        assertThatThrownBy(() -> PaimonPageSinkProvider.writeLayout(table, writeColumns, TESTING_TYPE_MANAGER))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to convert Paimon default value for column 'retry_count' with Paimon type INT");
                    assertThat(exception.getCause()).isInstanceOf(RuntimeException.class);
                });
    }

    @Test
    public void testWriteLayoutWrapsInvalidPaimonDefaultValueWhenTypeFormattingFails()
    {
        DataType unstableIntType = unstableSqlFormattingIntType();
        DataField badDefaultField = new DataField(1, "retry_count", unstableIntType).newDefaultValue("'not-an-int'");
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                badDefaultField));
        List<PaimonColumnHandle> writeColumns = List.of(PaimonColumnHandle.of("id", DataTypes.INT()));

        assertThatThrownBy(() -> PaimonPageSinkProvider.writeLayout(table, writeColumns, TESTING_TYPE_MANAGER))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception)
                            .hasMessage("Failed to convert Paimon default value for column 'retry_count' with Paimon type %s"
                                    .formatted(unstableIntType.getClass().getName()));
                    assertThat(exception.getCause()).isInstanceOf(RuntimeException.class);
                });
    }

    @Test
    public void testWriteColumnsMatchLatestTableSchemaCaseInsensitively()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "ID", DataTypes.INT()),
                DataTypes.FIELD(1, "Name", DataTypes.STRING())));
        List<PaimonColumnHandle> writeColumns = List.of(
                PaimonColumnHandle.of("id", DataTypes.INT()),
                PaimonColumnHandle.of("name", DataTypes.STRING()));

        assertThatCode(() -> PaimonPageSinkProvider.validateWriteColumns(table, writeColumns))
                .doesNotThrowAnyException();
    }

    @Test
    public void testWriteColumnMissingFromLatestTableSchemaFailsFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(
                table,
                List.of(PaimonColumnHandle.of("zip", DataTypes.STRING()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Write column 'zip' is not present in latest Paimon table schema [id]");
    }

    @Test
    public void testDuplicateWriteColumnFailsFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table,
                List.of(
                        PaimonColumnHandle.of("id", DataTypes.INT()),
                        PaimonColumnHandle.of("id", DataTypes.INT()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Write column 'id' appears more than once");
    }

    @Test
    public void testCaseInsensitiveDuplicateWriteColumnFailsFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(table,
                List.of(
                        PaimonColumnHandle.of("id", DataTypes.INT()),
                        PaimonColumnHandle.of("ID", DataTypes.INT()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Write column 'ID' appears more than once");
    }

    @Test
    public void testCaseInsensitiveDuplicateLatestTableFieldFailsFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "ID", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(
                table,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Latest Paimon table schema contains case-insensitive duplicate field name 'id'");
    }

    @Test
    public void testWriteColumnTypeMismatchWithLatestTableSchemaFailsFast()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteColumns(
                table,
                List.of(PaimonColumnHandle.of("id", DataTypes.STRING()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Write column 'id' type STRING does not match latest Paimon table schema type INT");
    }

    @Test
    public void testMergeWriteColumnsMustMatchLatestTableSchemaOrder()
    {
        FileStoreTable table = fileStoreTable(BucketMode.HASH_FIXED, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING())));

        assertThatCode(() -> PaimonPageSinkProvider.validateMergeWriteColumns(table,
                List.of(
                        PaimonColumnHandle.of("id", DataTypes.INT()),
                        PaimonColumnHandle.of("name", DataTypes.STRING()))))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeWriteColumns(
                table,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Merge write columns [id] must match latest Paimon table schema columns [id, name]");

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeWriteColumns(table,
                List.of(
                        PaimonColumnHandle.of("name", DataTypes.STRING()),
                        PaimonColumnHandle.of("id", DataTypes.INT()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Merge write columns [name, id] must match latest Paimon table schema columns [id, name]");

        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeWriteColumns(table,
                List.of(
                        PaimonColumnHandle.of("ID", DataTypes.INT()),
                        PaimonColumnHandle.of("name", DataTypes.STRING()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Merge write columns [ID, name] must match latest Paimon table schema columns [id, name]");
    }

    @Test
    public void testMergeSinkRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(PaimonPageSinkProvider.getMergeTableHandle(new PaimonMergeTableHandle(tableHandle)))
                .isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonPageSinkProvider.getMergeTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeHandle is null");

        assertThatThrownBy(() -> PaimonPageSinkProvider.getMergeTableHandle(mergeTableHandle(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeHandle tableHandle is null");

        assertThatThrownBy(() -> PaimonPageSinkProvider.getMergeTableHandle(mergeTableHandle(tableHandle)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon merge sink requires PaimonMergeTableHandle, got: %s",
                        mergeTableHandle(tableHandle).getClass().getName());

        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        assertThatThrownBy(() -> PaimonPageSinkProvider.getMergeTableHandle(mergeTableHandle(wrongTableHandle)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon merge sink requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testPageSinkRequiresMatchingTrinoAndPaimonTypeMetadata()
    {
        assertThatThrownBy(() -> new PaimonPageSink(null, List.of(INTEGER), List.of()))
                .hasMessage("writer is null");

        assertThatThrownBy(() -> new PaimonPageSink(writer(), Collections.singletonList(null), List.of(DataTypes.INT())))
                .hasMessage("columnTypes contains null type");

        assertThatThrownBy(() -> new PaimonPageSink(writer(), List.of(INTEGER), Collections.singletonList(null)))
                .hasMessage("logicalTypes contains null type");

        assertThatThrownBy(() -> new PaimonPageSink(writer(), List.of(INTEGER), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("columnTypes and logicalTypes size mismatch: 1 != 0");

        assertThatThrownBy(() -> new PaimonPageSink(
                writer(),
                List.of(INTEGER),
                List.of(DataTypes.INT()),
                new int[] {1},
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("inputChannels contains channel outside field range: 1");

        assertThatThrownBy(() -> new PaimonPageSink(
                writer(),
                List.of(INTEGER, INTEGER),
                List.of(DataTypes.INT(), DataTypes.INT()),
                new int[] {1, -1},
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("inputChannels does not contain input page channel: 0");

        assertThatThrownBy(() -> new PaimonPageSink(
                writer(),
                List.of(INTEGER),
                List.of(DataTypes.INT()),
                new int[] {0},
                new Object[0],
                null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("defaultValues and columnTypes size mismatch: 0 != 1");
    }

    @Test
    public void testPageSinkRequiresPageShapeToMatchExplicitWriteColumns()
    {
        PaimonPageSink pageSink = new PaimonPageSink(writer(), List.of(INTEGER), List.of(DataTypes.INT()));

        assertThatThrownBy(() -> pageSink.appendPage(new Page(
                1,
                writeNativeValue(INTEGER, 1L),
                writeNativeValue(INTEGER, 2L))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception)
                            .hasMessage("Failed to write data to Paimon: page channel count (2) must match write column count (1)");
                    assertThat(exception.getCause())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("page channel count (2) must match write column count (1)");
                });
    }

    @Test
    public void testPageSinkReportsCompletedBytesAfterSuccessfulWrite()
    {
        PaimonPageSink pageSink = new PaimonPageSink(writer(), List.of(INTEGER), List.of(DataTypes.INT()));
        Page page = new Page(1, writeNativeValue(INTEGER, 7L));

        assertThat(page.getSizeInBytes()).isPositive();
        assertThat(pageSink.getCompletedBytes()).isZero();

        pageSink.appendPage(page);

        assertThat(pageSink.getCompletedBytes()).isEqualTo(page.getSizeInBytes());
    }

    @Test
    public void testPageSinkDoesNotReportCompletedBytesForFailedWrite()
    {
        PaimonPageSink pageSink = new PaimonPageSink(
                writer(List.of(), new IOException("write failed"), null, null),
                List.of(INTEGER),
                List.of(DataTypes.INT()));
        Page page = new Page(1, writeNativeValue(INTEGER, 7L));

        assertThatThrownBy(() -> pageSink.appendPage(page))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Failed to write data to Paimon: write failed");

        assertThat(pageSink.getCompletedBytes()).isZero();
    }

    @Test
    public void testPageSinkMapsInputColumnsIntoLatestSchemaRow()
    {
        AtomicReference<Object[]> writeArguments = new AtomicReference<>();
        PaimonPageSink pageSink = new PaimonPageSink(
                writer(List.of(), writeArguments),
                List.of(INTEGER, VARCHAR, VARCHAR, VARCHAR),
                List.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()),
                new int[] {1, 0, -1, -1},
                new Object[] {null, null, BinaryString.fromString("00000"), null},
                null);

        pageSink.appendPage(new Page(
                1,
                writeNativeValue(VARCHAR, Slices.utf8Slice("alice")),
                writeNativeValue(INTEGER, 7L)));

        assertThat(writeArguments.get()).hasSize(1);
        InternalRow row = (InternalRow) writeArguments.get()[0];
        assertThat(row.getFieldCount()).isEqualTo(4);
        assertThat(row.getInt(0)).isEqualTo(7);
        assertThat(row.getString(1).toString()).isEqualTo("alice");
        assertThat(row.isNullAt(2)).isFalse();
        assertThat(row.getString(2).toString()).isEqualTo("00000");
        assertThat(row.isNullAt(3)).isTrue();
    }

    @Test
    public void testPageSinkNormalizesBinaryDefaultValues()
    {
        AtomicReference<Object[]> writeArguments = new AtomicReference<>();
        PaimonPageSink pageSink = new PaimonPageSink(
                writer(List.of(), writeArguments),
                List.of(VARBINARY),
                List.of(DataTypes.BINARY(4)),
                new int[] {-1},
                new Object[] {new byte[] {'a', 'b'}},
                null);

        pageSink.appendPage(new Page(1));

        InternalRow row = (InternalRow) writeArguments.get()[0];
        assertThat(row.getBinary(0)).containsExactly((byte) 'a', (byte) 'b', (byte) 0, (byte) 0);
    }

    @Test
    public void testPageSinkRequiresRowKind()
    {
        PaimonPageSink pageSink = new PaimonPageSink(writer(), List.of(INTEGER), List.of(DataTypes.INT()));

        assertThatThrownBy(() -> pageSink.writePage(new Page(1, writeNativeValue(INTEGER, 1L)), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rowKind is null");
    }

    @Test
    public void testDynamicBucketPageSinkWritesWithAssignedBucket()
    {
        AtomicReference<Object[]> writeArguments = new AtomicReference<>();
        AtomicBoolean assignerPrepared = new AtomicBoolean();
        BatchTableWrite writer = writer(List.of(), writeArguments);
        PaimonPageSink.DynamicBucketWriter dynamicBucketWriter = new PaimonPageSink.DynamicBucketWriter(
                new RowPartitionKeyExtractor(TableSchema.create(1, new Schema(
                        DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())).getFields(),
                        List.of(),
                        List.of("id"),
                        Map.of(CoreOptions.BUCKET.key(), "-1"),
                        ""))),
                new BucketAssigner()
                {
                    @Override
                    public int assign(BinaryRow partition, int keyHash)
                    {
                        assertThat(partition).isNotNull();
                        assertThat(keyHash).isNotZero();
                        return 3;
                    }

                    @Override
                    public void prepareCommit(long commitIdentifier)
                    {
                        assertThat(commitIdentifier).isEqualTo(BatchWriteBuilder.COMMIT_IDENTIFIER);
                        assignerPrepared.set(true);
                    }
                });
        PaimonPageSink pageSink = new PaimonPageSink(
                writer,
                List.of(INTEGER),
                List.of(DataTypes.INT()),
                dynamicBucketWriter);

        pageSink.appendPage(new Page(1, writeNativeValue(INTEGER, 11L)));
        assertThat(writeArguments.get()).hasSize(2);
        assertThat(writeArguments.get()[0]).isInstanceOf(PaimonRow.class);
        assertThat(writeArguments.get()[1]).isEqualTo(3);

        assertThat(pageSink.finish().join()).isEmpty();
        assertThat(assignerPrepared).isTrue();
    }

    @Test
    public void testDynamicBucketWriterUsesPageSinkTaskPartitionAsAssigner()
    {
        AtomicReference<Object[]> writeArguments = new AtomicReference<>();
        FileStoreTable table = writeReadyFileStoreTable(
                new AtomicBoolean(),
                new AtomicReference<>(),
                new AtomicBoolean(),
                List.of(),
                Map.of(
                        CoreOptions.BUCKET.key(), "-1",
                        CoreOptions.DYNAMIC_BUCKET_ASSIGNER_PARALLELISM.key(), "4",
                        CoreOptions.DYNAMIC_BUCKET_INITIAL_BUCKETS.key(), "1"),
                List.of("id"),
                BucketMode.HASH_DYNAMIC);
        PaimonPageSink pageSink = new PaimonPageSink(
                writer(List.of(), writeArguments),
                List.of(INTEGER),
                List.of(DataTypes.INT()),
                PaimonPageSinkProvider.dynamicBucketWriter(table, true, pageSinkId(2), 4));

        pageSink.appendPage(new Page(1, writeNativeValue(INTEGER, 11L)));

        assertThat(writeArguments.get()).hasSize(2);
        assertThat(writeArguments.get()[1]).isEqualTo(2);
    }

    @Test
    public void testDynamicBucketNumAssignersFollowsPaimonInitialBucketsSemantics()
    {
        assertThat(PaimonDynamicBucketUtils.dynamicBucketNumAssigners(
                new CoreOptions(new Options(Map.of())),
                4))
                .isEqualTo(4);
        assertThat(PaimonDynamicBucketUtils.dynamicBucketNumAssigners(
                new CoreOptions(new Options(Map.of(CoreOptions.DYNAMIC_BUCKET_INITIAL_BUCKETS.key(), "2"))),
                4))
                .isEqualTo(2);
        assertThat(PaimonDynamicBucketUtils.dynamicBucketNumAssigners(
                new CoreOptions(new Options(Map.of(CoreOptions.DYNAMIC_BUCKET_INITIAL_BUCKETS.key(), "8"))),
                4))
                .isEqualTo(4);
        assertThatThrownBy(() -> PaimonDynamicBucketUtils.dynamicBucketNumAssigners(
                new CoreOptions(new Options(Map.of(CoreOptions.DYNAMIC_BUCKET_INITIAL_BUCKETS.key(), "0"))),
                4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamic-bucket.initial-buckets must be positive: 0");
    }

    @Test
    public void testDynamicBucketWriterRejectsTaskPartitionOutsideAssignerParallelism()
    {
        FileStoreTable table = writeReadyFileStoreTable(
                new AtomicBoolean(),
                new AtomicReference<>(),
                new AtomicBoolean(),
                List.of(),
                Map.of(CoreOptions.BUCKET.key(), "-1"),
                List.of("id"),
                BucketMode.HASH_DYNAMIC);

        assertThatThrownBy(() -> PaimonPageSinkProvider.dynamicBucketWriter(table, true, pageSinkId(2), 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon HASH_DYNAMIC writer task partition 2 is outside assigner parallelism 2");
    }

    @Test
    public void testDynamicBucketWriterUsesPlannedAssignerParallelism()
    {
        FileStoreTable table = writeReadyFileStoreTable(
                new AtomicBoolean(),
                new AtomicReference<>(),
                new AtomicBoolean(),
                List.of(),
                Map.of(CoreOptions.BUCKET.key(), "-1"),
                List.of("id"),
                BucketMode.HASH_DYNAMIC);

        assertThatThrownBy(() -> PaimonPageSinkProvider.dynamicBucketWriter(
                table,
                true,
                pageSinkId(4),
                OptionalInt.of(4),
                8))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon HASH_DYNAMIC writer task partition 4 is outside assigner parallelism 4");

        assertThatCode(() -> PaimonPageSinkProvider.dynamicBucketWriter(
                table,
                true,
                pageSinkId(4),
                OptionalInt.empty(),
                8))
                .doesNotThrowAnyException();
    }

    @Test
    public void testPageSinkTaskPartitionIdIsDecodedFromPageSinkId()
    {
        assertThat(PaimonPageSinkProvider.pageSinkTaskPartitionId(pageSinkId(0))).isEqualTo(0);
        assertThat(PaimonPageSinkProvider.pageSinkTaskPartitionId(pageSinkId(17))).isEqualTo(17);
        assertThat(PaimonPageSinkProvider.pageSinkTaskPartitionId(() -> (9L << 32) + (42L << 8) + 7L))
                .isEqualTo(42);
    }

    @Test
    public void testPageSinkWriteExceptionsUsePaimonErrorCodes()
    {
        IllegalArgumentException contractViolation = new IllegalArgumentException("metadata mismatch");
        IOException writeFailure = new IOException("write failed");
        TrinoException alreadyMapped = new TrinoException(PAIMON_WRITER_DATA_ERROR, "already mapped");
        UnsupportedOperationException unsupported = new UnsupportedOperationException("unsupported nested type");
        UnsupportedOperationException unsupportedWithoutMessage = new UnsupportedOperationException();
        RuntimeException runtimeFailure = new RuntimeException("runtime write failed");
        RuntimeException nestedContractViolation = new RuntimeException(new RuntimeException(contractViolation));
        RuntimeException nestedAlreadyMapped = new RuntimeException(new RuntimeException(alreadyMapped));
        RuntimeException nestedUnsupported = new RuntimeException(new RuntimeException(unsupported));

        assertThat(PaimonPageSink.wrapWriteException(contractViolation))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon: metadata mismatch");
                    assertThat(exception.getCause()).isSameAs(contractViolation);
                });
        assertThat(PaimonPageSink.wrapWriteException(alreadyMapped)).isSameAs(alreadyMapped);
        assertThat(PaimonPageSink.wrapWriteException(unsupported))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon write uses features which are not supported by the Trino connector: unsupported nested type");
                    assertThat(exception.getCause()).isSameAs(unsupported);
                });
        assertThat(PaimonPageSink.wrapWriteException(unsupportedWithoutMessage))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon write uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isSameAs(unsupportedWithoutMessage);
                });
        assertThat(PaimonPageSink.wrapWriteException(runtimeFailure))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon: runtime write failed");
                    assertThat(exception.getCause()).isSameAs(runtimeFailure);
                });
        assertThat(PaimonPageSink.wrapWriteException(writeFailure))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon: write failed");
                    assertThat(exception.getCause()).isSameAs(writeFailure);
                });
        assertThat(PaimonPageSink.wrapWriteException(nestedContractViolation))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon: metadata mismatch");
                    assertThat(exception.getCause()).isSameAs(contractViolation);
                });
        assertThat(PaimonPageSink.wrapWriteException(nestedAlreadyMapped)).isSameAs(alreadyMapped);
        assertThat(PaimonPageSink.wrapWriteException(nestedUnsupported))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon write uses features which are not supported by the Trino connector: unsupported nested type");
                    assertThat(exception.getCause()).isSameAs(unsupported);
                });

        assertThat(PaimonPageSink.wrapWriterCloseException(contractViolation))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_CLOSE_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to close Paimon writer");
                    assertThat(exception.getCause()).isSameAs(contractViolation);
                });
        assertThat(PaimonPageSink.wrapWriterCloseException(alreadyMapped)).isSameAs(alreadyMapped);
        assertThat(PaimonPageSink.wrapWriterCloseException(unsupported))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon writer close uses features which are not supported by the Trino connector: unsupported nested type");
                    assertThat(exception.getCause()).isSameAs(unsupported);
                });
        assertThat(PaimonPageSink.wrapWriterCloseException(unsupportedWithoutMessage))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon writer close uses features which are not supported by the Trino connector");
                    assertThat(exception.getCause()).isSameAs(unsupportedWithoutMessage);
                });
        assertThat(PaimonPageSink.wrapWriterCloseException(writeFailure))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_CLOSE_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to close Paimon writer");
                    assertThat(exception.getCause()).isSameAs(writeFailure);
                });
        assertThat(PaimonPageSink.wrapWriterCloseException(nestedAlreadyMapped)).isSameAs(alreadyMapped);
        assertThat(PaimonPageSink.wrapWriterCloseException(nestedUnsupported))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon writer close uses features which are not supported by the Trino connector: unsupported nested type");
                    assertThat(exception.getCause()).isSameAs(unsupported);
                });

        assertThat(PaimonPageSink.wrapIoManagerCloseException(nestedAlreadyMapped)).isSameAs(alreadyMapped);
    }

    @Test
    public void testPageSinkCloseFailureDoesNotHideCommitFailure()
    {
        IllegalStateException commitFailure = new IllegalStateException("commit failed");
        IllegalArgumentException closeFailure = new IllegalArgumentException("close failed");

        RuntimeException actual = PaimonPageSink.closeWriter(writer(closeFailure), commitFailure);

        assertThat(actual).isSameAs(commitFailure);
        assertThat(actual.getSuppressed())
                .singleElement()
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_CLOSE_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to close Paimon writer");
                    assertThat(exception.getCause()).isSameAs(closeFailure);
                });
    }

    @Test
    public void testPageSinkCloseFailureIsThrownWhenCommitSucceeds()
    {
        IllegalArgumentException closeFailure = new IllegalArgumentException("close failed");

        RuntimeException actual = PaimonPageSink.closeWriter(writer(closeFailure), null);

        assertThat(actual)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_CLOSE_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to close Paimon writer");
                    assertThat(exception.getCause()).isSameAs(closeFailure);
                });
    }

    @Test
    public void testPageSinkAbortWrapsCheckedCloseFailures()
    {
        PaimonPageSink pageSink = new PaimonPageSink(
                writer(List.of(), null, null, new IOException("close failed")),
                List.of(INTEGER),
                List.of(DataTypes.INT()));

        assertThatThrownBy(pageSink::abort)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_CLOSE_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to close Paimon writer");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class)
                            .hasMessage("close failed");
                });
    }

    @Test
    public void testPageSinkAbortWrapsRuntimeIoManagerCloseFailures()
    {
        RuntimeException closeFailure = new IllegalStateException("close failed");
        TestingIoManager ioManager = new TestingIoManager()
        {
            @Override
            public void close()
                    throws Exception
            {
                super.close();
                throw closeFailure;
            }
        };
        PaimonPageSink pageSink = new PaimonPageSink(
                writer(),
                List.of(INTEGER),
                List.of(DataTypes.INT()),
                new int[] {0},
                new Object[] {null},
                null,
                null,
                ioManager);

        assertThatThrownBy(pageSink::abort)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_CLOSE_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to close Paimon writer IO manager");
                    assertThat(exception.getCause()).isSameAs(closeFailure);
                });
        assertThat(ioManager.isClosed()).isTrue();
    }

    @Test
    public void testPageSinkAbortClosesIoManager()
    {
        TestingIoManager ioManager = new TestingIoManager();
        PaimonPageSink pageSink = new PaimonPageSink(
                writer(),
                List.of(INTEGER),
                List.of(DataTypes.INT()),
                new int[] {0},
                new Object[] {null},
                null,
                null,
                ioManager);

        pageSink.abort();

        assertThat(ioManager.isClosed()).isTrue();
    }

    @Test
    public void testPageSinkTerminalCloseIsIdempotent()
    {
        AtomicInteger closeCount = new AtomicInteger();
        TestingIoManager ioManager = new TestingIoManager();
        PaimonPageSink pageSink = new PaimonPageSink(
                writer(List.of(),
                        null,
                        null,
                        null,
                        new AtomicReference<>(),
                        new AtomicReference<>(),
                        new AtomicReference<>(),
                        closeCount),
                List.of(INTEGER),
                List.of(DataTypes.INT()),
                new int[] {0},
                new Object[] {null},
                null,
                null,
                ioManager);

        pageSink.abort();
        pageSink.abort();

        assertThat(closeCount).hasValue(1);
        assertThat(ioManager.closeCount()).isEqualTo(1);
    }

    @Test
    public void testPageSinkRunsPaimonOperationsWithPluginClassLoader()
            throws Exception
    {
        ClassLoader callerClassLoader = new ClassLoader(null) {};
        ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader pluginClassLoader = PaimonPageSink.class.getClassLoader();
        AtomicReference<ClassLoader> writeClassLoader = new AtomicReference<>();
        AtomicReference<ClassLoader> prepareCommitClassLoader = new AtomicReference<>();
        AtomicReference<ClassLoader> writerCloseClassLoader = new AtomicReference<>();
        AtomicReference<ClassLoader> ioManagerCloseClassLoader = new AtomicReference<>();
        TestingIoManager ioManager = classLoaderCheckingIoManager(ioManagerCloseClassLoader);
        PaimonPageSink pageSink = new PaimonPageSink(
                classLoaderCheckingWriter(writeClassLoader, prepareCommitClassLoader, writerCloseClassLoader),
                List.of(INTEGER),
                List.of(DataTypes.INT()),
                new int[] {0},
                new Object[] {null},
                null,
                null,
                ioManager);

        try {
            Thread.currentThread().setContextClassLoader(callerClassLoader);

            pageSink.appendPage(new Page(1, writeNativeValue(INTEGER, 1L)));
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(callerClassLoader);

            assertThat(pageSink.finish().join()).isEmpty();
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(callerClassLoader);
        }
        finally {
            Thread.currentThread().setContextClassLoader(previousClassLoader);
        }

        assertThat(writeClassLoader.get()).isSameAs(pluginClassLoader);
        assertThat(prepareCommitClassLoader.get()).isSameAs(pluginClassLoader);
        assertThat(writerCloseClassLoader.get()).isSameAs(pluginClassLoader);
        assertThat(ioManagerCloseClassLoader.get()).isSameAs(pluginClassLoader);

        AtomicReference<ClassLoader> abortWriterCloseClassLoader = new AtomicReference<>();
        AtomicReference<ClassLoader> abortIoManagerCloseClassLoader = new AtomicReference<>();
        PaimonPageSink abortSink = new PaimonPageSink(
                classLoaderCheckingWriter(new AtomicReference<>(), new AtomicReference<>(), abortWriterCloseClassLoader),
                List.of(INTEGER),
                List.of(DataTypes.INT()),
                new int[] {0},
                new Object[] {null},
                null,
                null,
                classLoaderCheckingIoManager(abortIoManagerCloseClassLoader));

        try {
            Thread.currentThread().setContextClassLoader(callerClassLoader);

            abortSink.abort();
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(callerClassLoader);
        }
        finally {
            Thread.currentThread().setContextClassLoader(previousClassLoader);
        }

        assertThat(abortWriterCloseClassLoader.get()).isSameAs(pluginClassLoader);
        assertThat(abortIoManagerCloseClassLoader.get()).isSameAs(pluginClassLoader);
    }

    @Test
    public void testPageSinkRejectsWritesAfterAbort()
    {
        PaimonPageSink pageSink = new PaimonPageSink(writer(), List.of(INTEGER), List.of(DataTypes.INT()));

        pageSink.abort();

        assertThatThrownBy(() -> pageSink.appendPage(new Page(1, writeNativeValue(INTEGER, 1L))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon page sink is already closed");
    }

    @Test
    public void testPageSinkWriteAndFinishWrapCheckedFailures()
    {
        IOException writeFailure = new IOException("write failed");
        PaimonPageSink failingWriteSink = new PaimonPageSink(
                writer(List.of(), writeFailure, null, null),
                List.of(INTEGER),
                List.of(DataTypes.INT()));

        assertThatThrownBy(() -> failingWriteSink.appendPage(new Page(1, writeNativeValue(INTEGER, 1L))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon: write failed");
                    assertThat(exception.getCause()).isSameAs(writeFailure);
                });

        IllegalArgumentException writeRuntimeFailure = new IllegalArgumentException("bad row");
        PaimonPageSink failingRuntimeWriteSink = new PaimonPageSink(
                writer(List.of(), writeRuntimeFailure, null, null),
                List.of(INTEGER),
                List.of(DataTypes.INT()));

        assertThatThrownBy(() -> failingRuntimeWriteSink.appendPage(new Page(1, writeNativeValue(INTEGER, 1L))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon: bad row");
                    assertThat(exception.getCause()).isSameAs(writeRuntimeFailure);
                });

        IOException prepareFailure = new IOException("prepare failed");
        PaimonPageSink failingFinishSink = new PaimonPageSink(
                writer(List.of(), null, prepareFailure, null),
                List.of(INTEGER),
                List.of(DataTypes.INT()));

        assertThatThrownBy(() -> failingFinishSink.finish().join())
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon: prepare failed");
                    assertThat(exception.getCause()).isSameAs(prepareFailure);
                });

        IllegalStateException prepareRuntimeFailure = new IllegalStateException("prepare failed");
        PaimonPageSink failingRuntimeFinishSink = new PaimonPageSink(
                writer(List.of(), null, prepareRuntimeFailure, null),
                List.of(INTEGER),
                List.of(DataTypes.INT()));

        assertThatThrownBy(() -> failingRuntimeFinishSink.finish().join())
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon: prepare failed");
                    assertThat(exception.getCause()).isSameAs(prepareRuntimeFailure);
                });
    }

    @Test
    public void testPageSinkProviderWrapsWriterInitializationUnsupportedFailures()
    {
        UnsupportedOperationException writerFailure = new UnsupportedOperationException(UNSUPPORTED_FORMAT_WRITE_MESSAGE);
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writerInitializationFailingFileStoreTable(
                        new AtomicReference<>(),
                        writerFailure,
                        Map.of(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_JSON))));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(
                        PaimonColumnHandle.of("payload", DataTypes.VARIANT(), TESTING_TYPE_MANAGER)));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .build();

        assertThatThrownBy(() -> provider.createPageSink(null, session, (ConnectorInsertTableHandle) tableHandle, Optional.empty(), null))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon write uses features which are not supported by the Trino connector: "
                            + UNSUPPORTED_FORMAT_WRITE_MESSAGE);
                    assertThat(exception.getCause()).isSameAs(writerFailure);
                });
    }

    @Test
    public void testPageSinkProviderRejectsUnsupportedDefaultParquetTypesBeforeWriterInitialization()
    {
        UnsupportedOperationException writerFailure = new UnsupportedOperationException("writer should not initialize");
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(
                writerInitializationFailingFileStoreTable(new AtomicReference<>(), writerFailure)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(
                        PaimonColumnHandle.of("payload", DataTypes.VARIANT(), TESTING_TYPE_MANAGER)));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .build();

        assertThatThrownBy(() -> provider.createPageSink(null, session, (ConnectorInsertTableHandle) tableHandle, Optional.empty(), null))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon write uses features which are not supported by the Trino connector: "
                            + UNSUPPORTED_FORMAT_WRITE_MESSAGE);
                    assertThat(exception.getCause()).isNotSameAs(writerFailure);
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage(UNSUPPORTED_FORMAT_WRITE_MESSAGE);
                });
    }

    @Test
    public void testPageSinkProviderRejectsOrcTimeColumnsBeforeWriterInitialization()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "event_time", DataTypes.TIME(3)));
        AtomicReference<IOManager> writeIoManager = new AtomicReference<>();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(writeReadyFileStoreTable(
                new AtomicBoolean(),
                new AtomicReference<>(),
                new AtomicBoolean(),
                List.of(),
                Map.of(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_ORC),
                List.of(),
                BucketMode.HASH_FIXED,
                new AtomicReference<>(),
                writeIoManager,
                rowType)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("event_time", DataTypes.TIME(3))));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .build();

        assertThatThrownBy(() -> provider.createPageSink(null, session, (ConnectorInsertTableHandle) tableHandle, Optional.empty(), null))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon write uses features which are not supported by the Trino connector: "
                            + ORC_TIME_WRITE_MESSAGE);
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage(ORC_TIME_WRITE_MESSAGE);
                });
        assertThat(writeIoManager.get()).isNull();
    }

    @Test
    public void testPageSinkProviderDoesNotRejectNativeFileFormatsUnsupportedOnlyByTrinoWriter()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));
        AtomicReference<IOManager> writeIoManager = new AtomicReference<>();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(writeReadyFileStoreTable(
                new AtomicBoolean(),
                new AtomicReference<>(),
                new AtomicBoolean(),
                List.of(),
                Map.of(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT_JSON),
                List.of(),
                BucketMode.HASH_FIXED,
                new AtomicReference<>(),
                writeIoManager,
                rowType)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(
                        PaimonColumnHandle.of("payload", DataTypes.VARIANT(), TESTING_TYPE_MANAGER)));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .build();

        ConnectorPageSink pageSink = provider.createPageSink(null, session, (ConnectorInsertTableHandle) tableHandle, Optional.empty(), null);

        assertThat(writeIoManager.get()).isNotNull();
        pageSink.abort();
    }

    @Test
    public void testPageSinkProviderDoesNotRejectRowTrackingTablesUnsupportedOnlyByTrinoWriter()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "picture", DataTypes.BLOB()));
        AtomicReference<IOManager> writeIoManager = new AtomicReference<>();
        PaimonPageSinkProvider provider = new PaimonPageSinkProvider(metadataFactory(writeReadyFileStoreTable(
                new AtomicBoolean(),
                new AtomicReference<>(),
                new AtomicBoolean(),
                List.of(),
                Map.of(
                        CoreOptions.ROW_TRACKING_ENABLED.key(), "true",
                        CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true"),
                List.of(),
                BucketMode.HASH_FIXED,
                new AtomicReference<>(),
                writeIoManager,
                rowType)));
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withWriteColumns(List.of(PaimonColumnHandle.of("picture", DataTypes.BLOB())));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .build();

        ConnectorPageSink pageSink = provider.createPageSink(null, session, (ConnectorInsertTableHandle) tableHandle, Optional.empty(), null);

        assertThat(writeIoManager.get()).isNotNull();
        pageSink.abort();
    }

    @Test
    public void testVariantWriteFailuresUseStableConnectorErrors()
    {
        io.trino.spi.type.Type jsonType = TESTING_TYPE_MANAGER.getType(new TypeDescriptor(JSON));
        PaimonPageSink pageSink = new PaimonPageSink(variantValidatingWriter(), List.of(jsonType), List.of(DataTypes.VARIANT()));

        assertThatThrownBy(() -> pageSink.appendPage(new Page(
                1,
                writeNativeValue(jsonType, Slices.utf8Slice("{broken")))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon: Failed to parse Variant from JSON");
                    assertThat(exception.getCause()).isInstanceOf(RuntimeException.class)
                            .hasMessage("Failed to parse Variant from JSON");
                    assertThat(exception.getCause().getCause()).isInstanceOf(IOException.class);
                });

        PaimonPageSink unsupportedVariantSink = new PaimonPageSink(variantValidatingWriter(), List.of(INTEGER), List.of(DataTypes.VARIANT()));
        assertThatThrownBy(() -> unsupportedVariantSink.appendPage(new Page(1, writeNativeValue(INTEGER, 1L))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon write uses features which are not supported by the Trino connector: "
                            + "Paimon VARIANT requires Trino JSON type metadata");
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage("Paimon VARIANT requires Trino JSON type metadata");
                });
    }

    @Test
    public void testBinaryLengthWriteFailuresUseStableConnectorErrors()
    {
        PaimonPageSink pageSink = new PaimonPageSink(
                binaryReadingWriter(),
                List.of(VARBINARY),
                List.of(DataTypes.VARBINARY(3)));

        assertThatThrownBy(() -> pageSink.appendPage(new Page(
                1,
                writeNativeValue(VARBINARY, Slices.utf8Slice("abcd")))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception)
                            .hasMessage("Failed to write data to Paimon: Cannot write 4 bytes to Paimon VARBINARY(3); value would be truncated");
                    assertThat(exception.getCause())
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("Cannot write 4 bytes to Paimon VARBINARY(3); value would be truncated");
                });
    }

    @Test
    public void testPageSinkFinishRejectsNullCommitMessages()
    {
        PaimonPageSink pageSink = new PaimonPageSink(writer(List.of()), List.of(INTEGER), List.of(DataTypes.INT()));
        assertThat(pageSink.finish().join()).isEmpty();

        assertThatThrownBy(() -> new PaimonPageSink(
                writer((List<CommitMessage>) null),
                List.of(INTEGER),
                List.of(DataTypes.INT())).finish())
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon: Paimon writer returned null commit messages");
                    assertThat(exception.getCause())
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("Paimon writer returned null commit messages");
                });

        assertThatThrownBy(() -> new PaimonPageSink(
                writer(Collections.singletonList(null)),
                List.of(INTEGER),
                List.of(DataTypes.INT())).finish())
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_WRITER_DATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to write data to Paimon: Paimon writer returned null commit message");
                    assertThat(exception.getCause())
                            .isInstanceOf(NullPointerException.class)
                            .hasMessage("Paimon writer returned null commit message");
                });
    }

    private static void assertUnsupportedWriteBucketMode(BucketMode bucketMode)
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.validateWriteBucketMode(fileStoreTable(bucketMode)))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining(
                            "Unsupported table bucket mode: " + bucketMode + " for Paimon writes");
                });
    }

    private static void assertUnsupportedMergeBucketMode(BucketMode bucketMode)
    {
        assertThatThrownBy(() -> PaimonPageSinkProvider.validateMergeBucketMode(fileStoreTable(bucketMode)))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining(
                            "Unsupported table bucket mode: " + bucketMode + " for Paimon merge writes");
                });
    }

    private static FileStore<?> fileStore()
    {
        return (FileStore<?>) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {FileStore.class},
                (_, method, _) -> switch (method.getName()) {
                    case "snapshotManager", "newIndexFileHandler" -> null;
                    case "toString" -> "testing-file-store";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode)
    {
        return fileStoreTable(bucketMode, new AtomicBoolean());
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode, RowType rowType)
    {
        return fileStoreTable(bucketMode, new AtomicBoolean(), rowType);
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode, AtomicBoolean copiedWithLatestSchema)
    {
        return fileStoreTable(bucketMode, copiedWithLatestSchema, DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT())));
    }

    private static FileStoreTable fileStoreTable(
            BucketMode bucketMode,
            AtomicBoolean copiedWithLatestSchema,
            RowType rowType)
    {
        return fileStoreTable(bucketMode, copiedWithLatestSchema, rowType, List.of("id"), Map.of());
    }

    private static FileStoreTable fileStoreTable(
            BucketMode bucketMode,
            AtomicBoolean copiedWithLatestSchema,
            RowType rowType,
            List<String> primaryKeys,
            Map<String, String> options)
    {
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "bucketMode" -> bucketMode;
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of();
                    case "primaryKeys" -> primaryKeys;
                    case "options" -> options;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            List.of(),
                            primaryKeys,
                            mergeOptions(Map.of(CoreOptions.BUCKET.key(), "7"), options),
                            ""));
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield proxy;
                    }
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "toString" -> "testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table table()
    {
        return (Table) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (_, method, _) -> switch (method.getName()) {
                    case "toString" -> "testing-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static InnerTable innerTable()
    {
        return (InnerTable) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {InnerTable.class},
                (_, method, _) -> switch (method.getName()) {
                    case "toString" -> "testing-inner-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ConnectorMergeTableHandle mergeTableHandle(ConnectorTableHandle tableHandle)
    {
        return () -> tableHandle;
    }

    private static PaimonMetadataFactory failingInitMetadataFactory()
    {
        return new PaimonMetadataFactory(new Options(), _ -> {
            throw new AssertionError("filesystem should not be used");
        }, TESTING_TYPE_MANAGER)
        {
            @Override
            public PaimonMetadata create()
            {
                return new PaimonMetadata(new FailingInitCatalog(), TESTING_TYPE_MANAGER);
            }
        };
    }

    private static PaimonMetadataFactory metadataFactory(FileStoreTable table)
    {
        return new PaimonMetadataFactory(new Options(), _ -> {
            throw new AssertionError("filesystem should not be used");
        }, TESTING_TYPE_MANAGER)
        {
            @Override
            public PaimonMetadata create()
            {
                return new PaimonMetadata(new TestingCatalog(table), TESTING_TYPE_MANAGER);
            }
        };
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions)
    {
        return writeReadyFileStoreTable(copiedWithLatestSchema, copyWithoutTimeTravelOptions, new AtomicBoolean());
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled)
    {
        return writeReadyFileStoreTable(
                copiedWithLatestSchema,
                copyWithoutTimeTravelOptions,
                overwriteEnabled,
                List.of(),
                Map.of());
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled,
            AtomicReference<MemoryPoolFactory> writeBufferPool)
    {
        return writeReadyFileStoreTable(
                copiedWithLatestSchema,
                copyWithoutTimeTravelOptions,
                overwriteEnabled,
                writeBufferPool,
                new AtomicReference<>());
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled,
            AtomicReference<MemoryPoolFactory> writeBufferPool,
            AtomicReference<IOManager> writeIoManager)
    {
        return writeReadyFileStoreTable(
                copiedWithLatestSchema,
                copyWithoutTimeTravelOptions,
                overwriteEnabled,
                List.of(),
                Map.of(),
                List.of("id"),
                BucketMode.HASH_FIXED,
                writeBufferPool,
                writeIoManager);
    }

    private static FileStoreTable writeReadyPartitionedFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled,
            boolean dynamicPartitionOverwrite)
    {
        return writeReadyFileStoreTable(
                copiedWithLatestSchema,
                copyWithoutTimeTravelOptions,
                overwriteEnabled,
                List.of("pt"),
                Map.of(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), String.valueOf(dynamicPartitionOverwrite)));
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled,
            List<String> partitionKeys,
            Map<String, String> options)
    {
        return writeReadyFileStoreTable(
                copiedWithLatestSchema,
                copyWithoutTimeTravelOptions,
                overwriteEnabled,
                partitionKeys,
                options,
                List.of("id"));
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled,
            List<String> partitionKeys,
            Map<String, String> options,
            List<String> primaryKeys)
    {
        return writeReadyFileStoreTable(
                copiedWithLatestSchema,
                copyWithoutTimeTravelOptions,
                overwriteEnabled,
                partitionKeys,
                options,
                primaryKeys,
                BucketMode.HASH_FIXED);
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled,
            List<String> partitionKeys,
            Map<String, String> options,
            List<String> primaryKeys,
            BucketMode bucketMode)
    {
        return writeReadyFileStoreTable(
                copiedWithLatestSchema,
                copyWithoutTimeTravelOptions,
                overwriteEnabled,
                partitionKeys,
                options,
                primaryKeys,
                bucketMode,
                new AtomicReference<>(),
                new AtomicReference<>());
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled,
            List<String> partitionKeys,
            Map<String, String> options,
            List<String> primaryKeys,
            BucketMode bucketMode,
            AtomicReference<MemoryPoolFactory> writeBufferPool)
    {
        return writeReadyFileStoreTable(
                copiedWithLatestSchema,
                copyWithoutTimeTravelOptions,
                overwriteEnabled,
                partitionKeys,
                options,
                primaryKeys,
                bucketMode,
                writeBufferPool,
                new AtomicReference<>());
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled,
            List<String> partitionKeys,
            Map<String, String> options,
            List<String> primaryKeys,
            BucketMode bucketMode,
            AtomicReference<MemoryPoolFactory> writeBufferPool,
            AtomicReference<IOManager> writeIoManager)
    {
        return writeReadyFileStoreTable(
                copiedWithLatestSchema,
                copyWithoutTimeTravelOptions,
                overwriteEnabled,
                partitionKeys,
                options,
                primaryKeys,
                bucketMode,
                writeBufferPool,
                writeIoManager,
                ID_ROW_TYPE);
    }

    private static FileStoreTable writeReadyFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            AtomicBoolean overwriteEnabled,
            List<String> partitionKeys,
            Map<String, String> options,
            List<String> primaryKeys,
            BucketMode bucketMode,
            AtomicReference<MemoryPoolFactory> writeBufferPool,
            AtomicReference<IOManager> writeIoManager,
            RowType rowType)
    {
        BatchTableWrite writer = writer(writeBufferPool, writeIoManager);
        BatchWriteBuilder batchWriteBuilder = (BatchWriteBuilder) Proxy
                .newProxyInstance(
                        PaimonPageSinkProviderTest.class.getClassLoader(),
                        new Class<?>[] {BatchWriteBuilder.class},
                        (proxy, method, _) -> switch (method.getName()) {
                            case "newWrite" -> writer;
                            case "withOverwrite" -> {
                                overwriteEnabled.set(true);
                                yield proxy;
                            }
                            case "tableName" -> "testing";
                            case "rowType" -> rowType;
                            case "newWriteSelector" -> Optional.empty();
                            case "toString" -> "testing-batch-write-builder";
                            default -> throw new UnsupportedOperationException(method.getName());
                        });
        AtomicReference<FileStoreTable> latestTableRef = new AtomicReference<>();
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bucketMode" -> bucketMode;
                    case "rowType" -> rowType;
                    case "partitionKeys" -> partitionKeys;
                    case "primaryKeys" -> primaryKeys;
                    case "options" -> options;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            partitionKeys,
                            primaryKeys,
                            mergeOptions(Map.of(CoreOptions.BUCKET.key(), "7"), options),
                            ""));
                    case "store" -> fileStore();
                    case "newBatchWriteBuilder" -> batchWriteBuilder;
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield proxy;
                    }
                    case "copy" -> proxy;
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield proxy;
                    }
                    case "toString" -> "latest-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        latestTableRef.set(latestTable);
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bucketMode" -> bucketMode;
                    case "rowType" -> rowType;
                    case "partitionKeys" -> partitionKeys;
                    case "primaryKeys" -> primaryKeys;
                    case "options" -> options;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            partitionKeys,
                            primaryKeys,
                            mergeOptions(Map.of(CoreOptions.BUCKET.key(), "7"), options),
                            ""));
                    case "store" -> fileStore();
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield latestTableRef.get();
                    }
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTableRef.get();
                    }
                    case "copy" -> proxy;
                    case "newBatchWriteBuilder" -> throw new AssertionError(
                            "stale FileStoreTable should not create BatchWriteBuilder before latest-schema refresh");
                    case "toString" -> "stale-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Map<String, String> mergeOptions(Map<String, String> first, Map<String, String> second)
    {
        HashMap<String, String> result = new HashMap<>();
        result.putAll(first);
        result.putAll(second);
        return Map.copyOf(result);
    }

    private static ConnectorPageSinkId pageSinkId(int taskPartitionId)
    {
        return () -> (long) taskPartitionId << 8;
    }

    private static FileStoreTable writerInitializationFailingFileStoreTable(
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            RuntimeException writerFailure)
    {
        return writerInitializationFailingFileStoreTable(copyWithoutTimeTravelOptions, writerFailure, Map.of());
    }

    private static FileStoreTable writerInitializationFailingFileStoreTable(
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            RuntimeException writerFailure,
            Map<String, String> options)
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "payload", DataTypes.VARIANT()));
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of();
                    case "options" -> options;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            List.of(),
                            List.of(),
                            options,
                            ""));
                    case "newBatchWriteBuilder" -> throw writerFailure;
                    case "copyWithLatestSchema", "copy" -> proxy;
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield proxy;
                    }
                    case "toString" -> "writer-initialization-failing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (_, method, args) -> switch (method.getName()) {
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield latestTable;
                    }
                    case "copyWithLatestSchema" -> latestTable;
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of();
                    case "options" -> options;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            List.of(),
                            List.of(),
                            options,
                            ""));
                    case "toString" -> "stale-writer-initialization-failing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static MemoryOwner memoryOwner(long memoryOccupancy)
    {
        return new MemoryOwner()
        {
            @Override
            public void setMemoryPool(MemorySegmentPool memoryPool) {}

            @Override
            public long memoryOccupancy()
            {
                return memoryOccupancy;
            }

            @Override
            public void flushMemory() {}
        };
    }

    private static class TestingIoManager
            extends IOManagerImpl
    {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicInteger closeCount = new AtomicInteger();

        private TestingIoManager()
        {
            super(System.getProperty("java.io.tmpdir"));
        }

        @Override
        public void close()
                throws Exception
        {
            closeCount.incrementAndGet();
            closed.set(true);
            super.close();
        }

        private boolean isClosed()
        {
            return closed.get();
        }

        private int closeCount()
        {
            return closeCount.get();
        }
    }

    private static TestingIoManager classLoaderCheckingIoManager(AtomicReference<ClassLoader> closeClassLoader)
    {
        return new TestingIoManager()
        {
            @Override
            public void close()
                    throws Exception
            {
                closeClassLoader.set(Thread.currentThread().getContextClassLoader());
                super.close();
            }
        };
    }

    private static class TestingCatalog
            extends PaimonCatalog
    {
        private final FileStoreTable table;

        private TestingCatalog(FileStoreTable table)
        {
            super(new Options(), _ -> {
                throw new AssertionError("filesystem should not be used");
            });
            this.table = table;
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public Table getTable(Identifier identifier)
        {
            return table;
        }
    }

    private static class FailingInitCatalog
            extends PaimonCatalog
    {
        private FailingInitCatalog()
        {
            super(new Options(), _ -> {
                throw new AssertionError("filesystem should not be used");
            });
        }

        @Override
        public void initSession(ConnectorSession connectorSession)
        {
            throw new AssertionError("catalog should not be initialized for malformed page-sink session");
        }

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            throw new AssertionError("catalog should not be initialized for malformed page-sink session");
        }
    }

    private static BatchTableWrite writer()
    {
        return writer(List.of(), null, null, null);
    }

    private static BatchTableWrite classLoaderCheckingWriter(
            AtomicReference<ClassLoader> writeClassLoader,
            AtomicReference<ClassLoader> prepareCommitClassLoader,
            AtomicReference<ClassLoader> closeClassLoader)
    {
        return (BatchTableWrite) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {BatchTableWrite.class},
                (_, method, _) -> switch (method.getName()) {
                    case "write" -> {
                        writeClassLoader.set(Thread.currentThread().getContextClassLoader());
                        yield null;
                    }
                    case "prepareCommit" -> {
                        prepareCommitClassLoader.set(Thread.currentThread().getContextClassLoader());
                        yield List.of();
                    }
                    case "close" -> {
                        closeClassLoader.set(Thread.currentThread().getContextClassLoader());
                        yield null;
                    }
                    case "toString" -> "classloader-checking-writer";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static BatchTableWrite writer(AtomicReference<MemoryPoolFactory> writeBufferPool)
    {
        return writer(writeBufferPool, new AtomicReference<>());
    }

    private static BatchTableWrite writer(
            AtomicReference<MemoryPoolFactory> writeBufferPool,
            AtomicReference<IOManager> writeIoManager)
    {
        return writer(List.of(), null, null, null, new AtomicReference<>(), writeBufferPool, writeIoManager);
    }

    private static BatchTableWrite writer(RuntimeException closeFailure)
    {
        return writer(List.of(), null, null, closeFailure);
    }

    private static BatchTableWrite writer(List<CommitMessage> commitMessages)
    {
        return writer(commitMessages, null, null, null);
    }

    private static BatchTableWrite writer(
            List<CommitMessage> commitMessages,
            AtomicReference<Object[]> writeArguments)
    {
        return writer(commitMessages, null, null, null, writeArguments);
    }

    private static BatchTableWrite writer(
            List<CommitMessage> commitMessages,
            RuntimeException closeFailure)
    {
        return writer(commitMessages, null, null, closeFailure);
    }

    private static BatchTableWrite writer(
            List<CommitMessage> commitMessages,
            Exception writeFailure,
            Exception prepareFailure,
            Exception closeFailure)
    {
        return writer(commitMessages, writeFailure, prepareFailure, closeFailure, new AtomicReference<>());
    }

    private static BatchTableWrite writer(
            List<CommitMessage> commitMessages,
            Exception writeFailure,
            Exception prepareFailure,
            Exception closeFailure,
            AtomicReference<Object[]> writeArguments)
    {
        return writer(
                commitMessages,
                writeFailure,
                prepareFailure,
                closeFailure,
                writeArguments,
                new AtomicReference<>());
    }

    private static BatchTableWrite writer(
            List<CommitMessage> commitMessages,
            Exception writeFailure,
            Exception prepareFailure,
            Exception closeFailure,
            AtomicReference<Object[]> writeArguments,
            AtomicReference<MemoryPoolFactory> writeBufferPool)
    {
        return writer(
                commitMessages,
                writeFailure,
                prepareFailure,
                closeFailure,
                writeArguments,
                writeBufferPool,
                new AtomicReference<>());
    }

    private static BatchTableWrite writer(
            List<CommitMessage> commitMessages,
            Exception writeFailure,
            Exception prepareFailure,
            Exception closeFailure,
            AtomicReference<Object[]> writeArguments,
            AtomicReference<MemoryPoolFactory> writeBufferPool,
            AtomicReference<IOManager> writeIoManager)
    {
        return writer(
                commitMessages,
                writeFailure,
                prepareFailure,
                closeFailure,
                writeArguments,
                writeBufferPool,
                writeIoManager,
                new AtomicInteger());
    }

    private static BatchTableWrite writer(
            List<CommitMessage> commitMessages,
            Exception writeFailure,
            Exception prepareFailure,
            Exception closeFailure,
            AtomicReference<Object[]> writeArguments,
            AtomicReference<MemoryPoolFactory> writeBufferPool,
            AtomicReference<IOManager> writeIoManager,
            AtomicInteger closeCount)
    {
        return (BatchTableWrite) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {BatchTableWrite.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "withIOManager" -> {
                        writeIoManager.set((IOManager) args[0]);
                        yield proxy;
                    }
                    case "withMemoryPoolFactory" -> {
                        writeBufferPool.set((MemoryPoolFactory) args[0]);
                        yield proxy;
                    }
                    case "write" -> {
                        if (writeFailure != null) {
                            throw writeFailure;
                        }
                        writeArguments.set(args);
                        yield null;
                    }
                    case "prepareCommit" -> {
                        if (prepareFailure != null) {
                            throw prepareFailure;
                        }
                        yield commitMessages;
                    }
                    case "close" -> {
                        closeCount.incrementAndGet();
                        if (closeFailure != null) {
                            throw closeFailure;
                        }
                        yield null;
                    }
                    case "toString" -> "testing-writer";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static BatchTableWrite variantValidatingWriter()
    {
        return (BatchTableWrite) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {BatchTableWrite.class},
                (_, method, args) -> switch (method.getName()) {
                    case "write" -> {
                        ((InternalRow) args[0]).getVariant(0);
                        yield null;
                    }
                    case "prepareCommit" -> List.of();
                    case "close" -> null;
                    case "toString" -> "variant-validating-writer";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static DataType unstableSqlFormattingIntType()
    {
        return new DataType(true, DataTypeRoot.INTEGER)
        {
            @Override
            public int defaultSize()
            {
                return DataTypes.INT().defaultSize();
            }

            @Override
            public DataType copy(boolean isNullable)
            {
                return this;
            }

            @Override
            public String asSQLString()
            {
                throw new IllegalStateException("type SQL rendering failed");
            }

            @Override
            public String toString()
            {
                throw new IllegalStateException("type string rendering failed");
            }

            @Override
            public <R> R accept(DataTypeVisitor<R> visitor)
            {
                return DataTypes.INT().accept(visitor);
            }
        };
    }

    private static BatchTableWrite binaryReadingWriter()
    {
        return (BatchTableWrite) Proxy.newProxyInstance(
                PaimonPageSinkProviderTest.class.getClassLoader(),
                new Class<?>[] {BatchTableWrite.class},
                (_, method, args) -> switch (method.getName()) {
                    case "write" -> {
                        ((InternalRow) args[0]).getBinary(0);
                        yield null;
                    }
                    case "prepareCommit" -> List.of();
                    case "close" -> null;
                    case "toString" -> "binary-reading-writer";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
