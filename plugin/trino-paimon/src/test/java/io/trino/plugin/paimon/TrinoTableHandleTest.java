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

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import io.trino.type.TypeDeserializer;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FullTextSearchTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.VectorSearchTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_CREATION_TIME;
import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_FILE_CREATION_TIME;
import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_SNAPSHOT;
import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_TAG;
import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_TIMESTAMP;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TrinoTableHandleTest
{
    private static final PaimonCatalog TESTING_CATALOG = new PaimonCatalog(new Options(), unsupportedFileSystemFactory());
    private final JsonCodec<PaimonTableHandle> codec = tableHandleJsonCodec();
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
            .build();

    private static JsonCodec<PaimonTableHandle> tableHandleJsonCodec()
    {
        JsonMapperProvider jsonMapperProvider = new JsonMapperProvider();
        jsonMapperProvider
                .setJsonDeserializers(Map.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        return new JsonCodecFactory(jsonMapperProvider).jsonCodec(PaimonTableHandle.class);
    }

    @Test
    public void testPrestoTableHandle()
    {
        PaimonTableHandle expected = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        testRoundTrip(expected);
    }

    @Test
    public void testCreateTableOperationRoundTrip()
    {
        PaimonTableHandle expected = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withCreateTableOperation(PaimonTableHandle.CREATE_TABLE_AS_SELECT_OPERATION);

        testRoundTrip(expected);
    }

    @Test
    public void testTableHandleDoesNotExposeSnapshotOperationType()
    {
        assertThat(Arrays.stream(PaimonTableHandle.class.getDeclaredConstructors())
                .flatMap(constructor -> Arrays.stream(constructor.getGenericParameterTypes()))
                .map(java.lang.reflect.Type::getTypeName))
                .noneMatch(typeName -> typeName.contains("org.apache.paimon.Snapshot$Operation"));
        assertThat(Arrays.stream(PaimonTableHandle.class.getDeclaredMethods())
                .flatMap(TrinoTableHandleTest::methodTypes)
                .map(java.lang.reflect.Type::getTypeName))
                .noneMatch(typeName -> typeName.contains("org.apache.paimon.Snapshot$Operation"));
    }

    @Test
    public void testLegacyTableHandleJsonDefaultsCreateTableOperationToEmpty()
    {
        PaimonTableHandle expected = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = removeJsonField(codec.toJson(expected), "createTableOperation");

        assertThat(codec.fromJson(json).getCreateTableOperation()).isEmpty();
    }

    private static Stream<java.lang.reflect.Type> methodTypes(Method method)
    {
        return Stream.concat(
                Stream.of(method.getGenericReturnType()),
                Arrays.stream(method.getGenericParameterTypes()));
    }

    @Test
    public void testTableWithDynamicOptionsMergesHandleAndSessionOptions()
            throws Exception
    {
        Map<String, String> handleOptions = Map.of("custom.option", "value");
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                handleOptions,
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        Table table = capturingTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        SCAN_TIMESTAMP, 1234L))
                .build();

        assertThat(handle.tableWithDynamicOptions(TESTING_CATALOG, session)).isSameAs(table);
        assertThat(copiedOptions.get())
                .containsEntry("custom.option", "value")
                .containsEntry(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), "1234")
                .doesNotContainKey(CoreOptions.SCAN_SNAPSHOT_ID.key());
    }

    @Test
    public void testDynamicOptionsAreNormalizedOnHandleCreation()
    {
        PaimonTableHandle handle = new PaimonTableHandle("test", "user", Map.ofEntries(
                entry(" " + CoreOptions.SCAN_SNAPSHOT_ID.key() + " ", " 123 "),
                entry(CoreOptions.SCAN_IGNORE_LOST_FILE.key(), " true "),
                entry(CoreOptions.CONSUMER_ID.key(), " consumer-1 "),
                entry("custom.option", " custom value ")));

        assertThat(handle.getDynamicOptions()).containsExactlyInAnyOrderEntriesOf(Map.of(
                CoreOptions.SCAN_SNAPSHOT_ID.key(), "123",
                CoreOptions.SCAN_IGNORE_LOST_FILE.key(), "true",
                CoreOptions.CONSUMER_ID.key(), " consumer-1 ",
                "custom.option", " custom value "));
    }

    @Test
    public void testDuplicateDynamicOptionKeysAreRejectedAfterNormalization()
    {
        assertThatThrownBy(() -> new PaimonTableHandle("test", "user", Map.of(
                CoreOptions.SCAN_IGNORE_LOST_FILE.key(), "true",
                " " + CoreOptions.SCAN_IGNORE_LOST_FILE.key() + " ", "false")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "dynamicOptions contains duplicate key after normalization: '%s'",
                        CoreOptions.SCAN_IGNORE_LOST_FILE.key());
    }

    @Test
    public void testTableWithDynamicOptionsMergesSessionTagOption()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        Table table = capturingTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_TAG, " tag-2 "))
                .build();

        assertThat(handle.tableWithDynamicOptions(TESTING_CATALOG, session)).isSameAs(table);
        assertThat(copiedOptions.get())
                .containsExactlyEntriesOf(Map.of(CoreOptions.SCAN_TAG_NAME.key(), "tag-2"));
    }

    @Test
    public void testTableWithDynamicOptionsDoesNotAddFormatProviderOptionsForFileStoreTables()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of("custom.option", "value"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        FileStoreTable table = capturingReadFileStoreTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_TAG, "tag-2"))
                .build();

        assertThat(handle.tableWithDynamicOptions(TESTING_CATALOG, session)).isSameAs(table);
        assertThat(copiedOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value",
                CoreOptions.SCAN_TAG_NAME.key(), "tag-2"));
    }

    @Test
    public void testTableWithDynamicOptionsUsesPluginContextClassLoader()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        AtomicReference<ClassLoader> copyContextClassLoader = new AtomicReference<>();
        FileStoreTable table = contextCapturingFileStoreTable("copy", copyContextClassLoader);
        setCachedTable(handle, TESTING_CATALOG, table);

        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader sentinel = new ClassLoader(null) {};
        Thread.currentThread().setContextClassLoader(sentinel);
        try {
            ConnectorSession session = TestingConnectorSession.builder()
                    .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                    .setPropertyValues(Map.of(SCAN_TAG, "tag-2"))
                    .build();

            assertThat(handle.tableWithDynamicOptions(TESTING_CATALOG, session)).isSameAs(table);
            assertThat(copyContextClassLoader.get()).isSameAs(PaimonTableHandle.class.getClassLoader());
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(sentinel);
        }
        finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    public void testTableWithDynamicOptionsMergesPaimon15SessionCreationTimeOptions()
            throws Exception
    {
        assertSessionScanSelectionMerged(
                Map.of(SCAN_FILE_CREATION_TIME, 1000L),
                Map.of(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(), "1000"));
        assertSessionScanSelectionMerged(
                Map.of(SCAN_CREATION_TIME, 2000L),
                Map.of(CoreOptions.SCAN_CREATION_TIME_MILLIS.key(), "2000"));
    }

    @Test
    public void testTableWithDynamicOptionsPrefersExplicitTimeTravelSelectionOverSessionProperties()
            throws Exception
    {
        Map<String, String> handleOptions = Map.of(CoreOptions.SCAN_VERSION.key(), "tag-1");
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                handleOptions,
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        Table table = capturingTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        SCAN_TIMESTAMP, 1234L,
                        SCAN_SNAPSHOT, 9L,
                        SCAN_TAG, "tag-2",
                        SCAN_FILE_CREATION_TIME, 1000L,
                        SCAN_CREATION_TIME, 2000L))
                .build();

        assertThat(handle.tableWithDynamicOptions(TESTING_CATALOG, session)).isSameAs(table);
        assertThat(copiedOptions.get())
                .containsExactlyInAnyOrderEntriesOf(Map.of(CoreOptions.SCAN_VERSION.key(), "tag-1"));
    }

    @Test
    public void testTableWithDynamicOptionsPrefersExplicitIncrementalSelectionOverSessionProperties()
            throws Exception
    {
        Map<String, String> handleOptions = Map.of(
                CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2",
                CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta");
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                handleOptions,
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        Table table = capturingTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        SCAN_TIMESTAMP, 1234L,
                        SCAN_SNAPSHOT, 9L,
                        SCAN_FILE_CREATION_TIME, 1000L,
                        SCAN_CREATION_TIME, 2000L))
                .build();

        assertThat(handle.tableWithDynamicOptions(TESTING_CATALOG, session)).isSameAs(table);
        assertThat(copiedOptions.get()).containsExactlyInAnyOrderEntriesOf(handleOptions);
    }

    @Test
    public void testTableWithDynamicOptionsPrefersExplicitIncrementalAutoTagSelectionOverSessionProperties()
            throws Exception
    {
        Map<String, String> handleOptions = Map.of(
                CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), "2024-12-04");
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                handleOptions,
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        Table table = capturingTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        SCAN_TIMESTAMP, 1234L,
                        SCAN_SNAPSHOT, 9L,
                        SCAN_TAG, "tag-2",
                        SCAN_FILE_CREATION_TIME, 1000L,
                        SCAN_CREATION_TIME, 2000L))
                .build();

        assertThat(handle.tableWithDynamicOptions(TESTING_CATALOG, session)).isSameAs(table);
        assertThat(copiedOptions.get()).containsExactlyInAnyOrderEntriesOf(handleOptions);
    }

    @Test
    public void testTableWithDynamicOptionsRejectsConflictingSessionScanPropertiesForOrdinaryScans()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, capturingTable(new AtomicReference<>()));

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        SCAN_TIMESTAMP, 1234L,
                        SCAN_SNAPSHOT, 9L))
                .build();

        assertThatThrownBy(() -> handle.tableWithDynamicOptions(TESTING_CATALOG, session))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_SESSION_PROPERTY.toErrorCode());
                    assertThat(exception).hasMessage(
                            "Only one of %s, %s, %s, %s or %s session properties may be set",
                            SCAN_TIMESTAMP,
                            SCAN_SNAPSHOT,
                            SCAN_TAG,
                            SCAN_FILE_CREATION_TIME,
                            SCAN_CREATION_TIME);
                });
    }

    @Test
    public void testTableWithDynamicOptionsRejectsConflictingPaimon15SessionCreationTimePropertiesForOrdinaryScans()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, capturingTable(new AtomicReference<>()));

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        SCAN_FILE_CREATION_TIME, 1000L,
                        SCAN_CREATION_TIME, 2000L))
                .build();

        assertThatThrownBy(() -> handle.tableWithDynamicOptions(TESTING_CATALOG, session))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_SESSION_PROPERTY.toErrorCode());
                    assertThat(exception).hasMessage(
                            "Only one of %s, %s, %s, %s or %s session properties may be set",
                            SCAN_TIMESTAMP,
                            SCAN_SNAPSHOT,
                            SCAN_TAG,
                            SCAN_FILE_CREATION_TIME,
                            SCAN_CREATION_TIME);
                });
    }

    @Test
    public void testTableWithWriteDynamicOptionsDropsStartupSelections()
            throws Exception
    {
        assertWriteDynamicOptionsDropsReadOptions(Map.of(CoreOptions.SCAN_VERSION.key(), "tag-1"));
        assertWriteDynamicOptionsDropsReadOptions(Map.of(CoreOptions.STREAM_SCAN_MODE.key(), "FROM_SNAPSHOT"));
        assertWriteDynamicOptionsDropsReadOptions(Map.of(CoreOptions.BATCH_SCAN_MODE.key(), "compact"));
        assertWriteDynamicOptionsDropsReadOptions(Map.of(CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"));
        assertWriteDynamicOptionsDropsReadOptions(Map.of(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(), "1000"));
        assertWriteDynamicOptionsDropsReadOptions(Map.of(CoreOptions.SCAN_CREATION_TIME_MILLIS.key(), "2000"));
    }

    @Test
    public void testTableWithWriteDynamicOptionsDropsIncrementalAutoTagSelection()
            throws Exception
    {
        Map<String, String> handleOptions = Map.of(
                "custom.option", "value",
                CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), "2024-12-04");
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                handleOptions,
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        FileStoreTable table = capturingFileStoreTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        assertThat(handle.tableWithWriteDynamicOptions(TESTING_CATALOG)).isSameAs(table);
        assertThat(copiedOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
    }

    @Test
    public void testTableWithWriteDynamicOptionsUsesPluginContextClassLoader()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of("custom.option", "value"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        AtomicReference<ClassLoader> copyContextClassLoader = new AtomicReference<>();
        FileStoreTable table = contextCapturingFileStoreTable("copyWithoutTimeTravel", copyContextClassLoader);
        setCachedTable(handle, TESTING_CATALOG, table);

        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader sentinel = new ClassLoader(null) {};
        Thread.currentThread().setContextClassLoader(sentinel);
        try {
            assertThat(handle.tableWithWriteDynamicOptions(TESTING_CATALOG)).isSameAs(table);
            assertThat(copyContextClassLoader.get()).isSameAs(PaimonTableHandle.class.getClassLoader());
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(sentinel);
        }
        finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private static void assertSessionScanSelectionMerged(
            Map<String, Object> sessionProperties,
            Map<String, String> expectedDynamicOptions)
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of("custom.option", "value"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        Table table = capturingTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(sessionProperties)
                .build();

        Map<String, String> expectedOptions = new HashMap<>(expectedDynamicOptions);
        expectedOptions.put("custom.option", "value");
        assertThat(handle.tableWithDynamicOptions(TESTING_CATALOG, session)).isSameAs(table);
        assertThat(copiedOptions.get()).containsExactlyInAnyOrderEntriesOf(expectedOptions);
    }

    private static void assertWriteDynamicOptionsDropsReadOptions(Map<String, String> readOptions)
            throws Exception
    {
        Map<String, String> handleOptions = new HashMap<>(readOptions);
        handleOptions.put("custom.option", "value");
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                handleOptions,
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        FileStoreTable table = capturingFileStoreTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        assertThat(handle.tableWithWriteDynamicOptions(TESTING_CATALOG)).isSameAs(table);
        assertThat(copiedOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
    }

    @Test
    public void testTableWithWriteDynamicOptionsDropsIncrementalReadOnlyAuxiliaryOptions()
            throws Exception
    {
        Map<String, String> handleOptions = Map.of(
                "custom.option", "value",
                CoreOptions.INCREMENTAL_BETWEEN.key(), "tag-a,tag-b",
                CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta",
                CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true");
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                handleOptions,
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        FileStoreTable table = capturingFileStoreTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        assertThat(handle.tableWithWriteDynamicOptions(TESTING_CATALOG)).isSameAs(table);
        assertThat(copiedOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
    }

    @Test
    public void testTableWithWriteDynamicOptionsDropsRuntimeScanOptions()
            throws Exception
    {
        Map<String, String> handleOptions = Map.ofEntries(
                entry("custom.option", "value"),
                entry(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key(), "snapshot_branch"),
                entry(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key(), "delta_branch"),
                entry(CoreOptions.SCAN_FALLBACK_BRANCH.key(), "fallback_branch"),
                entry(CoreOptions.SCAN_FALLBACK_BRANCH_READ_FAIL_FAST.key(), "true"),
                entry(CoreOptions.SCAN_IGNORE_LOST_FILE.key(), "true"),
                entry(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "4"),
                entry(CoreOptions.SCAN_MAX_SPLITS_PER_TASK.key(), "32"),
                entry(CoreOptions.SCAN_PRIMARY_BRANCH.key(), "primary_branch"),
                entry(CoreOptions.STREAMING_READ_OVERWRITE.key(), "true"),
                entry(CoreOptions.CONSUMER_ID.key(), "streaming-job"),
                entry(CoreOptions.CONSUMER_IGNORE_PROGRESS.key(), "true"));
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                handleOptions,
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        FileStoreTable table = capturingFileStoreTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        assertThat(handle.tableWithWriteDynamicOptions(TESTING_CATALOG)).isSameAs(table);
        assertThat(copiedOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
    }

    @Test
    public void testTableWithWriteDynamicOptionsDoesNotAddFormatOverrideForVariantWriteColumns()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of("custom.option", "value"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withWriteColumns(List.of(
                        PaimonColumnHandle.of("id", DataTypes.INT()),
                        PaimonColumnHandle.of("payload", DataTypes.VARIANT(), TESTING_TYPE_MANAGER)));

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        FileStoreTable table = capturingFileStoreTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        assertThat(handle.tableWithWriteDynamicOptions(TESTING_CATALOG)).isSameAs(table);
        assertThat(copiedOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
    }

    @Test
    public void testTableWithWriteDynamicOptionsDoesNotAddFormatOverrideForNestedVariantWriteColumns()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of("custom.option", "value"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withWriteColumns(List.of(
                        PaimonColumnHandle.of("id", DataTypes.INT()),
                        PaimonColumnHandle.of(
                                "payloads",
                                DataTypes.ARRAY(DataTypes.VARIANT()),
                                TESTING_TYPE_MANAGER)));

        AtomicReference<Map<String, String>> copiedOptions = new AtomicReference<>();
        FileStoreTable table = capturingFileStoreTable(copiedOptions);
        setCachedTable(handle, TESTING_CATALOG, table);

        assertThat(handle.tableWithWriteDynamicOptions(TESTING_CATALOG)).isSameAs(table);
        assertThat(copiedOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
    }

    @Test
    public void testTableWithWriteDynamicOptionsKeepsNonFileStoreTableUntouched()
            throws Exception
    {
        Table table = tableWithComment("table comment");
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(CoreOptions.SCAN_VERSION.key(), "2"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, table);

        assertThat(handle.tableWithWriteDynamicOptions(TESTING_CATALOG)).isSameAs(table);
    }

    @Test
    public void testHistoricalReadSchemaRecognizesExplicitVersionSelection()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(CoreOptions.SCAN_VERSION.key(), "tag-1"));

        assertThat(handle.usesHistoricalReadSchema(SESSION)).isTrue();
    }

    @Test
    public void testHistoricalReadSchemaRecognizesSessionSnapshotSelection()
    {
        PaimonTableHandle handle = new PaimonTableHandle("test", "user", Map.of());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_SNAPSHOT, 9L))
                .build();

        assertThat(handle.usesHistoricalReadSchema(session)).isTrue();
    }

    @Test
    public void testHistoricalReadSchemaRecognizesPaimon15SessionCreationTimeSelection()
    {
        PaimonTableHandle handle = new PaimonTableHandle("test", "user", Map.of());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_CREATION_TIME, 2000L))
                .build();

        assertThat(handle.usesHistoricalReadSchema(session)).isTrue();
    }

    @Test
    public void testSearchWrapperTablesFailFast()
            throws Exception
    {
        PaimonTableHandle vectorSearchHandle = new PaimonTableHandle(
                "test",
                "vector_search",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(vectorSearchHandle, TESTING_CATALOG, VectorSearchTable.create(
                innerTable(),
                new VectorSearch(new float[] {1.0f}, 1, "embedding")));

        assertThatThrownBy(() -> vectorSearchHandle.table(TESTING_CATALOG))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon vector search tables are not supported by the Trino connector");
                });

        PaimonTableHandle fullTextSearchHandle = new PaimonTableHandle(
                "test",
                "full_text_search",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(fullTextSearchHandle, TESTING_CATALOG, FullTextSearchTable.create(
                innerTable(),
                new FullTextSearch("content", "paimon", 1)));

        assertThatThrownBy(() -> fullTextSearchHandle.table(TESTING_CATALOG))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon full-text search tables are not supported by the Trino connector");
                });
    }

    @Test
    public void testStaleTableHandleReportsTableNotFound()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "missing",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> handle.table(new MissingTableCatalog()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(TABLE_NOT_FOUND.toErrorCode());
                    assertThat(exception).hasMessage("Paimon table 'test.missing' does not exist");
                });
    }

    @Test
    public void testMissingColumnReportsColumnNotFound()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, tableWithColumns());

        assertThatThrownBy(() -> handle.columnHandle(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION, "missing"))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(COLUMN_NOT_FOUND.toErrorCode());
                    assertThat(exception).hasMessage("Column 'missing' does not exist in Paimon table 'test.user'");
                });
    }

    @Test
    public void testColumnHandleRejectsCaseInsensitiveDuplicateFieldNames()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, tableWithRowType(DataTypes.ROW(
                DataTypes.FIELD(0, "ID", DataTypes.INT()),
                DataTypes.FIELD(1, "id", DataTypes.STRING()))));

        assertThatThrownBy(() -> handle.columnHandle(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION, "id"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon row type contains case-insensitive duplicate field name 'id'");
    }

    @Test
    public void testColumnHandleUnsupportedTypeWithoutMessageReportsStableNotSupported()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, tableWithRowType(DataTypes.ROW(
                DataTypes.FIELD(0, "id", unsupportedDataTypeWithoutMessage()))));

        assertThatThrownBy(() -> handle.columnHandle(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION, "id"))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception)
                            .hasMessage("Unsupported Paimon column 'id' with type UNSUPPORTED_TEST_TYPE: UnsupportedOperationException");
                });
    }

    @Test
    public void testTableMetadataIncludesPaimonTableComment()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, tableWithComment("table comment"));

        assertThat(handle.tableMetadata(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION).getComment())
                .contains("table comment");
    }

    @Test
    public void testTableMetadataIgnoresEmptyPaimonComments()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, tableWithRowTypeAndComment(
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT(), "")),
                ""));

        var metadata = handle.tableMetadata(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION);
        assertThat(metadata.getComment())
                .isEmpty();
        assertThat(metadata.getColumns().get(0).getComment())
                .isEmpty();
    }

    @Test
    public void testTableMetadataRefreshesLatestFileStoreSchema()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        AtomicReference<Boolean> copiedWithLatestSchema = new AtomicReference<>(false);
        setCachedTable(handle, TESTING_CATALOG, staleFileStoreTable(
                copiedWithLatestSchema,
                DataTypes.ROW(DataTypes.FIELD(0, "old_id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "new_id", DataTypes.BIGINT()))));

        ConnectorTableMetadata metadata = handle.tableMetadata(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION);

        assertThat(copiedWithLatestSchema.get()).isTrue();
        assertThat(metadata.getColumns()).extracting(ColumnMetadata::getName).containsExactly("new_id");
        assertThat(metadata.getColumns()).extracting(ColumnMetadata::getType).containsExactly(BIGINT);
    }

    @Test
    public void testTableMetadataReflectsSchemaOptionsWithoutLeakingReadDynamicOptions()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(CoreOptions.SCAN_VERSION.key(), "tag-1"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        AtomicReference<Boolean> copiedWithLatestSchema = new AtomicReference<>(false);
        setCachedTable(handle, TESTING_CATALOG, fileStoreTableWithOptions(
                copiedWithLatestSchema,
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "pt", DataTypes.STRING())),
                Map.of(
                        CoreOptions.BUCKET.key(), "7",
                        CoreOptions.BUCKET_KEY.key(), "id",
                        CoreOptions.VECTOR_FILE_FORMAT.key(), "lance"),
                List.of("id"),
                List.of("pt")));

        ConnectorTableMetadata metadata = handle.tableMetadata(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION);

        assertThat(copiedWithLatestSchema.get()).isFalse();
        assertThat(metadata.getProperties())
                .containsEntry(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("id"))
                .containsEntry(PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of("pt"))
                .containsEntry("bucket", "7")
                .containsEntry("bucket_key", "id")
                .containsEntry("vector_file_format", "lance")
                .doesNotContainKey("scan_version");
    }

    @Test
    public void testTableMetadataUnsupportedTypeWithoutMessageReportsStableNotSupported()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, tableWithRowType(DataTypes.ROW(
                DataTypes.FIELD(0, "id", unsupportedDataTypeWithoutMessage()))));

        assertThatThrownBy(() -> handle.tableMetadata(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception)
                            .hasMessage("Unsupported Paimon type UNSUPPORTED_TEST_TYPE: UnsupportedOperationException");
                });
    }

    @Test
    public void testTableMetadataUnsupportedTypeWithBrokenTypeStringReportsStableNotSupported()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, tableWithRowType(DataTypes.ROW(
                DataTypes.FIELD(0, "id", unsupportedDataTypeWithBrokenSqlString()))));

        assertThatThrownBy(() -> handle.tableMetadata(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception)
                            .hasMessage("Unsupported Paimon type io.trino.plugin.paimon.TrinoTableHandleTest$UnsupportedTestingDataType: UnsupportedOperationException");
                });
    }

    @Test
    public void testTableUsesPluginContextClassLoader()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of("custom.option", "value"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        AtomicReference<ClassLoader> copyContextClassLoader = new AtomicReference<>();
        FileStoreTable table = contextCapturingFileStoreTable("copy", copyContextClassLoader);
        setCachedTable(handle, TESTING_CATALOG, table);

        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader sentinel = new ClassLoader(null) {};
        Thread.currentThread().setContextClassLoader(sentinel);
        try {
            assertThat(handle.table(TESTING_CATALOG)).isSameAs(table);
            assertThat(copyContextClassLoader.get()).isSameAs(PaimonTableHandle.class.getClassLoader());
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(sentinel);
        }
        finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    public void testColumnHandleRefreshesLatestFileStoreSchema()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        AtomicReference<Boolean> copiedWithLatestSchema = new AtomicReference<>(false);
        setCachedTable(handle, TESTING_CATALOG, staleFileStoreTable(
                copiedWithLatestSchema,
                DataTypes.ROW(DataTypes.FIELD(0, "old_id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "new_id", DataTypes.BIGINT()))));

        PaimonColumnHandle columnHandle = handle.columnHandle(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION, "new_id");

        assertThat(copiedWithLatestSchema.get()).isTrue();
        assertThat(columnHandle.getColumnName()).isEqualTo("new_id");
        assertThat(columnHandle.getTrinoType()).isEqualTo(BIGINT);
    }

    @Test
    public void testMetadataLookupDoesNotRefreshNonFileStoreTable()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        Table table = tableWithRowType(DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())));
        setCachedTable(handle, TESTING_CATALOG, table);

        assertThat(handle.columnMetadatas(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION))
                .extracting(ColumnMetadata::getName)
                .containsExactly("id");
    }

    @Test
    public void testTableHandleRuntimeDependenciesAreRequired()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, tableWithColumns());

        assertThatThrownBy(() -> handle.table(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("catalog is null");
        assertThatThrownBy(() -> handle.tableWithDynamicOptions(null, TestingConnectorSession.SESSION))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("catalog is null");
        assertThatThrownBy(() -> handle.tableWithDynamicOptions(TESTING_CATALOG, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> handle.tableMetadata(TESTING_CATALOG, null, SESSION))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("typeManager is null");
        assertThatThrownBy(() -> handle.columnMetadatas(TESTING_CATALOG, null, SESSION))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("typeManager is null");
        assertThatThrownBy(() -> handle.columnHandle(TESTING_CATALOG, null, SESSION, "id"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("typeManager is null");
        assertThatThrownBy(() -> handle.columnHandle(TESTING_CATALOG, TESTING_TYPE_MANAGER, SESSION, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("field is null");
    }

    @Test
    public void testMergeTableHandleRejectsMissingTableHandle()
    {
        JsonCodec<PaimonMergeTableHandle> mergeHandleCodec = new JsonCodecFactory(new JsonMapperProvider())
                .jsonCodec(PaimonMergeTableHandle.class);

        assertThatThrownBy(() -> mergeHandleCodec.fromJson("{}"))
                .rootCause()
                .hasMessageContaining("Missing required creator property 'tableHandle'");
    }

    @Test
    public void testMergeTableHandleRejectsMissingMetadataDeleteFallback()
    {
        JsonCodec<PaimonMergeTableHandle> mergeHandleCodec = new JsonCodecFactory(new JsonMapperProvider())
                .jsonCodec(PaimonMergeTableHandle.class);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = removeJsonField(
                mergeHandleCodec.toJson(new PaimonMergeTableHandle(tableHandle)),
                "metadataDeleteFallback");

        assertThatThrownBy(() -> mergeHandleCodec.fromJson(json))
                .rootCause()
                .hasMessageContaining("Missing required creator property 'metadataDeleteFallback'");
    }

    @Test
    public void testMergeTableHandleRoundTripsMetadataDeleteFallback()
    {
        JsonCodec<PaimonMergeTableHandle> mergeHandleCodec = new JsonCodecFactory(new JsonMapperProvider())
                .jsonCodec(PaimonMergeTableHandle.class);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        PaimonMergeTableHandle handle = mergeHandleCodec.fromJson(
                mergeHandleCodec.toJson(PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle)));

        assertThat(handle.getTableHandle()).isEqualTo(tableHandle);
        assertThat(handle.isMetadataDeleteFallback()).isTrue();
    }

    @Test
    public void testMergeTableHandleRejectsUnknownJsonFields()
    {
        JsonCodec<PaimonMergeTableHandle> mergeHandleCodec = new JsonCodecFactory(new JsonMapperProvider())
                .jsonCodec(PaimonMergeTableHandle.class);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = appendJsonField(
                mergeHandleCodec.toJson(new PaimonMergeTableHandle(tableHandle)),
                "\"unexpectedField\":true");

        assertThatThrownBy(() -> mergeHandleCodec.fromJson(json))
                .hasRootCauseMessage("Unknown PaimonMergeTableHandle JSON field: unexpectedField");
    }

    @Test
    public void testMergeTableHandleAcceptsTrinoTypedJsonField()
    {
        JsonCodec<PaimonMergeTableHandle> mergeHandleCodec = new JsonCodecFactory(new JsonMapperProvider())
                .jsonCodec(PaimonMergeTableHandle.class);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = appendJsonField(
                mergeHandleCodec.toJson(new PaimonMergeTableHandle(tableHandle)),
                "\"@type\":\"%s\"".formatted(typedHandleId(PaimonMergeTableHandle.class)));

        assertThat(mergeHandleCodec.fromJson(json).getTableHandle()).isEqualTo(tableHandle);
    }

    @Test
    public void testMergeTableHandleRejectsInvalidTrinoTypedJsonField()
    {
        JsonCodec<PaimonMergeTableHandle> mergeHandleCodec = new JsonCodecFactory(new JsonMapperProvider())
                .jsonCodec(PaimonMergeTableHandle.class);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = appendJsonField(
                mergeHandleCodec.toJson(new PaimonMergeTableHandle(tableHandle)),
                "\"@type\":{\"name\":\"paimon\"}");

        assertThatThrownBy(() -> mergeHandleCodec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonMergeTableHandle JSON @type field");
    }

    @Test
    public void testMergeTableHandleRejectsConnectorNameOnlyTypedJsonField()
    {
        JsonCodec<PaimonMergeTableHandle> mergeHandleCodec = new JsonCodecFactory(new JsonMapperProvider())
                .jsonCodec(PaimonMergeTableHandle.class);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = appendJsonField(
                mergeHandleCodec.toJson(new PaimonMergeTableHandle(tableHandle)),
                "\"@type\":\"paimon\"");

        assertThatThrownBy(() -> mergeHandleCodec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonMergeTableHandle JSON @type field");
    }

    @Test
    public void testTableHandleRejectsUnknownJsonFields()
    {
        PaimonTableHandle expected = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = appendJsonField(codec.toJson(expected), "\"unexpectedField\":true");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Unknown PaimonTableHandle JSON field: unexpectedField");
    }

    @Test
    public void testTableHandleAcceptsTrinoTypedJsonField()
    {
        PaimonTableHandle expected = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = appendJsonField(codec.toJson(expected), "\"@type\":\"%s\"".formatted(typedHandleId(PaimonTableHandle.class)));

        assertThat(codec.fromJson(json)).isEqualTo(expected);
    }

    @Test
    public void testTableHandleRejectsInvalidTrinoTypedJsonField()
    {
        PaimonTableHandle expected = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = appendJsonField(codec.toJson(expected), "\"@type\":[\"paimon\"]");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonTableHandle JSON @type field");
    }

    @Test
    public void testTableHandleRejectsConnectorNameOnlyTypedJsonField()
    {
        PaimonTableHandle expected = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = appendJsonField(codec.toJson(expected), "\"@type\":\"paimon\"");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonTableHandle JSON @type field");
    }

    @Test
    public void testTableHandleAcceptsTrinoTypedJsonFieldInWriteColumns()
    {
        List<ColumnHandle> writeColumns = List.of(
                PaimonColumnHandle.of("id", DataTypes.INT()),
                PaimonColumnHandle.of("name", DataTypes.STRING()));
        PaimonTableHandle expected = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withWriteColumns(writeColumns);
        String json = codec.toJson(expected)
                .replace("\"columnName\":\"id\"",
                        "\"@type\":\"%s\",\"columnName\":\"id\"".formatted(typedHandleId(PaimonColumnHandle.class)));

        assertThat(codec.fromJson(json)).isEqualTo(expected);
    }

    @Test
    public void testTableCacheIsNotSerialized()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        setCachedTable(handle, TESTING_CATALOG, capturingTable(new AtomicReference<>()));

        assertThat(codec.toJson(handle)).doesNotContain("tablesByCatalog");
    }

    private void testRoundTrip(PaimonTableHandle expected)
    {
        String json = codec.toJson(expected);
        PaimonTableHandle actual = codec.fromJson(json);
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.getSchemaName()).isEqualTo(expected.getSchemaName());
        assertThat(actual.getTableName()).isEqualTo(expected.getTableName());
        assertThat(actual.getFilter()).isEqualTo(expected.getFilter());
        assertThat(actual.getProjectedColumns()).isEqualTo(expected.getProjectedColumns());
        assertThat(actual.getWriteColumns()).isEqualTo(expected.getWriteColumns());
        assertThat(actual.getLimit()).isEqualTo(expected.getLimit());
        assertThat(actual.getDeletePartitionSpecs()).isEqualTo(expected.getDeletePartitionSpecs());
        assertThat(actual.getCreateTableOperation()).isEqualTo(expected.getCreateTableOperation());
    }

    private static String appendJsonField(String json, String field)
    {
        return json.substring(0, json.length() - 1) + "," + field + "}";
    }

    private static String removeJsonField(String json, String fieldName)
    {
        int fieldStart = findTopLevelJsonField(json, fieldName);
        int valueStart = json.indexOf(':', fieldStart) + 1;
        int fieldEnd = findJsonValueEnd(json, valueStart);

        int removeStart = fieldStart;
        int removeEnd = fieldEnd;
        if (fieldStart > 1 && json.charAt(fieldStart - 1) == ',') {
            removeStart = fieldStart - 1;
        }
        else if (fieldEnd < json.length() - 1 && json.charAt(fieldEnd) == ',') {
            removeEnd = fieldEnd + 1;
        }
        return json.substring(0, removeStart) + json.substring(removeEnd);
    }

    private static String replaceJsonField(String json, String fieldName, String replacementValue)
    {
        int fieldStart = findTopLevelJsonField(json, fieldName);
        int valueStart = json.indexOf(':', fieldStart) + 1;
        int fieldEnd = findJsonValueEnd(json, valueStart);
        return json.substring(0, valueStart) + replacementValue + json.substring(fieldEnd);
    }

    private static int findTopLevelJsonField(String json, String fieldName)
    {
        String quotedField = "\"" + fieldName + "\"";
        boolean inString = false;
        boolean escaped = false;
        int depth = 0;
        for (int index = 0; index < json.length(); index++) {
            char value = json.charAt(index);
            if (inString) {
                if (escaped) {
                    escaped = false;
                }
                else if (value == '\\') {
                    escaped = true;
                }
                else if (value == '"') {
                    inString = false;
                }
                continue;
            }
            if (value == '"') {
                if (depth == 1 && json.startsWith(quotedField, index)) {
                    return index;
                }
                inString = true;
            }
            else if (value == '{' || value == '[') {
                depth++;
            }
            else if (value == '}' || value == ']') {
                depth--;
            }
        }
        throw new IllegalArgumentException("JSON field not found: " + fieldName);
    }

    private static int findJsonValueEnd(String json, int valueStart)
    {
        boolean inString = false;
        boolean escaped = false;
        int depth = 0;
        for (int index = valueStart; index < json.length(); index++) {
            char value = json.charAt(index);
            if (inString) {
                if (escaped) {
                    escaped = false;
                }
                else if (value == '\\') {
                    escaped = true;
                }
                else if (value == '"') {
                    inString = false;
                }
                continue;
            }
            if (value == '"') {
                inString = true;
            }
            else if (value == '{' || value == '[') {
                depth++;
            }
            else if (value == '}' || value == ']') {
                if (depth == 0) {
                    return index;
                }
                depth--;
            }
            else if (value == ',' && depth == 0) {
                return index;
            }
        }
        return json.length() - 1;
    }

    private static String typedHandleId(Class<?> handleClass)
    {
        return "paimon:" + handleClass.getName();
    }

    @Test
    public void testWriteColumnsRoundTrip()
    {
        List<ColumnHandle> writeColumns = List.of(
                PaimonColumnHandle.of("id", DataTypes.INT()),
                PaimonColumnHandle.of("name", DataTypes.STRING()));
        PaimonTableHandle expected = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withWriteColumns(writeColumns);

        testRoundTrip(expected);
    }

    @Test
    public void testEmptyWriteColumnsFailFast()
    {
        PaimonTableHandle missingWriteColumns = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> missingWriteColumns.withWriteColumns(List.of()))
                .hasMessage("writeColumns is empty");

        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.of(List.<PaimonColumnHandle>of()),
                OptionalLong.empty()))
                .hasMessage("writeColumns is empty");

        assertThatThrownBy(() -> PaimonPageSinkProvider.getWriteColumns(missingWriteColumns))
                .hasMessage("Paimon page sink requires explicit write columns");
    }

    @Test
    public void testJsonRejectsEmptyWriteColumns()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> codec.fromJson(replaceJsonField(codec.toJson(handle), "writeColumns", "[]")))
                .hasRootCauseMessage("writeColumns is empty");
    }

    @Test
    public void testWriteColumnsRejectNulls()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> handle.withWriteColumns(null))
                .hasMessage("writeColumns is null");
        assertThatThrownBy(() -> handle.withWriteColumns(Collections.singletonList(null)))
                .hasMessage("column is null");
    }

    @Test
    public void testWriteColumnsRequirePaimonColumnHandles()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        ColumnHandle wrongColumn = new ColumnHandle() {};

        assertThatThrownBy(() -> handle.withWriteColumns(List.of(wrongColumn)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon table handle requires PaimonColumnHandle, got: %s",
                        wrongColumn.getClass().getName());
    }

    @Test
    public void testTableHandleRejectsNullDynamicOptions()
    {
        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Collections.singletonMap("", "value"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .hasMessage("dynamicOptions contains blank key");

        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Collections.singletonMap(null, "value"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .hasMessage("dynamicOptions contains null key");

        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Collections.singletonMap("scan.tag-name", null),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .hasMessage("dynamicOptions contains null value for key 'scan.tag-name'");

        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Collections.singletonMap("scan.tag-name", " "),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .hasMessage("dynamicOptions contains blank value for key 'scan.tag-name'");
    }

    @Test
    public void testTableHandleRejectsConflictingStartupSelections()
    {
        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Map.of(
                        CoreOptions.SCAN_SNAPSHOT_ID.key(), "1",
                        CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), "2024-12-04"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicOptions may contain only one startup selection, got keys: [incremental-to-auto-tag, scan.snapshot-id]");
    }

    @Test
    public void testTableHandleRejectsRawScanModeDynamicOption()
    {
        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Map.of(
                        CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.LATEST.toString()),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicOptions key 'scan.mode' is not supported; use explicit scan selector keys instead");
    }

    @Test
    public void testTableHandleRejectsPaimon15CreationTimeStartupSelectionConflicts()
    {
        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Map.of(
                        CoreOptions.SCAN_CREATION_TIME_MILLIS.key(), "1000",
                        CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(), "2000"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicOptions may contain only one startup selection, got keys: [scan.creation-time-millis, scan.file-creation-time-millis]");
    }

    @Test
    public void testJsonRejectsConflictingStartupSelections()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = replaceJsonField(
                codec.toJson(handle),
                "dynamicOptions",
                "{\"scan.snapshot-id\":\"1\",\"incremental-to-auto-tag\":\"2024-12-04\"}");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("dynamicOptions may contain only one startup selection, got keys: [incremental-to-auto-tag, scan.snapshot-id]");
    }

    @Test
    public void testJsonRejectsRawScanModeDynamicOption()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = replaceJsonField(
                codec.toJson(handle),
                "dynamicOptions",
                "{\"scan.mode\":\"latest\"}");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("dynamicOptions key 'scan.mode' is not supported; use explicit scan selector keys instead");
    }

    @Test
    public void testTableHandleRejectsIncrementalScanModeWithoutIncrementalWindow()
    {
        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Map.of(
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicOptions key 'incremental-between-scan-mode' requires 'incremental-between' or 'incremental-between-timestamp'");
    }

    @Test
    public void testTableHandleRejectsIncrementalTagToSnapshotWithoutIncrementalBetween()
    {
        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Map.of(
                        CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicOptions key 'incremental-between-tag-to-snapshot' requires 'incremental-between'");
    }

    @Test
    public void testTableHandleRejectsInvalidIncrementalAuxiliaryOptionValues()
    {
        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Map.of(
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2",
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "invalid"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicOptions contains invalid value for key 'incremental-between-scan-mode'");

        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Map.of(
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2",
                        CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "not-a-boolean"),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicOptions contains invalid value for key 'incremental-between-tag-to-snapshot'");
    }

    @Test
    public void testTableHandleRejectsBlankSchemaAndTableNames()
    {
        assertThatThrownBy(() -> new PaimonTableHandle(
                "",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .hasMessage("schemaName is blank");

        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()))
                .hasMessage("tableName is blank");
    }

    @Test
    public void testTableHandleRejectsNullProjectionAndWriteColumns()
    {
        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.of(Collections.singletonList(null)),
                Optional.empty(),
                OptionalLong.empty()))
                .hasMessage("projectedColumns contains null column");

        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.of(Collections.singletonList(null)),
                OptionalLong.empty()))
                .hasMessage("writeColumns contains null column");
    }

    @Test
    public void testTableHandleRejectsWrongProjectionAndWriteColumnTypes()
    {
        ColumnHandle wrongColumn = new ColumnHandle() {};
        @SuppressWarnings({"unchecked", "rawtypes"})
        Optional<List<PaimonColumnHandle>> projectedColumns = (Optional) Optional.of(List.of(wrongColumn));
        @SuppressWarnings({"unchecked", "rawtypes"})
        Optional<List<PaimonColumnHandle>> writeColumns = (Optional) Optional.of(List.of(wrongColumn));

        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                projectedColumns,
                Optional.empty(),
                OptionalLong.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "projectedColumns requires PaimonColumnHandle, got: %s",
                        wrongColumn.getClass().getName());

        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                writeColumns,
                OptionalLong.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "writeColumns requires PaimonColumnHandle, got: %s",
                        wrongColumn.getClass().getName());
    }

    @Test
    public void testJsonRejectsNullDynamicOptionValue()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> codec.fromJson(replaceJsonField(codec.toJson(handle), "dynamicOptions", "{\"\":\"value\"}")))
                .hasRootCauseMessage("dynamicOptions contains blank key");

        String json = replaceJsonField(codec.toJson(handle), "dynamicOptions", "{\"scan.tag-name\":null}");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("dynamicOptions contains null value for key 'scan.tag-name'");

        assertThatThrownBy(() -> codec.fromJson(replaceJsonField(codec.toJson(handle), "dynamicOptions", "{\"scan.tag-name\":\" \"}")))
                .hasRootCauseMessage("dynamicOptions contains blank value for key 'scan.tag-name'");
    }

    @Test
    public void testJsonRejectsNullProjectionAndWriteColumnElements()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThatThrownBy(() -> codec.fromJson(replaceJsonField(codec.toJson(handle), "projectedColumns", "[null]")))
                .hasRootCauseMessage("projectedColumns contains null column");
        assertThatThrownBy(() -> codec.fromJson(replaceJsonField(codec.toJson(handle), "writeColumns", "[null]")))
                .hasRootCauseMessage("writeColumns contains null column");
    }

    @Test
    public void testJsonMissingPlanFieldsFails()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        String json = codec.toJson(handle);

        assertMissingTableHandleJsonField(json, "schemaName");
        assertMissingTableHandleJsonField(json, "tableName");
        assertMissingTableHandleJsonField(json, "dynamicOptions");
        assertMissingTableHandleJsonField(json, "filter");
        assertMissingTableHandleJsonField(json, "projectedColumns");
        assertMissingTableHandleJsonField(json, "writeColumns");
        assertMissingTableHandleJsonField(json, "limit");
    }

    @Test
    public void testNegativeLimitFailsFast()
    {
        assertThatThrownBy(() -> new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(-1)))
                .hasMessage("limit must be non-negative");

        PaimonTableHandle handle = new PaimonTableHandle(
                "test",
                "user",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(1));
        String json = codec.toJson(handle).replace("\"limit\":1", "\"limit\":-1");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("limit must be non-negative");
    }

    private void assertMissingTableHandleJsonField(String json, String fieldName)
    {
        assertThatThrownBy(() -> codec.fromJson(removeJsonField(json, fieldName)))
                .rootCause()
                .hasMessageContaining("Missing required creator property '%s'".formatted(fieldName));
    }

    private static Table capturingTable(AtomicReference<Map<String, String>> copiedOptions)
    {
        AtomicReference<Table> tableReference = new AtomicReference<>();
        Table table = (Table) Proxy.newProxyInstance(Table.class.getClassLoader(), new Class<?>[] {Table.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("copy")) {
                        copiedOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        return tableReference.get();
                    }
                    if (method.getName().equals("toString")) {
                        return "capturingTable";
                    }
                    if (method.getName().equals("hashCode")) {
                        return System.identityHashCode(proxy);
                    }
                    if (method.getName().equals("equals")) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        tableReference.set(table);
        return table;
    }

    private static FileStoreTable capturingFileStoreTable(AtomicReference<Map<String, String>> copiedOptions)
    {
        AtomicReference<FileStoreTable> tableReference = new AtomicReference<>();
        FileStoreTable table = (FileStoreTable) Proxy.newProxyInstance(
                FileStoreTable.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("copyWithoutTimeTravel")) {
                        copiedOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        return tableReference.get();
                    }
                    if (method.getName().equals("toString")) {
                        return "capturingFileStoreTable";
                    }
                    if (method.getName().equals("hashCode")) {
                        return System.identityHashCode(proxy);
                    }
                    if (method.getName().equals("equals")) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        tableReference.set(table);
        return table;
    }

    private static FileStoreTable capturingReadFileStoreTable(AtomicReference<Map<String, String>> copiedOptions)
    {
        AtomicReference<FileStoreTable> tableReference = new AtomicReference<>();
        FileStoreTable table = (FileStoreTable) Proxy.newProxyInstance(
                FileStoreTable.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("copy")) {
                        copiedOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        return tableReference.get();
                    }
                    if (method.getName().equals("toString")) {
                        return "capturingReadFileStoreTable";
                    }
                    if (method.getName().equals("hashCode")) {
                        return System.identityHashCode(proxy);
                    }
                    if (method.getName().equals("equals")) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        tableReference.set(table);
        return table;
    }

    private static FileStoreTable contextCapturingFileStoreTable(String copyMethodName, AtomicReference<ClassLoader> copyContextClassLoader)
    {
        AtomicReference<FileStoreTable> tableReference = new AtomicReference<>();
        FileStoreTable table = (FileStoreTable) Proxy.newProxyInstance(
                FileStoreTable.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> {
                    if (method.getName().equals(copyMethodName)) {
                        copyContextClassLoader.set(Thread.currentThread().getContextClassLoader());
                        return tableReference.get();
                    }
                    if (method.getName().equals("toString")) {
                        return "contextCapturingFileStoreTable";
                    }
                    if (method.getName().equals("hashCode")) {
                        return System.identityHashCode(proxy);
                    }
                    if (method.getName().equals("equals")) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        tableReference.set(table);
        return table;
    }

    private static InnerTable innerTable()
    {
        return (InnerTable) Proxy.newProxyInstance(
                InnerTable.class.getClassLoader(),
                new Class<?>[] {InnerTable.class},
                (_, method, _) -> {
                    if (method.getName().equals("toString")) {
                        return "testing-inner-table";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table tableWithColumns()
    {
        return tableWithComment(null);
    }

    private static Table tableWithComment(String comment)
    {
        return tableWithRowTypeAndComment(DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())), comment);
    }

    private static Table tableWithRowType(RowType rowType)
    {
        return tableWithRowTypeAndComment(rowType, null);
    }

    private static FileStoreTable staleFileStoreTable(
            AtomicReference<Boolean> copiedWithLatestSchema,
            RowType staleRowType,
            RowType latestRowType)
    {
        Map<String, String> options = Map.of();
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                Table.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "coreOptions" -> new CoreOptions(new Options());
                    case "rowType" -> latestRowType;
                    case "primaryKeys" -> List.of();
                    case "partitionKeys" -> List.of();
                    case "options" -> options;
                    case "schema" -> TableSchema.create(
                            1,
                            new Schema(latestRowType.getFields(), List.of(), List.of(), options, ""));
                    case "comment" -> Optional.empty();
                    case "copy" -> proxy;
                    case "toString" -> "latest-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (FileStoreTable) Proxy.newProxyInstance(
                Table.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTable;
                    }
                    case "coreOptions" -> new CoreOptions(new Options());
                    case "rowType" -> staleRowType;
                    case "primaryKeys" -> List.of();
                    case "partitionKeys" -> List.of();
                    case "options" -> options;
                    case "schema" -> TableSchema.create(
                            1,
                            new Schema(staleRowType.getFields(), List.of(), List.of(), options, ""));
                    case "comment" -> Optional.empty();
                    case "copy" -> proxy;
                    case "toString" -> "stale-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable fileStoreTableWithOptions(
            AtomicReference<Boolean> copiedWithLatestSchema,
            RowType rowType,
            Map<String, String> options,
            List<String> primaryKeys,
            List<String> partitionKeys)
    {
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                Table.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "rowType" -> rowType;
                    case "primaryKeys" -> primaryKeys;
                    case "partitionKeys" -> partitionKeys;
                    case "schema" -> TableSchema.create(
                            1,
                            new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, ""));
                    case "comment" -> Optional.empty();
                    case "copy" -> proxy;
                    case "copyWithLatestSchema" -> proxy;
                    case "toString" -> "latest-file-store-table-with-options";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (FileStoreTable) Proxy.newProxyInstance(
                Table.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTable;
                    }
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "rowType" -> rowType;
                    case "primaryKeys" -> primaryKeys;
                    case "partitionKeys" -> partitionKeys;
                    case "schema" -> TableSchema.create(
                            1,
                            new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, ""));
                    case "comment" -> Optional.empty();
                    case "copy" -> proxy;
                    case "toString" -> "stale-file-store-table-with-options";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table tableWithRowTypeAndComment(RowType rowType, String comment)
    {
        return (Table) Proxy.newProxyInstance(
                Table.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "rowType" -> rowType;
                    case "primaryKeys" -> List.of();
                    case "partitionKeys" -> List.of();
                    case "options" -> Map.of();
                    case "comment" -> Optional.ofNullable(comment);
                    case "copy" -> proxy;
                    case "toString" -> "tableWithColumns";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static DataType unsupportedDataTypeWithoutMessage()
    {
        return new UnsupportedTestingDataType(false);
    }

    private static DataType unsupportedDataTypeWithBrokenSqlString()
    {
        return new UnsupportedTestingDataType(true);
    }

    private static class UnsupportedTestingDataType
            extends DataType
    {
        private final boolean brokenSqlString;

        private UnsupportedTestingDataType(boolean brokenSqlString)
        {
            super(true, DataTypeRoot.BOOLEAN);
            this.brokenSqlString = brokenSqlString;
        }

        @Override
        public DataTypeRoot getTypeRoot()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int defaultSize()
        {
            return 1;
        }

        @Override
        public DataType copy(boolean isNullable)
        {
            return this;
        }

        @Override
        public String asSQLString()
        {
            if (brokenSqlString) {
                throw new UnsupportedOperationException();
            }
            return "UNSUPPORTED_TEST_TYPE";
        }

        @Override
        public <R> R accept(DataTypeVisitor<R> visitor)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static PaimonCatalog testingCatalog()
    {
        return TESTING_CATALOG;
    }

    private static class MissingTableCatalog
            extends PaimonCatalog
    {
        private MissingTableCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public Table getTable(Identifier identifier)
                throws Catalog.TableNotExistException
        {
            assertThat(identifier.getDatabaseName()).isEqualTo("test");
            assertThat(identifier.getObjectName()).isEqualTo("missing");
            throw new Catalog.TableNotExistException(identifier);
        }

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }
    }

    private static TrinoFileSystemFactory unsupportedFileSystemFactory()
    {
        return _ -> {
            throw new UnsupportedOperationException("filesystem is not used by this test");
        };
    }

    private static void setCachedTable(PaimonTableHandle handle, Catalog catalog, Table table)
    {
        handle.cacheTable(catalog, table);
    }
}
