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
package io.trino.plugin.paimon.functions.tablechanges;

import io.airlift.slice.Slices;
import io.trino.plugin.paimon.PaimonColumnHandle;
import io.trino.plugin.paimon.PaimonMetadata;
import io.trino.plugin.paimon.PaimonMetadataFactory;
import io.trino.plugin.paimon.PaimonTableHandle;
import io.trino.plugin.paimon.PaimonTableOptionUtils;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FullTextSearchTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.VectorSearchTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TableChangesFunctionTest
{
    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String INCREMENTAL_BETWEEN_SCAN_MODE =
            PaimonTableOptionUtils.convertOptionKey(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key()).toUpperCase(ENGLISH);
    private static final String INCREMENTAL_BETWEEN =
            PaimonTableOptionUtils.convertOptionKey(CoreOptions.INCREMENTAL_BETWEEN.key()).toUpperCase(ENGLISH);
    private static final String INCREMENTAL_BETWEEN_TIMESTAMP =
            PaimonTableOptionUtils.convertOptionKey(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key()).toUpperCase(ENGLISH);
    private static final String INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT =
            PaimonTableOptionUtils.convertOptionKey(CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key()).toUpperCase(ENGLISH);
    private static final String INCREMENTAL_TO_AUTO_TAG =
            PaimonTableOptionUtils.convertOptionKey(CoreOptions.INCREMENTAL_TO_AUTO_TAG.key()).toUpperCase(ENGLISH);

    @Test
    public void testAnalyzeBuildsExplicitProjectedColumnsAndOptions()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "payload", DataTypes.STRING()));
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(rowType, Map.of("existing", "value")))));
        RecordingAccessControl accessControl = new RecordingAccessControl();

        TableFunctionAnalysis analysis = function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, "1,2",
                INCREMENTAL_BETWEEN_SCAN_MODE, "delta")), accessControl);

        Descriptor descriptor = analysis.getReturnedType().orElseThrow();
        assertThat(descriptor.getFields())
                .extracting(field -> field.getName().orElseThrow())
                .containsExactly("id", "payload");
        assertThat(descriptor.getFields())
                .extracting(field -> field.getType().orElseThrow().getDisplayName())
                .containsExactly("integer", "varchar");

        PaimonTableHandle handle = (PaimonTableHandle) analysis.getHandle();
        assertThat(handle.getProjectedColumns()).hasValueSatisfying(columns ->
                assertThat(columns)
                        .extracting(PaimonColumnHandle::getColumnName)
                        .containsExactly("id", "payload"));
        assertThat(handle.getWriteColumns()).isEmpty();
        assertThat(handle.getDynamicOptions())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2",
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta"));
        assertThat(accessControl.getSelectedTable()).isEqualTo(new SchemaTableName("schema", "table"));
        assertThat(accessControl.getSelectedColumns()).containsExactlyInAnyOrder("id", "payload");
    }

    @Test
    public void testAnalyzeNormalizesIncrementalWindowWhitespace()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        TableFunctionAnalysis snapshotAnalysis = function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, " 1 , 2 ")), new RecordingAccessControl());
        assertThat(((PaimonTableHandle) snapshotAnalysis.getHandle()).getDynamicOptions())
                .containsEntry(CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2");

        TableFunctionAnalysis timestampAnalysis = function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN_TIMESTAMP, " 1000 , 2000 ")), new RecordingAccessControl());
        assertThat(((PaimonTableHandle) timestampAnalysis.getHandle()).getDynamicOptions())
                .containsEntry(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(), "1000,2000");

        TableFunctionAnalysis scanModeAnalysis = function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, " 1 , 2 ",
                INCREMENTAL_BETWEEN_SCAN_MODE, " delta ")), new RecordingAccessControl());
        assertThat(((PaimonTableHandle) scanModeAnalysis.getHandle()).getDynamicOptions())
                .containsEntry(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta");
    }

    @Test
    public void testAnalyzePassesIncrementalTimestampAndDefaultScanMode()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        TableFunctionAnalysis analysis = function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN_TIMESTAMP, "1000,2000")), new RecordingAccessControl());

        PaimonTableHandle handle = (PaimonTableHandle) analysis.getHandle();
        assertThat(handle.getDynamicOptions())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(), "1000,2000",
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "auto"));
    }

    @Test
    public void testAnalyzePassesIncrementalToAutoTagWithoutScanMode()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        TableFunctionAnalysis analysis = function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_TO_AUTO_TAG, " 2024-12-04\t")), new RecordingAccessControl());

        assertThat(((PaimonTableHandle) analysis.getHandle()).getDynamicOptions())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), "2024-12-04"));
    }

    @Test
    public void testAnalyzePassesIncrementalBetweenTagToSnapshotOption()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        TableFunctionAnalysis analysis = function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, "tag-a,tag-b",
                INCREMENTAL_BETWEEN_SCAN_MODE, "delta",
                INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT, true)), new RecordingAccessControl());

        assertThat(((PaimonTableHandle) analysis.getHandle()).getDynamicOptions())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "tag-a,tag-b",
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta",
                        CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true"));
    }

    @Test
    public void testAnalyzeOmitsIncrementalBetweenTagToSnapshotOptionByDefault()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        TableFunctionAnalysis analysis = function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, "tag-a,tag-b",
                INCREMENTAL_BETWEEN_SCAN_MODE, "delta")), new RecordingAccessControl());

        assertThat(((PaimonTableHandle) analysis.getHandle()).getDynamicOptions())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "tag-a,tag-b",
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta"))
                .doesNotContainKey(CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key());
    }

    @Test
    public void testAnalyzeUsesLatestFileStoreSchemaForReturnedDescriptorAndProjection()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType latestRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "new_id", DataTypes.BIGINT()),
                DataTypes.FIELD(1, "payload", DataTypes.STRING()));
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(
                        staleFileStoreTable(copiedWithLatestSchema, latestRowType))));
        RecordingAccessControl accessControl = new RecordingAccessControl();

        TableFunctionAnalysis analysis = function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, "1,2")), accessControl);

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(analysis.getReturnedType().orElseThrow().getFields())
                .extracting(field -> field.getName().orElseThrow())
                .containsExactly("new_id", "payload");
        assertThat(((PaimonTableHandle) analysis.getHandle()).getProjectedColumns())
                .hasValueSatisfying(columns -> assertThat(columns)
                        .extracting(PaimonColumnHandle::getColumnName)
                        .containsExactly("new_id", "payload"));
        assertThat(accessControl.getSelectedColumns()).containsExactlyInAnyOrder("new_id", "payload");
    }

    @Test
    public void testAnalyzeUsesOnlyRequestedIncrementalWindow()
    {
        TableChangesFunction timestampFunction = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of(
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "stale-start,stale-end")))));

        TableFunctionAnalysis timestampAnalysis = timestampFunction.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN_TIMESTAMP, "1000,2000")), new RecordingAccessControl());

        assertThat(((PaimonTableHandle) timestampAnalysis.getHandle()).getDynamicOptions())
                .doesNotContainKey(CoreOptions.INCREMENTAL_BETWEEN.key())
                .containsEntry(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(), "1000,2000");

        TableChangesFunction snapshotFunction = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of(
                        CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(), "stale-start,stale-end")))));

        TableFunctionAnalysis snapshotAnalysis = snapshotFunction.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, "1,2")), new RecordingAccessControl());

        assertThat(((PaimonTableHandle) snapshotAnalysis.getHandle()).getDynamicOptions())
                .containsEntry(CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2")
                .doesNotContainKey(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key());
    }

    @Test
    public void testAnalyzeRejectsMissingIncrementalWindow()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of()), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("One of INCREMENTAL_BETWEEN, INCREMENTAL_BETWEEN_TIMESTAMP or INCREMENTAL_TO_AUTO_TAG must be provided");
                });
    }

    @Test
    public void testAnalyzeTreatsInvalidAsRealIncrementalWindowValue()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, "invalid")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("INCREMENTAL_BETWEEN must be two non-empty values separated by a comma");
                });
    }

    @Test
    public void testAnalyzeRejectsConflictingIncrementalWindows()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, "1,2",
                INCREMENTAL_BETWEEN_TIMESTAMP, "1000,2000")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("Only one of INCREMENTAL_BETWEEN, INCREMENTAL_BETWEEN_TIMESTAMP or INCREMENTAL_TO_AUTO_TAG may be provided");
                });
    }

    @Test
    public void testAnalyzeRejectsIncrementalToAutoTagConflictingWithOtherIncrementalModes()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, "1,2",
                INCREMENTAL_TO_AUTO_TAG, "2024-12-04")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("Only one of INCREMENTAL_BETWEEN, INCREMENTAL_BETWEEN_TIMESTAMP or INCREMENTAL_TO_AUTO_TAG may be provided");
                });
    }

    @Test
    public void testAnalyzeRejectsIncrementalBetweenTagToSnapshotWithoutIncrementalBetween()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN_TIMESTAMP, "1000,2000",
                INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT, true)), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT requires INCREMENTAL_BETWEEN");
                });
    }

    @Test
    public void testAnalyzeRejectsBlankIncrementalToAutoTagBeforeCatalogLookup()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new MissingTablePaimonCatalog()));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_TO_AUTO_TAG, " ")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("INCREMENTAL_TO_AUTO_TAG may not be blank");
                });
    }

    @Test
    public void testAnalyzeRejectsMalformedIncrementalWindowsBeforeCatalogLookup()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new MissingTablePaimonCatalog()));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, "1")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("INCREMENTAL_BETWEEN must be two non-empty values separated by a comma");
                });

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN_TIMESTAMP, "1000,")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("INCREMENTAL_BETWEEN_TIMESTAMP must be two non-empty values separated by a comma");
                });
    }

    @Test
    public void testAnalyzeRejectsBlankRequiredArgumentsBeforeCatalogLookup()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new MissingTablePaimonCatalog()));

        Map<String, Argument> blankSchemaArguments = arguments(Map.of(INCREMENTAL_BETWEEN, "1,2"));
        blankSchemaArguments.put(SCHEMA_NAME, scalar(" "));
        assertThatThrownBy(() -> function.analyze(SESSION, null, blankSchemaArguments, new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("table_changes argument SCHEMA_NAME may not be blank");
                });

        Map<String, Argument> blankTableArguments = arguments(Map.of(INCREMENTAL_BETWEEN, "1,2"));
        blankTableArguments.put(TABLE_NAME, scalar("\t"));
        assertThatThrownBy(() -> function.analyze(SESSION, null, blankTableArguments, new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("table_changes argument TABLE_NAME may not be blank");
                });

        Map<String, Argument> blankScanModeArguments = arguments(Map.of(
                INCREMENTAL_BETWEEN, "1,2",
                INCREMENTAL_BETWEEN_SCAN_MODE, " "));
        assertThatThrownBy(() -> function.analyze(SESSION, null, blankScanModeArguments, new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("table_changes argument INCREMENTAL_BETWEEN_SCAN_MODE may not be blank");
                });
    }

    @Test
    public void testAnalyzeRejectsInvalidScanModeBeforeCatalogLookup()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new MissingTablePaimonCatalog()));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(
                INCREMENTAL_BETWEEN, "1,2",
                INCREMENTAL_BETWEEN_SCAN_MODE, "unknown")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("Invalid INCREMENTAL_BETWEEN_SCAN_MODE: unknown");
                    assertThat(rootCause(exception)).hasMessageContaining("Expected one of");
                });
    }

    @Test
    public void testAnalyzeRejectsMissingOrNullArguments()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        assertThatThrownBy(() -> function.analyze(SESSION, null, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("arguments is null");

        assertThatThrownBy(() -> function.analyze(null, null, arguments(Map.of(INCREMENTAL_BETWEEN, "1,2")), new RecordingAccessControl()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");

        Map<String, Argument> missingSchemaArguments = arguments(Map.of(INCREMENTAL_BETWEEN, "1,2"));
        missingSchemaArguments.remove(SCHEMA_NAME);
        assertThatThrownBy(() -> function.analyze(SESSION, null, missingSchemaArguments, new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("SCHEMA_NAME argument not found");
                });

        Map<String, Argument> nullTableArguments = arguments(Map.of(INCREMENTAL_BETWEEN, "1,2"));
        nullTableArguments.put(TABLE_NAME, new ScalarArgument(VARCHAR, null));
        assertThatThrownBy(() -> function.analyze(SESSION, null, nullTableArguments, new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("table_changes argument TABLE_NAME may not be null");
                });
    }

    @Test
    public void testAnalyzeRequiresAccessControl()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(INCREMENTAL_BETWEEN, "1,2")), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("accessControl is null");
    }

    @Test
    public void testAnalyzeRejectsMalformedArgumentState()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(table(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        Map<String, Argument> wrongTypeArguments = arguments(Map.of(INCREMENTAL_BETWEEN, "1,2"));
        wrongTypeArguments.put(TABLE_NAME, new TestingArgument());
        assertThatThrownBy(() -> function.analyze(SESSION, null, wrongTypeArguments, new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessageContaining("Unsupported argument type for TABLE_NAME");
                });

        Map<String, Argument> wrongValueArguments = arguments(Map.of(INCREMENTAL_BETWEEN, "1,2"));
        wrongValueArguments.put(INCREMENTAL_BETWEEN, new ScalarArgument(VARCHAR, 1L));
        assertThatThrownBy(() -> function.analyze(SESSION, null, wrongValueArguments, new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("Unsupported argument value for INCREMENTAL_BETWEEN: java.lang.Long");
                });

        Map<String, Argument> wrongBooleanValueArguments = arguments(Map.of(INCREMENTAL_BETWEEN, "1,2"));
        wrongBooleanValueArguments.put(INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT, new ScalarArgument(BOOLEAN, "true"));
        assertThatThrownBy(() -> function.analyze(SESSION, null, wrongBooleanValueArguments, new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("Unsupported argument value for INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT: java.lang.String");
                });
    }

    @Test
    public void testAnalyzeReportsMissingTableAsFunctionArgumentError()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new MissingTablePaimonCatalog()));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(INCREMENTAL_BETWEEN, "1,2")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode());
                    assertThat(exception).hasMessage("Table not found: schema.table");
                });
    }

    @Test
    public void testAnalyzeMapsNestedCatalogFailures()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new RuntimeFailingPaimonCatalog(
                        new RuntimeException(new Catalog.DatabaseNotExistException("schema")))));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(INCREMENTAL_BETWEEN, "1,2")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(SCHEMA_NOT_FOUND.toErrorCode());
                    assertThat(exception).hasMessage("Schema 'schema' does not exist");
                });
    }

    @Test
    public void testAnalyzeRejectsNonFileStoreTable()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(nonFileStoreTable(DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT())), Map.of()))));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(INCREMENTAL_BETWEEN, "1,2")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining("Paimon system.table_changes requires FileStoreTable, but got:");
                });
    }

    @Test
    public void testAnalyzeRejectsVectorSearchTable()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(VectorSearchTable.create(
                        innerTable(),
                        new VectorSearch(new float[] {1.0f}, 1, "embedding")))));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(INCREMENTAL_BETWEEN, "1,2")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon vector search tables are not supported by the Trino connector");
                });
    }

    @Test
    public void testAnalyzeRejectsFullTextSearchTable()
    {
        TableChangesFunction function = new TableChangesFunction(
                new TestingMetadataFactory(new TestingPaimonCatalog(FullTextSearchTable.create(
                        innerTable(),
                        new FullTextSearch("content", "paimon", 1)))));

        assertThatThrownBy(() -> function.analyze(SESSION, null, arguments(Map.of(INCREMENTAL_BETWEEN, "1,2")), new RecordingAccessControl()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon full-text search tables are not supported by the Trino connector");
                });
    }

    private static Map<String, Argument> arguments(Map<String, ?> overrides)
    {
        Map<String, Argument> arguments = new HashMap<>();
        arguments.put(SCHEMA_NAME, scalar("schema"));
        arguments.put(TABLE_NAME, scalar("table"));
        arguments.put(INCREMENTAL_BETWEEN_SCAN_MODE, scalar("auto"));
        arguments.put(INCREMENTAL_BETWEEN, scalar(null));
        arguments.put(INCREMENTAL_BETWEEN_TIMESTAMP, scalar(null));
        arguments.put(INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT, scalar(false));
        arguments.put(INCREMENTAL_TO_AUTO_TAG, scalar(null));
        overrides.forEach((key, value) -> arguments.put(key, scalar(value)));
        return arguments;
    }

    private static ScalarArgument scalar(Object value)
    {
        if (value == null || value instanceof String) {
            return new ScalarArgument(VARCHAR, Optional.ofNullable((String) value).map(Slices::utf8Slice).orElse(null));
        }
        if (value instanceof Boolean bool) {
            return new ScalarArgument(BOOLEAN, bool);
        }
        throw new IllegalArgumentException("Unsupported testing scalar value: " + value.getClass().getName());
    }

    private static Throwable rootCause(Throwable throwable)
    {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }

    private static FileStoreTable table(RowType rowType, Map<String, String> options)
    {
        return (FileStoreTable) Proxy.newProxyInstance(
                TableChangesFunctionTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy", "copyWithLatestSchema" -> proxy;
                    case "options" -> options;
                    case "rowType" -> rowType;
                    case "toString" -> "testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table nonFileStoreTable(RowType rowType, Map<String, String> options)
    {
        return (Table) Proxy.newProxyInstance(
                TableChangesFunctionTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (_, method, _) -> switch (method.getName()) {
                    case "options" -> options;
                    case "rowType" -> rowType;
                    case "toString" -> "testing-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static InnerTable innerTable()
    {
        return (InnerTable) Proxy.newProxyInstance(
                TableChangesFunctionTest.class.getClassLoader(),
                new Class<?>[] {InnerTable.class},
                (_, method, _) -> switch (method.getName()) {
                    case "toString" -> "testing-inner-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable staleFileStoreTable(AtomicBoolean copiedWithLatestSchema, RowType latestRowType)
    {
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                TableChangesFunctionTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy", "copyWithLatestSchema" -> proxy;
                    case "options" -> Map.of();
                    case "rowType" -> latestRowType;
                    case "toString" -> "latest-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (FileStoreTable) Proxy.newProxyInstance(
                TableChangesFunctionTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTable;
                    }
                    case "options" -> Map.of();
                    case "rowType" -> throw new AssertionError(
                            "stale table rowType must not be used for table_changes analysis");
                    case "toString" -> "stale-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static class TestingMetadataFactory
            extends PaimonMetadataFactory
    {
        private final PaimonMetadata metadata;

        private TestingMetadataFactory(PaimonCatalog catalog)
        {
            super(new Options(), _ -> {
                throw new UnsupportedOperationException("filesystem is not used by this test");
            }, TESTING_TYPE_MANAGER);
            this.metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        }

        @Override
        public PaimonMetadata create()
        {
            return metadata;
        }
    }

    private static class TestingPaimonCatalog
            extends PaimonCatalog
    {
        private final Table table;

        private TestingPaimonCatalog(Table table)
        {
            super(new Options(), _ -> {
                throw new UnsupportedOperationException("filesystem is not used by this test");
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
            assertThat(identifier.getDatabaseName()).isEqualTo("schema");
            assertThat(identifier.getObjectName()).isEqualTo("table");
            return table;
        }
    }

    private static class MissingTablePaimonCatalog
            extends PaimonCatalog
    {
        private MissingTablePaimonCatalog()
        {
            super(new Options(), _ -> {
                throw new UnsupportedOperationException("filesystem is not used by this test");
            });
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
                throws Catalog.TableNotExistException
        {
            throw new Catalog.TableNotExistException(identifier);
        }
    }

    private static class RuntimeFailingPaimonCatalog
            extends PaimonCatalog
    {
        private final RuntimeException failure;

        private RuntimeFailingPaimonCatalog(RuntimeException failure)
        {
            super(new Options(), _ -> {
                throw new UnsupportedOperationException("filesystem is not used by this test");
            });
            this.failure = failure;
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
            throw failure;
        }
    }

    private static class TestingArgument
            extends Argument {}

    private static class RecordingAccessControl
            implements ConnectorAccessControl
    {
        private SchemaTableName selectedTable;
        private Set<String> selectedColumns;

        @Override
        public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
        {
            this.selectedTable = tableName;
            this.selectedColumns = Set.copyOf(columnNames);
        }

        private SchemaTableName getSelectedTable()
        {
            return selectedTable;
        }

        private Set<String> getSelectedColumns()
        {
            return selectedColumns;
        }
    }
}
