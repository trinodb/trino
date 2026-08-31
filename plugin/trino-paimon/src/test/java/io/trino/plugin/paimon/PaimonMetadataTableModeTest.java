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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition.Last;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import io.trino.type.TypeDeserializer;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.LocalZoneTimestamp;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FullTextSearchTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.VectorSearchTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.system.AuditLogTable;
import org.apache.paimon.table.system.RowTrackingTable;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewImpl;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.LocalDate;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_COMMIT_ERROR;
import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_METADATA_ERROR;
import static io.trino.plugin.paimon.PaimonSchemaProperties.COMMENT_PROPERTY;
import static io.trino.plugin.paimon.PaimonSchemaProperties.LOCATION_PROPERTY;
import static io.trino.plugin.paimon.PaimonSchemaProperties.OWNER_PROPERTY;
import static io.trino.plugin.paimon.PaimonSessionProperties.SCAN_SNAPSHOT;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.READ_ONLY_VIOLATION;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonMetadataTableModeTest
{
    private static final String TRINO_SCHEMA_OWNER_TYPE_PROPERTY = "trino.owner-type";
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
            .build();
    private static final JsonCodec<PaimonTableHandle> TABLE_HANDLE_CODEC = tableHandleJsonCodec();

    private static JsonCodec<PaimonTableHandle> tableHandleJsonCodec()
    {
        JsonMapperProvider jsonMapperProvider = new JsonMapperProvider();
        jsonMapperProvider.setJsonDeserializers(Map.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        return new JsonCodecFactory(jsonMapperProvider).jsonCodec(PaimonTableHandle.class);
    }

    @Test
    public void testMetadataRejectsNullDependencies()
    {
        assertThatThrownBy(() -> new PaimonMetadata(null, TESTING_TYPE_MANAGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("catalog is null");

        assertThatThrownBy(() -> new PaimonMetadata(new TestingPaimonCatalog(table()), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("typeManager is null");
    }

    @Test
    public void testMetadataSupportsMissingColumnsOnInsert()
    {
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table()), TESTING_TYPE_MANAGER);

        assertThat(metadata.supportsMissingColumnsOnInsert()).isTrue();
    }

    @Test
    public void testSystemSchemaIsExposed()
    {
        PaimonMetadata metadata = new PaimonMetadata(new SchemaQueryCatalog(), TESTING_TYPE_MANAGER);

        assertThat(metadata.schemaExists(SESSION, SYSTEM_DATABASE_NAME)).isTrue();
        assertThat(metadata.listSchemaNames(SESSION)).containsExactly("alpha", "beta", SYSTEM_DATABASE_NAME);
    }

    @Test
    public void testSystemSchemaListsGlobalSystemTables()
    {
        PaimonMetadata metadata = new PaimonMetadata(new SchemaQueryCatalog(), TESTING_TYPE_MANAGER);

        assertThat(metadata.listTables(SESSION, Optional.of(SYSTEM_DATABASE_NAME)))
                .containsExactlyElementsOf(SystemTableLoader.loadGlobalTableNames(
                                Options.fromMap(Map.of(CatalogOptions.CATALOG_OPTIONS_TABLE_ENABLED.key(), "true"))).stream()
                        .map(table -> new SchemaTableName(SYSTEM_DATABASE_NAME, table))
                        .toList());
    }

    @Test
    public void testSystemSchemaListTablesDoesNotQueryCatalog()
    {
        PaimonMetadata metadata = new PaimonMetadata(new SystemSchemaRejectingCatalog(), TESTING_TYPE_MANAGER);

        assertThat(metadata.listTables(SESSION, Optional.of(SYSTEM_DATABASE_NAME)))
                .containsExactlyElementsOf(SystemTableLoader.loadGlobalTableNames(
                                Options.fromMap(Map.of(CatalogOptions.CATALOG_OPTIONS_TABLE_ENABLED.key(), "true"))).stream()
                        .map(table -> new SchemaTableName(SYSTEM_DATABASE_NAME, table))
                        .toList());
        assertThat(metadata.listTables(SESSION, Optional.empty()))
                .containsExactly(
                        new SchemaTableName("alpha", "t1"),
                        new SchemaTableName(SYSTEM_DATABASE_NAME, "tables"),
                        new SchemaTableName(SYSTEM_DATABASE_NAME, "partitions"),
                        new SchemaTableName(SYSTEM_DATABASE_NAME, "all_table_options"),
                        new SchemaTableName(SYSTEM_DATABASE_NAME, "catalog_options"));
    }

    @Test
    public void testSystemSchemaWritesAreRejected()
    {
        PaimonMetadata metadata = new PaimonMetadata(new CapturingDdlCatalog(), TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName(SYSTEM_DATABASE_NAME, "all_tables"),
                List.of(new ColumnMetadata("id", BIGINT)));
        PaimonTableHandle systemTableHandle = new PaimonTableHandle(SYSTEM_DATABASE_NAME, "all_tables", Map.of());

        assertTrinoError(
                () -> metadata.createTable(SESSION, tableMetadata, SaveMode.FAIL),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon create table is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.renameTable(
                        SESSION,
                        systemTableHandle,
                        new SchemaTableName(SYSTEM_DATABASE_NAME, "catalog_options")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename table is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.dropTable(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop table is not supported for the system schema 'sys'");
    }

    @Test
    public void testSystemSchemaDdlAndAlterOperationsAreRejected()
    {
        PaimonMetadata metadata = new PaimonMetadata(new CapturingDdlCatalog(), TESTING_TYPE_MANAGER);
        PaimonTableHandle systemTableHandle = new PaimonTableHandle(SYSTEM_DATABASE_NAME, "all_tables", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("database_name", DataTypes.STRING());

        assertTrinoError(
                () -> metadata.createSchema(SESSION, SYSTEM_DATABASE_NAME, Map.of(), null),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon create schema is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.dropSchema(SESSION, SYSTEM_DATABASE_NAME, false),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop schema is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.setSchemaAuthorization(
                        SESSION,
                        SYSTEM_DATABASE_NAME,
                        new TrinoPrincipal(PrincipalType.USER, "schema_owner")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set schema authorization is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.getSchemaOwner(SESSION, SYSTEM_DATABASE_NAME),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon schema owner is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.setTableAuthorization(
                        SESSION,
                        new SchemaTableName(SYSTEM_DATABASE_NAME, "all_tables"),
                        new TrinoPrincipal(PrincipalType.USER, "table_owner")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set table authorization is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.setTableProperties(SESSION, systemTableHandle, Map.of("bucket", Optional.of("4"))),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set table properties is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.getInsertLayout(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon insert layout is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.beginInsert(SESSION, systemTableHandle, List.of(columnHandle), RetryMode.NO_RETRIES),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon begin insert is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.getRowChangeParadigm(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon row change paradigm is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.getMergeRowIdColumnHandle(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon merge row id is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.getUpdateLayout(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon update layout is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.beginMerge(SESSION, systemTableHandle, Map.of(), RetryMode.NO_RETRIES),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon begin merge is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.addColumn(SESSION, systemTableHandle, new ColumnMetadata("extra", INTEGER), new Last()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon add column is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.renameColumn(SESSION, systemTableHandle, columnHandle, "renamed"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename column is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.dropColumn(SESSION, systemTableHandle, columnHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop column is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.setTableComment(SESSION, systemTableHandle, Optional.of("comment")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set table comment is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.setColumnComment(SESSION, systemTableHandle, columnHandle, Optional.of("comment")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set column comment is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.setColumnType(SESSION, systemTableHandle, columnHandle, VARCHAR),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set column type is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.dropNotNullConstraint(SESSION, systemTableHandle, columnHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop not null constraint is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.addField(SESSION, systemTableHandle, List.of("database_name"), "nested", VARCHAR, false),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon add field is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.dropField(SESSION, systemTableHandle, columnHandle, List.of("nested")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop field is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.renameField(SESSION, systemTableHandle, List.of("database_name", "nested"), "renamed"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename field is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.setFieldType(SESSION, systemTableHandle, List.of("database_name", "nested"), VARCHAR),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set field type is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.truncateTable(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon truncate table is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.applyDelete(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon delete is not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.executeDelete(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon delete is not supported for the system schema 'sys'");
    }

    @Test
    public void testSystemTableWritesAreRejectedBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle systemTableHandle = new PaimonTableHandle("schema", "table$snapshots", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("snapshot_id", DataTypes.BIGINT());
        ConnectorTableMetadata systemTableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table$snapshots"),
                List.of(new ColumnMetadata("snapshot_id", BIGINT)));

        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.beginCreateTable(
                        SESSION,
                        systemTableMetadata,
                        Optional.empty(),
                        RetryMode.NO_RETRIES,
                        false),
                "create table");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.beginCreateTable(
                        SESSION,
                        systemTableMetadata,
                        Optional.empty(),
                        RetryMode.NO_RETRIES,
                        true),
                "create table");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.getInsertLayout(SESSION, systemTableHandle),
                "insert layout");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.beginInsert(
                        SESSION,
                        systemTableHandle,
                        List.of(columnHandle),
                        RetryMode.NO_RETRIES),
                "begin insert");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.finishInsert(
                        SESSION,
                        systemTableHandle,
                        List.of(),
                        List.of(),
                        List.of()),
                "finish insert");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.getRowChangeParadigm(SESSION, systemTableHandle),
                "row change paradigm");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.getMergeRowIdColumnHandle(SESSION, systemTableHandle),
                "merge row id");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.getUpdateLayout(SESSION, systemTableHandle),
                "update layout");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.beginMerge(
                        SESSION,
                        systemTableHandle,
                        Map.of(),
                        RetryMode.NO_RETRIES),
                "begin merge");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.finishMerge(
                        SESSION,
                        new PaimonMergeTableHandle(systemTableHandle),
                        List.of(),
                        List.of(),
                        List.of()),
                "finish merge");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.setTableProperties(
                        SESSION,
                        systemTableHandle,
                        Map.of("bucket", Optional.of("4"))),
                "set table properties");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.setTableAuthorization(
                        SESSION,
                        new SchemaTableName("schema", "table$snapshots"),
                        new TrinoPrincipal(PrincipalType.USER, "table_owner")),
                "set table authorization");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.renameTable(
                        SESSION,
                        systemTableHandle,
                        new SchemaTableName("schema", "renamed")),
                "rename table");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.renameTable(
                        SESSION,
                        new PaimonTableHandle("schema", "table", Map.of()),
                        new SchemaTableName("schema", "table$snapshots")),
                "rename table");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.dropTable(SESSION, systemTableHandle),
                "drop table");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.addColumn(
                        SESSION,
                        systemTableHandle,
                        new ColumnMetadata("extra", INTEGER),
                        new Last()),
                "add column");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.renameColumn(
                        SESSION,
                        systemTableHandle,
                        columnHandle,
                        "renamed"),
                "rename column");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.dropColumn(SESSION, systemTableHandle, columnHandle),
                "drop column");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.setTableComment(
                        SESSION,
                        systemTableHandle,
                        Optional.of("comment")),
                "set table comment");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.setColumnComment(
                        SESSION,
                        systemTableHandle,
                        columnHandle,
                        Optional.of("comment")),
                "set column comment");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.setColumnType(
                        SESSION,
                        systemTableHandle,
                        columnHandle,
                        VARCHAR),
                "set column type");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.dropNotNullConstraint(
                        SESSION,
                        systemTableHandle,
                        columnHandle),
                "drop not null constraint");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.addField(
                        SESSION,
                        systemTableHandle,
                        List.of("snapshot_id"),
                        "nested",
                        VARCHAR,
                        false),
                "add field");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.dropField(
                        SESSION,
                        systemTableHandle,
                        columnHandle,
                        List.of("nested")),
                "drop field");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.renameField(
                        SESSION,
                        systemTableHandle,
                        List.of("snapshot_id", "nested"),
                        "renamed"),
                "rename field");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.setFieldType(
                        SESSION,
                        systemTableHandle,
                        List.of("snapshot_id", "nested"),
                        VARCHAR),
                "set field type");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.truncateTable(SESSION, systemTableHandle),
                "truncate table");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.applyDelete(SESSION, systemTableHandle),
                "delete");
        assertSystemTableWriteRejected(
                catalog,
                () -> metadata.executeDelete(SESSION, systemTableHandle),
                "delete");
    }

    @Test
    public void testTableStatisticsUsesPaimonSnapshotStats()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.BIGINT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()),
                DataTypes.FIELD(2, "extra", DataTypes.INT()),
                DataTypes.FIELD(3, "event_date", DataTypes.DATE()),
                DataTypes.FIELD(4, "event_time", DataTypes.TIMESTAMP(6)),
                DataTypes.FIELD(5, "event_time_tz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));
        Statistics statistics = new Statistics(7, 3, 100L, 4096L, Map.of(
                "id", ColStats.newColStats(0, 20L, 1L, 99L, 5L, 8L, 8L),
                "extra", ColStats.newColStats(99, 1L, 100, 200, 0L, 4L, 4L),
                "old_extra", ColStats.newColStats(2, 7L, 10, 20, 10L, 4L, 4L),
                "missing", ColStats.newColStats(9, 1L, null, null, 0L, 4L, 4L),
                "name", ColStats.newColStats(
                        1,
                        null,
                        BinaryString.fromString("a"),
                        BinaryString.fromString("z"),
                        25L,
                        12L,
                        64L),
                "event_date", ColStats.newColStats(3, null, 10, 20, 0L, 4L, 4L),
                "event_time", ColStats.newColStats(
                        4,
                        null,
                        Timestamp.fromMicros(1_000_000L),
                        Timestamp.fromMicros(2_500_000L),
                        0L,
                        8L,
                        8L),
                "event_time_tz", ColStats.newColStats(
                        5,
                        null,
                        LocalZoneTimestamp.fromMicros(1_000_000L),
                        LocalZoneTimestamp.fromMicros(2_500_000L),
                        0L,
                        8L,
                        8L)));
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(statisticsTable(rowType, Optional.of(statistics))),
                TESTING_TYPE_MANAGER);

        TableStatistics tableStatistics = metadata.getTableStatistics(
                SESSION,
                new PaimonTableHandle("schema", "table", Map.of()));

        assertThat(tableStatistics.getRowCount().getValue()).isEqualTo(100);
        assertThat(tableStatistics.getColumnStatistics()).hasSize(6);

        ColumnStatistics idStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("id", DataTypes.BIGINT()));
        assertThat(idStats.getDistinctValuesCount().getValue()).isEqualTo(20);
        assertThat(idStats.getNullsFraction().getValue()).isEqualTo(0.05);
        assertThat(idStats.getDataSize().getValue()).isEqualTo(760);
        assertThat(idStats.getRange()).contains(new DoubleRange(1, 99));

        ColumnStatistics nameStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("name", DataTypes.STRING()));
        assertThat(nameStats.getDistinctValuesCount().isUnknown()).isTrue();
        assertThat(nameStats.getNullsFraction().getValue()).isEqualTo(0.25);
        assertThat(nameStats.getDataSize().getValue()).isEqualTo(900);
        assertThat(nameStats.getRange()).isEmpty();

        ColumnStatistics extraStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("extra", DataTypes.INT()));
        assertThat(extraStats.getDistinctValuesCount().getValue()).isEqualTo(7);
        assertThat(extraStats.getNullsFraction().getValue()).isEqualTo(0.1);
        assertThat(extraStats.getRange()).contains(new DoubleRange(10, 20));

        ColumnStatistics dateStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("event_date", DataTypes.DATE()));
        assertThat(dateStats.getRange()).contains(new DoubleRange(10, 20));

        ColumnStatistics timestampStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("event_time", DataTypes.TIMESTAMP(6)));
        assertThat(timestampStats.getRange()).contains(new DoubleRange(1_000_000, 2_500_000));

        ColumnStatistics timestampWithTimeZoneStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("event_time_tz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));
        assertThat(timestampWithTimeZoneStats.getRange()).contains(new DoubleRange(1_000, 2_500));

        assertThat(tableStatistics.getColumnStatistics())
                .doesNotContainKey(PaimonColumnHandle.of("missing", DataTypes.INT()));
    }

    @Test
    public void testTableStatisticsSkipsDataSizeForInvalidNullCount()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "missing_null_count", DataTypes.STRING()),
                DataTypes.FIELD(1, "negative_null_count", DataTypes.STRING()),
                DataTypes.FIELD(2, "too_many_nulls", DataTypes.STRING()));
        Statistics statistics = new Statistics(7, 3, 10L, 4096L, Map.of(
                "missing_null_count", ColStats.newColStats(0, null, null, null, null, 3L, 3L),
                "negative_null_count", ColStats.newColStats(1, null, null, null, -1L, 3L, 3L),
                "too_many_nulls", ColStats.newColStats(2, null, null, null, 11L, 3L, 3L)));
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(statisticsTable(rowType, Optional.of(statistics))),
                TESTING_TYPE_MANAGER);

        TableStatistics tableStatistics = metadata.getTableStatistics(
                SESSION,
                new PaimonTableHandle("schema", "table", Map.of()));

        ColumnStatistics missingNullCountStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("missing_null_count", DataTypes.STRING()));
        assertThat(missingNullCountStats.getNullsFraction().isUnknown()).isTrue();
        assertThat(missingNullCountStats.getDataSize().getValue()).isEqualTo(30);

        ColumnStatistics negativeNullCountStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("negative_null_count", DataTypes.STRING()));
        assertThat(negativeNullCountStats.getNullsFraction().isUnknown()).isTrue();
        assertThat(negativeNullCountStats.getDataSize().isUnknown()).isTrue();

        ColumnStatistics tooManyNullsStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("too_many_nulls", DataTypes.STRING()));
        assertThat(tooManyNullsStats.getNullsFraction().isUnknown()).isTrue();
        assertThat(tooManyNullsStats.getDataSize().isUnknown()).isTrue();
    }

    @Test
    public void testTableStatisticsSkipsInvalidDistinctCount()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "valid_distinct_count", DataTypes.STRING()),
                DataTypes.FIELD(1, "negative_distinct_count", DataTypes.STRING()),
                DataTypes.FIELD(2, "too_many_distinct_values", DataTypes.STRING()));
        Statistics statistics = new Statistics(7, 3, 10L, 4096L, Map.of(
                "valid_distinct_count", ColStats.newColStats(0, 7L, null, null, null, null, null),
                "negative_distinct_count", ColStats.newColStats(1, -1L, null, null, null, null, null),
                "too_many_distinct_values", ColStats.newColStats(2, 11L, null, null, null, null, null)));
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(statisticsTable(rowType, Optional.of(statistics))),
                TESTING_TYPE_MANAGER);

        TableStatistics tableStatistics = metadata.getTableStatistics(
                SESSION,
                new PaimonTableHandle("schema", "table", Map.of()));

        ColumnStatistics validDistinctStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("valid_distinct_count", DataTypes.STRING()));
        assertThat(validDistinctStats.getDistinctValuesCount().getValue()).isEqualTo(7);

        ColumnStatistics negativeDistinctStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("negative_distinct_count", DataTypes.STRING()));
        assertThat(negativeDistinctStats.getDistinctValuesCount().isUnknown()).isTrue();

        ColumnStatistics tooManyDistinctStats = tableStatistics.getColumnStatistics()
                .get(PaimonColumnHandle.of("too_many_distinct_values", DataTypes.STRING()));
        assertThat(tooManyDistinctStats.getDistinctValuesCount().isUnknown()).isTrue();

        RowType unknownRowCountRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "unknown_row_count", DataTypes.STRING()));
        Statistics unknownRowCountStatistics = new Statistics(7, 3, null, 4096L, Map.of(
                "unknown_row_count", ColStats.newColStats(0, 11L, null, null, null, null, null)));
        PaimonMetadata unknownRowCountMetadata = new PaimonMetadata(new TestingPaimonCatalog(statisticsTable(
                unknownRowCountRowType,
                Optional.of(unknownRowCountStatistics))), TESTING_TYPE_MANAGER);

        ColumnStatistics unknownRowCountStats = unknownRowCountMetadata.getTableStatistics(
                        SESSION,
                        new PaimonTableHandle("schema", "table", Map.of()))
                .getColumnStatistics()
                .get(PaimonColumnHandle.of("unknown_row_count", DataTypes.STRING()));
        assertThat(unknownRowCountStats.getDistinctValuesCount().getValue()).isEqualTo(11);
    }

    @Test
    public void testTableStatisticsReturnsUnknownWhenPaimonStatsAreMissingOrUnreadable()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));

        PaimonMetadata missingStatsMetadata = new PaimonMetadata(new TestingPaimonCatalog(statisticsTable(
                rowType,
                Optional.empty())), TESTING_TYPE_MANAGER);
        assertThat(missingStatsMetadata.getTableStatistics(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isEqualTo(TableStatistics.empty());

        PaimonMetadata failingStatsMetadata = new PaimonMetadata(
                new TestingPaimonCatalog(failingStatisticsTable(rowType)),
                TESTING_TYPE_MANAGER);
        assertThat(failingStatsMetadata.getTableStatistics(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isEqualTo(TableStatistics.empty());
    }

    @Test
    public void testTableStatisticsFallsBackToVisibleSplitRowCountWhenPaimonStatsAreMissing()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
        FileStoreTable table = statisticsFallbackFileStoreTable(
                rowType,
                List.of(testingSplit(2, OptionalLong.empty()), testingSplit(3, OptionalLong.empty())),
                List.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);

        TableStatistics tableStatistics = metadata.getTableStatistics(
                SESSION,
                new PaimonTableHandle("schema", "table", Map.of()));

        assertThat(tableStatistics.getRowCount().getValue()).isEqualTo(5);
        assertThat(tableStatistics.getColumnStatistics()).isEmpty();
    }

    @Test
    public void testTableStatisticsFallbackUsesMergedSplitRowCountForPrimaryKeyTables()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
        FileStoreTable table = statisticsFallbackFileStoreTable(
                rowType,
                List.of(testingSplit(100, OptionalLong.of(2)), testingSplit(100, OptionalLong.of(3))),
                List.of("id"));
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);

        assertThat(metadata.getTableStatistics(SESSION, new PaimonTableHandle("schema", "table", Map.of()))
                .getRowCount().getValue()).isEqualTo(5);
    }

    @Test
    public void testTableStatisticsFallsBackWhenPaimonStatsHaveNoUsableRowCount()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
        for (Long rowCount : Arrays.asList(null, -1L)) {
            Statistics statistics = new Statistics(7, 3, rowCount, 4096L, Map.of(
                    "id", ColStats.newColStats(0, 2L, 1L, 5L, 0L, 8L, 8L)));
            FileStoreTable table = statisticsFallbackFileStoreTable(
                    rowType,
                    List.of(testingSplit(2, OptionalLong.empty()), testingSplit(3, OptionalLong.empty())),
                    List.of(),
                    Optional.of(statistics));
            PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);

            TableStatistics tableStatistics = metadata.getTableStatistics(
                    SESSION,
                    new PaimonTableHandle("schema", "table", Map.of()));

            assertThat(tableStatistics.getRowCount().getValue()).isEqualTo(5);
            assertThat(tableStatistics.getColumnStatistics())
                    .containsKey(PaimonColumnHandle.of("id", DataTypes.BIGINT()));
        }
    }

    @Test
    public void testTableStatisticsFallbackReturnsUnknownWhenVisibleSplitRowCountIsNotExact()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));

        FileStoreTable primaryKeyTable = statisticsFallbackFileStoreTable(
                rowType,
                List.of(testingSplit(10, OptionalLong.empty())),
                List.of("id"));
        PaimonMetadata primaryKeyMetadata = new PaimonMetadata(new TestingPaimonCatalog(primaryKeyTable), TESTING_TYPE_MANAGER);
        assertThat(primaryKeyMetadata.getTableStatistics(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isEqualTo(TableStatistics.empty());

        FileStoreTable invalidRowCountTable = statisticsFallbackFileStoreTable(
                rowType,
                List.of(testingSplit(-1, OptionalLong.empty())),
                List.of());
        PaimonMetadata invalidRowCountMetadata = new PaimonMetadata(new TestingPaimonCatalog(invalidRowCountTable), TESTING_TYPE_MANAGER);
        assertThat(invalidRowCountMetadata.getTableStatistics(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isEqualTo(TableStatistics.empty());
    }

    @Test
    public void testTableStatisticsPreservesMappedTrinoFailures()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
        TrinoException failure = new TrinoException(PAIMON_METADATA_ERROR, "mapped statistics failure");
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(failingStatisticsTable(rowType, failure)),
                TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.getTableStatistics(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isSameAs(failure);

        PaimonMetadata nestedFailureMetadata = new PaimonMetadata(
                new TestingPaimonCatalog(failingStatisticsTable(rowType, new RuntimeException(new RuntimeException(failure)))),
                TESTING_TYPE_MANAGER);
        assertThatThrownBy(() -> nestedFailureMetadata.getTableStatistics(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isSameAs(failure);
    }

    @Test
    public void testTableStatisticsDoesNotApplyFullTableStatsToFilteredOrLimitedHandles()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
        Statistics statistics = new Statistics(7, 3, 100L, 4096L, Map.of());
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(statisticsTable(rowType, Optional.of(statistics))),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.getTableStatistics(SESSION, tableHandle.copy(OptionalLong.of(10))))
                .isEqualTo(TableStatistics.empty());

        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        assertThat(metadata.getTableStatistics(SESSION, tableHandle.copy(TupleDomain.withColumnDomains(Map.of(
                columnHandle, Domain.singleValue(BIGINT, 1L))))))
                .isEqualTo(TableStatistics.empty());
        assertThat(metadata.getTableStatistics(SESSION, new PaimonTableHandle("schema", "table", Map.of(
                CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"))))
                .isEqualTo(TableStatistics.empty());

        TableStatistics emptyFilteredStats = metadata.getTableStatistics(SESSION, tableHandle.copy(TupleDomain.none()));
        assertThat(emptyFilteredStats.getRowCount().getValue()).isEqualTo(0);
        assertThat(emptyFilteredStats.getColumnStatistics()).isEmpty();

        TableStatistics zeroLimitStats = metadata.getTableStatistics(SESSION, tableHandle.copy(OptionalLong.of(0)));
        assertThat(zeroLimitStats.getRowCount().getValue()).isEqualTo(0);
        assertThat(zeroLimitStats.getColumnStatistics()).isEmpty();
    }

    @Test
    public void testVersionedQueriesAreRejectedForSystemTables()
    {
        PaimonMetadata metadata = new PaimonMetadata(new CapturingDdlCatalog(), TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> metadata.getTableHandle(
                        SESSION,
                        new SchemaTableName(SYSTEM_DATABASE_NAME, "catalog_options"),
                        Optional.empty(),
                        Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L))),
                NOT_SUPPORTED.toErrorCode(),
                PaimonTableHandle.UNSUPPORTED_HISTORICAL_READ_MESSAGE);
        assertTrinoError(
                () -> metadata.getTableHandle(
                        SESSION,
                        new SchemaTableName("schema", "table$tags"),
                        Optional.empty(),
                        Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L))),
                NOT_SUPPORTED.toErrorCode(),
                PaimonTableHandle.UNSUPPORTED_HISTORICAL_READ_MESSAGE);
        assertTrinoError(
                () -> metadata.getTableHandle(
                        SESSION,
                        new SchemaTableName("schema", "table$branch_feature$tags"),
                        Optional.empty(),
                        Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L))),
                NOT_SUPPORTED.toErrorCode(),
                PaimonTableHandle.UNSUPPORTED_HISTORICAL_READ_MESSAGE);
    }

    @Test
    public void testInsertLayoutRequiresFileStoreTable()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertUnsupportedFileStoreTable(
                () -> metadata.getInsertLayout(SESSION, tableHandle),
                "Paimon insert layout requires FileStoreTable");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testMetadataFileStoreBoundariesRejectSearchWrapperTables()
            throws Exception
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        Slice fragment = commitFragment();

        PaimonMetadata vectorSearchMetadata = new PaimonMetadata(new TestingPaimonCatalog(VectorSearchTable.create(
                innerTable(),
                new VectorSearch(new float[] {1.0f}, 1, "embedding"))), TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> vectorSearchMetadata.getTableHandle(
                        SESSION,
                        new SchemaTableName("schema", "table"),
                        Map.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> vectorSearchMetadata.getRowChangeParadigm(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> vectorSearchMetadata.getInsertLayout(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> vectorSearchMetadata.getMergeRowIdColumnHandle(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> vectorSearchMetadata.getUpdateLayout(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> vectorSearchMetadata.beginMerge(SESSION, tableHandle, Map.of(), RetryMode.NO_RETRIES),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> vectorSearchMetadata.truncateTable(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> vectorSearchMetadata.applyDelete(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> vectorSearchMetadata.finishInsert(SESSION, tableHandle, List.of(), List.of(fragment), List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon vector search tables are not supported by the Trino connector");

        PaimonMetadata fullTextSearchMetadata = new PaimonMetadata(new TestingPaimonCatalog(FullTextSearchTable.create(
                innerTable(),
                new FullTextSearch("content", "paimon", 1))), TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> fullTextSearchMetadata.getTableHandle(
                        SESSION,
                        new SchemaTableName("schema", "table"),
                        Map.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> fullTextSearchMetadata.getRowChangeParadigm(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> fullTextSearchMetadata.getInsertLayout(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> fullTextSearchMetadata.getMergeRowIdColumnHandle(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> fullTextSearchMetadata.getUpdateLayout(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> fullTextSearchMetadata.beginMerge(SESSION, tableHandle, Map.of(), RetryMode.NO_RETRIES),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> fullTextSearchMetadata.truncateTable(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> fullTextSearchMetadata.applyDelete(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
        assertTrinoError(
                () -> fullTextSearchMetadata.finishInsert(SESSION, tableHandle, List.of(), List.of(fragment), List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon full-text search tables are not supported by the Trino connector");
    }

    @Test
    public void testMergeRowIdRequiresFileStoreTable()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertUnsupportedFileStoreTable(
                () -> metadata.getRowChangeParadigm(SESSION, tableHandle),
                "Paimon row-level change requires FileStoreTable");
        assertUnsupportedFileStoreTable(
                () -> metadata.getMergeRowIdColumnHandle(SESSION, tableHandle),
                "Paimon merge row id requires FileStoreTable");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testMergeRowIdRequiresPrimaryKeys()
    {
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                List.of());
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.getRowChangeParadigm(SESSION, tableHandle)).isEqualTo(DELETE_ROW_AND_INSERT_ROW);
        assertMetadataDeleteRowId(metadata.getMergeRowIdColumnHandle(SESSION, tableHandle));
        assertThat(metadata.getUpdateLayout(SESSION, tableHandle)).isEmpty();
        assertMetadataDeleteFallback(metadata.beginMerge(SESSION, tableHandle, Map.of(), RetryMode.NO_RETRIES));
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testRowLevelDeleteRequiresSupportedMergeEngine()
    {
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                List.of("id"),
                List.of("id"),
                "id",
                Map.of(CoreOptions.MERGE_ENGINE.key(), CoreOptions.MergeEngine.FIRST_ROW.toString()));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.getRowChangeParadigm(SESSION, tableHandle)).isEqualTo(DELETE_ROW_AND_INSERT_ROW);
        assertMetadataDeleteRowId(metadata.getMergeRowIdColumnHandle(SESSION, tableHandle));
        assertThat(metadata.getUpdateLayout(SESSION, tableHandle)).isEmpty();
        assertMetadataDeleteFallback(metadata.beginMerge(SESSION, tableHandle, Map.of(), RetryMode.NO_RETRIES));
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testMergeRowIdUsesPrimaryKeyFieldsInPrimaryKeyOrder()
    {
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "value", DataTypes.STRING()),
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "date", DataTypes.STRING())),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "value", DataTypes.STRING()),
                        DataTypes.FIELD(1, "id", DataTypes.INT()),
                        DataTypes.FIELD(2, "date", DataTypes.STRING())),
                List.of("date", "id"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        PaimonColumnHandle rowId = (PaimonColumnHandle) metadata.getMergeRowIdColumnHandle(SESSION, tableHandle);

        assertThat(rowId.getColumnName()).isEqualTo(PaimonColumnHandle.TRINO_ROW_ID_NAME);
        assertThat(((RowType) rowId.logicalType()).getFieldNames())
                .containsExactly("date", "id");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testMergeRowIdUsesLatestSchema()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                copiedWithLatestSchema,
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "id", DataTypes.INT()),
                        DataTypes.FIELD(1, "updated_key", DataTypes.STRING())),
                List.of("updated_key"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        PaimonColumnHandle rowId = (PaimonColumnHandle) metadata.getMergeRowIdColumnHandle(SESSION, tableHandle);

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(((RowType) rowId.logicalType()).getFieldNames())
                .containsExactly("updated_key");
    }

    @Test
    public void testRowTrackingBaseTableColumnsExposeHiddenMetadataColumns()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()));
        FileStoreTable table = rowTrackingFileStoreTable(copiedWithLatestSchema, rowType);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Map.of());

        List<ColumnMetadata> columns = handle.columnMetadatas(catalog.forSession(SESSION), TESTING_TYPE_MANAGER, SESSION);

        assertThat(columns)
                .extracting(ColumnMetadata::getName)
                .containsExactly("id", "name", "_row_id", "_sequence_number");
        assertThat(columns)
                .extracting(ColumnMetadata::isHidden)
                .containsExactly(false, false, true, true);
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testRowTrackingSystemTableColumnsAreVisible()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()));
        Table table = new RowTrackingTable(rowTrackingFileStoreTable(copiedWithLatestSchema, rowType));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Map.of());

        List<ColumnMetadata> columns = handle.columnMetadatas(catalog.forSession(SESSION), TESTING_TYPE_MANAGER, SESSION);

        assertThat(columns)
                .extracting(ColumnMetadata::getName)
                .containsExactly("id", "name", "_row_id", "_sequence_number");
        assertThat(columns)
                .extracting(ColumnMetadata::isHidden)
                .containsExactly(false, false, false, false);
    }

    @Test
    public void testAuditLogSystemTableSequenceNumberColumnIsVisible()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "pk", DataTypes.INT()),
                DataTypes.FIELD(1, "pt", DataTypes.INT()),
                DataTypes.FIELD(2, "col1", DataTypes.INT()));
        Table table = new AuditLogTable(sequenceNumberEnabledFileStoreTable(copiedWithLatestSchema, rowType));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Map.of());

        List<ColumnMetadata> columns = handle.columnMetadatas(catalog.forSession(SESSION), TESTING_TYPE_MANAGER, SESSION);

        assertThat(columns)
                .extracting(ColumnMetadata::getName)
                .containsExactly("rowkind", "_sequence_number", "pk", "pt", "col1");
        assertThat(columns)
                .extracting(ColumnMetadata::isHidden)
                .containsExactly(false, false, false, false, false);
    }

    @Test
    public void testGetColumnMetadataUsesVisibleSystemTableColumns()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "pk", DataTypes.INT()),
                DataTypes.FIELD(1, "pt", DataTypes.INT()),
                DataTypes.FIELD(2, "col1", DataTypes.INT()));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(
                new AuditLogTable(sequenceNumberEnabledFileStoreTable(copiedWithLatestSchema, rowType)));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of(
                "_sequence_number",
                SpecialFields.SEQUENCE_NUMBER.type());

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(SESSION, tableHandle, columnHandle);

        assertThat(columnMetadata.getName()).isEqualTo("_sequence_number");
        assertThat(columnMetadata.isHidden()).isFalse();
    }

    @Test
    public void testGetColumnMetadataKeepsMergeRowIdHidden()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of(
                PaimonColumnHandle.TRINO_ROW_ID_NAME,
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())));

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(SESSION, tableHandle, columnHandle);

        assertThat(columnMetadata.getName()).isEqualTo(PaimonColumnHandle.TRINO_ROW_ID_NAME);
        assertThat(columnMetadata.isHidden()).isTrue();
    }

    @Test
    public void testGetColumnMetadataReturnsOrdinaryColumnFromHandle()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType staleRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "stale comment"));
        RowType latestRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "latest comment"));
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(
                        BucketMode.HASH_FIXED,
                        copiedWithLatestSchema,
                        staleRowType,
                        latestRowType,
                        List.of("id"))),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT());

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(SESSION, tableHandle, columnHandle);

        assertThat(columnMetadata.getName()).isEqualTo("id");
        assertThat(columnMetadata.getComment()).contains("latest comment");
        assertThat(columnMetadata.isHidden()).isFalse();
        assertThat(copiedWithLatestSchema).isTrue();
    }

    @Test
    public void testGetColumnMetadataPreservesHistoricalSchema()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType staleRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "snapshot comment"));
        RowType latestRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "latest comment"));
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(
                        BucketMode.HASH_FIXED,
                        copiedWithLatestSchema,
                        staleRowType,
                        latestRowType,
                        List.of("id"))),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT());
        ConnectorSession historicalSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_SNAPSHOT, 9L))
                .build();

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(historicalSession, tableHandle, columnHandle);

        assertThat(columnMetadata.getName()).isEqualTo("id");
        assertThat(columnMetadata.getComment()).contains("snapshot comment");
        assertThat(copiedWithLatestSchema).isFalse();
    }

    @Test
    public void testGetColumnMetadataFallsBackToOrdinaryHandleAfterDdlRemovesColumn()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType staleRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "stale comment"),
                new DataField(1, "order_status", DataTypes.STRING(), "old comment"));
        RowType latestRowType = DataTypes.ROW(
                new DataField(0, "id", DataTypes.INT(), "stale comment"));
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(
                        BucketMode.HASH_FIXED,
                        copiedWithLatestSchema,
                        staleRowType,
                        latestRowType,
                        List.of("id"))),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle staleColumnHandle = PaimonColumnHandle.of("order_status", DataTypes.STRING());

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(SESSION, tableHandle, staleColumnHandle);

        assertThat(columnMetadata).isEqualTo(staleColumnHandle.getColumnMetadata());
        assertThat(copiedWithLatestSchema).isTrue();
    }

    @Test
    public void testGetColumnHandlesMapsColumnNamesToHandles()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle);

        assertThat(columnHandles).hasSize(1);
        assertThat(columnHandles).containsKey("id");
        assertThat(columnHandles.get("id")).isInstanceOf(PaimonColumnHandle.class);
        assertThat(((PaimonColumnHandle) columnHandles.get("id")).getColumnName()).isEqualTo("id");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testGetTableMetadata()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                List.of(),
                List.of("id"),
                "id"));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);

        assertThat(tableMetadata.getTable()).isEqualTo(new SchemaTableName("schema", "table"));
        assertThat(tableMetadata.getColumns()).extracting(ColumnMetadata::getName).containsExactly("id");
        assertThat(tableMetadata.getProperties())
                .containsEntry(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("id"))
                .containsEntry("bucket", "7")
                .containsEntry("bucket_key", "id");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testGetTablePropertiesReturnsEmptyProperties()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableProperties properties = metadata.getTableProperties(SESSION, tableHandle);

        assertThat(properties).isNotNull();
    }

    @Test
    public void testBeginInsertReturnsHandleWithWriteColumns()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());

        ConnectorInsertTableHandle insertHandle = metadata.beginInsert(SESSION, tableHandle, List.of(id), RetryMode.NO_RETRIES);

        assertThat(insertHandle).isInstanceOf(PaimonTableHandle.class);
        PaimonTableHandle result = (PaimonTableHandle) insertHandle;
        assertThat(result.getWriteColumns()).hasValueSatisfying(writeColumns ->
                assertThat(writeColumns).extracting(PaimonColumnHandle::getColumnName).containsExactly("id"));
    }

    @Test
    public void testListTableColumnsSkipsMissingTables()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public List<String> listTables(String databaseName)
            {
                return List.of("existing", "missing");
            }

            @Override
            public List<String> listViews(String databaseName)
            {
                return List.of();
            }

            @Override
            public Table getTable(Identifier identifier)
                    throws Catalog.TableNotExistException
            {
                if (identifier.getObjectName().equals("missing")) {
                    throw new Catalog.TableNotExistException(identifier);
                }
                return fileStoreTable(BucketMode.HASH_FIXED);
            }

            @Override
            public View getView(Identifier identifier)
                    throws Catalog.ViewNotExistException
            {
                throw new Catalog.ViewNotExistException(identifier);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(SESSION, new SchemaTablePrefix("schema"));

        assertThat(columns).hasSize(1);
        assertThat(columns).containsKey(new SchemaTableName("schema", "existing"));
    }

    @Test
    public void testStreamTableColumns()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public List<String> listTables(String databaseName)
            {
                return List.of("table");
            }

            @Override
            public List<String> listViews(String databaseName)
            {
                return List.of();
            }

            @Override
            public Table getTable(Identifier identifier)
            {
                return fileStoreTable(BucketMode.HASH_FIXED);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        Iterator<TableColumnsMetadata> columns = metadata.streamTableColumns(SESSION, new SchemaTablePrefix("schema"));

        assertThat(columns.hasNext()).isTrue();
        TableColumnsMetadata tableColumns = columns.next();
        assertThat(tableColumns.getTable()).isEqualTo(new SchemaTableName("schema", "table"));
        assertThat(tableColumns.getColumns()).hasValueSatisfying(list ->
                assertThat(list).extracting(ColumnMetadata::getName).containsExactly("id"));
        assertThat(columns.hasNext()).isFalse();
    }

    @Test
    public void testListTableColumnsIncludesPaimonViewColumns()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public List<String> listTables(String databaseName)
            {
                return List.of("table");
            }

            @Override
            public List<String> listViews(String databaseName)
            {
                return List.of("view");
            }

            @Override
            public Table getTable(Identifier identifier)
                    throws Catalog.TableNotExistException
            {
                if (identifier.getObjectName().equals("view")) {
                    throw new Catalog.TableNotExistException(identifier);
                }
                return fileStoreTable(BucketMode.HASH_FIXED);
            }

            @Override
            public View getView(Identifier identifier)
            {
                return paimonView(identifier, List.of(
                        DataTypes.FIELD(0, "value", DataTypes.BIGINT(), "value comment"),
                        DataTypes.FIELD(1, "label", DataTypes.STRING())));
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        SchemaTableName viewName = new SchemaTableName("schema", "view");
        Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(
                SESSION,
                new SchemaTablePrefix("schema"));

        assertThat(columns).containsKey(viewName);
        assertThat(columns.get(viewName))
                .satisfiesExactly(
                        column -> {
                            assertThat(column.getName()).isEqualTo("value");
                            assertThat(column.getType()).isEqualTo(BIGINT);
                            assertThat(column.getComment()).isEqualTo(Optional.of("value comment"));
                        },
                        column -> {
                            assertThat(column.getName()).isEqualTo("label");
                            assertThat(column.getType()).isEqualTo(VARCHAR);
                            assertThat(column.getComment()).isEmpty();
                        });

        Iterator<TableColumnsMetadata> streamedColumns = metadata.streamTableColumns(
                SESSION,
                new SchemaTablePrefix("schema", "view"));
        assertThat(streamedColumns.hasNext()).isTrue();
        TableColumnsMetadata viewColumns = streamedColumns.next();
        assertThat(viewColumns.getTable()).isEqualTo(viewName);
        assertThat(viewColumns.getColumns()).hasValueSatisfying(list ->
                assertThat(list).extracting(ColumnMetadata::getName).containsExactly("value", "label"));
        assertThat(streamedColumns.hasNext()).isFalse();
    }

    @Test
    public void testMergeRowIdFailsWhenPrimaryKeyIsMissingFromTableSchema()
    {
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                List.of("missing"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.getMergeRowIdColumnHandle(SESSION, tableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon primary key 'missing' is not present in table schema [id]");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testUpdateLayoutRequiresFileStoreTable()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertUnsupportedFileStoreTable(
                () -> metadata.getUpdateLayout(SESSION, tableHandle),
                "Paimon update layout requires FileStoreTable");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testInsertAndUpdateLayoutsUseLatestSchema()
            throws IOException
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType staleRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "old_bucket", DataTypes.INT()));
        RowType latestRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "new_bucket", DataTypes.INT()));
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                copiedWithLatestSchema,
                staleRowType,
                latestRowType,
                List.of("id", "new_bucket"),
                "new_bucket");
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableLayout insertLayout = metadata.getInsertLayout(SESSION, tableHandle).orElseThrow();
        TableSchema insertSchema = partitioningSchema(insertLayout.getPartitioning().orElseThrow());

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(insertLayout.getPartitionColumns()).containsExactly("id", "new_bucket");
        assertThat(insertSchema.fieldNames()).containsExactly("id", "new_bucket");
        assertThat(insertSchema.bucketKeys()).containsExactly("new_bucket");

        TableSchema updateSchema = partitioningSchema(metadata.getUpdateLayout(SESSION, tableHandle).orElseThrow());
        assertThat(updateSchema.fieldNames()).containsExactly("id", "new_bucket");
        assertThat(updateSchema.bucketKeys()).containsExactly("new_bucket");
    }

    @Test
    public void testFixedBucketInsertLayoutUsesPartitionAndBucketKeys()
            throws IOException
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.INT()),
                DataTypes.FIELD(1, "id", DataTypes.INT()),
                DataTypes.FIELD(2, "bucket_key", DataTypes.INT()));
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of("dt"),
                List.of("dt", "id", "bucket_key"),
                "bucket_key");
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableLayout insertLayout = metadata.getInsertLayout(SESSION, tableHandle).orElseThrow();

        assertThat(insertLayout.getPartitionColumns()).containsExactly("dt", "bucket_key");
    }

    @Test
    public void testNewTableLayoutUsesPaimonBucketMode()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata fixedBucketTable = new ConnectorTableMetadata(
                new SchemaTableName("schema", "fixed_bucket"),
                List.of(
                        new ColumnMetadata("dt", INTEGER),
                        new ColumnMetadata("id", INTEGER),
                        new ColumnMetadata("bucket_key", INTEGER)),
                Map.of(
                        PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of("dt"),
                        "bucket", "4",
                        "bucket_key", "bucket_key"));
        ConnectorTableMetadata hashDynamicTable = new ConnectorTableMetadata(
                new SchemaTableName("schema", "hash_dynamic"),
                List.of(
                        new ColumnMetadata("dt", INTEGER),
                        new ColumnMetadata("id", INTEGER)),
                Map.of(
                        PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("dt", "id"),
                        PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of("dt"),
                        "bucket", "-1"));
        ConnectorTableMetadata unawareTable = new ConnectorTableMetadata(
                new SchemaTableName("schema", "unaware"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of("bucket", "-1"));

        ConnectorTableLayout fixedLayout = metadata.getNewTableLayout(SESSION, fixedBucketTable).orElseThrow();
        TableSchema fixedSchema = partitioningSchema(fixedLayout.getPartitioning().orElseThrow());
        assertThat(fixedLayout.getPartitionColumns()).containsExactly("dt", "bucket_key");
        assertThat(fixedSchema.partitionKeys()).containsExactly("dt");
        assertThat(fixedSchema.bucketKeys()).containsExactly("bucket_key");

        ConnectorTableLayout dynamicLayout = metadata.getNewTableLayout(SESSION, hashDynamicTable).orElseThrow();
        assertThat(dynamicLayout.getPartitionColumns()).containsExactly("dt", "id");
        assertThat(dynamicLayout.getPartitioning().orElseThrow())
                .isInstanceOfSatisfying(PaimonPartitioningHandle.class, handle -> assertThat(handle.isSingleNode()).isFalse());

        assertThat(metadata.getNewTableLayout(SESSION, unawareTable)).isEmpty();
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testNewTableLayoutAppliesPaimonColumnCommentDirectives()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "directive_layout"),
                List.of(
                        new ColumnMetadata("id", INTEGER),
                        ColumnMetadata.builder()
                                .setName("embedding")
                                .setType(new ArrayType(REAL))
                                .setComment(Optional.of("__VECTOR_FIELD;3; embedding"))
                                .build(),
                        ColumnMetadata.builder()
                                .setName("picture")
                                .setType(VarbinaryType.VARBINARY)
                                .setComment(Optional.of("__BLOB_FIELD; profile picture"))
                                .build()),
                Map.of(
                        PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("id"),
                        "bucket", "-1"));

        ConnectorTableLayout layout = metadata.getNewTableLayout(SESSION, tableMetadata).orElseThrow();

        TableSchema schema = partitioningSchema(layout.getPartitioning().orElseThrow());
        assertThat(layout.getPartitionColumns()).containsExactly("id");
        assertThat(schema.fields()).extracting(field -> field.type().getTypeRoot())
                .containsExactly(DataTypeRoot.INTEGER, DataTypeRoot.VECTOR, DataTypeRoot.BLOB);
        assertThat(schema.fields()).extracting(DataField::description)
                .containsExactly(null, "embedding", "profile picture");
        assertThat(schema.options())
                .containsEntry(CoreOptions.BUCKET.key(), "-1")
                .containsEntry(CoreOptions.VECTOR_FIELD.key(), "embedding")
                .containsEntry(CoreOptions.BLOB_FIELD.key(), "picture");
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testNewTableLayoutRejectsUnsupportedBucketModes()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata keyDynamicTable = new ConnectorTableMetadata(
                new SchemaTableName("schema", "key_dynamic"),
                List.of(
                        new ColumnMetadata("dt", INTEGER),
                        new ColumnMetadata("id", INTEGER)),
                Map.of(
                        PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("id"),
                        PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of("dt"),
                        "bucket", "-1"));
        ConnectorTableMetadata postponeTable = new ConnectorTableMetadata(
                new SchemaTableName("schema", "postpone"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of("bucket", "-2"));

        ConnectorTableLayout keyDynamicLayout = metadata.getNewTableLayout(SESSION, keyDynamicTable).orElseThrow();
        assertThat(keyDynamicLayout.getPartitionColumns()).containsExactly("id");
        assertTrinoError(
                () -> metadata.getNewTableLayout(SESSION, postponeTable),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported table bucket mode: POSTPONE_MODE for Paimon new table layout");
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testBeginCreateTableRejectsUnsupportedBucketModesBeforeCreatingTable()
    {
        RowType keyDynamicRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.INT()),
                DataTypes.FIELD(1, "id", DataTypes.INT()));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.KEY_DYNAMIC,
                new AtomicBoolean(),
                keyDynamicRowType,
                keyDynamicRowType,
                List.of("dt"),
                List.of("id"),
                "id"));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata keyDynamicTable = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        new ColumnMetadata("dt", INTEGER),
                        new ColumnMetadata("id", INTEGER)),
                Map.of(
                        PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("id"),
                        PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of("dt"),
                        "bucket", "-1"));
        ConnectorTableMetadata postponeTable = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of("bucket", "-2"));

        ConnectorTableLayout keyDynamicLayout = metadata.getNewTableLayout(SESSION, keyDynamicTable).orElseThrow();
        PaimonTableHandle outputHandle = (PaimonTableHandle) metadata.beginCreateTable(
                SESSION,
                keyDynamicTable,
                Optional.of(keyDynamicLayout),
                RetryMode.NO_RETRIES,
                false);
        assertThat(outputHandle.isKeyDynamicBootstrapSnapshotPlanned()).isTrue();
        assertThat(outputHandle.getKeyDynamicBootstrapSnapshot()).isEmpty();
        assertThat(catalog.createdSchema).isNotNull();
        assertThat(catalog.createdSchema.partitionKeys()).containsExactly("dt");
        assertThat(catalog.createdSchema.primaryKeys()).containsExactly("id");

        assertTrinoError(
                () -> metadata.beginCreateTable(
                        SESSION,
                        postponeTable,
                        Optional.empty(),
                        RetryMode.NO_RETRIES,
                        false),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported table bucket mode: POSTPONE_MODE for Paimon create table");
        assertThat(catalog.createdSchema).isNotNull();
    }

    @Test
    public void testInsertLayoutIgnoresSessionScanSnapshotAndHandleStartupSelections()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions = new AtomicReference<>();
        FileStoreTable table = writePlanningFileStoreTable(copiedWithLatestSchema, copyWithoutTimeTravelOptions);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(
                        "custom.option", "value",
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2",
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta",
                        CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true"));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_SNAPSHOT, 9L))
                .build();

        assertThat(metadata.getInsertLayout(session, tableHandle)).isPresent();

        assertThat(copyWithoutTimeTravelOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testDynamicBucketWritePlanningUsesAssignerLayoutForRowLevelChanges()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(fileStoreTable(
                BucketMode.HASH_DYNAMIC,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of(),
                List.of("id"),
                "id",
                Map.of(
                        CoreOptions.BUCKET.key(), "-1",
                        CoreOptions.DYNAMIC_BUCKET_ASSIGNER_PARALLELISM.key(), "3"))),
                TESTING_TYPE_MANAGER,
                () -> 5);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableLayout insertLayout = metadata.getInsertLayout(SESSION, tableHandle).orElseThrow();
        assertThat(insertLayout.getPartitionColumns()).containsExactly("id");
        assertThat(insertLayout.getPartitioning().orElseThrow())
                .isInstanceOfSatisfying(PaimonPartitioningHandle.class, handle -> {
                    assertThat(handle.isSingleNode()).isFalse();
                    assertThat(handle.dynamicBucketAssignerParallelism()).hasValue(3);
                });
        PaimonTableHandle insertHandle = (PaimonTableHandle) metadata.beginInsert(
                SESSION,
                tableHandle,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT())),
                RetryMode.NO_RETRIES);
        assertThat(insertHandle.getDynamicBucketAssignerParallelism()).hasValue(3);

        assertThat(metadata.getRowChangeParadigm(SESSION, tableHandle)).isEqualTo(DELETE_ROW_AND_INSERT_ROW);
        assertThat(metadata.getMergeRowIdColumnHandle(SESSION, tableHandle))
                .isInstanceOfSatisfying(PaimonColumnHandle.class, rowId ->
                        assertThat(rowId.getColumnName()).isEqualTo(PaimonColumnHandle.TRINO_ROW_ID_NAME));
        assertThat(metadata.getUpdateLayout(SESSION, tableHandle).orElseThrow())
                .isInstanceOfSatisfying(PaimonPartitioningHandle.class, handle -> {
                    assertThat(handle.isSingleNode()).isFalse();
                    assertThat(handle.dynamicBucketAssignerParallelism()).hasValue(3);
                });
        assertThat(metadata.beginMerge(SESSION, tableHandle, Map.of(), RetryMode.NO_RETRIES))
                .isInstanceOfSatisfying(PaimonMergeTableHandle.class, mergeHandle ->
                        assertThat(mergeHandle.paimonTableHandle().getDynamicBucketAssignerParallelism()).hasValue(3));
    }

    @Test
    public void testDynamicBucketBeginMergeUsesPlannedRowLevelAssignerLayout()
    {
        AtomicInteger workerCount = new AtomicInteger(3);
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(fileStoreTable(
                BucketMode.HASH_DYNAMIC,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of(),
                List.of("id"),
                "id",
                Map.of(CoreOptions.BUCKET.key(), "-1"))),
                TESTING_TYPE_MANAGER,
                workerCount::get);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        PaimonPartitioningHandle updateLayout = (PaimonPartitioningHandle) metadata.getUpdateLayout(SESSION, tableHandle)
                .orElseThrow();
        assertThat(updateLayout.dynamicBucketAssignerParallelism()).hasValue(3);

        workerCount.set(5);
        PaimonMergeTableHandle mergeHandle = (PaimonMergeTableHandle) metadata.beginMerge(
                SESSION,
                tableHandle,
                Map.of(),
                RetryMode.NO_RETRIES);

        assertThat(mergeHandle.paimonTableHandle().getDynamicBucketAssignerParallelism()).hasValue(3);
    }

    @Test
    public void testDynamicBucketBeginInsertUsesPlannedAssignerLayout()
    {
        AtomicInteger workerCount = new AtomicInteger(3);
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(fileStoreTable(
                BucketMode.HASH_DYNAMIC,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of(),
                List.of("id"),
                "id",
                Map.of(CoreOptions.BUCKET.key(), "-1"))),
                TESTING_TYPE_MANAGER,
                workerCount::get);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableLayout insertLayout = metadata.getInsertLayout(SESSION, tableHandle).orElseThrow();
        assertThat(insertLayout.getPartitioning().orElseThrow())
                .isInstanceOfSatisfying(PaimonPartitioningHandle.class, handle ->
                        assertThat(handle.dynamicBucketAssignerParallelism()).hasValue(3));

        workerCount.set(5);
        PaimonTableHandle insertHandle = (PaimonTableHandle) metadata.beginInsert(
                SESSION,
                tableHandle,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT())),
                RetryMode.NO_RETRIES);

        assertThat(insertHandle.getDynamicBucketAssignerParallelism()).hasValue(3);
    }

    @Test
    public void testLayoutSerializationFailuresUsePaimonMetadataError()
    {
        IOException failure = new IOException("schema serialization failed");
        FileStoreTable table = nonSerializableSchemaFileStoreTable(failure);
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.getInsertLayout(SESSION, tableHandle))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to prepare Paimon insert layout for table 'schema.table'");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class);
                });
        assertThatThrownBy(() -> metadata.getUpdateLayout(SESSION, tableHandle))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to prepare Paimon update layout for table 'schema.table'");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class);
                });
    }

    @Test
    public void testBeginMergeRequiresFileStoreTable()
    {
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table()), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertUnsupportedFileStoreTable(
                () -> metadata.beginMerge(SESSION, tableHandle, Map.of(), RetryMode.NO_RETRIES),
                "Paimon merge requires FileStoreTable");
    }

    @Test
    public void testBeginMergeRejectsQueryRetriesBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.beginMerge(SESSION, tableHandle, Map.of(), RetryMode.RETRIES_ENABLED),
                NOT_SUPPORTED.toErrorCode(),
                "This connector does not support query retries");
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testBeginMergeUsesMetadataDeleteFallbackForUnsupportedRowLevelBucketMode()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(BucketMode.BUCKET_UNAWARE)),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.getRowChangeParadigm(SESSION, tableHandle)).isEqualTo(DELETE_ROW_AND_INSERT_ROW);
        assertMetadataDeleteRowId(metadata.getMergeRowIdColumnHandle(SESSION, tableHandle));
        assertThat(metadata.getUpdateLayout(SESSION, tableHandle)).isEmpty();
        assertMetadataDeleteFallback(metadata.beginMerge(SESSION, tableHandle, Map.of(), RetryMode.NO_RETRIES));
    }

    @Test
    public void testBeginMergeUsesLatestSchemaForExplicitWriteColumns()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RowType staleRowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        RowType latestRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "name", DataTypes.STRING()));
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                copiedWithLatestSchema,
                staleRowType,
                latestRowType);
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorMergeTableHandle mergeHandle = metadata.beginMerge(SESSION, tableHandle, Map.of(), RetryMode.NO_RETRIES);

        assertThat(copiedWithLatestSchema).isTrue();
        PaimonTableHandle writeHandle = (PaimonTableHandle) mergeHandle.getTableHandle();
        assertThat(writeHandle.getWriteColumns()).hasValueSatisfying(writeColumns ->
                assertThat(writeColumns).extracting(PaimonColumnHandle::getColumnName)
                        .containsExactly("id", "name"));
    }

    @Test
    public void testMergeMetadataPlanningIgnoresSessionScanSnapshotAndHandleStartupSelections()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions = new AtomicReference<>();
        FileStoreTable table = writePlanningFileStoreTable(copiedWithLatestSchema, copyWithoutTimeTravelOptions);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(
                        "custom.option", "value",
                        CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), "2024-12-04"));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_SNAPSHOT, 9L))
                .build();

        assertThat(metadata.getMergeRowIdColumnHandle(session, tableHandle)).isInstanceOf(PaimonColumnHandle.class);
        assertThat(metadata.getUpdateLayout(session, tableHandle)).isPresent();
        assertThat(metadata.beginMerge(session, tableHandle, Map.of(), RetryMode.NO_RETRIES))
                .isInstanceOf(PaimonMergeTableHandle.class);

        assertThat(copyWithoutTimeTravelOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testTruncateRequiresFileStoreTable()
    {
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table()), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertUnsupportedFileStoreTable(
                () -> metadata.truncateTable(SESSION, tableHandle),
                "Paimon truncate table requires FileStoreTable");
        assertUnsupportedFileStoreTable(
                () -> metadata.applyDelete(SESSION, tableHandle),
                "Paimon delete requires FileStoreTable");
        assertUnsupportedFileStoreTable(
                () -> metadata.executeDelete(SESSION, tableHandle),
                "Paimon delete requires FileStoreTable");
    }

    @Test
    public void testCommitRequiresFileStoreTable()
            throws Exception
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        Slice fragment = commitFragment();

        assertUnsupportedFileStoreTable(
                () -> metadata.finishInsert(SESSION, tableHandle, List.of(), List.of(fragment), List.of()),
                "Paimon commit writes requires FileStoreTable");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testCommitUsesLatestSchemaBeforeCreatingBatchWriteBuilder()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(copiedWithLatestSchema, committed);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.finishInsert(SESSION, tableHandle, List.of(), List.of(commitFragment()), List.of()))
                .isEmpty();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(committed).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testCommitIgnoresSessionScanSnapshotAndHandleStartupSelections()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions = new AtomicReference<>();
        FileStoreTable table = commitFileStoreTable(copiedWithLatestSchema, committed, copyWithoutTimeTravelOptions);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(
                        "custom.option", "value",
                        CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2",
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), "delta",
                        CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true"));
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(SCAN_SNAPSHOT, 9L))
                .build();

        assertThat(metadata.finishInsert(session, tableHandle, List.of(), List.of(commitFragment()), List.of()))
                .isEmpty();

        assertThat(copyWithoutTimeTravelOptions.get()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "custom.option", "value"));
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(committed).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testInsertOverwriteAppliesToFinishInsertOnly()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        AtomicReference<String> operation = new AtomicReference<>();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                overwriteEnabled,
                operation);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        assertThat(metadata.finishInsert(overwriteSession, tableHandle, List.of(), List.of(commitFragment()), List.of()))
                .isEmpty();

        assertThat(overwriteEnabled).isTrue();
        assertThat(operation).hasValue("OVERWRITE");
        assertThat(committed).isTrue();
    }

    @Test
    public void testInsertOverwriteCommitsEmptyFragments()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        AtomicReference<String> operation = new AtomicReference<>();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                overwriteEnabled,
                operation);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        assertThat(metadata.finishInsert(overwriteSession, tableHandle, List.of(), List.of(), List.of()))
                .isEmpty();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(overwriteEnabled).isTrue();
        assertThat(operation).hasValue("OVERWRITE");
        assertThat(committed).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testInsertOverwriteDoesNotApplyToFinishMerge()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        AtomicReference<String> operation = new AtomicReference<>();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                overwriteEnabled,
                operation);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();

        metadata.finishMerge(overwriteSession, new PaimonMergeTableHandle(tableHandle), List.of(), List.of(commitFragment()), List.of());

        assertThat(overwriteEnabled).isFalse();
        assertThat(operation).hasValue("MERGE");
        assertThat(committed).isTrue();
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackTruncatesOnlyWhenAllRowsWereDeleted()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                List.of(testingSplit(2, OptionalLong.empty()), testingSplit(3, OptionalLong.empty())),
                List.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.finishMerge(
                SESSION,
                PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                List.of(),
                List.of(PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(2),
                        PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(3)),
                List.of());

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isTrue();
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackRejectsLimitedHandle()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                List.of(testingSplit(1, OptionalLong.empty())),
                List.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(1));

        assertTrinoError(
                () -> metadata.finishMerge(
                        SESSION,
                        PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                        List.of(),
                        List.of(PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(1)),
                        List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon metadata delete fallback can only delete all rows or complete partitions from an unlimited table handle");
        assertThat(copiedWithLatestSchema).isFalse();
        assertThat(truncated).isFalse();
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackRejectsPrecomputedPartitionSpecs()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                List.of(testingSplit(1, OptionalLong.empty())),
                List.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withDeletePartitionSpecs(List.of(Map.of("ds", "2026-07-01")));

        assertTrinoError(
                () -> metadata.finishMerge(
                        SESSION,
                        PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                        List.of(),
                        List.of(PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(1)),
                        List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon metadata delete fallback can only delete all rows or complete partitions from an unlimited table handle");
        assertThat(copiedWithLatestSchema).isFalse();
        assertThat(truncated).isFalse();
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackRejectsFilteredHandle()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                List.of(testingSplit(1, OptionalLong.empty())),
                List.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertTrinoError(
                () -> metadata.finishMerge(
                        SESSION,
                        PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                        List.of(),
                        List.of(PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(1)),
                        List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon metadata delete fallback can only delete all rows or complete partitions from an unlimited table handle");
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackTruncatesCompletePartitions()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        AtomicBoolean partitionFilterApplied = new AtomicBoolean();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "ds", DataTypes.STRING()),
                DataTypes.FIELD(1, "id", DataTypes.INT()));
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                partitionFilterApplied,
                List.of(testingSplit(2, OptionalLong.empty())),
                rowType,
                List.of("ds"),
                List.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonColumnHandle ds = PaimonColumnHandle.of("ds", DataTypes.STRING());
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(ds, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-07-01")))),
                Optional.of(List.of(id)),
                Optional.empty(),
                OptionalLong.empty());

        metadata.finishMerge(
                SESSION,
                PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                List.of(),
                List.of(PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(2)),
                List.of());

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(partitionFilterApplied).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).containsExactly(Map.of("ds", "2026-07-01"));
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackRejectsPartialPartitionDelete()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        AtomicBoolean partitionFilterApplied = new AtomicBoolean();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "ds", DataTypes.STRING()),
                DataTypes.FIELD(1, "id", DataTypes.INT()));
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                partitionFilterApplied,
                List.of(testingSplit(2, OptionalLong.empty())),
                rowType,
                List.of("ds"),
                List.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonColumnHandle ds = PaimonColumnHandle.of("ds", DataTypes.STRING());
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(ds, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-07-01")))),
                Optional.of(List.of(id)),
                Optional.empty(),
                OptionalLong.empty());

        assertTrinoError(
                () -> metadata.finishMerge(
                        SESSION,
                        PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                        List.of(),
                        List.of(PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(1)),
                        List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon metadata delete fallback can only delete complete partitions; query deleted 1 rows but selected partitions currently contain 2 rows");
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(partitionFilterApplied).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackNoOpsWhenNoRowsWereDeleted()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                List.of(testingSplit(10, OptionalLong.empty())),
                List.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(1));

        metadata.finishMerge(
                SESSION,
                PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                List.of(),
                List.of(),
                List.of());

        assertThat(copiedWithLatestSchema).isFalse();
        assertThat(truncated).isFalse();
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackRejectsPartialDelete()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                List.of(testingSplit(10, OptionalLong.empty())),
                List.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.finishMerge(
                        SESSION,
                        PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                        List.of(),
                        List.of(PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(4)),
                        List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon metadata delete fallback can only delete all rows; query deleted 4 rows but table currently contains 10 rows");
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackRejectsNegativeCurrentRowCount()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                List.of(testingSplit(-1, OptionalLong.empty())),
                List.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.finishMerge(
                        SESSION,
                        PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                        List.of(),
                        List.of(PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(1)),
                        List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon metadata delete fallback cannot determine the current row count because Paimon reported a negative split row count: -1");
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackRejectsOverflowingCurrentRowCount()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                List.of(
                        testingSplit(Long.MAX_VALUE - 1, OptionalLong.empty()),
                        testingSplit(10, OptionalLong.empty())),
                List.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.finishMerge(
                        SESSION,
                        PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                        List.of(),
                        List.of(PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(1)),
                        List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon metadata delete fallback cannot determine the current row count because Paimon split row counts exceed the supported range");
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackRejectsOverflowingMergedCurrentRowCount()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                List.of(
                        testingSplit(100, OptionalLong.of(Long.MAX_VALUE - 1)),
                        testingSplit(100, OptionalLong.of(10))),
                List.of("id"));
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.finishMerge(
                        SESSION,
                        PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                        List.of(),
                        List.of(PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(1)),
                        List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon metadata delete fallback cannot determine the current row count because Paimon split row counts exceed the supported range");
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
    }

    @Test
    public void testFinishMergeMetadataDeleteFallbackRejectsPrimaryKeyTableWithoutMergedRowCounts()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                List.of(testingSplit(10, OptionalLong.empty())),
                List.of("id"));
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.finishMerge(
                        SESSION,
                        PaimonMergeTableHandle.forMetadataDeleteFallback(tableHandle),
                        List.of(),
                        List.of(PaimonMetadataDeleteMergeSink.encodeDeletedRowCount(10)),
                        List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon metadata delete fallback cannot determine the current row count for primary-key tables");
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
    }

    @Test
    public void testFinishCreateTableMarksCreateTableAsSelectOperation()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        AtomicReference<String> operation = new AtomicReference<>();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                new AtomicBoolean(),
                operation);
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withCreateTableOperation(PaimonTableHandle.CREATE_TABLE_AS_SELECT_OPERATION);

        metadata.finishCreateTable(SESSION, tableHandle, List.of(commitFragment()), List.of());

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(operation).hasValue(PaimonTableHandle.CREATE_TABLE_AS_SELECT_OPERATION);
        assertThat(committed).isTrue();
    }

    @Test
    public void testFinishCreateTableMarksCreateOrReplaceTableAsSelectOperation()
            throws Exception
    {
        AtomicBoolean committed = new AtomicBoolean();
        AtomicReference<String> operation = new AtomicReference<>();
        FileStoreTable table = commitFileStoreTable(
                new AtomicBoolean(),
                committed,
                new AtomicReference<>(),
                null,
                new AtomicBoolean(),
                operation);
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of())
                .withCreateTableOperation(PaimonTableHandle.CREATE_OR_REPLACE_TABLE_AS_SELECT_OPERATION);

        metadata.finishCreateTable(SESSION, tableHandle, List.of(commitFragment()), List.of());

        assertThat(operation).hasValue(PaimonTableHandle.CREATE_OR_REPLACE_TABLE_AS_SELECT_OPERATION);
        assertThat(committed).isTrue();
    }

    @Test
    public void testInsertErrorRejectsExistingNonPartitionedTable()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                new AtomicBoolean(),
                List.of(new PartitionEntry(BinaryRow.EMPTY_ROW, 1, 1, 1, 1, 1)),
                List.of(),
                Map.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        ConnectorSession errorSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR.name()))
                .build();
        Slice fragment = commitFragment();

        assertTrinoError(
                () -> metadata.finishInsert(
                        errorSession,
                        new PaimonTableHandle("schema", "table", Map.of()),
                        List.of(),
                        List.of(fragment),
                        List.of()),
                READ_ONLY_VIOLATION.toErrorCode(),
                "Cannot insert into an existing non-partitioned Paimon table: schema.table");
        assertThat(committed).isFalse();
    }

    @Test
    public void testInsertErrorRejectsExistingPartition()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        BinaryRow partition = partitionRow("p1");
        Map<String, String> partitionSpec = Map.of("pt", "p1");
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                new AtomicBoolean(),
                List.of(),
                List.of("pt"),
                Map.of());
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(
                table,
                List.of(new Partition(partitionSpec, 1, 1, 1, 1, 1, true)));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorSession errorSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR.name()))
                .build();
        Slice fragment = commitFragment(partition);

        assertTrinoError(
                () -> metadata.finishInsert(
                        errorSession,
                        new PaimonTableHandle("schema", "table", Map.of()),
                        List.of(),
                        List.of(fragment),
                        List.of()),
                READ_ONLY_VIOLATION.toErrorCode(),
                "Cannot insert into an existing partition of Paimon table: schema.table");
        assertThat(committed).isFalse();
        assertThat(catalog.listedPartitionsByNames).containsExactly(List.of(partitionSpec));
    }

    @Test
    public void testInsertErrorAllowsNewPartition()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                new AtomicBoolean(),
                List.of(new PartitionEntry(partitionRow("p1"), 1, 1, 1, 1, 1)),
                List.of("pt"),
                Map.of());
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        ConnectorSession errorSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR.name()))
                .build();
        Slice fragment = commitFragment(partitionRow("p2"));

        assertThat(metadata.finishInsert(
                errorSession,
                new PaimonTableHandle("schema", "table", Map.of()),
                List.of(),
                List.of(fragment),
                List.of())).isEmpty();
        assertThat(committed).isTrue();
    }

    @Test
    public void testInsertErrorChecksOnlyWrittenPartitionNames()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                new AtomicBoolean(),
                List.of(),
                List.of("pt"),
                Map.of());
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table, List.of());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorSession errorSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR.name()))
                .build();

        assertThat(metadata.finishInsert(
                errorSession,
                new PaimonTableHandle("schema", "table", Map.of()),
                List.of(),
                List.of(
                        commitFragment(partitionRow("p1")),
                        commitFragment(partitionRow("p1")),
                        commitFragment(partitionRow("p2"))),
                List.of())).isEmpty();

        assertThat(committed).isTrue();
        assertThat(catalog.listedPartitionsByNames).containsExactly(List.of(
                Map.of("pt", "p1"),
                Map.of("pt", "p2")));
    }

    @Test
    public void testInsertErrorBatchesWrittenPartitionNameChecks()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                new AtomicBoolean(),
                List.of(),
                List.of("pt"),
                Map.of());
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table, List.of());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorSession errorSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR.name()))
                .build();
        List<Slice> fragments = new ArrayList<>();
        for (int i = 0; i < 1001; i++) {
            fragments.add(commitFragment(partitionRow("p" + i)));
        }

        assertThat(metadata.finishInsert(
                errorSession,
                new PaimonTableHandle("schema", "table", Map.of()),
                List.of(),
                fragments,
                List.of())).isEmpty();

        assertThat(committed).isTrue();
        assertThat(catalog.listedPartitionsByNames).hasSize(2);
        assertThat(catalog.listedPartitionsByNames.get(0)).hasSize(1000);
        assertThat(catalog.listedPartitionsByNames.get(1)).containsExactly(Map.of("pt", "p1000"));
    }

    @Test
    public void testInsertErrorChecksPartitionsOnWriteBranch()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                new AtomicBoolean(),
                List.of(),
                List.of("pt"),
                Map.of(CoreOptions.BRANCH.key(), "dev"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table, List.of());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorSession errorSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.ERROR.name()))
                .build();

        assertThat(metadata.finishInsert(
                errorSession,
                new PaimonTableHandle("schema", "table", Map.of()),
                List.of(),
                List.of(commitFragment(partitionRow("p1"))),
                List.of())).isEmpty();

        assertThat(committed).isTrue();
        assertThat(catalog.listedPartitionIdentifiers).singleElement()
                .satisfies(identifier -> {
                    assertThat(identifier.getTableName()).isEqualTo("table");
                    assertThat(identifier.getBranchNameOrDefault()).isEqualTo("dev");
                });
    }

    @Test
    public void testInsertOverwriteRejectsPartitionedTableWithoutDynamicPartitionOverwrite()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        AtomicBoolean overwriteEnabled = new AtomicBoolean();
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                null,
                overwriteEnabled,
                List.of(),
                List.of("pt"),
                Map.of(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false"));
        PaimonMetadata metadata = new PaimonMetadata(new TestingPaimonCatalog(table), TESTING_TYPE_MANAGER);
        ConnectorSession overwriteSession = TestingConnectorSession.builder()
                .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                .setPropertyValues(Map.of(
                        PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, PaimonSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE.name()))
                .build();
        Slice fragment = commitFragment(partitionRow("p1"));

        assertTrinoError(
                () -> metadata.finishInsert(
                        overwriteSession,
                        new PaimonTableHandle("schema", "table", Map.of()),
                        List.of(),
                        List.of(fragment),
                        List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon insert overwrite requires dynamic-partition-overwrite=true for partitioned tables");
        assertThat(overwriteEnabled).isFalse();
        assertThat(committed).isFalse();
    }

    @Test
    public void testTruncateUsesLatestSchemaBeforeCreatingBatchWriteBuilder()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        FileStoreTable table = truncateFileStoreTable(copiedWithLatestSchema, truncated);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.truncateTable(SESSION, new PaimonTableHandle("schema", "table", Map.of()));

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteAcceptsUnfilteredFileStoreTable()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        FileStoreTable table = truncateFileStoreTable(copiedWithLatestSchema, truncated, truncatedPartitions);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        Optional<ConnectorTableHandle> deleteHandle = metadata.applyDelete(SESSION, tableHandle);

        assertThat(deleteHandle).contains(tableHandle);
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteRejectsLimitedFullTableHandle()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        FileStoreTable table = truncateFileStoreTable(copiedWithLatestSchema, truncated, truncatedPartitions);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle limitedHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(1));

        assertThat(metadata.applyDelete(SESSION, limitedHandle)).isEmpty();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteAcceptsProjectedFullTableHandle()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        FileStoreTable table = truncateFileStoreTable(copiedWithLatestSchema, truncated, truncatedPartitions);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.of(List.of(id)),
                Optional.empty(),
                OptionalLong.empty());

        Optional<ConnectorTableHandle> deleteHandle = metadata.applyDelete(SESSION, tableHandle);

        assertThat(deleteHandle).contains(tableHandle);
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteAcceptsCompletePartitionFilter()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                DataTypes.FIELD(1, "region", DataTypes.INT()),
                DataTypes.FIELD(2, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "region"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.STRING());
        PaimonColumnHandle region = PaimonColumnHandle.of("region", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26")),
                        region, Domain.singleValue(INTEGER, 7L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        Optional<ConnectorTableHandle> deleteHandle = metadata.applyDelete(SESSION, tableHandle);

        assertThat(deleteHandle).isPresent();
        PaimonTableHandle partitionDeleteHandle = (PaimonTableHandle) deleteHandle.orElseThrow();
        assertThat(partitionDeleteHandle.getDeletePartitionSpecs())
                .contains(List.of(Map.of(
                        "dt", "2026-06-26",
                        "region", "7")));
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteMatchesPartitionFilterColumnsCaseInsensitively()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                DataTypes.FIELD(1, "region", DataTypes.INT()),
                DataTypes.FIELD(2, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "region"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("DT", DataTypes.STRING());
        PaimonColumnHandle region = PaimonColumnHandle.of("REGION", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26")),
                        region, Domain.singleValue(INTEGER, 7L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        Optional<ConnectorTableHandle> deleteHandle = metadata.applyDelete(SESSION, tableHandle);

        assertThat(deleteHandle).isPresent();
        PaimonTableHandle partitionDeleteHandle = (PaimonTableHandle) deleteHandle.orElseThrow();
        assertThat(partitionDeleteHandle.getDeletePartitionSpecs())
                .contains(List.of(Map.of(
                        "dt", "2026-06-26",
                        "region", "7")));
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteIgnoresUnsafeTimePartitionValue()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "event_time", new org.apache.paimon.types.TimeType(6)),
                DataTypes.FIELD(1, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("event_time"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle eventTime = PaimonColumnHandle.of("event_time", new org.apache.paimon.types.TimeType(6));
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        eventTime, Domain.singleValue(TIME_MICROS, 12_345L * PICOSECONDS_PER_MILLISECOND + 1))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        Optional<ConnectorTableHandle> deleteHandle = metadata.applyDelete(SESSION, tableHandle);

        assertThat(deleteHandle).isEmpty();
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteAcceptsDiscretePartitionFilters()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                DataTypes.FIELD(1, "region", DataTypes.INT()),
                DataTypes.FIELD(2, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "region"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.STRING());
        PaimonColumnHandle region = PaimonColumnHandle.of("region", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.multipleValues(VARCHAR, List.of(
                                Slices.utf8Slice("2026-06-26"),
                                Slices.utf8Slice("2026-06-27"))),
                        region, Domain.multipleValues(INTEGER, List.of(7L, 8L)))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        Optional<ConnectorTableHandle> deleteHandle = metadata.applyDelete(SESSION, tableHandle);

        assertThat(deleteHandle).isPresent();
        PaimonTableHandle partitionDeleteHandle = (PaimonTableHandle) deleteHandle.orElseThrow();
        assertThat(partitionDeleteHandle.getDeletePartitionSpecs()).contains(List.of(
                Map.of("dt", "2026-06-26", "region", "7"),
                Map.of("dt", "2026-06-26", "region", "8"),
                Map.of("dt", "2026-06-27", "region", "7"),
                Map.of("dt", "2026-06-27", "region", "8")));
        assertThat(partitionDeleteHandle.getDeletePartitionSpecs().orElseThrow().get(0).keySet())
                .containsExactly("dt", "region");
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteUsesPaimonLegacyPartitionFormattingByDefault()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.DATE()),
                DataTypes.FIELD(1, "event_time", DataTypes.TIMESTAMP(6)),
                DataTypes.FIELD(2, "amount", DataTypes.DECIMAL(10, 2)),
                DataTypes.FIELD(3, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "event_time", "amount"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.DATE());
        PaimonColumnHandle eventTime = PaimonColumnHandle.of("event_time", DataTypes.TIMESTAMP(6));
        PaimonColumnHandle amount = PaimonColumnHandle.of("amount", DataTypes.DECIMAL(10, 2));
        DecimalType trinoDecimalType = createDecimalType(10, 2);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.singleValue(DATE, LocalDate.of(2026, 6, 26).toEpochDay()),
                        eventTime, Domain.singleValue(createTimestampType(6), 1_782_477_296_000_000L),
                        amount, Domain.singleValue(trinoDecimalType, 12345L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        Optional<ConnectorTableHandle> deleteHandle = metadata.applyDelete(SESSION, tableHandle);

        assertThat(deleteHandle).isPresent();
        PaimonTableHandle partitionDeleteHandle = (PaimonTableHandle) deleteHandle.orElseThrow();
        assertThat(partitionDeleteHandle.getDeletePartitionSpecs())
                .contains(List.of(Map.of(
                        "dt", "20630",
                        "event_time", "2026-06-26T12:34:56",
                        "amount", "123.45")));
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteUsesPaimonNonLegacyPartitionFormatting()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.DATE()),
                DataTypes.FIELD(1, "event_time", DataTypes.TIMESTAMP(6)),
                DataTypes.FIELD(2, "amount", DataTypes.DECIMAL(10, 2)),
                DataTypes.FIELD(3, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "event_time", "amount"),
                Map.of(
                        CoreOptions.BUCKET.key(), "1",
                        CoreOptions.PARTITION_GENERATE_LEGACY_NAME.key(), "false"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.DATE());
        PaimonColumnHandle eventTime = PaimonColumnHandle.of("event_time", DataTypes.TIMESTAMP(6));
        PaimonColumnHandle amount = PaimonColumnHandle.of("amount", DataTypes.DECIMAL(10, 2));
        DecimalType trinoDecimalType = createDecimalType(10, 2);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.singleValue(DATE, LocalDate.of(2026, 6, 26).toEpochDay()),
                        eventTime, Domain.singleValue(createTimestampType(6), 1_782_477_296_000_000L),
                        amount, Domain.singleValue(trinoDecimalType, 12345L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        Optional<ConnectorTableHandle> deleteHandle = metadata.applyDelete(SESSION, tableHandle);

        assertThat(deleteHandle).isPresent();
        PaimonTableHandle partitionDeleteHandle = (PaimonTableHandle) deleteHandle.orElseThrow();
        assertThat(partitionDeleteHandle.getDeletePartitionSpecs())
                .contains(List.of(Map.of(
                        "dt", "2026-06-26",
                        "event_time", "2026-06-26 12:34:56.000000",
                        "amount", "123.45")));
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteUsesPaimonDefaultPartitionNameForNullPartitions()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                DataTypes.FIELD(1, "region", DataTypes.INT()),
                DataTypes.FIELD(2, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "region"),
                Map.of(
                        CoreOptions.BUCKET.key(), "1",
                        CoreOptions.PARTITION_DEFAULT_NAME.key(), "__NULL_PARTITION__"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.STRING());
        PaimonColumnHandle region = PaimonColumnHandle.of("region", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.create(ValueSet.of(VARCHAR, Slices.utf8Slice("2026-06-26")), true),
                        region, Domain.onlyNull(INTEGER))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        Optional<ConnectorTableHandle> deleteHandle = metadata.applyDelete(SESSION, tableHandle);

        assertThat(deleteHandle).isPresent();
        PaimonTableHandle partitionDeleteHandle = (PaimonTableHandle) deleteHandle.orElseThrow();
        assertThat(partitionDeleteHandle.getDeletePartitionSpecs()).contains(List.of(
                Map.of("dt", "2026-06-26", "region", "__NULL_PARTITION__"),
                Map.of("dt", "__NULL_PARTITION__", "region", "__NULL_PARTITION__")));
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testPartitionDeleteHandleJsonRoundTrip()
    {
        Map<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put("dt", "2026-06-26");
        partitionSpec.put("region", "7");
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withDeletePartitionSpecs(List.of(partitionSpec));

        PaimonTableHandle roundTripped = TABLE_HANDLE_CODEC.fromJson(TABLE_HANDLE_CODEC.toJson(handle));

        assertThat(roundTripped).isEqualTo(handle);
        assertThat(roundTripped.getDeletePartitionSpecs())
                .contains(List.of(Map.of(
                        "dt", "2026-06-26",
                        "region", "7")));
        assertThat(roundTripped.getDeletePartitionSpecs().orElseThrow().get(0).keySet())
                .containsExactly("dt", "region");
    }

    @Test
    public void testDynamicBucketAssignerParallelismHandleJsonRoundTrip()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withDynamicBucketAssignerParallelism(OptionalInt.of(4));

        PaimonTableHandle roundTripped = TABLE_HANDLE_CODEC.fromJson(TABLE_HANDLE_CODEC.toJson(handle));

        assertThat(roundTripped).isEqualTo(handle);
        assertThat(roundTripped.getDynamicBucketAssignerParallelism()).hasValue(4);
    }

    @Test
    public void testPartitionDeleteHandleCopiesPartitionSpecs()
    {
        Map<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put("dt", "2026-06-26");
        List<Map<String, String>> partitionSpecs = new ArrayList<>();
        partitionSpecs.add(partitionSpec);
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withDeletePartitionSpecs(partitionSpecs);

        partitionSpec.put("dt", "2026-06-27");
        partitionSpecs.add(Map.of("dt", "2026-06-28"));

        assertThat(handle.getDeletePartitionSpecs())
                .contains(List.of(Map.of("dt", "2026-06-26")));
    }

    @Test
    public void testApplyDeleteDoesNotAcceptUnsafeFilteredTableHandle()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                DataTypes.FIELD(1, "region", DataTypes.INT()),
                DataTypes.FIELD(2, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "region"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.STRING());
        PaimonColumnHandle region = PaimonColumnHandle.of("region", DataTypes.INT());

        PaimonTableHandle nonPartitionHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        PaimonTableHandle missingPartitionHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26")))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        PaimonTableHandle rangePartitionHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26")),
                        region, Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 7L, true, 8L, true)), false))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(metadata.applyDelete(SESSION, nonPartitionHandle)).isEmpty();
        assertThat(metadata.applyDelete(SESSION, missingPartitionHandle)).isEmpty();
        assertThat(metadata.applyDelete(SESSION, rangePartitionHandle)).isEmpty();
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyDeleteDoesNotExpandTooManyPartitionFilters()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                DataTypes.FIELD(1, "region", DataTypes.INT()),
                DataTypes.FIELD(2, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "region"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.STRING());
        PaimonColumnHandle region = PaimonColumnHandle.of("region", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.multipleValues(VARCHAR, IntStream.range(0, 33)
                                .mapToObj(index -> Slices.utf8Slice("2026-06-" + index))
                                .toList()),
                        region, Domain.multipleValues(INTEGER, IntStream.range(0, 32)
                                .mapToObj(Integer::toUnsignedLong)
                                .toList()))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(metadata.applyDelete(SESSION, tableHandle)).isEmpty();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testExecuteDeleteUsesPaimonTruncateFastPath()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        FileStoreTable table = truncateFileStoreTable(copiedWithLatestSchema, truncated, truncatedPartitions);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThat(metadata.executeDelete(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isEmpty();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isTrue();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testExecuteDeleteUsesPaimonPartitionTruncateFastPath()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                DataTypes.FIELD(1, "region", DataTypes.INT()),
                DataTypes.FIELD(2, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "region"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.STRING());
        PaimonColumnHandle region = PaimonColumnHandle.of("region", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26")),
                        region, Domain.singleValue(INTEGER, 7L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        ConnectorTableHandle deleteHandle = metadata.applyDelete(SESSION, tableHandle).orElseThrow();
        assertThat(metadata.executeDelete(SESSION, deleteHandle)).isEmpty();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).containsExactly(Map.of(
                "dt", "2026-06-26",
                "region", "7"));
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testExecuteDeleteNormalizesPartitionDeleteSpecsBeforeTruncate()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.DATE()),
                DataTypes.FIELD(1, "region", DataTypes.INT()),
                DataTypes.FIELD(2, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "region"),
                Map.of(
                        CoreOptions.BUCKET.key(), "1",
                        CoreOptions.PARTITION_GENERATE_LEGACY_NAME.key(), "false"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.DATE());
        PaimonColumnHandle region = PaimonColumnHandle.of("region", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.singleValue(DATE, LocalDate.of(2026, 6, 26).toEpochDay()),
                        region, Domain.singleValue(INTEGER, 7L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        Map<String, String> reversedPartitionSpec = new LinkedHashMap<>();
        reversedPartitionSpec.put("region", "7");
        reversedPartitionSpec.put("dt", "2026-06-26");

        assertThat(metadata.executeDelete(SESSION, tableHandle.withDeletePartitionSpecs(List.of(reversedPartitionSpec))))
                .isEmpty();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).containsExactly(Map.of(
                "dt", "2026-06-26",
                "region", "7"));
        assertThat(truncatedPartitions.get().get(0).keySet()).containsExactly("dt", "region");
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testExecuteDeleteAcceptsPartitionDeleteSpecsInDifferentOrder()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                DataTypes.FIELD(1, "region", DataTypes.INT()),
                DataTypes.FIELD(2, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "region"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.STRING());
        PaimonColumnHandle region = PaimonColumnHandle.of("region", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.multipleValues(VARCHAR, List.of(
                                Slices.utf8Slice("2026-06-26"),
                                Slices.utf8Slice("2026-06-27"))),
                        region, Domain.singleValue(INTEGER, 7L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withDeletePartitionSpecs(List.of(
                        Map.of("dt", "2026-06-27", "region", "7"),
                        Map.of("dt", "2026-06-26", "region", "7")));

        assertThat(metadata.executeDelete(SESSION, tableHandle)).isEmpty();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).containsExactly(
                Map.of("dt", "2026-06-27", "region", "7"),
                Map.of("dt", "2026-06-26", "region", "7"));
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testTruncateUnsupportedCommitFailuresUseNotSupported()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        RuntimeException truncateFailure = new UnsupportedOperationException("truncate is unsupported by catalog");
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                new AtomicReference<>(),
                truncateFailure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> metadata.truncateTable(SESSION, new PaimonTableHandle("schema", "table", Map.of())),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon truncate table uses features which are not supported by the Trino connector: truncate is unsupported by catalog");

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testPartitionDeleteUnsupportedCommitFailuresUseNotSupported()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RuntimeException truncateFailure = new UnsupportedOperationException("partition truncate is unsupported");
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                DataTypes.FIELD(1, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt"),
                Map.of(CoreOptions.BUCKET.key(), "1"),
                truncateFailure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.STRING());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26")))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        ConnectorTableHandle deleteHandle = metadata.applyDelete(SESSION, tableHandle).orElseThrow();
        assertTrinoError(
                () -> metadata.executeDelete(SESSION, deleteHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon delete uses features which are not supported by the Trino connector: partition truncate is unsupported");

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testExecuteDeleteRejectsFilteredTableHandle()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED)),
                TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertTrinoError(
                () -> metadata.executeDelete(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon delete requires an unfiltered table handle or a validated partition delete handle");

        PaimonTableHandle systemTableHandle = new PaimonTableHandle(
                SYSTEM_DATABASE_NAME,
                "all_tables",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        assertTrinoError(
                () -> metadata.executeDelete(SESSION, systemTableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon delete is not supported for the system schema 'sys'");
    }

    @Test
    public void testExecuteDeleteRejectsTooManyPartitionDeleteSpecs()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED)),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle baseHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        List<Map<String, String>> tooManyPartitionSpecs = IntStream.rangeClosed(0, 1024)
                .mapToObj(index -> Map.of("dt", "2026-06-" + index))
                .toList();
        assertTrinoError(
                () -> metadata.executeDelete(SESSION, baseHandle.withDeletePartitionSpecs(tooManyPartitionSpecs)),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon partition delete requires between 1 and 1024 partition specs");
    }

    @Test
    public void testExecuteDeleteRejectsInvalidPartitionDeleteSpecs()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                DataTypes.FIELD(1, "region", DataTypes.INT()),
                DataTypes.FIELD(2, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "region"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle baseHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertTrinoError(
                () -> metadata.executeDelete(SESSION, baseHandle.withDeletePartitionSpecs(List.of(Map.of(
                        "dt", "2026-06-26")))),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon partition delete requires complete partition specs for keys: [dt, region]");
        assertTrinoError(
                () -> metadata.executeDelete(SESSION, baseHandle.withDeletePartitionSpecs(List.of(Map.of(
                        "dt", "2026-06-26",
                        "region", "not-an-int")))),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon partition delete requires valid Paimon partition values");

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testExecuteDeleteRejectsPartitionDeleteSpecsMismatchedWithFilter()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                DataTypes.FIELD(1, "region", DataTypes.INT()),
                DataTypes.FIELD(2, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("dt", "region"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.STRING());
        PaimonColumnHandle region = PaimonColumnHandle.of("region", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(
                        dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26")),
                        region, Domain.singleValue(INTEGER, 7L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty())
                .withDeletePartitionSpecs(List.of(Map.of(
                        "dt", "2026-06-27",
                        "region", "7")));

        assertTrinoError(
                () -> metadata.executeDelete(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon delete requires partition delete specs to match the table handle filter");

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testExecuteDeleteRejectsPartitionDeleteSpecsForUnpartitionedTable()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean truncated = new AtomicBoolean();
        AtomicReference<List<Map<String, String>>> truncatedPartitions = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        FileStoreTable table = truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of());
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle baseHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertTrinoError(
                () -> metadata.executeDelete(SESSION, baseHandle.withDeletePartitionSpecs(List.of(Map.of(
                        "id", "1")))),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon partition delete requires a partitioned table");

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(truncated).isFalse();
        assertThat(truncatedPartitions.get()).isNull();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testEmptyCommitDoesNotInitializeCatalog()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.finishInsert(SESSION, tableHandle, List.of(), List.of(), List.of()))
                .isEmpty();
        assertThat(catalog.initialized).isFalse();

        assertThat(metadata.finishCreateTable(SESSION, tableHandle, List.of(), List.of()))
                .isEmpty();
        assertThat(catalog.initialized).isFalse();

        metadata.finishMerge(SESSION, new PaimonMergeTableHandle(tableHandle), List.of(), List.of(), List.of());
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testSystemSchemaFinishWritesAreRejectedBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle systemTableHandle = new PaimonTableHandle(SYSTEM_DATABASE_NAME, "all_tables", Map.of());

        assertTrinoError(
                () -> metadata.finishInsert(SESSION, systemTableHandle, List.of(), List.of(), List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon finish insert is not supported for the system schema 'sys'");
        assertThat(catalog.initialized).isFalse();

        assertTrinoError(
                () -> metadata.finishCreateTable(SESSION, systemTableHandle, List.of(), List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon finish create table is not supported for the system schema 'sys'");
        assertThat(catalog.initialized).isFalse();

        assertTrinoError(
                () -> metadata.finishMerge(
                        SESSION,
                        new PaimonMergeTableHandle(systemTableHandle),
                        List.of(),
                        List.of(),
                        List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon finish merge is not supported for the system schema 'sys'");
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testCommitFragmentsAreValidatedBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.finishInsert(null, tableHandle, List.of(), List.of(), List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishCreateTable(null, tableHandle, List.of(), List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishMerge(
                null,
                new PaimonMergeTableHandle(tableHandle),
                List.of(),
                List.of(),
                List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishInsert(SESSION, tableHandle, List.of(), null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fragments is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishCreateTable(SESSION, tableHandle, null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fragments is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishMerge(
                SESSION,
                new PaimonMergeTableHandle(tableHandle),
                List.of(),
                null,
                List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fragments is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishInsert(
                SESSION,
                tableHandle,
                List.of(),
                Collections.singletonList(null),
                List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fragments contains null fragment");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishMerge(
                SESSION,
                new PaimonMergeTableHandle(tableHandle),
                List.of(),
                Collections.singletonList(null),
                List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fragments contains null fragment");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishInsert(SESSION, tableHandle, List.of(), List.of(Slices.wrappedBuffer(new byte[] {
                1, 2, 3,
        })), List.of()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_COMMIT_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to deserialize Paimon commit fragment");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class);
                });
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishMerge(
                SESSION,
                new PaimonMergeTableHandle(tableHandle),
                List.of(),
                List.of(Slices.wrappedBuffer(new byte[] {1, 2, 3})),
                List.of()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_COMMIT_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to deserialize Paimon commit fragment");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class);
                });
        assertThat(catalog.initialized).isFalse();

        byte[] negativeBinaryRowLength = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        assertThatThrownBy(() -> metadata.finishInsert(
                SESSION,
                tableHandle,
                List.of(),
                List.of(Slices.wrappedBuffer(negativeBinaryRowLength)),
                List.of()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_COMMIT_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to deserialize Paimon commit fragment");
                    assertThat(exception.getCause()).isInstanceOf(NegativeArraySizeException.class);
                });
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.finishMerge(
                SESSION,
                new PaimonMergeTableHandle(tableHandle),
                List.of(),
                List.of(Slices.wrappedBuffer(negativeBinaryRowLength)),
                List.of()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_COMMIT_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to deserialize Paimon commit fragment");
                    assertThat(exception.getCause()).isInstanceOf(NegativeArraySizeException.class);
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testFinishInsertCommitFailuresUsePaimonCommitError()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        RuntimeException commitFailure = new RuntimeException("commit failed");
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                commitFailure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.finishInsert(SESSION, tableHandle, List.of(), List.of(commitFragment()), List.of()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_COMMIT_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to commit Paimon write fragments");
                    assertThat(exception.getCause()).isSameAs(commitFailure);
                });
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(committed).isFalse();
    }

    @Test
    public void testFinishInsertIllegalStateCommitFailuresUsePaimonCommitError()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        RuntimeException commitFailure = new IllegalStateException("commit state changed");
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                commitFailure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.finishInsert(SESSION, tableHandle, List.of(), List.of(commitFragment()), List.of()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_COMMIT_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to commit Paimon write fragments");
                    assertThat(exception.getCause()).isSameAs(commitFailure);
                });
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(committed).isFalse();
    }

    @Test
    public void testFinishInsertUnsupportedCommitFailuresUseNotSupported()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        RuntimeException commitFailure = new UnsupportedOperationException("commit feature is unsupported");
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                commitFailure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        Slice fragment = commitFragment();

        assertTrinoError(
                () -> metadata.finishInsert(SESSION, tableHandle, List.of(), List.of(fragment), List.of()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon commit uses features which are not supported by the Trino connector: commit feature is unsupported");
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(committed).isFalse();
    }

    @Test
    public void testFinishInsertNestedUnsupportedCommitFailuresUseNotSupported()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        UnsupportedOperationException unsupported = new UnsupportedOperationException("nested commit feature is unsupported");
        RuntimeException commitFailure = new RuntimeException(new RuntimeException(unsupported));
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                commitFailure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        Slice fragment = commitFragment();

        assertThatThrownBy(() -> metadata.finishInsert(SESSION, tableHandle, List.of(), List.of(fragment), List.of()))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("Paimon commit uses features which are not supported by the Trino connector: nested commit feature is unsupported");
                    assertThat(exception.getCause()).isSameAs(unsupported);
                });
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(committed).isFalse();
    }

    @Test
    public void testFinishInsertNestedTrinoCommitFailuresArePreserved()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        AtomicBoolean committed = new AtomicBoolean();
        TrinoException mapped = new TrinoException(PAIMON_COMMIT_ERROR, "already mapped");
        RuntimeException commitFailure = new RuntimeException(new RuntimeException(mapped));
        FileStoreTable table = commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                new AtomicReference<>(),
                commitFailure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        Slice fragment = commitFragment();

        assertThatThrownBy(() -> metadata.finishInsert(SESSION, tableHandle, List.of(), List.of(fragment), List.of()))
                .isSameAs(mapped);
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(committed).isFalse();
    }

    @Test
    public void testApplyLimitInitializesCatalogBeforeFilteredTableLookup()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(metadata.applyLimit(SESSION, tableHandle, 10))
                .isPresent()
                .get()
                .extracting(result -> (PaimonTableHandle) result.getHandle())
                .satisfies(handle -> assertThat(handle.getLimit()).hasValue(10));
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyLimitRefreshesLatestFileStoreSchemaForPartitionFilter()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                copiedWithLatestSchema,
                DataTypes.ROW(DataTypes.FIELD(0, "old_id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "new_id", DataTypes.INT())),
                List.of("new_id"),
                List.of("new_id"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle newId = PaimonColumnHandle.of("new_id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(newId, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(metadata.applyLimit(SESSION, tableHandle, 10))
                .isPresent()
                .get()
                .extracting(result -> (PaimonTableHandle) result.getHandle())
                .satisfies(handle -> assertThat(handle.getLimit()).hasValue(10));
        assertThat(copiedWithLatestSchema).isTrue();
    }

    @Test
    public void testApplyLimitMatchesPartitionFilterColumnsCaseInsensitively()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(DataTypes.FIELD(0, "dt", DataTypes.STRING())),
                DataTypes.ROW(DataTypes.FIELD(0, "dt", DataTypes.STRING())),
                List.of("dt"),
                List.of("dt")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("DT", DataTypes.STRING());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26")))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(metadata.applyLimit(SESSION, tableHandle, 10))
                .isPresent()
                .get()
                .extracting(result -> (PaimonTableHandle) result.getHandle())
                .satisfies(handle -> assertThat(handle.getLimit()).hasValue(10));
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyLimitShortCircuitsExistingLimitBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.withColumnDomains(Map.of(id, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));

        assertThat(metadata.applyLimit(SESSION, tableHandle, 10))
                .isEmpty();
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyLimitShortCircuitsTupleDomainNoneBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.none(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertThat(metadata.applyLimit(SESSION, tableHandle, 10))
                .isPresent()
                .get()
                .extracting(result -> (PaimonTableHandle) result.getHandle())
                .satisfies(handle -> {
                    assertThat(handle.getFilter()).isEqualTo(TupleDomain.none());
                    assertThat(handle.getLimit()).hasValue(10);
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyFilterValidatesInputsBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};

        assertThatThrownBy(() -> metadata.applyFilter(null, tableHandle, Constraint.alwaysTrue()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyFilter(SESSION, wrongTableHandle, Constraint.alwaysTrue()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon filter pushdown requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyFilter(SESSION, tableHandle, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("constraint is null");
        assertThat(catalog.initialized).isFalse();

        ColumnHandle wrongColumnHandle = new ColumnHandle() {};
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                wrongColumnHandle, Domain.singleValue(INTEGER, 1L))));
        assertThatThrownBy(() -> metadata.applyFilter(SESSION, tableHandle, constraint))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon filter pushdown requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyFilterShortCircuitsTrivialConstraintsBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.applyFilter(SESSION, tableHandle, Constraint.alwaysTrue()))
                .isEmpty();
        assertThat(catalog.initialized).isFalse();

        assertThat(metadata.applyFilter(SESSION, tableHandle, Constraint.alwaysFalse()))
                .isPresent()
                .get()
                .satisfies(result -> {
                    assertThat(((PaimonTableHandle) result.getHandle()).getFilter()).isEqualTo(TupleDomain.none());
                    assertThat(result.getRemainingFilter()).isEqualTo(TupleDomain.all());
                    assertThat(result.getRemainingExpression()).contains(TRUE);
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyFilterShortCircuitsTupleDomainNoneHandleBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.none(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                id, Domain.singleValue(INTEGER, 1L))));

        assertThat(metadata.applyFilter(SESSION, tableHandle, constraint)).isEmpty();
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyFilterAllowsPartitionPushdownAfterAcceptedLimitAndRefreshesLatestSchema()
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                copiedWithLatestSchema,
                DataTypes.ROW(DataTypes.FIELD(0, "old_id", DataTypes.INT())),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                        DataTypes.FIELD(1, "id", DataTypes.INT())),
                List.of("dt"),
                List.of("dt"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("DT", DataTypes.STRING());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26")))));

        assertThat(metadata.applyFilter(SESSION, tableHandle, constraint))
                .isPresent()
                .get()
                .satisfies(result -> {
                    PaimonTableHandle filteredHandle = (PaimonTableHandle) result.getHandle();
                    assertThat(filteredHandle.getLimit()).hasValue(5);
                    assertThat(filteredHandle.getFilter().getDomains().orElseThrow())
                            .containsOnly(Map.entry(dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26"))));
                    assertThat(result.getRemainingFilter()).isEqualTo(TupleDomain.all());
                    assertThat(result.getRemainingExpression()).contains(TRUE);
                });
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyFilterSkipsNonPartitionPushdownAfterAcceptedLimit()
    {
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                        DataTypes.FIELD(1, "id", DataTypes.INT())),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                        DataTypes.FIELD(1, "id", DataTypes.INT())),
                List.of("dt"),
                List.of("dt"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                id, Domain.singleValue(INTEGER, 1L))));

        assertThat(metadata.applyFilter(SESSION, tableHandle, constraint)).isEmpty();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyFilterSkipsMixedPartitionAndNonPartitionPushdownAfterAcceptedLimit()
    {
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                        DataTypes.FIELD(1, "id", DataTypes.INT())),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                        DataTypes.FIELD(1, "id", DataTypes.INT())),
                List.of("dt"),
                List.of("dt"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.STRING());
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.INT());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26")),
                id, Domain.singleValue(INTEGER, 1L))));

        assertThat(metadata.applyFilter(SESSION, tableHandle, constraint)).isEmpty();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyFilterSkipsPartitionPushdownWithResidualFilterAfterAcceptedLimit()
    {
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                        DataTypes.FIELD(1, "payload", DataTypes.BYTES())),
                DataTypes.ROW(
                        DataTypes.FIELD(0, "dt", DataTypes.STRING()),
                        DataTypes.FIELD(1, "payload", DataTypes.BYTES())),
                List.of("dt"),
                List.of("dt"));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle dt = PaimonColumnHandle.of("dt", DataTypes.STRING());
        PaimonColumnHandle payload = PaimonColumnHandle.of("payload", DataTypes.BYTES());
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                dt, Domain.singleValue(VARCHAR, Slices.utf8Slice("2026-06-26")),
                payload, Domain.singleValue(VarbinaryType.VARBINARY, Slices.utf8Slice("x")))));

        assertThat(metadata.applyFilter(SESSION, tableHandle, constraint)).isEmpty();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testApplyFilterStillShortCircuitsAlwaysFalseAfterAcceptedLimitBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));

        assertThat(metadata.applyFilter(SESSION, tableHandle, Constraint.alwaysFalse()))
                .isPresent()
                .get()
                .satisfies(result -> {
                    assertThat(((PaimonTableHandle) result.getHandle()).getFilter()).isEqualTo(TupleDomain.none());
                    assertThat(result.getRemainingFilter()).isEqualTo(TupleDomain.all());
                    assertThat(result.getRemainingExpression()).contains(TRUE);
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyProjectionValidatesInputsBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};

        assertThatThrownBy(() -> metadata.applyProjection(
                null,
                tableHandle,
                List.of(new Variable("id", BIGINT)),
                Map.of("id", id)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyProjection(SESSION, wrongTableHandle, List.of(new Variable(
                "id",
                BIGINT)), Map.of("id", id)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon projection pushdown requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyProjection(SESSION, tableHandle, null, Map.of("id", id)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("projections is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyProjection(
                SESSION,
                tableHandle,
                List.of(new Variable("id", BIGINT)),
                null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("assignments is null");
        assertThat(catalog.initialized).isFalse();

        ColumnHandle wrongColumnHandle = new ColumnHandle() {};
        assertThatThrownBy(() -> metadata.applyProjection(
                SESSION,
                tableHandle,
                List.of(new Variable("id", BIGINT)),
                Map.of("id", wrongColumnHandle)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon projection pushdown requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyProjectionIsOrderSensitive()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle name = PaimonColumnHandle.of("name", DataTypes.STRING());
        PaimonTableHandle projectedHandle = new PaimonTableHandle("schema", "table", Map.of())
                .copy(Optional.of(List.of(id, name)));

        assertThat(metadata.applyProjection(
                SESSION,
                projectedHandle,
                List.of(new Variable("id", BIGINT), new Variable("name", VarcharType.VARCHAR)),
                assignments(id, name)))
                .isEmpty();

        assertThat(metadata.applyProjection(
                SESSION,
                projectedHandle,
                List.of(new Variable("name", VarcharType.VARCHAR), new Variable("id", BIGINT)),
                assignments(name, id)))
                .isPresent()
                .get()
                .satisfies(result -> assertThat(((PaimonTableHandle) result.getHandle()).getProjectedColumns())
                        .hasValueSatisfying(columns -> assertThat(columns).containsExactly(name, id)));
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyProjectionUsesProjectionOrderInsteadOfAssignmentMapOrder()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle name = PaimonColumnHandle.of("name", DataTypes.STRING());
        PaimonTableHandle projectedHandle = new PaimonTableHandle("schema", "table", Map.of())
                .copy(Optional.of(List.of(id, name)));

        Map<String, ColumnHandle> assignments = new LinkedHashMap<>();
        assignments.put("name_7", name);
        assignments.put("id_8", id);

        assertThat(metadata.applyProjection(
                SESSION,
                projectedHandle,
                List.of(new Variable("id_8", BIGINT), new Variable("name_7", VarcharType.VARCHAR)),
                assignments))
                .isEmpty();
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyProjectionDeduplicatesRepeatedProjectionVariables()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(metadata.applyProjection(
                SESSION,
                tableHandle,
                List.of(new Call(
                        BIGINT,
                        ADD_FUNCTION_NAME,
                        List.of(new Variable("id_8", BIGINT), new Variable("id_8", BIGINT)))),
                Map.of("id_8", id)))
                .isPresent()
                .get()
                .satisfies(result -> {
                    assertThat(((PaimonTableHandle) result.getHandle()).getProjectedColumns())
                            .hasValueSatisfying(columns -> assertThat(columns).containsExactly(id));
                    assertThat(result.getAssignments())
                            .extracting(Assignment::getVariable)
                            .containsExactly("id_8");
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyProjectionOrdersExpressionInputsByFirstUse()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonColumnHandle id = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle amount = PaimonColumnHandle.of("amount", DataTypes.BIGINT());
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        Map<String, ColumnHandle> assignments = new LinkedHashMap<>();
        assignments.put("amount_3", amount);
        assignments.put("id_8", id);

        assertThat(metadata.applyProjection(
                SESSION,
                tableHandle,
                List.of(new Call(
                        BIGINT,
                        ADD_FUNCTION_NAME,
                        List.of(new Variable("id_8", BIGINT), new Variable("amount_3", BIGINT)))),
                assignments))
                .isPresent()
                .get()
                .satisfies(result -> {
                    assertThat(((PaimonTableHandle) result.getHandle()).getProjectedColumns())
                            .hasValueSatisfying(columns -> assertThat(columns).containsExactly(id, amount));
                    assertThat(result.getAssignments())
                            .extracting(Assignment::getVariable)
                            .containsExactly("id_8", "amount_3");
                });
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testApplyLimitValidatesInputsBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};

        assertThatThrownBy(() -> metadata.applyLimit(null, tableHandle, 10))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyLimit(SESSION, wrongTableHandle, 10))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon limit pushdown requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();

        assertThatThrownBy(() -> metadata.applyLimit(SESSION, tableHandle, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("limit must be non-negative");
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testFinishCreateTableRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());

        assertThat(PaimonMetadata.getOutputTableHandle(tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonMetadata.getOutputTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");

        ConnectorOutputTableHandle wrongHandle = new ConnectorOutputTableHandle() {};
        assertThatThrownBy(() -> PaimonMetadata.getOutputTableHandle(wrongHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon finish create table requires PaimonTableHandle, got: %s",
                        wrongHandle.getClass().getName());

        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        assertThatThrownBy(() -> metadata.finishCreateTable(SESSION, wrongHandle, List.of(), List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon finish create table requires PaimonTableHandle, got: %s",
                        wrongHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testFinishInsertRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonMetadata.getInsertTableHandle(tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonMetadata.getInsertTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("insertHandle is null");

        ConnectorInsertTableHandle wrongHandle = new ConnectorInsertTableHandle() {};
        assertThatThrownBy(() -> PaimonMetadata.getInsertTableHandle(wrongHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon finish insert requires PaimonTableHandle, got: %s",
                        wrongHandle.getClass().getName());
    }

    @Test
    public void testFinishMergeRequiresPaimonTableHandle()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonMetadata.getMergeTableHandle(new PaimonMergeTableHandle(tableHandle))).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonMetadata.getMergeTableHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeTableHandle is null");

        assertThatThrownBy(() -> PaimonMetadata.getMergeTableHandle(mergeTableHandle(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("mergeTableHandle tableHandle is null");

        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        assertThatThrownBy(() -> PaimonMetadata.getMergeTableHandle(mergeTableHandle(wrongTableHandle)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon finish merge requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testMetadataTableHandleValidation()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThat(PaimonMetadata.getTableHandle("testing", tableHandle)).isSameAs(tableHandle);

        assertThatThrownBy(() -> PaimonMetadata.getTableHandle("testing", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");

        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        assertThatThrownBy(() -> PaimonMetadata.getTableHandle("testing", wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon testing requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testMetadataColumnHandleValidation()
    {
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT());

        assertThat(PaimonMetadata.getColumnHandle("testing", columnHandle)).isSameAs(columnHandle);

        assertThatThrownBy(() -> PaimonMetadata.getColumnHandle("testing", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("columnHandle is null");

        ColumnHandle wrongColumnHandle = new ColumnHandle() {};
        assertThatThrownBy(() -> PaimonMetadata.getColumnHandle("testing", wrongColumnHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon testing requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
    }

    @Test
    public void testCommonMetadataEntrypointsRequirePaimonTableHandle()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED)),
                TESTING_TYPE_MANAGER);
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        TestingPaimonCatalog beginMergeCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata beginMergeMetadata = new PaimonMetadata(beginMergeCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog tableMetadataCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata tableMetadata = new PaimonMetadata(tableMetadataCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog columnHandlesCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata columnHandlesMetadata = new PaimonMetadata(columnHandlesCatalog, TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.getInsertLayout(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon insert layout requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.beginInsert(
                SESSION,
                wrongTableHandle,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT())),
                RetryMode.NO_RETRIES))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon begin insert requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.getMergeRowIdColumnHandle(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon merge row id requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.getUpdateLayout(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon update layout requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.beginMerge(SESSION, wrongTableHandle, Map.of(), RetryMode.NO_RETRIES))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon begin merge requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> beginMergeMetadata.beginMerge(SESSION, wrongTableHandle, Map.of(), RetryMode.NO_RETRIES))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon begin merge requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(beginMergeCatalog.initialized).isFalse();
        assertThatThrownBy(() -> tableMetadata.getTableMetadata(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon table metadata requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(tableMetadataCatalog.initialized).isFalse();
        assertThatThrownBy(() -> columnHandlesMetadata.getColumnHandles(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon column handles requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(columnHandlesCatalog.initialized).isFalse();
    }

    @Test
    public void testColumnMetadataRequiresPaimonHandles()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ColumnHandle wrongColumnHandle = new ColumnHandle() {};

        assertThatThrownBy(() -> metadata.getColumnMetadata(
                SESSION,
                new ConnectorTableHandle() {},
                PaimonColumnHandle.of("id", DataTypes.INT())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Paimon column metadata requires PaimonTableHandle");
        assertThatThrownBy(() -> metadata.getColumnMetadata(SESSION, tableHandle, wrongColumnHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon column metadata requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testGetColumnHandlesInitializesCatalogBeforeTableLookup()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle);

        assertThat(catalog.initialized).isTrue();
        assertThat(columnHandles.keySet()).containsExactly("id");
        assertThat(columnHandles.get("id")).isInstanceOf(PaimonColumnHandle.class);
    }

    @Test
    public void testCommonMetadataEntrypointsRejectNullSessionBeforeCatalogInitialization()
    {
        TestingPaimonCatalog tableMetadataCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata tableMetadata = new PaimonMetadata(tableMetadataCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog columnHandlesCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata columnHandlesMetadata = new PaimonMetadata(columnHandlesCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog schemaExistsCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata schemaExistsMetadata = new PaimonMetadata(schemaExistsCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog listSchemasCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata listSchemasMetadata = new PaimonMetadata(listSchemasCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog versionedTableHandleCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata versionedTableHandleMetadata = new PaimonMetadata(
                versionedTableHandleCatalog,
                TESTING_TYPE_MANAGER);
        TestingPaimonCatalog directTableHandleCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata directTableHandleMetadata = new PaimonMetadata(directTableHandleCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog tablePropertiesCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata tablePropertiesMetadata = new PaimonMetadata(tablePropertiesCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog rowChangeCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata rowChangeMetadata = new PaimonMetadata(rowChangeCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog insertLayoutCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata insertLayoutMetadata = new PaimonMetadata(insertLayoutCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog mergeRowIdCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata mergeRowIdMetadata = new PaimonMetadata(mergeRowIdCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog updateLayoutCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata updateLayoutMetadata = new PaimonMetadata(updateLayoutCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog beginInsertCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata beginInsertMetadata = new PaimonMetadata(beginInsertCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog beginMergeCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata beginMergeMetadata = new PaimonMetadata(beginMergeCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog columnMetadataCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata columnMetadata = new PaimonMetadata(columnMetadataCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog listTableColumnsCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata listTableColumnsMetadata = new PaimonMetadata(listTableColumnsCatalog, TESTING_TYPE_MANAGER);
        TestingPaimonCatalog streamTableColumnsCatalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata streamTableColumnsMetadata = new PaimonMetadata(streamTableColumnsCatalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT());
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};
        SchemaTableName tableName = new SchemaTableName("schema", "table");

        assertThatThrownBy(() -> tableMetadata.getTableMetadata(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(tableMetadataCatalog.initialized).isFalse();

        assertThatThrownBy(() -> columnHandlesMetadata.getColumnHandles(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(columnHandlesCatalog.initialized).isFalse();

        assertThatThrownBy(() -> schemaExistsMetadata.schemaExists(null, "schema"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(schemaExistsCatalog.initialized).isFalse();

        assertThatThrownBy(() -> schemaExistsMetadata.schemaExists(SESSION, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThat(schemaExistsCatalog.initialized).isFalse();

        assertThatThrownBy(() -> listSchemasMetadata.listSchemaNames(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(listSchemasCatalog.initialized).isFalse();

        assertThatThrownBy(() -> versionedTableHandleMetadata.getTableHandle(
                null,
                tableName,
                Optional.empty(),
                Optional.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(versionedTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> versionedTableHandleMetadata.getTableHandle(
                SESSION,
                null,
                Optional.empty(),
                Optional.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableName is null");
        assertThat(versionedTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> versionedTableHandleMetadata.getTableHandle(
                SESSION,
                tableName,
                null,
                Optional.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("startVersion is null");
        assertThat(versionedTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> versionedTableHandleMetadata.getTableHandle(
                SESSION,
                tableName,
                Optional.empty(),
                null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("endVersion is null");
        assertThat(versionedTableHandleCatalog.initialized).isFalse();

        assertTrinoError(
                () -> versionedTableHandleMetadata.getTableHandle(
                        SESSION,
                        tableName,
                        Optional.empty(),
                        Optional.of(new ConnectorTableVersion(
                                PointerType.TARGET_ID,
                                VARCHAR,
                                Slices.utf8Slice(" ")))),
                INVALID_ARGUMENTS.toErrorCode(),
                "Paimon table version may not be blank");
        assertThat(versionedTableHandleCatalog.initialized).isFalse();

        assertThat(versionedTableHandleMetadata.getTableHandle(
                SESSION,
                tableName,
                Optional.empty(),
                Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L))))
                .isEqualTo(new PaimonTableHandle(
                        "schema",
                        "table",
                        Map.of(CoreOptions.SCAN_VERSION.key(), "1")));
        assertThat(versionedTableHandleCatalog.initialized).isTrue();

        assertThat(versionedTableHandleMetadata.getTableHandle(
                SESSION,
                tableName,
                Optional.empty(),
                Optional.of(new ConnectorTableVersion(
                        PointerType.TARGET_ID,
                        VARCHAR,
                        Slices.utf8Slice("tag-1")))))
                .isEqualTo(new PaimonTableHandle(
                        "schema",
                        "table",
                        Map.of(CoreOptions.SCAN_VERSION.key(), "tag-1")));
        assertThat(versionedTableHandleCatalog.initialized).isTrue();

        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(null, tableName, Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(SESSION, null, Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableName is null");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(SESSION, tableName, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicOptions is null");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        Map<String, String> nullDynamicOptionKey = new HashMap<>();
        nullDynamicOptionKey.put(null, "value");
        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(SESSION, tableName, nullDynamicOptionKey))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicOptions contains null key");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(SESSION, tableName, Map.of(" ", "value")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicOptions contains blank key");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        Map<String, String> nullDynamicOptionValue = new HashMap<>();
        nullDynamicOptionValue.put(CoreOptions.SCAN_TAG_NAME.key(), null);
        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(SESSION, tableName, nullDynamicOptionValue))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicOptions contains null value for key 'scan.tag-name'");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> directTableHandleMetadata.getTableHandle(
                SESSION,
                tableName,
                Map.of(CoreOptions.SCAN_TAG_NAME.key(), " ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicOptions contains blank value for key 'scan.tag-name'");
        assertThat(directTableHandleCatalog.initialized).isFalse();

        assertThatThrownBy(() -> tablePropertiesMetadata.getTableProperties(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(tablePropertiesCatalog.initialized).isFalse();

        assertThatThrownBy(() -> tablePropertiesMetadata.getTableProperties(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon table properties requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(tablePropertiesCatalog.initialized).isFalse();

        assertThatThrownBy(() -> rowChangeMetadata.getRowChangeParadigm(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(rowChangeCatalog.initialized).isFalse();

        assertThatThrownBy(() -> rowChangeMetadata.getRowChangeParadigm(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon row change paradigm requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThat(rowChangeCatalog.initialized).isFalse();

        assertThatThrownBy(() -> insertLayoutMetadata.getInsertLayout(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(insertLayoutCatalog.initialized).isFalse();

        assertThatThrownBy(() -> mergeRowIdMetadata.getMergeRowIdColumnHandle(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(mergeRowIdCatalog.initialized).isFalse();

        assertThatThrownBy(() -> updateLayoutMetadata.getUpdateLayout(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(updateLayoutCatalog.initialized).isFalse();

        assertThatThrownBy(() -> beginInsertMetadata.beginInsert(
                null,
                tableHandle,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT())),
                RetryMode.NO_RETRIES))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(beginInsertCatalog.initialized).isFalse();

        assertThatThrownBy(() -> beginInsertMetadata.beginInsert(
                SESSION,
                tableHandle,
                List.of(PaimonColumnHandle.of("id", DataTypes.INT())),
                null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("retryMode is null");
        assertThat(beginInsertCatalog.initialized).isFalse();

        assertTrinoError(
                () -> beginInsertMetadata.beginInsert(
                        SESSION,
                        tableHandle,
                        List.of(PaimonColumnHandle.of("id", DataTypes.INT())),
                        RetryMode.RETRIES_ENABLED),
                NOT_SUPPORTED.toErrorCode(),
                "This connector does not support query retries");
        assertThat(beginInsertCatalog.initialized).isFalse();

        assertThatThrownBy(() -> beginMergeMetadata.beginMerge(null, tableHandle, Map.of(), RetryMode.NO_RETRIES))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(beginMergeCatalog.initialized).isFalse();

        assertThatThrownBy(() -> beginMergeMetadata.beginMerge(SESSION, tableHandle, Map.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("retryMode is null");
        assertThat(beginMergeCatalog.initialized).isFalse();

        assertThatThrownBy(() -> columnMetadata.getColumnMetadata(null, tableHandle, columnHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(columnMetadataCatalog.initialized).isFalse();

        assertThatThrownBy(() -> listTableColumnsMetadata.listTableColumns(
                null,
                new SchemaTablePrefix("schema", "table")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(listTableColumnsCatalog.initialized).isFalse();

        assertThatThrownBy(() -> streamTableColumnsMetadata.streamTableColumns(
                null,
                new SchemaTablePrefix("schema", "table")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(streamTableColumnsCatalog.initialized).isFalse();
    }

    @Test
    public void testDdlEntrypointsRequirePaimonTableHandle()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED)),
                TESTING_TYPE_MANAGER);
        ConnectorTableHandle wrongTableHandle = new ConnectorTableHandle() {};

        assertThatThrownBy(() -> metadata.setTableProperties(
                SESSION,
                wrongTableHandle,
                Map.of("bucket", Optional.of("4"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon set table properties requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.setTableProperties(
                null,
                new PaimonTableHandle("schema", "table", Map.of()),
                Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setTableProperties(
                SESSION,
                new PaimonTableHandle("schema", "table", Map.of()),
                null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties is null");
        assertThatThrownBy(() -> metadata.renameTable(
                SESSION,
                wrongTableHandle,
                new SchemaTableName("schema", "target")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon rename table requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.dropTable(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon drop table requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.addColumn(
                SESSION,
                wrongTableHandle,
                new ColumnMetadata("id", INTEGER),
                new Last()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon add column requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.setTableComment(SESSION, wrongTableHandle, Optional.of("comment")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon set table comment requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.addField(SESSION, wrongTableHandle, List.of(), "nested", INTEGER, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon add field requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.renameField(SESSION, wrongTableHandle, List.of("row", "field"), "renamed"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon rename field requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.setFieldType(SESSION, wrongTableHandle, List.of("row", "field"), INTEGER))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon set field type requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.truncateTable(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon truncate table requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.applyDelete(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon delete requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.executeDelete(SESSION, wrongTableHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon delete requires PaimonTableHandle, got: %s",
                        wrongTableHandle.getClass().getName());
    }

    @Test
    public void testDdlEntrypointsRejectNullSessionBeforeCatalogInitialization()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT());

        assertThatThrownBy(() -> metadata.renameTable(null, tableHandle, new SchemaTableName("schema", "target")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.dropTable(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.addColumn(null, tableHandle, new ColumnMetadata("value", INTEGER), new Last()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.dropColumn(null, tableHandle, columnHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setTableComment(null, tableHandle, Optional.of("comment")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setColumnComment(null, tableHandle, columnHandle, Optional.of("comment")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setColumnType(null, tableHandle, columnHandle, INTEGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.dropNotNullConstraint(null, tableHandle, columnHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.addField(null, tableHandle, List.of(), "nested", INTEGER, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.dropField(null, tableHandle, columnHandle, List.of("nested")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.renameField(null, tableHandle, List.of("row", "nested"), "renamed"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setFieldType(null, tableHandle, List.of("row", "nested"), INTEGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.truncateTable(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.applyDelete(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.executeDelete(null, tableHandle))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThat(catalog.initialized).isFalse();
    }

    @Test
    public void testEmptySetTablePropertiesIsNoOp()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setTableProperties(SESSION, tableHandle, Map.of());

        assertThat(catalog.initialized).isFalse();
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testDdlColumnEntrypointsRequirePaimonColumnHandle()
    {
        PaimonMetadata metadata = new PaimonMetadata(
                new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED)),
                TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ColumnHandle wrongColumnHandle = new ColumnHandle() {};

        assertThatThrownBy(() -> metadata.renameColumn(SESSION, tableHandle, wrongColumnHandle, "renamed"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon rename column requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.dropColumn(SESSION, tableHandle, wrongColumnHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon drop column requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.setColumnComment(
                SESSION,
                tableHandle,
                wrongColumnHandle,
                Optional.of("comment")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon set column comment requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.setColumnType(SESSION, tableHandle, wrongColumnHandle, INTEGER))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon set column type requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.dropNotNullConstraint(SESSION, tableHandle, wrongColumnHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon drop not null constraint requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
        assertThatThrownBy(() -> metadata.dropField(SESSION, tableHandle, wrongColumnHandle, List.of("field")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon drop field requires PaimonColumnHandle, got: %s",
                        wrongColumnHandle.getClass().getName());
    }

    @Test
    public void testDdlRejectsSystemColumnsBeforeCatalogAlter()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle rowId = PaimonColumnHandle.of(PaimonColumnHandle.TRINO_ROW_ID_NAME, DataTypes.BIGINT());
        PaimonColumnHandle sequenceNumber = PaimonColumnHandle.of(
                PaimonColumnHandle.PAIMON_SEQUENCE_NUMBER_NAME,
                DataTypes.BIGINT());
        PaimonColumnHandle valueKind = PaimonColumnHandle.of("_VALUE_KIND", DataTypes.TINYINT());
        PaimonColumnHandle visibleColumn = PaimonColumnHandle.of("payload", DataTypes.ROW(
                DataTypes.FIELD(0, "zip", DataTypes.INT())));

        assertTrinoError(
                () -> metadata.addColumn(
                        SESSION,
                        tableHandle,
                        new ColumnMetadata(PaimonColumnHandle.PAIMON_ROW_ID_NAME, BIGINT),
                        new Last()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon add column is not supported for system column '_row_id'");
        assertTrinoError(
                () -> metadata.addColumn(
                        SESSION,
                        tableHandle,
                        new ColumnMetadata("_KEY_id", BIGINT),
                        new Last()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon add column is not supported for system column '_key_id'");
        assertTrinoError(
                () -> metadata.renameColumn(SESSION, tableHandle, rowId, "renamed"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename column is not supported for system column '$row_id'");
        assertTrinoError(
                () -> metadata.renameColumn(
                        SESSION,
                        tableHandle,
                        visibleColumn,
                        PaimonColumnHandle.PAIMON_SEQUENCE_NUMBER_NAME),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename column is not supported for system column '_SEQUENCE_NUMBER'");
        assertTrinoError(
                () -> metadata.renameColumn(SESSION, tableHandle, visibleColumn, "rowkind"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename column is not supported for system column 'rowkind'");
        assertTrinoError(
                () -> metadata.dropColumn(SESSION, tableHandle, sequenceNumber),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop column is not supported for system column '_SEQUENCE_NUMBER'");
        assertTrinoError(
                () -> metadata.dropColumn(SESSION, tableHandle, valueKind),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop column is not supported for system column '_VALUE_KIND'");
        assertTrinoError(
                () -> metadata.setColumnComment(SESSION, tableHandle, rowId, Optional.of("comment")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set column comment is not supported for system column '$row_id'");
        assertTrinoError(
                () -> metadata.setColumnType(SESSION, tableHandle, rowId, BIGINT),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set column type is not supported for system column '$row_id'");
        assertTrinoError(
                () -> metadata.dropNotNullConstraint(SESSION, tableHandle, rowId),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop not null constraint is not supported for system column '$row_id'");
        assertTrinoError(
                () -> metadata.addField(
                        SESSION,
                        tableHandle,
                        List.of(),
                        PaimonColumnHandle.PAIMON_ROW_ID_NAME,
                        BIGINT,
                        false),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon add field is not supported for system column '_ROW_ID'");
        assertTrinoError(
                () -> metadata.addField(
                        SESSION,
                        tableHandle,
                        List.of(PaimonColumnHandle.PAIMON_ROW_ID_NAME),
                        "nested",
                        BIGINT,
                        false),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon add field is not supported for system column '_ROW_ID'");
        assertTrinoError(
                () -> metadata.addField(
                        SESSION,
                        tableHandle,
                        List.of("_KEY_payload"),
                        "nested",
                        BIGINT,
                        false),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon add field is not supported for system column '_KEY_payload'");
        assertTrinoError(
                () -> metadata.dropField(SESSION, tableHandle, rowId, List.of("nested")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop field is not supported for system column '$row_id'");
        assertTrinoError(
                () -> metadata.renameField(
                        SESSION,
                        tableHandle,
                        List.of(PaimonColumnHandle.PAIMON_ROW_ID_NAME, "nested"),
                        "renamed"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename field is not supported for system column '_ROW_ID'");
        assertTrinoError(
                () -> metadata.setFieldType(
                        SESSION,
                        tableHandle,
                        List.of(PaimonColumnHandle.PAIMON_SEQUENCE_NUMBER_NAME, "nested"),
                        BIGINT),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set field type is not supported for system column '_SEQUENCE_NUMBER'");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testNestedFieldDdlUsesExplicitFieldPaths()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "payload", DataTypes.ROW(
                        DataTypes.FIELD(1, "zip", DataTypes.INT()),
                        DataTypes.FIELD(2, "country", DataTypes.STRING()))));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle rowColumn = PaimonColumnHandle.of("payload", rowType.getField("payload").type());

        metadata.addField(SESSION, tableHandle, List.of("payload"), "city", INTEGER, false);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.AddColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("payload", "city"));

        metadata.dropField(SESSION, tableHandle, rowColumn, List.of("zip"));
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.DropColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("payload", "zip"));

        metadata.renameField(SESSION, tableHandle, List.of("payload", "zip"), "postal_code");
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.RenameColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("payload", "zip");
                    assertThat(change.newName()).isEqualTo("postal_code");
                });

        metadata.setFieldType(SESSION, tableHandle, List.of("payload", "zip"), INTEGER);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnType.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("payload", "zip");
                    assertThat(change.newDataType().getTypeRoot()).isEqualTo(DataTypeRoot.INTEGER);
                    assertThat(change.keepNullability()).isTrue();
                });
    }

    @Test
    public void testNestedFieldDdlCanonicalizesPaimonFieldPath()
    {
        RowType zipType = DataTypes.ROW(
                DataTypes.FIELD(2, "Code", DataTypes.INT()),
                DataTypes.FIELD(3, "Suffix", DataTypes.STRING()));
        RowType payloadType = DataTypes.ROW(
                DataTypes.FIELD(1, "Zip", zipType));
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "Payload", payloadType));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle rowColumn = PaimonColumnHandle.of("Payload", rowType.getField("Payload").type());

        metadata.addField(SESSION, tableHandle, List.of("payload", "zip"), "new_Code", INTEGER, false);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.AddColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("Payload", "Zip", "new_Code"));

        metadata.dropField(SESSION, tableHandle, rowColumn, List.of("zip", "code"));
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.DropColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("Payload", "Zip", "Code"));

        metadata.renameField(SESSION, tableHandle, List.of("payload", "zip", "code"), "PostalCode");
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.RenameColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("Payload", "Zip", "Code");
                    assertThat(change.newName()).isEqualTo("PostalCode");
                });

        metadata.setFieldType(SESSION, tableHandle, List.of("payload", "zip", "code"), BIGINT);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnType.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("Payload", "Zip", "Code");
                    assertThat(change.newDataType().getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
                });
    }

    @Test
    public void testNestedFieldDdlPreservesArrayAndMapMarkers()
    {
        RowType valueType = DataTypes.ROW(
                DataTypes.FIELD(2, "Code", DataTypes.INT()),
                DataTypes.FIELD(3, "City", DataTypes.STRING()));
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "Payload", DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), valueType))));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle rowColumn = PaimonColumnHandle.of("Payload", rowType.getField("Payload").type());

        metadata.addField(
                SESSION,
                tableHandle,
                List.of("payload", "ELEMENT", "VALUE"),
                "postal_code",
                INTEGER,
                false);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.AddColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly(
                                "Payload",
                                "element",
                                "value",
                                "postal_code"));

        metadata.dropField(SESSION, tableHandle, rowColumn, List.of("ELEMENT", "VALUE", "City"));
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.DropColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("Payload", "element", "value", "City"));

        metadata.renameField(SESSION, tableHandle, List.of("payload", "ELEMENT", "VALUE", "Code"), "zip_code");
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.RenameColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("Payload", "element", "value", "Code");
                    assertThat(change.newName()).isEqualTo("zip_code");
                });

        metadata.setFieldType(SESSION, tableHandle, List.of("payload", "ELEMENT", "VALUE", "Code"), BIGINT);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnType.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("Payload", "element", "value", "Code");
                    assertThat(change.newDataType().getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
                });
    }

    @Test
    public void testNestedFieldDdlRejectsMissingArrayAndMapMarkers()
    {
        RowType valueType = DataTypes.ROW(
                DataTypes.FIELD(2, "Code", DataTypes.INT()));
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "Payload", DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), valueType))));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle rowColumn = PaimonColumnHandle.of("Payload", rowType.getField("Payload").type());

        assertTrinoError(
                () -> metadata.addField(
                        SESSION,
                        tableHandle,
                        List.of("payload", "value"),
                        "new_code",
                        INTEGER,
                        false),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon nested field schema change for array element must use 'element' in field path 'Payload.value.new_code'");
        assertTrinoError(
                () -> metadata.dropField(SESSION, tableHandle, rowColumn, List.of("element", "Code")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon nested field schema change for map value must use 'value' in field path 'Payload.element.Code'");
        assertTrinoError(
                () -> metadata.setFieldType(SESSION, tableHandle, List.of("payload", "element"), BIGINT),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon nested field schema change must target a row field, not collection marker 'element' in field path 'Payload.element'");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testTopLevelDdlCanonicalizesPaimonColumnName()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "Id", DataTypes.INT().notNull()),
                DataTypes.FIELD(1, "Payload", DataTypes.STRING().notNull()));
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of(),
                List.of("Id"),
                "Id");
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle payloadColumn = PaimonColumnHandle.of("payload", DataTypes.STRING().notNull());

        metadata.renameColumn(SESSION, tableHandle, payloadColumn, "renamed_payload");
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.RenameColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("Payload");
                    assertThat(change.newName()).isEqualTo("renamed_payload");
                });

        metadata.dropColumn(SESSION, tableHandle, payloadColumn);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.DropColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("Payload"));

        metadata.setColumnComment(SESSION, tableHandle, payloadColumn, Optional.of("payload comment"));
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnComment.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("Payload");
                    assertThat(change.newDescription()).isEqualTo("payload comment");
                });

        metadata.setColumnType(SESSION, tableHandle, payloadColumn, BIGINT);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnType.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("Payload");
                    assertThat(change.newDataType().getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
                    assertThat(change.newDataType().isNullable()).isFalse();
                });

        metadata.dropNotNullConstraint(SESSION, tableHandle, payloadColumn);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnNullability.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("Payload");
                    assertThat(change.newNullability()).isTrue();
                });
    }

    @Test
    public void testDdlRejectsCaseInsensitiveDuplicateColumnTargets()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "Id", DataTypes.INT()),
                DataTypes.FIELD(1, "Payload", DataTypes.STRING()));
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of(),
                List.of(),
                "");
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.addColumn(SESSION, tableHandle, new ColumnMetadata("payload", VARCHAR), new Last()),
                COLUMN_ALREADY_EXISTS.toErrorCode(),
                "Column 'payload' already exists in Paimon schema scope 'schema.table'");
        assertTrinoError(
                () -> metadata.renameColumn(
                        SESSION,
                        tableHandle,
                        PaimonColumnHandle.of("id", DataTypes.INT()),
                        "payload"),
                COLUMN_ALREADY_EXISTS.toErrorCode(),
                "Column 'payload' already exists in Paimon schema scope 'schema.table'");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testNestedDdlRejectsCaseInsensitiveDuplicateFieldTargets()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "Payload", DataTypes.ROW(
                        DataTypes.FIELD(1, "Zip", DataTypes.INT()),
                        DataTypes.FIELD(2, "City", DataTypes.STRING()))));
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of(),
                List.of(),
                "");
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.addField(SESSION, tableHandle, List.of("payload"), "zip", INTEGER, false),
                COLUMN_ALREADY_EXISTS.toErrorCode(),
                "Column 'zip' already exists in Paimon schema scope 'Payload.zip'");
        assertTrinoError(
                () -> metadata.renameField(SESSION, tableHandle, List.of("payload", "city"), "zip"),
                COLUMN_ALREADY_EXISTS.toErrorCode(),
                "Column 'zip' already exists in Paimon schema scope 'Payload.City'");
        assertThatCode(() -> metadata.addField(SESSION, tableHandle, List.of("payload"), "zip", INTEGER, true))
                .doesNotThrowAnyException();

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testDdlProtectsKeyColumnsCaseInsensitively()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "Id", DataTypes.INT().notNull()),
                DataTypes.FIELD(1, "Dt", DataTypes.STRING()),
                DataTypes.FIELD(2, "Payload", DataTypes.STRING()));
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of("Dt"),
                List.of("Id"),
                "Id");
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle primaryKey = PaimonColumnHandle.of("id", DataTypes.INT().notNull());
        PaimonColumnHandle partitionKey = PaimonColumnHandle.of("dt", DataTypes.STRING());

        assertTrinoError(
                () -> metadata.renameColumn(SESSION, tableHandle, partitionKey, "event_date"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename column is not supported: Cannot rename partition column: [dt]");
        assertTrinoError(
                () -> metadata.renameColumn(SESSION, tableHandle, primaryKey, "new_id"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename column is not supported: Cannot rename primary key");
        assertTrinoError(
                () -> metadata.dropColumn(SESSION, tableHandle, partitionKey),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot drop partition key or primary key: [dt]");
        assertTrinoError(
                () -> metadata.dropColumn(SESSION, tableHandle, primaryKey),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot drop partition key or primary key: [id]");
        assertTrinoError(
                () -> metadata.setColumnType(SESSION, tableHandle, partitionKey, VARCHAR),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set column type is not supported: Cannot update partition column: [dt]");
        assertTrinoError(
                () -> metadata.setColumnType(SESSION, tableHandle, primaryKey, VARCHAR),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set column type is not supported: Cannot update primary key");
        assertTrinoError(
                () -> metadata.dropNotNullConstraint(SESSION, tableHandle, primaryKey),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop not null constraint is not supported: Cannot change nullability of primary key");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testDdlRejectsUnsupportedPaimonKeyColumnEvolutionBeforeCatalogAlter()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                DataTypes.FIELD(1, "dt", DataTypes.STRING()),
                DataTypes.FIELD(2, "payload", DataTypes.STRING()));
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of("dt"),
                List.of("id"),
                "id");
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle primaryKey = PaimonColumnHandle.of("id", DataTypes.INT().notNull());
        PaimonColumnHandle partitionKey = PaimonColumnHandle.of("dt", DataTypes.STRING());

        assertTrinoError(
                () -> metadata.renameColumn(SESSION, tableHandle, partitionKey, "event_date"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename column is not supported: Cannot rename partition column: [dt]");
        assertTrinoError(
                () -> metadata.renameColumn(SESSION, tableHandle, primaryKey, "new_id"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename column is not supported: Cannot rename primary key");
        assertTrinoError(
                () -> metadata.dropColumn(SESSION, tableHandle, partitionKey),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot drop partition key or primary key: [dt]");
        assertTrinoError(
                () -> metadata.dropColumn(SESSION, tableHandle, primaryKey),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot drop partition key or primary key: [id]");
        assertTrinoError(
                () -> metadata.setColumnType(SESSION, tableHandle, partitionKey, VARCHAR),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set column type is not supported: Cannot update partition column: [dt]");
        assertTrinoError(
                () -> metadata.setColumnType(SESSION, tableHandle, primaryKey, VARCHAR),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set column type is not supported: Cannot update primary key");
        assertTrinoError(
                () -> metadata.dropNotNullConstraint(SESSION, tableHandle, primaryKey),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop not null constraint is not supported: Cannot change nullability of primary key");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testRenamePrimaryKeyColumnIsRejectedBeforeCatalogAlter()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                DataTypes.FIELD(1, "payload", DataTypes.STRING()));
        FileStoreTable table = fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                rowType,
                rowType,
                List.of(),
                List.of("id"),
                "id");
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.renameColumn(
                        SESSION,
                        tableHandle,
                        PaimonColumnHandle.of("id", DataTypes.INT().notNull()),
                        "new_id"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename column is not supported: Cannot rename primary key");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testBlobColumnRenameAndTypeChangeAreRejectedBeforeCatalogAlter()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "picture", DataTypes.BLOB()),
                DataTypes.FIELD(1, "payload", DataTypes.STRING()));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle blobColumn = PaimonColumnHandle.of("picture", DataTypes.BLOB());

        assertTrinoError(
                () -> metadata.renameColumn(SESSION, tableHandle, blobColumn, "renamed_picture"),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon rename column is not supported: Cannot rename BLOB column: [picture]");
        assertTrinoError(
                () -> metadata.setColumnType(SESSION, tableHandle, blobColumn, BIGINT),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon set column type is not supported: Cannot change column type involving BLOB: [picture] BLOB -> BIGINT");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetColumnCommentUsesPaimonCommentSchemaChange()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "payload", DataTypes.BYTES()));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle column = PaimonColumnHandle.of("payload", DataTypes.BYTES());

        metadata.setColumnComment(SESSION, tableHandle, column, Optional.of("__BLOB_FIELD; display bytes"));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnComment.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("payload");
                    assertThat(change.newDescription()).isEqualTo("__BLOB_FIELD; display bytes");
                });
    }

    @Test
    public void testNestedFieldDdlRejectsMalformedPathsBeforeCatalogAlter()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle rowColumn = PaimonColumnHandle.of("payload", DataTypes.ROW(
                DataTypes.FIELD(0, "zip", DataTypes.INT())));

        assertThatThrownBy(() -> metadata.addField(SESSION, tableHandle, List.of("payload"), " ", INTEGER, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fieldName contains blank field");
        assertThatThrownBy(() -> metadata.addField(
                SESSION,
                tableHandle,
                List.of("payload", " "),
                "city",
                INTEGER,
                false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("parentPath contains blank field");
        assertThatThrownBy(() -> metadata.addField(
                SESSION,
                tableHandle,
                Arrays.asList("payload", (String) null),
                "city",
                INTEGER,
                false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("parentPath contains null field");
        assertThatThrownBy(() -> metadata.dropField(SESSION, tableHandle, rowColumn, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("drop field fieldPath is null");
        assertThatThrownBy(() -> metadata.dropField(SESSION, tableHandle, rowColumn, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("drop field fieldPath is empty");
        assertThatThrownBy(() -> metadata.dropField(SESSION, tableHandle, rowColumn, List.of(" ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("drop field fieldPath contains blank field");
        assertThatThrownBy(() -> metadata.renameField(SESSION, tableHandle, null, "renamed"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rename field fieldPath is null");
        assertThatThrownBy(() -> metadata.renameField(SESSION, tableHandle, List.of("payload"), "renamed"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("rename field fieldPath must include a column name and nested field");
        assertThatThrownBy(() -> metadata.renameField(SESSION, tableHandle, List.of("payload", " "), "renamed"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("rename field fieldPath contains blank field");
        assertThatThrownBy(() -> metadata.renameField(SESSION, tableHandle, List.of("payload", "zip"), " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("target contains blank field");
        assertThatThrownBy(() -> metadata.setFieldType(SESSION, tableHandle, List.of("payload"), INTEGER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("set field type fieldPath must include a column name and nested field");
        assertThatThrownBy(() -> metadata.setFieldType(
                SESSION,
                tableHandle,
                Arrays.asList("payload", (String) null),
                INTEGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("set field type fieldPath contains null field");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testDdlRejectsMalformedArgumentsBeforeCatalogAlter()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle rowColumn = PaimonColumnHandle.of("payload", DataTypes.ROW(
                DataTypes.FIELD(0, "zip", DataTypes.INT())));

        assertThatThrownBy(() -> metadata.renameTable(SESSION, tableHandle, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("newTableName is null");
        assertThatThrownBy(() -> metadata.addColumn(SESSION, tableHandle, null, new Last()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("column is null");
        assertThatThrownBy(() -> metadata.renameColumn(SESSION, tableHandle, rowColumn, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("target contains blank field");
        assertThatThrownBy(() -> metadata.setTableComment(SESSION, tableHandle, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("comment is null");
        assertThatThrownBy(() -> metadata.setColumnComment(SESSION, tableHandle, rowColumn, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("comment is null");
        assertThatThrownBy(() -> metadata.setColumnType(SESSION, tableHandle, rowColumn, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("type is null");
        assertThatThrownBy(() -> metadata.addField(SESSION, tableHandle, List.of("payload"), "city", null, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("type is null");
        assertThatThrownBy(() -> metadata.setFieldType(SESSION, tableHandle, List.of("payload", "zip"), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("type is null");
        assertThatThrownBy(() -> metadata.setTableAuthorization(
                null,
                new SchemaTableName("schema", "table"),
                new TrinoPrincipal(PrincipalType.USER, "table_owner")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setTableAuthorization(
                SESSION,
                null,
                new TrinoPrincipal(PrincipalType.USER, "table_owner")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableName is null");
        assertThatThrownBy(() -> metadata.setTableAuthorization(SESSION, new SchemaTableName("schema", "table"), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("principal is null");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSchemaAndCreateTableDdlRejectsMalformedArgumentsBeforeCatalogInitialization()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)));

        assertThatThrownBy(() -> metadata.createSchema(null, "schema", Map.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.setSchemaAuthorization(
                null,
                "schema",
                new TrinoPrincipal(PrincipalType.USER, "schema_owner")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.getSchemaOwner(null, "schema"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.createSchema(SESSION, "schema", null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties is null");
        assertThatThrownBy(() -> metadata.createSchema(SESSION, " ", Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThatThrownBy(() -> metadata.setSchemaAuthorization(SESSION, "schema", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("principal is null");
        assertThatThrownBy(() -> metadata.setSchemaAuthorization(
                SESSION,
                " ",
                new TrinoPrincipal(PrincipalType.USER, "schema_owner")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThatThrownBy(() -> metadata.getSchemaOwner(SESSION, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThatThrownBy(() -> metadata.dropSchema(null, "schema", false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.dropSchema(SESSION, " ", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThatThrownBy(() -> metadata.listTables(null, Optional.of("schema")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.listTables(SESSION, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("schemaName is null");
        assertThatThrownBy(() -> metadata.listTables(SESSION, Optional.of(" ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schemaName cannot be null or empty");
        assertThatThrownBy(() -> metadata.createTable(null, tableMetadata, SaveMode.FAIL))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.createTable(SESSION, null, SaveMode.FAIL))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableMetadata is null");
        assertThatThrownBy(() -> metadata.createTable(SESSION, tableMetadata, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("saveMode is null");
        assertThatThrownBy(() -> metadata.beginCreateTable(
                null,
                tableMetadata,
                Optional.empty(),
                RetryMode.NO_RETRIES,
                false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> metadata.beginCreateTable(SESSION, null, Optional.empty(), RetryMode.NO_RETRIES, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableMetadata is null");
        assertThatThrownBy(() -> metadata.beginCreateTable(SESSION, tableMetadata, null, RetryMode.NO_RETRIES, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("layout is null");
        assertThatThrownBy(() -> metadata.beginCreateTable(SESSION, tableMetadata, Optional.empty(), null, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("retryMode is null");

        CapturingDdlCatalog retryCatalog = new CapturingDdlCatalog();
        PaimonMetadata retryMetadata = new PaimonMetadata(retryCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> retryMetadata.beginCreateTable(
                        SESSION,
                        tableMetadata,
                        Optional.empty(),
                        RetryMode.RETRIES_ENABLED,
                        false),
                NOT_SUPPORTED.toErrorCode(),
                "This connector does not support query retries");
        assertThat(retryCatalog.initialized).isFalse();
        assertThat(retryCatalog.createdSchema).isNull();

        ConnectorTableMetadata invalidProperties = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of("bucket", List.of("not a string")));
        assertThatThrownBy(() -> metadata.createTable(
                SESSION,
                invalidProperties,
                SaveMode.FAIL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'bucket' must be a string");

        ConnectorTableMetadata invalidPrimaryKey = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of(" ")));
        assertThatThrownBy(() -> metadata.createTable(
                SESSION,
                invalidPrimaryKey,
                SaveMode.FAIL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("primary_key contains blank value");

        ConnectorTableMetadata invalidPartitionedBy = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of(PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of(1)));
        assertThatThrownBy(() -> metadata.createTable(
                SESSION,
                invalidPartitionedBy,
                SaveMode.FAIL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitioned_by contains non-string value");

        ConnectorTableMetadata duplicatePrimaryKey = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("id", "id")));
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        duplicatePrimaryKey,
                        SaveMode.FAIL),
                INVALID_TABLE_PROPERTY.toErrorCode(),
                "Paimon primary_key must not contain duplicate columns: [id]");

        ConnectorTableMetadata duplicatePrimaryKeyCaseInsensitive = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("Id", INTEGER)),
                Map.of(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("id", "ID")));
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        duplicatePrimaryKeyCaseInsensitive,
                        SaveMode.FAIL),
                INVALID_TABLE_PROPERTY.toErrorCode(),
                "Paimon primary_key must not contain duplicate columns: [id]");

        ConnectorTableMetadata missingPrimaryKey = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("missing")));
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        missingPrimaryKey,
                        SaveMode.FAIL),
                INVALID_TABLE_PROPERTY.toErrorCode(),
                "Paimon primary_key columns not present in schema: [missing]");

        ConnectorTableMetadata duplicatePartitionKey = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("dt", VARCHAR)),
                Map.of(PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of("dt", "dt")));
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        duplicatePartitionKey,
                        SaveMode.FAIL),
                INVALID_TABLE_PROPERTY.toErrorCode(),
                "Paimon partitioned_by must not contain duplicate columns: [dt]");

        ConnectorTableMetadata duplicatePartitionKeyCaseInsensitive = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("Dt", VARCHAR)),
                Map.of(PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of("dt", "DT")));
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        duplicatePartitionKeyCaseInsensitive,
                        SaveMode.FAIL),
                INVALID_TABLE_PROPERTY.toErrorCode(),
                "Paimon partitioned_by must not contain duplicate columns: [dt]");

        ConnectorTableMetadata missingPartitionKey = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("dt", VARCHAR)),
                Map.of(PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of("missing")));
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        missingPartitionKey,
                        SaveMode.FAIL),
                INVALID_TABLE_PROPERTY.toErrorCode(),
                "Paimon partitioned_by columns not present in schema: [missing]");

        ConnectorTableMetadata duplicateColumnsCaseInsensitive = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("Id", INTEGER), new ColumnMetadata("id", BIGINT)));
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        duplicateColumnsCaseInsensitive,
                        SaveMode.FAIL),
                INVALID_TABLE_PROPERTY.toErrorCode(),
                "Paimon table columns must not contain case-insensitive duplicate columns: [id]");

        ConnectorTableMetadata systemColumn = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("_VALUE_KIND", INTEGER)));
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        systemColumn,
                        SaveMode.FAIL),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon create table is not supported for system column '_value_kind'");

        ConnectorTableMetadata keyPrefixedColumn = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("_KEY_id", INTEGER)));
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        keyPrefixedColumn,
                        SaveMode.FAIL),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon create table is not supported for system column '_key_id'");

        ConnectorTableMetadata systemPrimaryKey = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("rowkind")));
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        systemPrimaryKey,
                        SaveMode.FAIL),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon create table primary key is not supported for system column 'rowkind'");

        ConnectorTableMetadata systemPartitionKey = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)),
                Map.of(
                        PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of(PaimonColumnHandle.PAIMON_SEQUENCE_NUMBER_NAME)));
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        systemPartitionKey,
                        SaveMode.FAIL),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon create table partition key is not supported for system column '_SEQUENCE_NUMBER'");

        assertThat(catalog.initialized).isFalse();
        assertThat(catalog.createdSchema).isNull();
        assertThat(catalog.createdDatabase).isNull();
        assertThat(catalog.droppedDatabase).isNull();
    }

    @Test
    public void testCreateTableCanonicalizesKeyPropertyColumns()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        new ColumnMetadata("id", INTEGER),
                        new ColumnMetadata("dt", VARCHAR)),
                Map.of(
                        PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("ID"),
                        PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of("DT")));

        metadata.createTable(SESSION, tableMetadata, SaveMode.FAIL);

        assertThat(catalog.createdSchema.primaryKeys()).containsExactly("id");
        assertThat(catalog.createdSchema.partitionKeys()).containsExactly("dt");
    }

    @Test
    public void testDdlErrorsUseTrinoErrorCodes()
    {
        PaimonMetadata metadata = new PaimonMetadata(new FailingDdlCatalog(), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("missing", DataTypes.INT());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)));
        ConnectorTableMetadata missingSchemaTableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("missing_schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)));

        assertTrinoError(
                () -> metadata.createSchema(SESSION, "schema", Map.of(), null),
                SCHEMA_ALREADY_EXISTS.toErrorCode(),
                "Schema 'schema' already exists");
        assertTrinoError(
                () -> metadata.dropSchema(SESSION, "schema", false),
                SCHEMA_NOT_EMPTY.toErrorCode(),
                "Schema 'schema' is not empty");
        assertTrinoError(
                () -> metadata.createTable(SESSION, tableMetadata, SaveMode.FAIL),
                TABLE_ALREADY_EXISTS.toErrorCode(),
                "Table 'schema.table' already exists");
        assertThatCode(() -> metadata.createTable(SESSION, tableMetadata, SaveMode.IGNORE))
                .doesNotThrowAnyException();
        assertTrinoError(
                () -> metadata.createTable(SESSION, tableMetadata, SaveMode.REPLACE),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon create or replace table 'schema.table' is not supported: replace is not supported");
        assertTrinoError(
                () -> metadata.createTable(
                        SESSION,
                        missingSchemaTableMetadata,
                        SaveMode.FAIL),
                SCHEMA_NOT_FOUND.toErrorCode(),
                "Schema 'missing_schema' does not exist");
        assertTrinoError(
                () -> metadata.renameTable(SESSION, tableHandle, new SchemaTableName("schema", "target")),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
        assertTrinoError(
                () -> metadata.dropTable(SESSION, tableHandle),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
        assertTrinoError(
                () -> metadata.addColumn(SESSION, tableHandle, new ColumnMetadata("existing", INTEGER), new Last()),
                COLUMN_ALREADY_EXISTS.toErrorCode(),
                "Column 'existing' already exists in table 'schema.table'");
        assertTrinoError(
                () -> metadata.addField(SESSION, tableHandle, List.of(), "existing", INTEGER, false),
                COLUMN_ALREADY_EXISTS.toErrorCode(),
                "Column 'existing' already exists in table 'schema.table'");
        assertThatCode(() -> metadata.addField(SESSION, tableHandle, List.of(), "existing", INTEGER, true))
                .doesNotThrowAnyException();
        assertTrinoError(
                () -> metadata.addField(SESSION, tableHandle, List.of(), "missing_table", INTEGER, true),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
        PaimonMetadata existingTableMetadata = new PaimonMetadata(
                new ExistingTableFailingDdlCatalog(),
                TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> existingTableMetadata.renameColumn(SESSION, tableHandle, columnHandle, "renamed"),
                COLUMN_NOT_FOUND.toErrorCode(),
                "Column 'missing' does not exist in table 'schema.table'");
        assertTrinoError(
                () -> existingTableMetadata.dropColumn(SESSION, tableHandle, columnHandle),
                COLUMN_NOT_FOUND.toErrorCode(),
                "Column 'missing' does not exist in table 'schema.table'");
        assertTrinoError(
                () -> metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("4"))),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
        assertTrinoError(
                () -> metadata.setTableAuthorization(
                        SESSION,
                        new SchemaTableName("schema", "table"),
                        new TrinoPrincipal(PrincipalType.USER, "table_owner")),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
        assertTrinoError(
                () -> metadata.setTableComment(SESSION, tableHandle, Optional.of("comment")),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
        assertTrinoError(
                () -> metadata.truncateTable(SESSION, tableHandle),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
        assertTrinoError(
                () -> metadata.applyDelete(SESSION, tableHandle),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
        assertTrinoError(
                () -> metadata.executeDelete(SESSION, tableHandle),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
        assertThat(metadata.getTableHandle(
                SESSION,
                new SchemaTableName("schema", "table"),
                Optional.empty(),
                Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L))))
                .isNull();
        assertTrinoError(
                () -> metadata.listTables(SESSION, Optional.of("schema")),
                SCHEMA_NOT_FOUND.toErrorCode(),
                "Schema 'schema' does not exist");
    }

    @Test
    public void testTableColumnListingSkipsMissingExplicitTable()
    {
        PaimonMetadata metadata = new PaimonMetadata(new FailingDdlCatalog(), TESTING_TYPE_MANAGER);
        SchemaTablePrefix prefix = new SchemaTablePrefix("schema", "table");

        assertThat(metadata.listTableColumns(SESSION, prefix)).isEmpty();
        assertThat(metadata.streamTableColumns(SESSION, prefix).hasNext()).isFalse();
    }

    @Test
    public void testRuntimeAlterFailureUsesPaimonMetadataError()
    {
        IllegalStateException failure = new IllegalStateException("catalog invariant broken");
        PaimonMetadata metadata = new PaimonMetadata(new RuntimeFailingAlterCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("4"))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to alter Paimon table 'schema.table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.setTableAuthorization(
                SESSION,
                new SchemaTableName("schema", "table"),
                new TrinoPrincipal(PrincipalType.USER, "table_owner")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to alter Paimon table 'schema.table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testUnsupportedAlterFailureUsesNotSupported()
    {
        UnsupportedOperationException failure = new UnsupportedOperationException("Cannot change bucket when it is -1.");
        PaimonMetadata metadata = new PaimonMetadata(new RuntimeFailingAlterCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("4"))),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot change bucket when it is -1.");
    }

    @Test
    public void testNestedUnsupportedAlterFailureUsesNotSupported()
    {
        UnsupportedOperationException unsupported = new UnsupportedOperationException("nested alter feature is unsupported");
        RuntimeException failure = new RuntimeException(new RuntimeException(unsupported));
        PaimonMetadata metadata = new PaimonMetadata(new RuntimeFailingAlterCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("4"))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage("nested alter feature is unsupported");
                    assertThat(exception.getCause()).isSameAs(unsupported);
                });
    }

    @Test
    public void testNestedTrinoAlterFailuresArePreserved()
    {
        TrinoException mapped = new TrinoException(PAIMON_METADATA_ERROR, "already mapped alter failure");
        RuntimeException failure = new RuntimeException(new RuntimeException(mapped));
        PaimonMetadata metadata = new PaimonMetadata(new RuntimeFailingAlterCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("4"))))
                .isSameAs(mapped);
    }

    @Test
    public void testCheckedAlterFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingAlterCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("4"))))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to alter Paimon table 'schema.table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThatThrownBy(() -> metadata.setTableAuthorization(
                SESSION,
                new SchemaTableName("schema", "table"),
                new TrinoPrincipal(PrincipalType.USER, "table_owner")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to alter Paimon table 'schema.table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testSetTablePropertiesPropagatesExistingOptionsReadFailure()
    {
        IllegalStateException failure = new IllegalStateException("catalog options read failed");
        CapturingDdlCatalog catalog = new CapturingDdlCatalog()
        {
            @Override
            public Table getTable(Identifier identifier)
            {
                assertThat(identifier.getFullName()).isEqualTo("schema.table");
                throw failure;
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("4"))))
                .isSameAs(failure);
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testCheckedTruncateFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("truncate metastore I/O failed");
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable table = truncateFailingFileStoreTable(copiedWithLatestSchema, failure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.truncateTable(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to truncate Paimon table 'table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testCheckedExecuteDeleteFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("delete metastore I/O failed");
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        FileStoreTable table = truncateFailingFileStoreTable(copiedWithLatestSchema, failure);
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(table);
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.executeDelete(SESSION, new PaimonTableHandle("schema", "table", Map.of())))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to delete rows from Paimon table 'table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testCheckedCreateSchemaFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("schema metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingSchemaCatalog(failure), TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.createSchema(SESSION, "schema", Map.of(), null))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to create Paimon schema 'schema'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testCheckedDropSchemaFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("schema delete metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingDropSchemaCatalog(failure), TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.dropSchema(SESSION, "schema", false))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to drop Paimon schema 'schema'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testCheckedSetSchemaAuthorizationFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("schema authorization metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(
                new CheckedFailingSchemaAuthorizationCatalog(failure),
                TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.setSchemaAuthorization(
                SESSION,
                "schema",
                new TrinoPrincipal(PrincipalType.USER, "schema_owner")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to set authorization on Paimon schema 'schema'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testCheckedCreateTableFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("table create metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingCreateTableCatalog(failure), TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)));

        assertThatThrownBy(() -> metadata.createTable(SESSION, tableMetadata, SaveMode.FAIL))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to create Paimon table 'schema.table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testCheckedRenameTableFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("table rename metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingRenameTableCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.renameTable(SESSION, tableHandle, new SchemaTableName("schema", "target")))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to rename Paimon table 'schema.table' to 'schema.target'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testRuntimeWrappedRenameCatalogFailuresUseStandardErrors()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        Identifier source = new Identifier("schema", "table");
        Identifier target = new Identifier("target_schema", "target");

        PaimonMetadata missingSchemaMetadata = new PaimonMetadata(
                new RuntimeWrappedRenameFailureCatalog(new Catalog.DatabaseNotExistException(target.getDatabaseName())),
                TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> missingSchemaMetadata.renameTable(
                        SESSION,
                        tableHandle,
                        new SchemaTableName(target.getDatabaseName(), target.getObjectName())),
                SCHEMA_NOT_FOUND.toErrorCode(),
                "Schema 'target_schema' does not exist");

        PaimonMetadata missingSourceMetadata = new PaimonMetadata(
                new RuntimeWrappedRenameFailureCatalog(new Catalog.TableNotExistException(source)),
                TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> missingSourceMetadata.renameTable(
                        SESSION,
                        tableHandle,
                        new SchemaTableName(target.getDatabaseName(), target.getObjectName())),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");

        PaimonMetadata existingTargetMetadata = new PaimonMetadata(
                new RuntimeWrappedRenameFailureCatalog(new Catalog.TableAlreadyExistException(target)),
                TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> existingTargetMetadata.renameTable(
                        SESSION,
                        tableHandle,
                        new SchemaTableName(target.getDatabaseName(), target.getObjectName())),
                TABLE_ALREADY_EXISTS.toErrorCode(),
                "Table 'target_schema.target' already exists");

        PaimonMetadata deeplyWrappedExistingTargetMetadata = new PaimonMetadata(
                new RuntimeWrappedRenameFailureCatalog(new RuntimeException(new Catalog.TableAlreadyExistException(target))),
                TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> deeplyWrappedExistingTargetMetadata.renameTable(
                        SESSION,
                        tableHandle,
                        new SchemaTableName(target.getDatabaseName(), target.getObjectName())),
                TABLE_ALREADY_EXISTS.toErrorCode(),
                "Table 'target_schema.target' already exists");
    }

    @Test
    public void testCheckedDropTableFailureUsesPaimonMetadataError()
    {
        IOException failure = new IOException("table drop metastore I/O failed");
        PaimonMetadata metadata = new PaimonMetadata(new CheckedFailingDropTableCatalog(failure), TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertThatThrownBy(() -> metadata.dropTable(SESSION, tableHandle))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_METADATA_ERROR.toErrorCode());
                    assertThat(exception).hasMessage("Failed to drop Paimon table 'schema.table'");
                    assertThat(exception.getCause()).isSameAs(failure);
                });
    }

    @Test
    public void testSetTablePropertiesUsesPaimonOptionKeys()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setTableProperties(SESSION, tableHandle, Map.of(
                "variant_shredding_max_schema_width", Optional.of("64"),
                "vector_file_format", Optional.empty()));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges).hasSize(2);
        assertThat(catalog.lastAlterChanges)
                .anySatisfy(change -> {
                    assertThat(change).isInstanceOf(SchemaChange.SetOption.class);
                    SchemaChange.SetOption setOption = (SchemaChange.SetOption) change;
                    assertThat(setOption.key()).isEqualTo(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH.key());
                    assertThat(setOption.value()).isEqualTo("64");
                })
                .anySatisfy(change -> {
                    assertThat(change).isInstanceOf(SchemaChange.RemoveOption.class);
                    SchemaChange.RemoveOption removeOption = (SchemaChange.RemoveOption) change;
                    assertThat(removeOption.key()).isEqualTo(CoreOptions.VECTOR_FILE_FORMAT.key());
                });
    }

    @Test
    public void testSetTablePropertiesNormalizesPaimonOptionValues()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setTableProperties(SESSION, tableHandle, Map.of(
                "variant_shredding_max_schema_width", Optional.of(" 64 "),
                "vector_file_format", Optional.of(" lance ")));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges).hasSize(2);
        assertThat(catalog.lastAlterChanges)
                .anySatisfy(change -> {
                    assertThat(change).isInstanceOf(SchemaChange.SetOption.class);
                    SchemaChange.SetOption setOption = (SchemaChange.SetOption) change;
                    assertThat(setOption.key()).isEqualTo(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH.key());
                    assertThat(setOption.value()).isEqualTo("64");
                })
                .anySatisfy(change -> {
                    assertThat(change).isInstanceOf(SchemaChange.SetOption.class);
                    SchemaChange.SetOption setOption = (SchemaChange.SetOption) change;
                    assertThat(setOption.key()).isEqualTo(CoreOptions.VECTOR_FILE_FORMAT.key());
                    assertThat(setOption.value()).isEqualTo("lance");
                });
    }

    @Test
    public void testSetTablePropertiesRejectsDuplicatePaimonOptionKeys()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.setTableProperties(SESSION, tableHandle, Map.of(
                        "vector_file_format", Optional.of("lance"),
                        CoreOptions.VECTOR_FILE_FORMAT.key(), Optional.empty())),
                INVALID_TABLE_PROPERTY.toErrorCode(),
                "Multiple table properties map to Paimon option '" + CoreOptions.VECTOR_FILE_FORMAT.key() + "'");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTablePropertiesRejectsLayoutProperties()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.setTableProperties(SESSION, tableHandle, Map.of(
                        PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, Optional.of(List.of("id")),
                        PaimonTableOptions.PARTITIONED_BY_PROPERTY, Optional.of(List.of("dt")),
                        CoreOptions.PRIMARY_KEY.key(), Optional.of("id"),
                        CoreOptions.PARTITION.key(), Optional.of("dt"))),
                NOT_SUPPORTED.toErrorCode(),
                "The following properties cannot be updated: partition, partitioned_by, primary-key, primary_key");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTablePropertiesRejectsImmutablePaimonOptions()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.setTableProperties(SESSION, tableHandle, Map.of(
                        "merge_engine", Optional.of("partial-update"),
                        "sequence_snapshot_ordering", Optional.of("true"),
                        "bucket_key", Optional.of("id"))),
                NOT_SUPPORTED.toErrorCode(),
                "The following properties cannot be updated: bucket_key, merge_engine, sequence_snapshot_ordering");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTablePropertiesRejectsRuntimeReadSelectors()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.setTableProperties(SESSION, tableHandle, Map.of(
                        "incremental_between", Optional.of("1,2"),
                        "scan_snapshot_id", Optional.of("7"),
                        "scan_version", Optional.of("tag-1"))),
                NOT_SUPPORTED.toErrorCode(),
                "The following properties cannot be updated: incremental_between, scan_snapshot_id, scan_version");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTablePropertiesRejectsHiddenPaimonOptions()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.setTableProperties(SESSION, tableHandle, Map.of(
                        CoreOptions.PATH.key(), Optional.of("s3://warehouse/schema.db/table"),
                        CoreOptions.KEY_VALUE_SEQUENCE_NUMBER_ENABLED.key(), Optional.of("true"),
                        "materialized_table_refresh_status", Optional.of("SUCCESS"))),
                NOT_SUPPORTED.toErrorCode(),
                "The following properties cannot be updated: key-value.sequence_number.enabled, "
                        + "materialized_table_refresh_status, path");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTablePropertiesValidatesPaimonOptionUpdatesBeforeCatalogAlter()
    {
        CapturingDdlCatalog immutableCatalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED,
                new AtomicBoolean(),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT())),
                List.of(),
                List.of("id"),
                "id"));
        PaimonMetadata immutableMetadata = new PaimonMetadata(immutableCatalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> immutableMetadata.setTableProperties(
                        SESSION,
                        tableHandle,
                        Map.of("bucket_key", Optional.of("payload"))),
                NOT_SUPPORTED.toErrorCode(),
                "The following properties cannot be updated: bucket_key");
        assertThat(immutableCatalog.alterCalls).isEqualTo(0);

        CapturingDdlCatalog dynamicBucketCatalog = new CapturingDdlCatalog(schemaOptionsFileStoreTable(
                Map.of(CoreOptions.BUCKET.key(), "-1")));
        PaimonMetadata dynamicBucketMetadata = new PaimonMetadata(dynamicBucketCatalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> dynamicBucketMetadata.setTableProperties(
                        SESSION,
                        tableHandle,
                        Map.of("bucket", Optional.of("4"))),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot change bucket when it is -1.");
        assertThat(dynamicBucketCatalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTablePropertiesAllowsNoOpExistingOptionUpdates()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        AtomicInteger dynamicBucketLatestSnapshotCalls = new AtomicInteger();

        CapturingDdlCatalog dynamicBucketCatalog = new CapturingDdlCatalog(schemaOptionsFileStoreTable(
                Map.of(CoreOptions.BUCKET.key(), "-1"),
                true,
                dynamicBucketLatestSnapshotCalls));
        PaimonMetadata dynamicBucketMetadata = new PaimonMetadata(dynamicBucketCatalog, TESTING_TYPE_MANAGER);

        dynamicBucketMetadata.setTableProperties(
                SESSION,
                tableHandle,
                Map.of("bucket", Optional.of("-1")));

        assertThat(dynamicBucketCatalog.alterCalls).isEqualTo(1);
        assertThat(dynamicBucketLatestSnapshotCalls).hasValue(0);

        AtomicInteger pkClusteringLatestSnapshotCalls = new AtomicInteger();
        CapturingDdlCatalog pkClusteringCatalog = new CapturingDdlCatalog(schemaOptionsFileStoreTable(
                Map.of(
                        CoreOptions.PK_CLUSTERING_OVERRIDE.key(), "true",
                        CoreOptions.CLUSTERING_COLUMNS.key(), "id"),
                true,
                pkClusteringLatestSnapshotCalls));
        PaimonMetadata pkClusteringMetadata = new PaimonMetadata(pkClusteringCatalog, TESTING_TYPE_MANAGER);

        pkClusteringMetadata.setTableProperties(
                SESSION,
                tableHandle,
                Map.of("clustering_columns", Optional.of("id")));

        assertThat(pkClusteringCatalog.alterCalls).isEqualTo(1);
        assertThat(pkClusteringLatestSnapshotCalls).hasValue(0);
    }

    @Test
    public void testSetTablePropertiesAllowsSnapshotlessPaimonOptionChangesBeforeCatalogAlter()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        AtomicInteger getTableCalls = new AtomicInteger();
        AtomicInteger latestSnapshotCalls = new AtomicInteger();
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(schemaOptionsFileStoreTable(
                Map.of(
                        CoreOptions.BUCKET.key(), "-1",
                        CoreOptions.DELETION_VECTORS_ENABLED.key(), "false",
                        CoreOptions.IGNORE_DELETE.key(), "true",
                        CoreOptions.IGNORE_UPDATE_BEFORE.key(), "true",
                        CoreOptions.PK_CLUSTERING_OVERRIDE.key(), "true",
                        CoreOptions.CLUSTERING_COLUMNS.key(), "id"),
                false,
                latestSnapshotCalls))
        {
            @Override
            public Table getTable(Identifier identifier)
                    throws Catalog.TableNotExistException
            {
                getTableCalls.incrementAndGet();
                return super.getTable(identifier);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.setTableProperties(SESSION, tableHandle,
                Map.of(
                        "bucket", Optional.of("4"),
                        "deletion_vectors_enabled", Optional.of("true"),
                        "ignore_delete", Optional.of("false"),
                        "ignore_update_before", Optional.of("false"),
                        "clustering_columns", Optional.of("payload")));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges).hasSize(5);
        assertThat(getTableCalls).hasValue(1);
        assertThat(latestSnapshotCalls).hasValue(1);

        metadata.setTableProperties(SESSION, tableHandle,
                Map.of(
                        "bucket", Optional.empty(),
                        "clustering_columns", Optional.empty()));

        assertThat(catalog.alterCalls).isEqualTo(2);
        assertThat(catalog.lastAlterChanges).hasSize(2);
        assertThat(getTableCalls).hasValue(2);
        assertThat(latestSnapshotCalls).hasValue(2);
    }

    @Test
    public void testSetTablePropertiesValidatesPaimonDeletionVectorOptionUpdateBeforeCatalogAlter()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        CapturingDdlCatalog catalog = new CapturingDdlCatalog(schemaOptionsFileStoreTable(
                Map.of(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> metadata.setTableProperties(
                        SESSION,
                        tableHandle,
                        Map.of("deletion_vectors_enabled", Optional.of("true"))),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot change deletion vectors mode from false to true. If modifying table deletion-vectors mode "
                        + "without full-compaction, this may result in data duplication. If you are confident, "
                        + "you can set table option 'deletion-vectors.modifiable' = 'true' to allow deletion "
                        + "vectors modification.");
        assertThat(catalog.alterCalls).isEqualTo(0);

        CapturingDdlCatalog modifiableCatalog = new CapturingDdlCatalog(schemaOptionsFileStoreTable(
                Map.of(
                        CoreOptions.DELETION_VECTORS_ENABLED.key(), "false",
                        CoreOptions.DELETION_VECTORS_MODIFIABLE.key(), "true")));
        PaimonMetadata modifiableMetadata = new PaimonMetadata(modifiableCatalog, TESTING_TYPE_MANAGER);

        modifiableMetadata.setTableProperties(
                SESSION,
                tableHandle,
                Map.of("deletion_vectors_enabled", Optional.of("true")));

        assertThat(modifiableCatalog.alterCalls).isEqualTo(1);
    }

    @Test
    public void testSetTablePropertiesValidatesPaimonIgnoreOptionUpdatesBeforeCatalogAlter()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        CapturingDdlCatalog ignoreDeleteCatalog = new CapturingDdlCatalog(schemaOptionsFileStoreTable(
                Map.of(CoreOptions.IGNORE_DELETE.key(), "true")));
        PaimonMetadata ignoreDeleteMetadata = new PaimonMetadata(ignoreDeleteCatalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> ignoreDeleteMetadata.setTableProperties(
                        SESSION,
                        tableHandle,
                        Map.of("ignore_delete", Optional.of("false"))),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot change ignore-delete from true to false.");
        assertThat(ignoreDeleteCatalog.alterCalls).isEqualTo(0);

        CapturingDdlCatalog ignoreUpdateBeforeCatalog = new CapturingDdlCatalog(schemaOptionsFileStoreTable(
                Map.of(CoreOptions.IGNORE_UPDATE_BEFORE.key(), "true")));
        PaimonMetadata ignoreUpdateBeforeMetadata = new PaimonMetadata(ignoreUpdateBeforeCatalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> ignoreUpdateBeforeMetadata.setTableProperties(
                        SESSION,
                        tableHandle,
                        Map.of("ignore_update_before", Optional.of("false"))),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot change ignore-update-before from true to false.");
        assertThat(ignoreUpdateBeforeCatalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTablePropertiesValidatesPaimonClusteringOptionChangesBeforeCatalogAlter()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        CapturingDdlCatalog catalog = new CapturingDdlCatalog(schemaOptionsFileStoreTable(
                Map.of(
                        CoreOptions.PK_CLUSTERING_OVERRIDE.key(), "true",
                        CoreOptions.CLUSTERING_COLUMNS.key(), "id")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> metadata.setTableProperties(
                        SESSION,
                        tableHandle,
                        Map.of("clustering_columns", Optional.of("payload"))),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot change clustering.columns when pk-clustering-override enabled.");
        assertThat(catalog.alterCalls).isEqualTo(0);

        assertTrinoError(
                () -> metadata.setTableProperties(
                        SESSION,
                        tableHandle,
                        Map.of("clustering_columns", Optional.empty())),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot reset clustering.columns when pk-clustering-override enabled.");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTablePropertiesValidatesPaimonOptionRemovesBeforeCatalogAlter()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(schemaOptionsFileStoreTable(
                Map.of(CoreOptions.BUCKET.key(), "4")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.setTableProperties(
                        SESSION,
                        tableHandle,
                        Map.of("bucket", Optional.empty())),
                NOT_SUPPORTED.toErrorCode(),
                "Cannot reset bucket.");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTablePropertiesValidatesAgainstStoredOptionsNotHandleDynamicOptions()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(schemaOptionsFileStoreTable(
                Map.of(CoreOptions.BUCKET.key(), "4")));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Map.of(CoreOptions.BUCKET.key(), "-1"));

        metadata.setTableProperties(SESSION, tableHandle, Map.of("bucket", Optional.of("8")));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.SetOption.class, change -> {
                    assertThat(change.key()).isEqualTo(CoreOptions.BUCKET.key());
                    assertThat(change.value()).isEqualTo("8");
                });
    }

    @Test
    public void testSetTablePropertiesRejectsNullPropertyEntriesBeforeCatalogAlter()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        Map<String, Optional<Object>> nullKeyProperties = new HashMap<>();
        nullKeyProperties.put(null, Optional.of("64"));
        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle, nullKeyProperties))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties contains null property name");

        assertThatThrownBy(() -> metadata.setTableProperties(
                SESSION,
                tableHandle,
                Map.of(" ", Optional.of((Object) "64"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties contains blank property name");

        Map<String, Optional<Object>> nullOptionalProperties = new HashMap<>();
        nullOptionalProperties.put("vector_file_format", null);
        assertThatThrownBy(() -> metadata.setTableProperties(SESSION, tableHandle, nullOptionalProperties))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties contains null value for property 'vector_file_format'");

        assertThatThrownBy(() -> metadata.setTableProperties(
                SESSION,
                tableHandle,
                Map.of("vector_file_format", Optional.of((Object) List.of("lance")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'vector_file_format' must be a string");

        assertThatThrownBy(() -> metadata.setTableProperties(
                SESSION,
                tableHandle,
                Map.of("vector_file_format", Optional.of((Object) " "))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'vector_file_format' is blank");

        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testCreateSchemaDoesNotIgnoreExistingSchema()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.createSchema(
                SESSION,
                "schema",
                Map.of(
                        LOCATION_PROPERTY, "s3://warehouse/schema",
                        COMMENT_PROPERTY, "schema comment"),
                new TrinoPrincipal(PrincipalType.USER, "schema_owner"));

        assertThat(catalog.createdDatabase).isEqualTo("schema");
        assertThat(catalog.createDatabaseIgnoreIfExists).isFalse();
        assertThat(catalog.createdDatabaseProperties).containsExactlyInAnyOrderEntriesOf(Map.of(
                LOCATION_PROPERTY, "s3://warehouse/schema",
                COMMENT_PROPERTY, "schema comment",
                OWNER_PROPERTY, "schema_owner",
                TRINO_SCHEMA_OWNER_TYPE_PROPERTY, PrincipalType.USER.name()));

        metadata.createSchema(
                SESSION,
                "role_schema",
                Map.of(
                        LOCATION_PROPERTY, "s3://warehouse/role_schema",
                        COMMENT_PROPERTY, "role schema comment"),
                new TrinoPrincipal(PrincipalType.ROLE, "schema_role"));

        assertThat(catalog.createdDatabase).isEqualTo("role_schema");
        assertThat(catalog.createdDatabaseProperties).containsExactlyInAnyOrderEntriesOf(Map.of(
                LOCATION_PROPERTY, "s3://warehouse/role_schema",
                COMMENT_PROPERTY, "role schema comment",
                OWNER_PROPERTY, "schema_role",
                TRINO_SCHEMA_OWNER_TYPE_PROPERTY, PrincipalType.ROLE.name()));
    }

    @Test
    public void testCreateSchemaRejectsMalformedPropertiesBeforeCatalogInitialization()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThatThrownBy(() -> metadata.createSchema(SESSION, "schema", Map.of(" ", "value"), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties contains blank property name");
        assertThatThrownBy(() -> metadata.createSchema(SESSION, "schema", Map.of("location", List.of("s3://warehouse")), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'location' must be a string");
        assertThatThrownBy(() -> metadata.createSchema(SESSION, "schema", Map.of("location", " "), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'location' is blank");

        assertThat(catalog.initialized).isFalse();
        assertThat(catalog.createdDatabase).isNull();
    }

    @Test
    public void testGetSchemaPropertiesAndOwner()
    {
        PaimonMetadata metadata = new PaimonMetadata(new SchemaPropertiesCatalog(), TESTING_TYPE_MANAGER);

        assertThat(metadata.getSchemaProperties(SESSION, "schema")).containsExactlyInAnyOrderEntriesOf(Map.of(
                LOCATION_PROPERTY, "s3://warehouse/schema",
                COMMENT_PROPERTY, "schema comment"));
        assertThat(metadata.getSchemaOwner(SESSION, "schema"))
                .contains(new TrinoPrincipal(PrincipalType.USER, "schema_owner"));
        assertThat(metadata.getSchemaOwner(SESSION, "schema_with_role_owner"))
                .contains(new TrinoPrincipal(PrincipalType.ROLE, "schema_role"));
        assertThat(metadata.getSchemaProperties(SESSION, "schema_with_role_owner"))
                .isEmpty();
        assertThat(metadata.getSchemaOwner(SESSION, "schema_without_owner"))
                .isEmpty();
        assertThat(metadata.getSchemaOwner(SESSION, "schema_with_blank_owner"))
                .isEmpty();
        assertTrinoError(
                () -> metadata.getSchemaOwner(SESSION, "schema_with_invalid_owner_type"),
                PAIMON_METADATA_ERROR.toErrorCode(),
                "Invalid Paimon schema owner type 'GROUP'");

        assertTrinoError(
                () -> metadata.getSchemaProperties(SESSION, SYSTEM_DATABASE_NAME),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon schema properties are not supported for the system schema 'sys'");
        assertTrinoError(
                () -> metadata.getSchemaProperties(SESSION, "missing"),
                SCHEMA_NOT_FOUND.toErrorCode(),
                "Schema 'missing' does not exist");
        assertTrinoError(
                () -> metadata.getSchemaOwner(SESSION, "missing"),
                SCHEMA_NOT_FOUND.toErrorCode(),
                "Schema 'missing' does not exist");
    }

    @Test
    public void testSetSchemaAuthorizationStoresOwnerProperty()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.setSchemaAuthorization(SESSION, "schema", new TrinoPrincipal(PrincipalType.ROLE, "new_role"));

        assertThat(catalog.alteredDatabase).isEqualTo("schema");
        assertThat(catalog.alterDatabaseIgnoreIfNotExists).isFalse();
        assertThat(catalog.lastDatabasePropertyChanges).hasSize(2);
        assertThat(catalog.lastDatabasePropertyChanges.get(0))
                .isInstanceOfSatisfying(PropertyChange.SetProperty.class, change -> {
                    assertThat(change.property()).isEqualTo(OWNER_PROPERTY);
                    assertThat(change.value()).isEqualTo("new_role");
                });
        assertThat(catalog.lastDatabasePropertyChanges.get(1))
                .isInstanceOfSatisfying(PropertyChange.SetProperty.class, change -> {
                    assertThat(change.property()).isEqualTo(TRINO_SCHEMA_OWNER_TYPE_PROPERTY);
                    assertThat(change.value()).isEqualTo(PrincipalType.ROLE.name());
                });
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testDropSchemaPreservesCascadeFlag()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.dropSchema(SESSION, "schema", false);
        assertThat(catalog.droppedDatabase).isEqualTo("schema");
        assertThat(catalog.dropDatabaseIgnoreIfNotExists).isFalse();
        assertThat(catalog.dropDatabaseCascade).isFalse();

        metadata.dropSchema(SESSION, "schema", true);
        assertThat(catalog.dropDatabaseCascade).isTrue();
    }

    @Test
    public void testSchemaExistsReturnsTrueAndFalse()
    {
        SchemaQueryCatalog catalog = new SchemaQueryCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThat(metadata.schemaExists(SESSION, "existing_schema")).isTrue();
        assertThat(metadata.schemaExists(SESSION, "missing_schema")).isFalse();
    }

    @Test
    public void testListSchemaNames()
    {
        SchemaQueryCatalog catalog = new SchemaQueryCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertThat(metadata.listSchemaNames(SESSION)).containsExactly("alpha", "beta", SYSTEM_DATABASE_NAME);
    }

    @Test
    public void testDropSchemaTranslatesPaimonExceptions()
    {
        SchemaQueryCatalog notExistCatalog = new SchemaQueryCatalog()
        {
            @Override
            public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
                    throws Catalog.DatabaseNotExistException
            {
                throw new Catalog.DatabaseNotExistException(name);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(notExistCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> metadata.dropSchema(SESSION, "missing", false),
                SCHEMA_NOT_FOUND.toErrorCode(),
                "Schema 'missing' does not exist");

        SchemaQueryCatalog notEmptyCatalog = new SchemaQueryCatalog()
        {
            @Override
            public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
                    throws Catalog.DatabaseNotEmptyException
            {
                throw new Catalog.DatabaseNotEmptyException(name);
            }
        };
        PaimonMetadata metadata2 = new PaimonMetadata(notEmptyCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> metadata2.dropSchema(SESSION, "nonempty", false),
                SCHEMA_NOT_EMPTY.toErrorCode(),
                "Schema 'nonempty' is not empty");

        SchemaQueryCatalog alterMissingCatalog = new SchemaQueryCatalog()
        {
            @Override
            public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
                    throws Catalog.DatabaseNotExistException
            {
                throw new Catalog.DatabaseNotExistException(name);
            }
        };
        PaimonMetadata metadata3 = new PaimonMetadata(alterMissingCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> metadata3.setSchemaAuthorization(
                        SESSION,
                        "missing",
                        new TrinoPrincipal(PrincipalType.USER, "schema_owner")),
                SCHEMA_NOT_FOUND.toErrorCode(),
                "Schema 'missing' does not exist");
    }

    @Test
    public void testRenameTableSuccess()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        SchemaTableName newName = new SchemaTableName("schema", "renamed_table");

        metadata.renameTable(SESSION, tableHandle, newName);

        assertThat(catalog.renamedFromTable.getFullName()).isEqualTo("schema.table");
        assertThat(catalog.renamedToTable.getFullName()).isEqualTo("schema.renamed_table");
        assertThat(catalog.renamedIgnoreIfNotExists).isFalse();
    }

    @Test
    public void testRenameTableNotFound()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
                    throws Catalog.TableNotExistException
            {
                throw new Catalog.TableNotExistException(fromTable);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.renameTable(SESSION, tableHandle, new SchemaTableName("schema", "target")),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
    }

    @Test
    public void testRenameTableAlreadyExists()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
                    throws Catalog.TableAlreadyExistException
            {
                throw new Catalog.TableAlreadyExistException(toTable);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.renameTable(SESSION, tableHandle, new SchemaTableName("schema", "target")),
                TABLE_ALREADY_EXISTS.toErrorCode(),
                "Table 'schema.target' already exists");
    }

    @Test
    public void testDropTableSuccess()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.dropTable(SESSION, tableHandle);

        assertThat(catalog.droppedTable.getFullName()).isEqualTo("schema.table");
        assertThat(catalog.droppedTableIgnoreIfNotExists).isFalse();
    }

    @Test
    public void testDropTableNotFound()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
                    throws Catalog.TableNotExistException
            {
                throw new Catalog.TableNotExistException(identifier);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.dropTable(SESSION, tableHandle),
                TABLE_NOT_FOUND.toErrorCode(),
                "Table 'schema.table' does not exist");
    }

    @Test
    public void testListTablesWithSchema()
    {
        SchemaQueryCatalog catalog = new SchemaQueryCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.of("alpha"));
        assertThat(tables).containsExactly(
                new SchemaTableName("alpha", "t1"),
                new SchemaTableName("alpha", "t2"),
                new SchemaTableName("alpha", "v1"));
    }

    @Test
    public void testListTablesAllSchemas()
    {
        SchemaQueryCatalog catalog = new SchemaQueryCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        List<SchemaTableName> tables = metadata.listTables(SESSION, Optional.empty());
        assertThat(tables).containsExactly(
                new SchemaTableName("alpha", "t1"),
                new SchemaTableName("alpha", "t2"),
                new SchemaTableName("alpha", "v1"),
                new SchemaTableName("beta", "t3"),
                new SchemaTableName("beta", "v2"),
                new SchemaTableName(SYSTEM_DATABASE_NAME, "tables"),
                new SchemaTableName(SYSTEM_DATABASE_NAME, "partitions"),
                new SchemaTableName(SYSTEM_DATABASE_NAME, "all_table_options"),
                new SchemaTableName(SYSTEM_DATABASE_NAME, "catalog_options"));
    }

    @Test
    public void testListTableColumnsReusesTableHandleLookup()
    {
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(BucketMode.HASH_FIXED));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        Map<SchemaTableName, List<ColumnMetadata>> columns = metadata.listTableColumns(
                SESSION,
                new SchemaTablePrefix("schema", "table"));

        assertThat(columns).containsOnlyKeys(new SchemaTableName("schema", "table"));
        assertThat(columns.get(new SchemaTableName("schema", "table")))
                .extracting(ColumnMetadata::getName)
                .containsExactly("id");
        assertThat(catalog.getTableCalls()).isEqualTo(1);
    }

    @Test
    public void testRelationTypesMarkPaimonViews()
    {
        PaimonMetadata metadata = new PaimonMetadata(new SchemaQueryCatalog(), TESTING_TYPE_MANAGER);

        assertThat(metadata.getRelationTypes(SESSION, Optional.of("alpha")))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        new SchemaTableName("alpha", "t1"), RelationType.TABLE,
                        new SchemaTableName("alpha", "t2"), RelationType.TABLE,
                        new SchemaTableName("alpha", "v1"), RelationType.VIEW));
    }

    @Test
    public void testListTablesAndRelationTypesSkipPaimonViewsWithoutTrinoDialect()
    {
        PaimonMetadata metadata = new PaimonMetadata(new SchemaQueryCatalog()
        {
            @Override
            public List<String> listViews(String databaseName)
            {
                return databaseName.equals("alpha") ? List.of("spark_view", "trino_view") : List.of();
            }

            @Override
            public View getView(Identifier identifier)
            {
                if (identifier.getObjectName().equals("spark_view")) {
                    return new ViewImpl(
                            identifier,
                            List.of(DataTypes.FIELD(0, "id", DataTypes.BIGINT())),
                            "SELECT id FROM spark_table",
                            Map.of("spark", "SELECT id FROM spark_table"),
                            null,
                            Map.of());
                }
                return paimonView(identifier, List.of(DataTypes.FIELD(0, "id", DataTypes.BIGINT())));
            }
        }, TESTING_TYPE_MANAGER);

        assertThat(metadata.listTables(SESSION, Optional.of("alpha")))
                .containsExactly(
                        new SchemaTableName("alpha", "t1"),
                        new SchemaTableName("alpha", "t2"),
                        new SchemaTableName("alpha", "trino_view"));
        assertThat(metadata.getRelationTypes(SESSION, Optional.of("alpha")))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        new SchemaTableName("alpha", "t1"), RelationType.TABLE,
                        new SchemaTableName("alpha", "t2"), RelationType.TABLE,
                        new SchemaTableName("alpha", "trino_view"), RelationType.VIEW));
    }

    @Test
    public void testListTablesToleratesCatalogWithoutViewListing()
    {
        PaimonMetadata metadata = new PaimonMetadata(new SchemaQueryCatalog()
        {
            @Override
            public List<String> listViews(String databaseName)
            {
                throw new UnsupportedOperationException("view listing unavailable");
            }
        }, TESTING_TYPE_MANAGER);

        assertThat(metadata.listTables(SESSION, Optional.of("alpha")))
                .containsExactly(
                        new SchemaTableName("alpha", "t1"),
                        new SchemaTableName("alpha", "t2"));
        assertThat(metadata.getRelationTypes(SESSION, Optional.of("alpha")))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        new SchemaTableName("alpha", "t1"), RelationType.TABLE,
                        new SchemaTableName("alpha", "t2"), RelationType.TABLE));
    }

    @Test
    public void testListTablesDoesNotHideViewDefinitionFailures()
    {
        PaimonMetadata metadata = new PaimonMetadata(new SchemaQueryCatalog()
        {
            @Override
            public List<String> listViews(String databaseName)
            {
                return databaseName.equals("alpha") ? List.of("broken_view") : List.of();
            }

            @Override
            public View getView(Identifier identifier)
            {
                throw new TrinoException(NOT_SUPPORTED, "View definition uses an unsupported Paimon type");
            }
        }, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> metadata.listTables(SESSION, Optional.of("alpha")),
                NOT_SUPPORTED.toErrorCode(),
                "View definition uses an unsupported Paimon type");
    }

    @Test
    public void testListTablesSchemaNotFound()
    {
        SchemaQueryCatalog catalog = new SchemaQueryCatalog()
        {
            @Override
            public List<String> listTables(String databaseName)
                    throws Catalog.DatabaseNotExistException
            {
                throw new Catalog.DatabaseNotExistException(databaseName);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> metadata.listTables(SESSION, Optional.of("nonexistent")),
                SCHEMA_NOT_FOUND.toErrorCode(),
                "Schema 'nonexistent' does not exist");
    }

    @Test
    public void testCreateTableSchemaNotFound()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
                    throws Catalog.DatabaseNotExistException
            {
                throw new Catalog.DatabaseNotExistException(identifier.getDatabaseName());
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("missing_schema", "table"),
                List.of(ColumnMetadata.builder().setName("id").setType(BIGINT).build()));

        assertTrinoError(
                () -> metadata.createTable(SESSION, tableMetadata, SaveMode.FAIL),
                SCHEMA_NOT_FOUND.toErrorCode(),
                "Schema 'missing_schema' does not exist");
    }

    @Test
    public void testCreateTableAlreadyExists()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
                    throws Catalog.TableAlreadyExistException
            {
                throw new Catalog.TableAlreadyExistException(identifier);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(ColumnMetadata.builder().setName("id").setType(BIGINT).build()));

        assertTrinoError(
                () -> metadata.createTable(SESSION, tableMetadata, SaveMode.FAIL),
                TABLE_ALREADY_EXISTS.toErrorCode(),
                "Table 'schema.table' already exists");
    }

    @Test
    public void testCreateTableIgnoreIfExists()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
                    throws Catalog.TableAlreadyExistException
            {
                throw new Catalog.TableAlreadyExistException(identifier);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(ColumnMetadata.builder().setName("id").setType(BIGINT).build()));

        // SaveMode.IGNORE should not throw when table already exists
        metadata.createTable(SESSION, tableMetadata, SaveMode.IGNORE);
    }

    @Test
    public void testCreateTableReplaceModeUsesPaimonReplaceTable()
    {
        AtomicBoolean replaced = new AtomicBoolean();
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void replaceTable(Identifier identifier, Schema newSchema, boolean ignoreIfNotExists)
            {
                assertThat(identifier.getFullName()).isEqualTo("schema.table");
                assertThat(ignoreIfNotExists).isFalse();
                replaced.set(true);
            }

            @Override
            public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            {
                throw new AssertionError("CREATE OR REPLACE TABLE should use Paimon replaceTable");
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(ColumnMetadata.builder().setName("id").setType(BIGINT).build()));

        metadata.createTable(SESSION, tableMetadata, SaveMode.REPLACE);
        assertThat(replaced).isTrue();
    }

    @Test
    public void testCreateTableReplaceModeCreatesMissingTable()
    {
        AtomicBoolean created = new AtomicBoolean();
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
            @Override
            public void initSession(ConnectorSession connectorSession) {}

            @Override
            public Catalog forSession(ConnectorSession connectorSession)
            {
                return this;
            }

            @Override
            public void replaceTable(Identifier identifier, Schema newSchema, boolean ignoreIfNotExists)
                    throws Catalog.TableNotExistException
            {
                assertThat(identifier.getFullName()).isEqualTo("schema.table");
                assertThat(ignoreIfNotExists).isFalse();
                throw new Catalog.TableNotExistException(identifier);
            }

            @Override
            public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            {
                assertThat(identifier.getFullName()).isEqualTo("schema.table");
                assertThat(ignoreIfExists).isFalse();
                created.set(true);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(ColumnMetadata.builder().setName("id").setType(BIGINT).build()));

        metadata.createTable(SESSION, tableMetadata, SaveMode.REPLACE);
        assertThat(created).isTrue();
    }

    @Test
    public void testSetTablePropertiesSuccess()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setTableProperties(SESSION, tableHandle,
                Map.of(
                        "bucket", Optional.of((Object) "4"),
                        "removed_prop", Optional.empty()));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges).hasSize(2);

        SchemaChange setChange = catalog.lastAlterChanges.stream()
                .filter(c -> c instanceof SchemaChange.SetOption).findFirst().orElseThrow();
        assertThat(setChange)
                .isInstanceOfSatisfying(SchemaChange.SetOption.class, change -> {
                    assertThat(change.key()).isEqualTo("bucket");
                    assertThat(change.value()).isEqualTo("4");
                });

        SchemaChange removeChange = catalog.lastAlterChanges.stream()
                .filter(c -> c instanceof SchemaChange.RemoveOption).findFirst().orElseThrow();
        assertThat(removeChange)
                .isInstanceOfSatisfying(SchemaChange.RemoveOption.class, change ->
                        assertThat(change.key()).isEqualTo("removed_prop"));
    }

    @Test
    public void testSetTableAuthorizationStoresOwnerProperty()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        metadata.setTableAuthorization(
                SESSION,
                new SchemaTableName("schema", "table"),
                new TrinoPrincipal(PrincipalType.USER, "new_owner"));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.SetOption.class, change -> {
                    assertThat(change.key()).isEqualTo(OWNER_PROPERTY);
                    assertThat(change.value()).isEqualTo("new_owner");
                });
        assertThat(catalog.initialized).isTrue();
    }

    @Test
    public void testGetTableHandleRejectsStartVersion()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> metadata.getTableHandle(
                        SESSION,
                        new SchemaTableName("schema", "table"),
                        Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, INTEGER, 1L)),
                        Optional.empty()),
                NOT_SUPPORTED.toErrorCode(),
                "Read paimon table with start version is not supported");
    }

    @Test
    public void testBeginCreateTableUsesCreatedPaimonSchemaForWriteColumns()
    {
        AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions = new AtomicReference<>();
        CreatedSchemaCatalog catalog = new CreatedSchemaCatalog(createdVectorAndBlobTable(copyWithoutTimeTravelOptions));
        PaimonMetadata metadata = new PaimonMetadata(
                catalog,
                TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        ColumnMetadata.builder()
                                .setName("embedding")
                                .setType(new ArrayType(REAL))
                                .setComment(Optional.of("__VECTOR_FIELD;3; embedding"))
                                .build(),
                        ColumnMetadata.builder()
                                .setName("picture")
                                .setType(VarbinaryType.VARBINARY)
                                .setComment(Optional.of("__BLOB_FIELD; profile picture"))
                                .build()));

        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(
                SESSION,
                tableMetadata,
                Optional.empty(),
                RetryMode.NO_RETRIES,
                false);

        PaimonTableHandle handle = (PaimonTableHandle) outputHandle;
        assertThat(handle.getWriteColumns()).hasValueSatisfying(writeColumns -> {
            assertThat(writeColumns).extracting(PaimonColumnHandle::getColumnName)
                    .containsExactly("embedding", "picture");
            assertThat(writeColumns).extracting(column -> column.logicalType().getTypeRoot())
                    .containsExactly(DataTypeRoot.VECTOR, DataTypeRoot.BLOB);
        });
        assertThat(copyWithoutTimeTravelOptions.get()).isNull();
        assertThat(catalog.createdSchema.fields()).extracting(field -> field.type().getTypeRoot())
                .containsExactly(DataTypeRoot.ARRAY, DataTypeRoot.VARBINARY);
        assertThat(catalog.createdSchema.fields()).extracting(field -> field.description())
                .containsExactly("__VECTOR_FIELD;3; embedding", "__BLOB_FIELD; profile picture");
        assertThat(catalog.createdSchema.options())
                .doesNotContainKeys(CoreOptions.VECTOR_FIELD.key(), CoreOptions.BLOB_FIELD.key());
    }

    @Test
    public void testCreateTableRejectsInvalidPaimonColumnCommentDirectiveBeforeCatalogCall()
    {
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(ColumnMetadata.builder()
                        .setName("picture")
                        .setType(VarbinaryType.VARBINARY)
                        .setComment(Optional.of("__BLOB_MISSING; profile picture"))
                        .build()));

        CapturingDdlCatalog createCatalog = new CapturingDdlCatalog();
        PaimonMetadata createMetadata = new PaimonMetadata(createCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> createMetadata.createTable(
                        SESSION,
                        tableMetadata,
                        SaveMode.FAIL),
                NOT_SUPPORTED.toErrorCode(),
                "Invalid Paimon column comment directive for column 'picture': Unsupported BLOB directive in column comment: '__BLOB_MISSING; profile picture'. Supported directives are '__BLOB_FIELD', '__BLOB_DESCRIPTOR_FIELD' and '__BLOB_VIEW_FIELD'.");
        assertThat(createCatalog.initialized).isFalse();
        assertThat(createCatalog.createdSchema).isNull();

        CapturingDdlCatalog beginCreateCatalog = new CapturingDdlCatalog();
        PaimonMetadata beginCreateMetadata = new PaimonMetadata(beginCreateCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> beginCreateMetadata.beginCreateTable(
                        SESSION,
                        tableMetadata,
                        Optional.empty(),
                        RetryMode.NO_RETRIES,
                        false),
                NOT_SUPPORTED.toErrorCode(),
                "Invalid Paimon column comment directive for column 'picture': Unsupported BLOB directive in column comment: '__BLOB_MISSING; profile picture'. Supported directives are '__BLOB_FIELD', '__BLOB_DESCRIPTOR_FIELD' and '__BLOB_VIEW_FIELD'.");
        assertThat(beginCreateCatalog.initialized).isFalse();
        assertThat(beginCreateCatalog.createdSchema).isNull();
    }

    @Test
    public void testBeginCreateTableMatchesCreatedPaimonSchemaCaseInsensitively()
    {
        CreatedSchemaCatalog catalog = new CreatedSchemaCatalog(createdLowerCaseTable());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        new ColumnMetadata("ID", INTEGER),
                        new ColumnMetadata("VALUE", INTEGER)));

        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(
                SESSION,
                tableMetadata,
                Optional.empty(),
                RetryMode.NO_RETRIES,
                false);

        PaimonTableHandle handle = (PaimonTableHandle) outputHandle;
        assertThat(handle.getWriteColumns()).hasValueSatisfying(writeColumns ->
                assertThat(writeColumns).extracting(PaimonColumnHandle::getColumnName)
                        .containsExactly("id", "value"));
    }

    @Test
    public void testCreatedTableFieldIndexScansCreatedFieldsOnce()
    {
        int fieldCount = 100;
        AtomicInteger fieldReads = new AtomicInteger();
        List<DataField> fields = new AbstractList<>()
        {
            @Override
            public DataField get(int index)
            {
                fieldReads.incrementAndGet();
                return DataTypes.FIELD(index, "Column_" + index, DataTypes.INT());
            }

            @Override
            public int size()
            {
                return fieldCount;
            }
        };

        Map<String, DataField> fieldsByLowerName = PaimonMetadata.createdTableFieldsByLowerName(
                fields,
                new SchemaTableName("schema", "table"));

        assertThat(fieldReads.get()).isEqualTo(fieldCount);
        for (int index = 0; index < fieldCount; index++) {
            assertThat(fieldsByLowerName.get("column_" + index).id()).isEqualTo(index);
        }
        assertThat(fieldReads.get()).isEqualTo(fieldCount);
    }

    @Test
    public void testBeginCreateTableMarksCreateTableAsSelectOperation()
    {
        CreatedSchemaCatalog catalog = new CreatedSchemaCatalog(createdLowerCaseTable());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        new ColumnMetadata("id", INTEGER),
                        new ColumnMetadata("value", INTEGER)));

        PaimonTableHandle handle = (PaimonTableHandle) metadata.beginCreateTable(
                SESSION,
                tableMetadata,
                Optional.empty(),
                RetryMode.NO_RETRIES,
                false);

        assertThat(handle.getCreateTableOperation()).hasValue(PaimonTableHandle.CREATE_TABLE_AS_SELECT_OPERATION);
    }

    @Test
    public void testBeginCreateTableMarksCreateOrReplaceTableAsSelectOperation()
    {
        CreatedSchemaCatalog catalog = new CreatedSchemaCatalog(createdLowerCaseTable());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        new ColumnMetadata("id", INTEGER),
                        new ColumnMetadata("value", INTEGER)));

        PaimonTableHandle handle = (PaimonTableHandle) metadata.beginCreateTable(
                SESSION,
                tableMetadata,
                Optional.empty(),
                RetryMode.NO_RETRIES,
                true);

        assertThat(handle.getCreateTableOperation()).hasValue(PaimonTableHandle.CREATE_OR_REPLACE_TABLE_AS_SELECT_OPERATION);
    }

    @Test
    public void testBeginCreateTableRejectsDuplicateCaseInsensitiveCreatedFields()
    {
        CreatedSchemaCatalog catalog = new CreatedSchemaCatalog(createdDuplicateCaseTable());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("id", INTEGER)));

        assertThatThrownBy(() -> metadata.beginCreateTable(
                SESSION,
                tableMetadata,
                Optional.empty(),
                RetryMode.NO_RETRIES,
                false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Created Paimon table 'schema.table' schema contains case-insensitive duplicate field name 'id'");
    }

    @Test
    public void testBeginCreateTablePreservesExternalStorageBlobDirective()
    {
        CreatedSchemaCatalog catalog = new CreatedSchemaCatalog(createdExternalStorageBlobTable());
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        ColumnMetadata.builder()
                                .setName("picture")
                                .setType(VarbinaryType.VARBINARY)
                                .setComment(Optional.of("__BLOB_FIELD; external picture"))
                                .build()),
                Map.of());

        ConnectorOutputTableHandle outputHandle = metadata.beginCreateTable(
                SESSION,
                tableMetadata,
                Optional.empty(),
                RetryMode.NO_RETRIES,
                false);

        PaimonTableHandle handle = (PaimonTableHandle) outputHandle;
        assertThat(catalog.createdSchema.fields()).extracting(field -> field.type().getTypeRoot())
                .containsExactly(DataTypeRoot.VARBINARY);
        assertThat(catalog.createdSchema.fields()).extracting(field -> field.description())
                .containsExactly("__BLOB_FIELD; external picture");
        assertThat(handle.getWriteColumns()).hasValueSatisfying(writeColumns -> {
            assertThat(writeColumns).extracting(PaimonColumnHandle::getColumnName)
                    .containsExactly("picture");
            assertThat(writeColumns).extracting(column -> column.logicalType().getTypeRoot())
                    .containsExactly(DataTypeRoot.BLOB);
        });
    }

    @Test
    public void testCreateTablePreservesColumnNullability()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(
                        ColumnMetadata.builder()
                                .setName("nullable_col")
                                .setType(INTEGER)
                                .setNullable(true)
                                .build(),
                        ColumnMetadata.builder()
                                .setName("not_null_col")
                                .setType(INTEGER)
                                .setNullable(false)
                                .build()));

        metadata.createTable(SESSION, tableMetadata, SaveMode.FAIL);

        assertThat(catalog.createdSchema.fields()).extracting(field -> field.type().isNullable())
                .containsExactly(true, false);
    }

    @Test
    public void testDdlRejectsTemporalPrecisionUnsupportedByPaimon()
    {
        ConnectorTableMetadata unsupportedCreateTable = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("event_time", createTimestampType(12))));
        CapturingDdlCatalog createCatalog = new CapturingDdlCatalog();
        PaimonMetadata createMetadata = new PaimonMetadata(createCatalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> createMetadata.createTable(
                        SESSION,
                        unsupportedCreateTable,
                        SaveMode.FAIL),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon supports timestamp precision up to 9, got timestamp(12)");
        assertThat(createCatalog.initialized).isFalse();
        assertThat(createCatalog.createdSchema).isNull();

        CapturingDdlCatalog beginCreateCatalog = new CapturingDdlCatalog();
        PaimonMetadata beginCreateMetadata = new PaimonMetadata(beginCreateCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> beginCreateMetadata.beginCreateTable(
                        SESSION,
                        unsupportedCreateTable,
                        Optional.empty(),
                        RetryMode.NO_RETRIES,
                        false),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon supports timestamp precision up to 9, got timestamp(12)");
        assertThat(beginCreateCatalog.initialized).isFalse();
        assertThat(beginCreateCatalog.createdSchema).isNull();

        ConnectorTableMetadata unsupportedTimeCreateTable = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("event_time", io.trino.spi.type.TimeType.TIME_MICROS)));
        CapturingDdlCatalog createTimeCatalog = new CapturingDdlCatalog();
        PaimonMetadata createTimeMetadata = new PaimonMetadata(createTimeCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> createTimeMetadata.createTable(
                        SESSION,
                        unsupportedTimeCreateTable,
                        SaveMode.FAIL),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon stores time values with millisecond precision, got time(6)");
        assertThat(createTimeCatalog.initialized).isFalse();
        assertThat(createTimeCatalog.createdSchema).isNull();

        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        CapturingDdlCatalog addColumnCatalog = new CapturingDdlCatalog();
        PaimonMetadata addColumnMetadata = new PaimonMetadata(addColumnCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> addColumnMetadata.addColumn(
                        SESSION,
                        tableHandle,
                        new ColumnMetadata("event_time", io.trino.spi.type.TimeType.TIME_MICROS),
                        new Last()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon stores time values with millisecond precision, got time(6)");
        assertThat(addColumnCatalog.alterCalls).isEqualTo(0);

        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        CapturingDdlCatalog setColumnCatalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata setColumnMetadata = new PaimonMetadata(setColumnCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> setColumnMetadata.setColumnType(
                        SESSION,
                        tableHandle,
                        PaimonColumnHandle.of("id", DataTypes.INT()),
                        TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon supports timestamp with time zone precision up to 9, got timestamp(12) with time zone");
        assertThat(setColumnCatalog.alterCalls).isEqualTo(0);

        CapturingDdlCatalog addFieldCatalog = new CapturingDdlCatalog();
        PaimonMetadata addFieldMetadata = new PaimonMetadata(addFieldCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> addFieldMetadata.addField(
                        SESSION,
                        tableHandle,
                        List.of(),
                        "event_time",
                        createTimestampType(12),
                        false),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon supports timestamp precision up to 9, got timestamp(12)");
        assertThat(addFieldCatalog.initialized).isFalse();
        assertThat(addFieldCatalog.alterCalls).isEqualTo(0);

        CapturingDdlCatalog setFieldCatalog = new CapturingDdlCatalog();
        PaimonMetadata setFieldMetadata = new PaimonMetadata(setFieldCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> setFieldMetadata.setFieldType(
                        SESSION,
                        tableHandle,
                        List.of("payload", "event_time"),
                        createTimestampType(12)),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon supports timestamp precision up to 9, got timestamp(12)");
        assertThat(setFieldCatalog.initialized).isFalse();
        assertThat(setFieldCatalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testDdlRejectsStringLengthUnsupportedByPaimon()
    {
        ConnectorTableMetadata unsupportedCreateTable = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("code", io.trino.spi.type.CharType.createCharType(0))));
        CapturingDdlCatalog createCatalog = new CapturingDdlCatalog();
        PaimonMetadata createMetadata = new PaimonMetadata(createCatalog, TESTING_TYPE_MANAGER);

        assertTrinoError(
                () -> createMetadata.createTable(
                        SESSION,
                        unsupportedCreateTable,
                        SaveMode.FAIL),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon supports char length between 1 and 2147483647, got char(0)");
        assertThat(createCatalog.initialized).isFalse();
        assertThat(createCatalog.createdSchema).isNull();

        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        CapturingDdlCatalog addColumnCatalog = new CapturingDdlCatalog();
        PaimonMetadata addColumnMetadata = new PaimonMetadata(addColumnCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> addColumnMetadata.addColumn(
                        SESSION,
                        tableHandle,
                        new ColumnMetadata("code", VarcharType.createVarcharType(0)),
                        new Last()),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon supports varchar length between 1 and 2147483647, got varchar(0)");
        assertThat(addColumnCatalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testDdlUnsupportedTrinoTypeWithoutMessageReportsStableNotSupported()
    {
        ConnectorTableMetadata unsupportedCreateTable = new ConnectorTableMetadata(
                new SchemaTableName("schema", "table"),
                List.of(new ColumnMetadata("payload", unsupportedTrinoTypeWithoutMessage())));

        CapturingDdlCatalog createCatalog = new CapturingDdlCatalog();
        PaimonMetadata createMetadata = new PaimonMetadata(createCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> createMetadata.createTable(
                        SESSION,
                        unsupportedCreateTable,
                        SaveMode.FAIL),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported Trino type UNSUPPORTED_TEST_TYPE: UnsupportedOperationException");
        assertThat(createCatalog.initialized).isFalse();
        assertThat(createCatalog.createdSchema).isNull();

        CapturingDdlCatalog beginCreateCatalog = new CapturingDdlCatalog();
        PaimonMetadata beginCreateMetadata = new PaimonMetadata(beginCreateCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> beginCreateMetadata.beginCreateTable(
                        SESSION,
                        unsupportedCreateTable,
                        Optional.empty(),
                        RetryMode.NO_RETRIES,
                        false),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported Trino type UNSUPPORTED_TEST_TYPE: UnsupportedOperationException");
        assertThat(beginCreateCatalog.initialized).isFalse();
        assertThat(beginCreateCatalog.createdSchema).isNull();

        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        CapturingDdlCatalog addColumnCatalog = new CapturingDdlCatalog();
        PaimonMetadata addColumnMetadata = new PaimonMetadata(addColumnCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> addColumnMetadata.addColumn(
                        SESSION,
                        tableHandle,
                        new ColumnMetadata("payload", unsupportedTrinoTypeWithoutMessage()),
                        new Last()),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported Trino type UNSUPPORTED_TEST_TYPE: UnsupportedOperationException");
        assertThat(addColumnCatalog.alterCalls).isEqualTo(0);

        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        CapturingDdlCatalog setColumnCatalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata setColumnMetadata = new PaimonMetadata(setColumnCatalog, TESTING_TYPE_MANAGER);
        assertTrinoError(
                () -> setColumnMetadata.setColumnType(
                        SESSION,
                        tableHandle,
                        PaimonColumnHandle.of("id", DataTypes.INT()),
                        unsupportedTrinoTypeWithoutMessage()),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported Trino type UNSUPPORTED_TEST_TYPE: UnsupportedOperationException");
        assertThat(setColumnCatalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testExternalPaimonUnsupportedTypeWithoutMessageFailsMetadataCleanly()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "payload", unsupportedPaimonDataTypeWithoutMessage()));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of("id"), List.of("id"), "id"));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.getTableMetadata(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported Paimon type UNSUPPORTED_TEST_TYPE: UnsupportedOperationException");
        assertTrinoError(
                () -> metadata.beginMerge(SESSION, tableHandle, Map.of(), RetryMode.NO_RETRIES),
                NOT_SUPPORTED.toErrorCode(),
                "Unsupported Paimon column 'payload' with type UNSUPPORTED_TEST_TYPE: UnsupportedOperationException");
    }

    @Test
    public void testExternalPaimonCharLengthUnsupportedByTrinoFailsMetadataCleanly()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(
                0,
                "too_long",
                new org.apache.paimon.types.CharType(io.trino.spi.type.CharType.MAX_LENGTH + 1)));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        assertTrinoError(
                () -> metadata.getTableMetadata(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Trino supports char length up to 65536, got Paimon char(65537)");
        assertTrinoError(
                () -> metadata.getColumnHandles(SESSION, tableHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Trino supports char length up to 65536, got Paimon char(65537)");
    }

    @Test
    public void testExternalPaimonTimestampPrecisionIsPreservedInMetadata()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "ts0", new TimestampType(0)),
                DataTypes.FIELD(1, "ts2", new TimestampType(2)),
                DataTypes.FIELD(2, "tz1", new LocalZonedTimestampType(1)),
                DataTypes.FIELD(3, "tz2", new LocalZonedTimestampType(2)));
        TestingPaimonCatalog catalog = new TestingPaimonCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, tableHandle);

        assertThat(tableMetadata.getColumns())
                .extracting(column -> column.getType().getDisplayName())
                .containsExactly(
                        "timestamp(0)",
                        "timestamp(2)",
                        "timestamp(1) with time zone",
                        "timestamp(2) with time zone");
    }

    @Test
    public void testAddColumnRejectsNotNullBeforeCatalogAlter()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ColumnMetadata column = ColumnMetadata.builder()
                .setName("required_value")
                .setType(INTEGER)
                .setNullable(false)
                .build();

        assertTrinoError(
                () -> metadata.addColumn(SESSION, tableHandle, column, new Last()),
                NOT_SUPPORTED.toErrorCode(),
                "This connector does not support adding not null columns");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testAddColumnPreservesNullableTypeAndComment()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ColumnMetadata column = ColumnMetadata.builder()
                .setName("embedding")
                .setType(new ArrayType(REAL))
                .setNullable(true)
                .setComment(Optional.of("__VECTOR_FIELD;3; added embedding"))
                .build();

        metadata.addColumn(SESSION, tableHandle, column, new Last());

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.AddColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("embedding");
                    assertThat(change.dataType().isNullable()).isTrue();
                    assertThat(change.description()).isEqualTo("__VECTOR_FIELD;3; added embedding");
                });
    }

    @Test
    public void testAddColumnRejectsInvalidPaimonColumnCommentDirectiveBeforeCatalogAlter()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        ColumnMetadata column = ColumnMetadata.builder()
                .setName("embedding")
                .setType(INTEGER)
                .setComment(Optional.of("__VECTOR_FIELD;3; added embedding"))
                .build();

        assertTrinoError(
                () -> metadata.addColumn(SESSION, tableHandle, column, new Last()),
                NOT_SUPPORTED.toErrorCode(),
                "Invalid Paimon column comment directive for column 'embedding': Column embedding declared with a VECTOR directive must be of ARRAY type, but was INT.");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testRenameColumnUsesPaimonRenameSchemaChange()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "old_name", DataTypes.STRING()));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("old_name", DataTypes.STRING());

        metadata.renameColumn(SESSION, tableHandle, columnHandle, "new_name");

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.RenameColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("old_name");
                    assertThat(change.newName()).isEqualTo("new_name");
                });
    }

    @Test
    public void testDropColumnUsesPaimonDropSchemaChange()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "obsolete_col", DataTypes.STRING()));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("obsolete_col", DataTypes.STRING());

        metadata.dropColumn(SESSION, tableHandle, columnHandle);

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.DropColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("obsolete_col"));
    }

    @Test
    public void testDropLastColumnIsRejectedBeforeCatalogAlter()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "only_col", DataTypes.STRING()));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("only_col", DataTypes.STRING());

        assertTrinoError(
                () -> metadata.dropColumn(SESSION, tableHandle, columnHandle),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop column is not supported: Cannot drop all fields in table");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testSetTableCommentUsesPaimonUpdateCommentSchemaChange()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setTableComment(SESSION, tableHandle, Optional.of("table description"));
        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateComment.class, change ->
                        assertThat(change.comment()).isEqualTo("table description"));

        metadata.setTableComment(SESSION, tableHandle, Optional.empty());
        assertThat(catalog.alterCalls).isEqualTo(2);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateComment.class, change ->
                        assertThat(change.comment()).isNull());
    }

    @Test
    public void testSetColumnTypePreservesExistingPaimonNullability()
    {
        assertSetColumnTypePreservesExistingPaimonNullability(DataTypes.INT(), true);
        assertSetColumnTypePreservesExistingPaimonNullability(DataTypes.INT().notNull(), false);
    }

    private static void assertSetColumnTypePreservesExistingPaimonNullability(
            DataType existingType,
            boolean expectedNullable)
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", existingType));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setColumnType(
                SESSION,
                tableHandle,
                PaimonColumnHandle.of("id", existingType),
                BigintType.BIGINT);

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnType.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("id");
                    assertThat(change.newDataType().getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
                    assertThat(change.newDataType().isNullable()).isEqualTo(expectedNullable);
                    assertThat(change.keepNullability()).isTrue();
                });
    }

    @Test
    public void testSetColumnTypeUsesLatestPaimonNullabilityInsteadOfStaleColumnHandle()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT().notNull()));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setColumnType(
                SESSION,
                tableHandle,
                PaimonColumnHandle.of("id", DataTypes.INT()),
                BigintType.BIGINT);

        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnType.class, change ->
                        assertThat(change.newDataType().isNullable()).isFalse());
    }

    @Test
    public void testDropNotNullConstraint()
    {
        CapturingDdlCatalog catalog = new CapturingDdlCatalog();
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("id", DataTypes.INT().notNull());

        metadata.dropNotNullConstraint(SESSION, tableHandle, columnHandle);

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnNullability.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("id");
                    assertThat(change.newNullability()).isTrue();
                });
    }

    @Test
    public void testAddFieldSuccess()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "address", DataTypes.ROW(
                        DataTypes.FIELD(1, "city", DataTypes.STRING()))));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.addField(SESSION, tableHandle, List.of("address"), "street", VARCHAR, false);

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.AddColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("address", "street");
                    assertThat(change.dataType().getTypeRoot()).isEqualTo(DataTypeRoot.VARCHAR);
                });
    }

    @Test
    public void testAddFieldIgnoreExisting()
    {
        PaimonCatalog catalog = new PaimonCatalog(new Options(), unsupportedFileSystemFactory())
        {
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
                RowType rowType = DataTypes.ROW(
                        DataTypes.FIELD(0, "address", DataTypes.ROW(
                                DataTypes.FIELD(1, "street", DataTypes.STRING()))));
                return fileStoreTable(
                        BucketMode.HASH_FIXED,
                        new AtomicBoolean(),
                        rowType,
                        rowType,
                        List.of(),
                        List.of(),
                        "");
            }

            @Override
            public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
                    throws Catalog.ColumnAlreadyExistException
            {
                throw new Catalog.ColumnAlreadyExistException(identifier, ((SchemaChange.AddColumn) changes.get(0)).fieldNames()[0]);
            }
        };
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        // Should not throw when ignoreExisting=true
        metadata.addField(SESSION, tableHandle, List.of("address"), "street", VARCHAR, true);
    }

    @Test
    public void testDropFieldSuccess()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "address", DataTypes.ROW(
                        DataTypes.FIELD(1, "street", DataTypes.STRING()),
                        DataTypes.FIELD(2, "city", DataTypes.STRING()))));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("address", rowType.getField("address").type());

        metadata.dropField(SESSION, tableHandle, columnHandle, List.of("street"));

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.DropColumn.class, change ->
                        assertThat(change.fieldNames()).containsExactly("address", "street"));
    }

    @Test
    public void testDropLastNestedFieldIsRejectedBeforeCatalogAlter()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "address", DataTypes.ROW(
                        DataTypes.FIELD(1, "street", DataTypes.STRING()))));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("address", rowType.getField("address").type());

        assertTrinoError(
                () -> metadata.dropField(SESSION, tableHandle, columnHandle, List.of("street")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop field is not supported: Cannot drop all fields in table");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testDropLastCollectionNestedFieldIsRejectedBeforeCatalogAlter()
    {
        RowType valueType = DataTypes.ROW(
                DataTypes.FIELD(2, "Code", DataTypes.INT()));
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "Payload", DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), valueType))));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());
        PaimonColumnHandle columnHandle = PaimonColumnHandle.of("Payload", rowType.getField("Payload").type());

        assertTrinoError(
                () -> metadata.dropField(
                        SESSION,
                        tableHandle,
                        columnHandle,
                        List.of("element", "value", "Code")),
                NOT_SUPPORTED.toErrorCode(),
                "Paimon drop field is not supported: Cannot drop all fields in table");
        assertThat(catalog.alterCalls).isEqualTo(0);
    }

    @Test
    public void testRenameFieldSuccess()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "address", DataTypes.ROW(
                        DataTypes.FIELD(1, "street", DataTypes.STRING()))));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.renameField(SESSION, tableHandle, List.of("address", "street"), "road");

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.RenameColumn.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("address", "street");
                    assertThat(change.newName()).isEqualTo("road");
                });
    }

    @Test
    public void testSetFieldTypeSuccess()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "address", DataTypes.ROW(
                        DataTypes.FIELD(1, "zip", DataTypes.INT()))));
        CapturingDdlCatalog catalog = new CapturingDdlCatalog(fileStoreTable(
                BucketMode.HASH_FIXED, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), ""));
        PaimonMetadata metadata = new PaimonMetadata(catalog, TESTING_TYPE_MANAGER);
        PaimonTableHandle tableHandle = new PaimonTableHandle("schema", "table", Map.of());

        metadata.setFieldType(SESSION, tableHandle, List.of("address", "zip"), BIGINT);

        assertThat(catalog.alterCalls).isEqualTo(1);
        assertThat(catalog.lastAlterChanges)
                .singleElement()
                .isInstanceOfSatisfying(SchemaChange.UpdateColumnType.class, change -> {
                    assertThat(change.fieldNames()).containsExactly("address", "zip");
                    assertThat(change.newDataType().getTypeRoot()).isEqualTo(DataTypeRoot.BIGINT);
                    assertThat(change.keepNullability()).isTrue();
                });
    }

    private static void assertUnsupportedFileStoreTable(Runnable call, String message)
    {
        assertThatThrownBy(call::run)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessageContaining(message);
                });
    }

    private static void assertTrinoError(Runnable call, ErrorCode errorCode, String message)
    {
        assertThatThrownBy(call::run)
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(errorCode);
                    assertThat(exception).hasMessage(message);
                });
    }

    private static Type unsupportedTrinoTypeWithoutMessage()
    {
        return (Type) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Type.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getBaseName", "getDisplayName" -> throw new UnsupportedOperationException();
                    case "toString" -> "UNSUPPORTED_TEST_TYPE";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException("Unexpected Type method: " + method.getName());
                });
    }

    private static DataType unsupportedPaimonDataTypeWithoutMessage()
    {
        return new DataType(true, DataTypeRoot.VARIANT)
        {
            @Override
            public int defaultSize()
            {
                return 0;
            }

            @Override
            public DataType copy(boolean isNullable)
            {
                return this;
            }

            @Override
            public String asSQLString()
            {
                return "UNSUPPORTED_TEST_TYPE";
            }

            @Override
            public <R> R accept(DataTypeVisitor<R> visitor)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static void assertSystemTableWriteRejected(TestingPaimonCatalog catalog, Runnable call, String operation)
    {
        assertTrinoError(
                call,
                NOT_SUPPORTED.toErrorCode(),
                "Paimon " + operation + " is not supported for system table 'schema.table$snapshots'");
        assertThat(catalog.initialized).isFalse();
    }

    private static void assertMetadataDeleteRowId(ColumnHandle columnHandle)
    {
        assertThat(columnHandle).isInstanceOfSatisfying(PaimonColumnHandle.class, rowId -> {
            assertThat(rowId.getColumnName()).isEqualTo(PaimonColumnHandle.TRINO_ROW_ID_NAME);
            assertThat(rowId.isHidden()).isTrue();
            assertThat(((RowType) rowId.logicalType()).getFieldNames())
                    .containsExactly("_metadata_delete");
        });
    }

    private static void assertMetadataDeleteFallback(ConnectorMergeTableHandle mergeHandle)
    {
        assertThat(mergeHandle).isInstanceOfSatisfying(
                PaimonMergeTableHandle.class,
                paimonMergeHandle -> assertThat(paimonMergeHandle.isMetadataDeleteFallback()).isTrue());
    }

    private static Map<String, ColumnHandle> assignments(PaimonColumnHandle first, PaimonColumnHandle second)
    {
        Map<String, ColumnHandle> assignments = new LinkedHashMap<>();
        assignments.put(first.getColumnName(), first);
        assignments.put(second.getColumnName(), second);
        return assignments;
    }

    private static View paimonView(Identifier identifier, List<DataField> fields)
    {
        return new ViewImpl(
                identifier,
                fields,
                "SELECT value, label FROM table",
                Map.of("trino", "SELECT value, label FROM table"),
                null,
                Map.of());
    }

    private static FileStoreTable fileStoreTable(BucketMode bucketMode)
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        return fileStoreTable(bucketMode, new AtomicBoolean(), rowType, rowType, List.of("id"));
    }

    private static FileStoreTable unkeyedFileStoreTable(BucketMode bucketMode)
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        return fileStoreTable(bucketMode, new AtomicBoolean(), rowType, rowType, List.of(), List.of(), "id");
    }

    private static FileStoreTable fileStoreTable(
            BucketMode bucketMode,
            AtomicBoolean copiedWithLatestSchema,
            RowType rowType,
            RowType latestRowType)
    {
        return fileStoreTable(bucketMode, copiedWithLatestSchema, rowType, latestRowType, List.of("id"));
    }

    private static FileStoreTable fileStoreTable(
            BucketMode bucketMode,
            AtomicBoolean copiedWithLatestSchema,
            RowType rowType,
            RowType latestRowType,
            List<String> primaryKeys)
    {
        return fileStoreTable(
                bucketMode,
                copiedWithLatestSchema,
                rowType,
                latestRowType,
                List.of("id"),
                primaryKeys,
                "id");
    }

    private static FileStoreTable fileStoreTable(
            BucketMode bucketMode,
            AtomicBoolean copiedWithLatestSchema,
            RowType rowType,
            RowType latestRowType,
            List<String> partitionKeys,
            List<String> primaryKeys)
    {
        return fileStoreTable(
                bucketMode,
                copiedWithLatestSchema,
                rowType,
                latestRowType,
                partitionKeys,
                primaryKeys,
                "id");
    }

    private static FileStoreTable fileStoreTable(
            BucketMode bucketMode,
            AtomicBoolean copiedWithLatestSchema,
            RowType rowType,
            RowType latestRowType,
            List<String> primaryKeys,
            String bucketKey)
    {
        return fileStoreTable(
                bucketMode,
                copiedWithLatestSchema,
                rowType,
                latestRowType,
                List.of("id"),
                primaryKeys,
                bucketKey);
    }

    private static FileStoreTable fileStoreTable(
            BucketMode bucketMode,
            AtomicBoolean copiedWithLatestSchema,
            RowType rowType,
            RowType latestRowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            String bucketKey)
    {
        return fileStoreTable(
                bucketMode,
                copiedWithLatestSchema,
                rowType,
                latestRowType,
                partitionKeys,
                primaryKeys,
                bucketKey,
                Map.of());
    }

    private static FileStoreTable fileStoreTable(
            BucketMode bucketMode,
            AtomicBoolean copiedWithLatestSchema,
            RowType rowType,
            RowType latestRowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            String bucketKey,
            Map<String, String> options)
    {
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "bucketMode" -> bucketMode;
                    case "name" -> "testing_file_store_table";
                    case "rowType" -> rowType;
                    case "partitionKeys" -> partitionKeys;
                    case "primaryKeys" -> primaryKeys;
                    case "comment" -> Optional.empty();
                    case "options" -> options;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "latestSnapshot" -> Optional.of(true);
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            partitionKeys,
                            primaryKeys,
                            mergeOptions(Map.of(
                                    CoreOptions.BUCKET.key(), "7",
                                    CoreOptions.BUCKET_KEY.key(), bucketKey), options),
                            ""));
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield fileStoreTable(
                                bucketMode,
                                copiedWithLatestSchema,
                                latestRowType,
                                latestRowType,
                                partitionKeys,
                                primaryKeys,
                                bucketKey,
                                options);
                    }
                    case "snapshotManager" -> null;
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "catalogEnvironment" -> new CatalogEnvironment(
                            null, null, null, null, null, null, false, false)
                    {
                        @Override
                        public SnapshotCommit snapshotCommit(SnapshotManager ignored)
                        {
                            return new SnapshotCommit()
                            {
                                @Override
                                public boolean commit(String catalogName, Snapshot snapshot, String branch, List<PartitionStatistics> statistics)
                                {
                                    return true;
                                }

                                @Override
                                public void close() {}
                            };
                        }
                    };
                    case "toString" -> "testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable schemaOptionsFileStoreTable(Map<String, String> options)
    {
        return schemaOptionsFileStoreTable(options, true);
    }

    private static FileStoreTable schemaOptionsFileStoreTable(Map<String, String> options, boolean hasSnapshots)
    {
        return schemaOptionsFileStoreTable(options, hasSnapshots, new AtomicInteger());
    }

    private static FileStoreTable schemaOptionsFileStoreTable(
            Map<String, String> options,
            boolean hasSnapshots,
            AtomicInteger latestSnapshotCalls)
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        TableSchema schema = TableSchema.create(1, new Schema(rowType.getFields(), List.of(), List.of(), options, ""));
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "name" -> "schema_options_file_store_table";
                    case "rowType" -> rowType;
                    case "partitionKeys", "primaryKeys" -> List.of();
                    case "comment" -> Optional.empty();
                    case "options" -> options;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> schema;
                    case "latestSnapshot" -> {
                        latestSnapshotCalls.incrementAndGet();
                        yield hasSnapshots ? Optional.of(true) : Optional.empty();
                    }
                    case "copyWithLatestSchema", "copy", "copyWithoutTimeTravel" -> proxy;
                    case "toString" -> "schema-options-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Map<String, String> mergeOptions(Map<String, String> first, Map<String, String> second)
    {
        Map<String, String> result = new HashMap<>();
        result.putAll(first);
        result.putAll(second);
        return Map.copyOf(result);
    }

    private static FileStoreTable nonSerializableSchemaFileStoreTable(IOException failure)
    {
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "name" -> "non-serializable-schema-file-store-table";
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
                    case "partitionKeys" -> List.of();
                    case "primaryKeys" -> List.of("id");
                    case "comment" -> Optional.empty();
                    case "options" -> Map.of();
                    case "coreOptions" -> new CoreOptions(new Options(Map.of()));
                    case "schema" -> {
                        TableSchema schema = TableSchema.create(1, new Schema(
                                List.of(new DataField(0, "id", DataTypes.INT())),
                                List.of(),
                                List.of("id"),
                                Map.of(CoreOptions.BUCKET.key(), "7", CoreOptions.BUCKET_KEY.key(), "id"),
                                ""));
                        yield new TableSchema(
                                schema.version(),
                                schema.id(),
                                schema.fields(),
                                schema.highestFieldId(),
                                schema.partitionKeys(),
                                schema.primaryKeys(),
                                schema.options(),
                                schema.comment(),
                                schema.timeMillis())
                        {
                            private Object writeReplace()
                                    throws IOException
                            {
                                throw failure;
                            }
                        };
                    }
                    case "copyWithLatestSchema", "copy", "copyWithoutTimeTravel" -> proxy;
                    case "toString" -> "non-serializable-schema-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable rowTrackingFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            RowType rowType)
    {
        RowType latestRowType = rowType;
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of();
                    case "primaryKeys" -> List.of();
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            List.of(),
                            List.of(),
                            Map.of(CoreOptions.ROW_TRACKING_ENABLED.key(), "true"),
                            ""));
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield rowTrackingFileStoreTable(copiedWithLatestSchema, latestRowType);
                    }
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "options" -> Map.of(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
                    case "coreOptions" -> new CoreOptions(new Options(Map.of(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")));
                    case "toString" -> "testing-row-tracking-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable sequenceNumberEnabledFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            RowType rowType)
    {
        RowType latestRowType = rowType;
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "true");
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of("pt");
                    case "primaryKeys" -> List.of("pk", "pt");
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            List.of("pt"),
                            List.of("pk", "pt"),
                            Map.of(CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "true"),
                            ""));
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield sequenceNumberEnabledFileStoreTable(copiedWithLatestSchema, latestRowType);
                    }
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "options" -> options;
                    case "coreOptions" -> new CoreOptions(new Options(
                            Map.of(CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "true")));
                    case "toString" -> "testing-sequence-number-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable commitFileStoreTable(AtomicBoolean copiedWithLatestSchema, AtomicBoolean committed)
    {
        return commitFileStoreTable(copiedWithLatestSchema, committed, new AtomicReference<>(), null);
    }

    private static FileStoreTable writePlanningFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions)
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "pt", DataTypes.STRING()));
        AtomicReference<FileStoreTable> latestTableRef = new AtomicReference<>();
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "name" -> "latest-write-planning-file-store-table";
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of("pt");
                    case "primaryKeys" -> List.of("id");
                    case "comment" -> Optional.empty();
                    case "options" -> Map.of();
                    case "coreOptions" -> new CoreOptions(new Options(Map.of()));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            List.of("pt"),
                            List.of("id"),
                            Map.of(
                                    CoreOptions.BUCKET.key(), "7",
                                    CoreOptions.BUCKET_KEY.key(), "id"),
                            ""));
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield proxy;
                    }
                    case "copy" -> proxy;
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield proxy;
                    }
                    case "toString" -> "latest-write-planning-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        latestTableRef.set(latestTable);
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTableRef.get();
                    }
                    case "copy" -> proxy;
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield latestTableRef.get();
                    }
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "name" -> "stale-write-planning-file-store-table";
                    case "rowType" -> rowType;
                    case "partitionKeys" -> List.of("pt");
                    case "primaryKeys" -> List.of("id");
                    case "comment" -> Optional.empty();
                    case "options" -> Map.of();
                    case "coreOptions" -> new CoreOptions(new Options(Map.of()));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            List.of("pt"),
                            List.of("id"),
                            Map.of(
                                    CoreOptions.BUCKET.key(), "7",
                                    CoreOptions.BUCKET_KEY.key(), "id"),
                            ""));
                    case "toString" -> "stale-write-planning-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable commitFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean committed,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions)
    {
        return commitFileStoreTable(copiedWithLatestSchema, committed, copyWithoutTimeTravelOptions, null);
    }

    private static FileStoreTable commitFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean committed,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            RuntimeException commitFailure)
    {
        return commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                copyWithoutTimeTravelOptions,
                commitFailure,
                new AtomicBoolean());
    }

    private static FileStoreTable commitFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean committed,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            RuntimeException commitFailure,
            AtomicBoolean overwriteEnabled)
    {
        return commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                copyWithoutTimeTravelOptions,
                commitFailure,
                overwriteEnabled,
                new AtomicReference<>());
    }

    private static FileStoreTable commitFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean committed,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            RuntimeException commitFailure,
            AtomicBoolean overwriteEnabled,
            AtomicReference<String> operation)
    {
        return commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                copyWithoutTimeTravelOptions,
                commitFailure,
                overwriteEnabled,
                operation,
                List.of(),
                List.of(),
                Map.of());
    }

    private static FileStoreTable commitFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean committed,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            RuntimeException commitFailure,
            AtomicBoolean overwriteEnabled,
            List<PartitionEntry> existingPartitions,
            List<String> partitionKeys,
            Map<String, String> options)
    {
        return commitFileStoreTable(
                copiedWithLatestSchema,
                committed,
                copyWithoutTimeTravelOptions,
                commitFailure,
                overwriteEnabled,
                new AtomicReference<>(),
                existingPartitions,
                partitionKeys,
                options);
    }

    private static FileStoreTable commitFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean committed,
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions,
            RuntimeException commitFailure,
            AtomicBoolean overwriteEnabled,
            AtomicReference<String> operation,
            List<PartitionEntry> existingPartitions,
            List<String> partitionKeys,
            Map<String, String> options)
    {
        AtomicReference<FileStoreTable> latestTableRef = new AtomicReference<>();
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "pt", DataTypes.STRING()));
        Object snapshotReader = Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {SnapshotReader.class},
                (_, method, _) -> switch (method.getName()) {
                    case "partitionEntries" -> existingPartitions;
                    case "toString" -> "testing-snapshot-reader";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        BatchTableCommit commit = (BatchTableCommit) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchTableCommit.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "commit" -> {
                        assertThat(args).hasSize(1);
                        assertThat(args[0]).isInstanceOf(List.class);
                        if (commitFailure != null) {
                            throw commitFailure;
                        }
                        committed.set(true);
                        yield null;
                    }
                    case "withOperation" -> {
                        assertThat(args).hasSize(1);
                        operation.set(String.valueOf(args[0]));
                        yield proxy;
                    }
                    case "close", "abort", "withMetricRegistry" -> proxy;
                    case "toString" -> "testing-batch-table-commit";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        BatchWriteBuilder batchWriteBuilder = (BatchWriteBuilder) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchWriteBuilder.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "newCommit" -> commit;
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
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "newBatchWriteBuilder" -> batchWriteBuilder;
                    case "newSnapshotReader" -> snapshotReader;
                    case "rowType" -> rowType;
                    case "partitionKeys" -> partitionKeys;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            partitionKeys,
                            List.of(),
                            options,
                            ""));
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
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTableRef.get();
                    }
                    case "copy" -> proxy;
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield latestTableRef.get();
                    }
                    case "rowType" -> rowType;
                    case "partitionKeys" -> partitionKeys;
                    case "coreOptions" -> new CoreOptions(new Options(options));
                    case "schema" -> TableSchema.create(1, new Schema(
                            rowType.getFields(),
                            partitionKeys,
                            List.of(),
                            options,
                            ""));
                    case "newSnapshotReader" -> snapshotReader;
                    case "newBatchWriteBuilder" -> throw new AssertionError(
                            "stale FileStoreTable should not create BatchWriteBuilder before latest-schema refresh");
                    case "toString" -> "stale-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable truncateFileStoreTable(AtomicBoolean copiedWithLatestSchema, AtomicBoolean truncated)
    {
        return truncateFileStoreTable(copiedWithLatestSchema, truncated, new AtomicReference<>());
    }

    private static FileStoreTable truncateFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean truncated,
            AtomicReference<List<Map<String, String>>> truncatedPartitions)
    {
        return truncateFileStoreTable(copiedWithLatestSchema, truncated, truncatedPartitions, null);
    }

    private static FileStoreTable truncateFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean truncated,
            AtomicReference<List<Map<String, String>>> truncatedPartitions,
            RuntimeException truncateFailure)
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        return truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                List.of("id"),
                Map.of(CoreOptions.BUCKET.key(), "1"),
                truncateFailure);
    }

    private static FileStoreTable truncateFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean truncated,
            AtomicReference<List<Map<String, String>>> truncatedPartitions,
            RowType rowType,
            List<String> partitionKeys)
    {
        return truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                partitionKeys,
                Map.of(CoreOptions.BUCKET.key(), "1"));
    }

    private static FileStoreTable truncateFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean truncated,
            AtomicReference<List<Map<String, String>>> truncatedPartitions,
            RowType rowType,
            List<String> partitionKeys,
            Map<String, String> options)
    {
        return truncateFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                truncatedPartitions,
                rowType,
                partitionKeys,
                options,
                null);
    }

    private static FileStoreTable truncateFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean truncated,
            AtomicReference<List<Map<String, String>>> truncatedPartitions,
            RowType rowType,
            List<String> partitionKeys,
            Map<String, String> options,
            RuntimeException truncateFailure)
    {
        AtomicReference<FileStoreTable> latestTableRef = new AtomicReference<>();
        BatchTableCommit commit = (BatchTableCommit) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchTableCommit.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "truncateTable" -> {
                        if (truncateFailure != null) {
                            throw truncateFailure;
                        }
                        truncated.set(true);
                        yield null;
                    }
                    case "truncatePartitions" -> {
                        if (truncateFailure != null) {
                            throw truncateFailure;
                        }
                        @SuppressWarnings("unchecked")
                        List<Map<String, String>> partitions = (List<Map<String, String>>) args[0];
                        truncatedPartitions.set(partitions);
                        yield null;
                    }
                    case "close", "abort", "withMetricRegistry" -> proxy;
                    case "toString" -> "testing-truncate-batch-table-commit";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        BatchWriteBuilder batchWriteBuilder = (BatchWriteBuilder) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchWriteBuilder.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "newCommit" -> commit;
                    case "withOverwrite" -> proxy;
                    case "tableName" -> "testing";
                    case "rowType" -> rowType;
                    case "newWriteSelector" -> Optional.empty();
                    case "toString" -> "testing-truncate-batch-write-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        TableSchema schema = TableSchema.create(1, new Schema(
                rowType.getFields(),
                partitionKeys,
                List.of(),
                options,
                ""));
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "newBatchWriteBuilder" -> batchWriteBuilder;
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "rowType" -> rowType;
                    case "partitionKeys" -> partitionKeys;
                    case "coreOptions" -> new CoreOptions(new Options(schema.options()));
                    case "schema" -> schema;
                    case "copyWithLatestSchema", "copy", "copyWithoutTimeTravel" -> proxy;
                    case "toString" -> "latest-truncate-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        latestTableRef.set(latestTable);
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTableRef.get();
                    }
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "newBatchWriteBuilder" -> throw new AssertionError(
                            "stale FileStoreTable should not create BatchWriteBuilder before latest-schema refresh");
                    case "toString" -> "stale-truncate-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable metadataDeleteFallbackFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean truncated,
            List<Split> splits,
            List<String> primaryKeys)
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
        return metadataDeleteFallbackFileStoreTable(
                copiedWithLatestSchema,
                truncated,
                new AtomicReference<>(),
                new AtomicBoolean(),
                splits,
                rowType,
                List.of(),
                primaryKeys);
    }

    private static FileStoreTable metadataDeleteFallbackFileStoreTable(
            AtomicBoolean copiedWithLatestSchema,
            AtomicBoolean truncated,
            AtomicReference<List<Map<String, String>>> truncatedPartitions,
            AtomicBoolean partitionFilterApplied,
            List<Split> splits,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys)
    {
        AtomicReference<FileStoreTable> latestTableRef = new AtomicReference<>();
        BatchTableCommit commit = (BatchTableCommit) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchTableCommit.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "truncateTable" -> {
                        truncated.set(true);
                        yield null;
                    }
                    case "truncatePartitions" -> {
                        @SuppressWarnings("unchecked")
                        List<Map<String, String>> partitions = (List<Map<String, String>>) args[0];
                        truncatedPartitions.set(partitions);
                        yield null;
                    }
                    case "close", "abort", "withMetricRegistry" -> proxy;
                    case "toString" -> "testing-metadata-delete-batch-table-commit";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        BatchWriteBuilder batchWriteBuilder = (BatchWriteBuilder) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchWriteBuilder.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "newCommit" -> commit;
                    case "withOverwrite" -> proxy;
                    case "tableName" -> "testing";
                    case "rowType" -> rowType;
                    case "newWriteSelector" -> Optional.empty();
                    case "toString" -> "testing-metadata-delete-batch-write-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        ReadBuilder readBuilder = readBuilder(splits, rowType, partitionFilterApplied);
        TableSchema schema = TableSchema.create(1, new Schema(
                rowType.getFields(),
                partitionKeys,
                primaryKeys,
                Map.of(CoreOptions.BUCKET.key(), "-1"),
                ""));
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "newBatchWriteBuilder" -> batchWriteBuilder;
                    case "newReadBuilder" -> readBuilder;
                    case "bucketMode" -> BucketMode.BUCKET_UNAWARE;
                    case "rowType" -> rowType;
                    case "partitionKeys" -> partitionKeys;
                    case "primaryKeys" -> primaryKeys;
                    case "coreOptions" -> new CoreOptions(new Options(schema.options()));
                    case "schema" -> schema;
                    case "copyWithLatestSchema", "copy", "copyWithoutTimeTravel" -> proxy;
                    case "toString" -> "latest-metadata-delete-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        latestTableRef.set(latestTable);
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTableRef.get();
                    }
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "newBatchWriteBuilder", "newReadBuilder" -> throw new AssertionError(
                            "stale FileStoreTable should not be used for metadata delete fallback");
                    case "toString" -> "stale-metadata-delete-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder readBuilder(List<Split> splits, RowType rowType)
    {
        return readBuilder(splits, rowType, new AtomicBoolean());
    }

    private static ReadBuilder readBuilder(
            List<Split> splits,
            RowType rowType,
            AtomicBoolean partitionFilterApplied)
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "dropStats" -> proxy;
                    case "withPartitionFilter" -> {
                        partitionFilterApplied.set(true);
                        yield proxy;
                    }
                    case "newScan" -> tableScan(splits);
                    case "readType" -> rowType;
                    case "tableName" -> "testing-table";
                    case "toString" -> "testing-read-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static TableScan tableScan(List<Split> splits)
    {
        List<Split> copiedSplits = List.copyOf(splits);
        return (TableScan) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {TableScan.class},
                (_, method, _) -> switch (method.getName()) {
                    case "plan" -> (TableScan.Plan) () -> copiedSplits;
                    case "toString" -> "testing-table-scan";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Split testingSplit(long rowCount, OptionalLong mergedRowCount)
    {
        return new Split()
        {
            @Override
            public long rowCount()
            {
                return rowCount;
            }

            @Override
            public OptionalLong mergedRowCount()
            {
                return mergedRowCount;
            }
        };
    }

    private static FileStoreTable truncateFailingFileStoreTable(AtomicBoolean copiedWithLatestSchema, IOException failure)
    {
        AtomicReference<FileStoreTable> latestTableRef = new AtomicReference<>();
        BatchTableCommit commit = (BatchTableCommit) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchTableCommit.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "truncateTable" -> throw new RuntimeException(failure);
                    case "close", "abort", "withMetricRegistry" -> proxy;
                    case "toString" -> "testing-failing-truncate-batch-table-commit";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        BatchWriteBuilder batchWriteBuilder = (BatchWriteBuilder) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {BatchWriteBuilder.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "newCommit" -> commit;
                    case "withOverwrite" -> proxy;
                    case "tableName" -> "testing";
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT()));
                    case "newWriteSelector" -> Optional.empty();
                    case "toString" -> "testing-failing-truncate-batch-write-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "newBatchWriteBuilder" -> batchWriteBuilder;
                    case "bucketMode" -> BucketMode.HASH_FIXED;
                    case "copyWithLatestSchema", "copy", "copyWithoutTimeTravel" -> proxy;
                    case "toString" -> "latest-failing-truncate-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        latestTableRef.set(latestTable);
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTableRef.get();
                    }
                    case "copy", "copyWithoutTimeTravel" -> proxy;
                    case "newBatchWriteBuilder" -> throw new AssertionError(
                            "stale FileStoreTable should not create BatchWriteBuilder before latest-schema refresh");
                    case "toString" -> "stale-failing-truncate-testing-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static TableSchema partitioningSchema(ConnectorPartitioningHandle partitioningHandle)
    {
        assertThat(partitioningHandle).isInstanceOf(PaimonPartitioningHandle.class);
        return ((PaimonPartitioningHandle) partitioningHandle).getOriginalSchema();
    }

    private static FileStoreTable createdVectorAndBlobTable(
            AtomicReference<Map<String, String>> copyWithoutTimeTravelOptions)
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "embedding", DataTypes.VECTOR(3, DataTypes.FLOAT())),
                DataTypes.FIELD(1, "picture", DataTypes.BLOB()));
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "copyWithoutTimeTravel" -> {
                        copyWithoutTimeTravelOptions.set(Map.copyOf((Map<String, String>) args[0]));
                        yield proxy;
                    }
                    case "rowType" -> rowType;
                    case "toString" -> "created-vector-and-blob-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table createdLowerCaseTable()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "value", DataTypes.INT()));
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "rowType" -> rowType;
                    case "toString" -> "created-lower-case-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table createdDuplicateCaseTable()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "id", DataTypes.INT()),
                DataTypes.FIELD(1, "ID", DataTypes.INT()));
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "rowType" -> rowType;
                    case "toString" -> "created-duplicate-case-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table createdExternalStorageBlobTable()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "picture", DataTypes.BLOB()));
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "rowType" -> rowType;
                    case "toString" -> "created-external-storage-blob-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Slice commitFragment()
            throws IOException
    {
        return commitFragment(BinaryRow.EMPTY_ROW);
    }

    private static Slice commitFragment(BinaryRow partition)
            throws IOException
    {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        return Slices.wrappedBuffer(serializer.serialize(new CommitMessageImpl(
                partition,
                0,
                null,
                DataIncrement.emptyIncrement(),
                CompactIncrement.emptyIncrement())));
    }

    private static BinaryRow partitionRow(String value)
    {
        return new InternalRowSerializer(DataTypes.ROW(DataTypes.FIELD(0, "pt", DataTypes.STRING())))
                .toBinaryRow(GenericRow.of(BinaryString.fromString(value)));
    }

    private static ConnectorMergeTableHandle mergeTableHandle(ConnectorTableHandle tableHandle)
    {
        return () -> tableHandle;
    }

    private static Table table()
    {
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "toString" -> "testing-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table statisticsTable(RowType rowType, Optional<Statistics> statistics)
    {
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "rowType" -> rowType;
                    case "statistics" -> statistics;
                    case "toString" -> "statistics-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table failingStatisticsTable(RowType rowType)
    {
        return failingStatisticsTable(rowType, new RuntimeException("stats file is unreadable"));
    }

    private static Table failingStatisticsTable(RowType rowType, RuntimeException failure)
    {
        return (Table) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "rowType" -> rowType;
                    case "statistics" -> throw failure;
                    case "toString" -> "failing-statistics-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable statisticsFallbackFileStoreTable(
            RowType rowType,
            List<Split> splits,
            List<String> primaryKeys)
    {
        return statisticsFallbackFileStoreTable(rowType, splits, primaryKeys, Optional.empty());
    }

    private static FileStoreTable statisticsFallbackFileStoreTable(
            RowType rowType,
            List<Split> splits,
            List<String> primaryKeys,
            Optional<Statistics> statistics)
    {
        ReadBuilder readBuilder = readBuilder(splits, rowType);
        return (FileStoreTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy", "copyWithLatestSchema" -> proxy;
                    case "rowType" -> rowType;
                    case "statistics" -> statistics;
                    case "newReadBuilder" -> readBuilder;
                    case "primaryKeys" -> primaryKeys;
                    case "coreOptions" -> new CoreOptions(new Options());
                    case "toString" -> "statistics-fallback-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static InnerTable innerTable()
    {
        return (InnerTable) Proxy.newProxyInstance(
                PaimonMetadataTableModeTest.class.getClassLoader(),
                new Class<?>[] {InnerTable.class},
                (_, method, _) -> switch (method.getName()) {
                    case "toString" -> "testing-inner-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static class TestingPaimonCatalog
            extends PaimonCatalog
    {
        private final Table table;
        private final List<Partition> partitionsByNames;
        private final List<Identifier> listedPartitionIdentifiers = new ArrayList<>();
        private final List<List<Map<String, String>>> listedPartitionsByNames = new ArrayList<>();
        private final AtomicInteger getTableCalls = new AtomicInteger();
        private boolean initialized;

        private TestingPaimonCatalog(Table table)
        {
            this(table, List.of());
        }

        private TestingPaimonCatalog(Table table, List<Partition> partitionsByNames)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.table = table;
            this.partitionsByNames = partitionsByNames;
        }

        @Override
        public void initSession(ConnectorSession connectorSession)
        {
            assertThat(connectorSession).isNotNull();
            initialized = true;
        }

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            assertThat(connectorSession).isNotNull();
            initialized = true;
            return this;
        }

        @Override
        public Table getTable(Identifier identifier)
        {
            assertThat(initialized).isTrue();
            assertThat(identifier.getDatabaseName()).isEqualTo("schema");
            assertThat(identifier.getObjectName()).isEqualTo("table");
            getTableCalls.incrementAndGet();
            return table;
        }

        private int getTableCalls()
        {
            return getTableCalls.get();
        }

        @Override
        public List<Partition> listPartitionsByNames(Identifier identifier, List<Map<String, String>> partitions)
        {
            assertThat(initialized).isTrue();
            assertThat(identifier.getDatabaseName()).isEqualTo("schema");
            assertThat(identifier.getTableName()).isEqualTo("table");
            listedPartitionIdentifiers.add(identifier);
            listedPartitionsByNames.add(List.copyOf(partitions));
            return partitionsByNames.stream()
                    .filter(partition -> partitions.contains(partition.spec()))
                    .toList();
        }
    }

    private static class FailingDdlCatalog
            extends PaimonCatalog
    {
        private FailingDdlCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists)
                throws Catalog.DatabaseAlreadyExistException
        {
            assertThat(name).isEqualTo("schema");
            assertThat(ignoreIfExists).isFalse();
            throw new Catalog.DatabaseAlreadyExistException(name);
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
                throws Catalog.DatabaseAlreadyExistException
        {
            assertThat(properties).isEmpty();
            createDatabase(name, ignoreIfExists);
        }

        @Override
        public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
                throws Catalog.DatabaseNotEmptyException
        {
            assertThat(name).isEqualTo("schema");
            assertThat(ignoreIfNotExists).isFalse();
            assertThat(cascade).isFalse();
            throw new Catalog.DatabaseNotEmptyException(name);
        }

        @Override
        public List<String> listTables(String databaseName)
                throws Catalog.DatabaseNotExistException
        {
            assertThat(databaseName).isEqualTo("schema");
            throw new Catalog.DatabaseNotExistException(databaseName);
        }

        @Override
        public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
                throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException
        {
            assertThat(identifier.getObjectName()).isEqualTo("table");
            if (identifier.getDatabaseName().equals("missing_schema")) {
                throw new Catalog.DatabaseNotExistException(identifier.getDatabaseName());
            }
            assertThat(identifier.getDatabaseName()).isEqualTo("schema");
            throw new Catalog.TableAlreadyExistException(identifier);
        }

        @Override
        public void replaceTable(Identifier identifier, Schema newSchema, boolean ignoreIfNotExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            assertThat(ignoreIfNotExists).isFalse();
            throw new UnsupportedOperationException("replace is not supported");
        }

        @Override
        public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
                throws Catalog.TableNotExistException
        {
            assertThat(fromTable.getFullName()).isEqualTo("schema.table");
            assertThat(toTable.getFullName()).isEqualTo("schema.target");
            throw new Catalog.TableNotExistException(fromTable);
        }

        @Override
        public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
                throws Catalog.TableNotExistException
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            throw new Catalog.TableNotExistException(identifier);
        }

        @Override
        public Table getTable(Identifier identifier)
                throws Catalog.TableNotExistException
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            throw new Catalog.TableNotExistException(identifier);
        }

        @Override
        public View getView(Identifier identifier)
                throws Catalog.ViewNotExistException
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            throw new Catalog.ViewNotExistException(identifier);
        }

        @Override
        public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
                throws Catalog.TableNotExistException, Catalog.ColumnAlreadyExistException,
                Catalog.ColumnNotExistException
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            SchemaChange change = changes.get(0);
            if (change instanceof SchemaChange.AddColumn addColumn) {
                if (addColumn.fieldNames()[0].equals("missing_table")) {
                    throw new Catalog.TableNotExistException(identifier);
                }
                throw new Catalog.ColumnAlreadyExistException(identifier, addColumn.fieldNames()[0]);
            }
            if (change instanceof SchemaChange.RenameColumn renameColumn) {
                throw new Catalog.ColumnNotExistException(identifier, renameColumn.fieldNames()[0]);
            }
            if (change instanceof SchemaChange.DropColumn dropColumn) {
                throw new Catalog.ColumnNotExistException(identifier, dropColumn.fieldNames()[0]);
            }
            if (change instanceof SchemaChange.SetOption) {
                throw new Catalog.TableNotExistException(identifier);
            }
            if (change instanceof SchemaChange.UpdateComment) {
                throw new Catalog.TableNotExistException(identifier);
            }
            throw new AssertionError("Unexpected schema change: " + change);
        }
    }

    private static class ExistingTableFailingDdlCatalog
            extends FailingDdlCatalog
    {
        @Override
        public Table getTable(Identifier identifier)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            return unkeyedFileStoreTable(BucketMode.HASH_FIXED);
        }
    }

    private static class CreatedSchemaCatalog
            extends PaimonCatalog
    {
        private final Table table;
        private Schema createdSchema;

        private CreatedSchemaCatalog(Table table)
        {
            super(new Options(), unsupportedFileSystemFactory());
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
        public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            this.createdSchema = schema;
        }

        @Override
        public void replaceTable(Identifier identifier, Schema newSchema, boolean ignoreIfNotExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            this.createdSchema = newSchema;
        }

        @Override
        public Table getTable(Identifier identifier)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            return table;
        }
    }

    private static class CapturingDdlCatalog
            extends PaimonCatalog
    {
        private final Optional<Table> table;
        private String createdDatabase;
        private Boolean createDatabaseIgnoreIfExists;
        private Map<String, String> createdDatabaseProperties;
        private String droppedDatabase;
        private Boolean dropDatabaseIgnoreIfNotExists;
        private Boolean dropDatabaseCascade;
        private String alteredDatabase;
        private Boolean alterDatabaseIgnoreIfNotExists;
        private List<PropertyChange> lastDatabasePropertyChanges = List.of();
        private Schema createdSchema;
        private boolean initialized;
        private int alterCalls;
        private List<SchemaChange> lastAlterChanges = List.of();
        private Identifier renamedFromTable;
        private Identifier renamedToTable;
        private boolean renamedIgnoreIfNotExists;
        private Identifier droppedTable;
        private boolean droppedTableIgnoreIfNotExists;

        private CapturingDdlCatalog()
        {
            this(unkeyedFileStoreTable(BucketMode.HASH_FIXED));
        }

        private CapturingDdlCatalog(Table table)
        {
            this(Optional.of(table));
        }

        private CapturingDdlCatalog(Optional<Table> table)
        {
            super(new Options(), unsupportedFileSystemFactory());
            this.table = requireNonNull(table, "table is null");
        }

        @Override
        public void initSession(ConnectorSession connectorSession)
        {
            initialized = true;
        }

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            initialized = true;
            return this;
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists)
        {
            this.createdDatabase = name;
            this.createDatabaseIgnoreIfExists = ignoreIfExists;
            this.createdDatabaseProperties = Map.of();
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
        {
            this.createdDatabase = name;
            this.createDatabaseIgnoreIfExists = ignoreIfExists;
            this.createdDatabaseProperties = Map.copyOf(properties);
        }

        @Override
        public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
        {
            this.droppedDatabase = name;
            this.dropDatabaseIgnoreIfNotExists = ignoreIfNotExists;
            this.dropDatabaseCascade = cascade;
        }

        @Override
        public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
        {
            this.alteredDatabase = name;
            this.alterDatabaseIgnoreIfNotExists = ignoreIfNotExists;
            this.lastDatabasePropertyChanges = List.copyOf(changes);
        }

        @Override
        public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            this.createdSchema = schema;
        }

        @Override
        public Table getTable(Identifier identifier)
                throws TableNotExistException
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            return table.orElseThrow(() -> new TableNotExistException(identifier));
        }

        @Override
        public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            assertThat(ignoreIfNotExists).isFalse();
            alterCalls++;
            lastAlterChanges = List.copyOf(changes);
        }

        @Override
        public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
        {
            renamedFromTable = fromTable;
            renamedToTable = toTable;
            renamedIgnoreIfNotExists = ignoreIfNotExists;
        }

        @Override
        public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
        {
            droppedTable = identifier;
            droppedTableIgnoreIfNotExists = ignoreIfNotExists;
        }
    }

    private static class RuntimeFailingAlterCatalog
            extends PaimonCatalog
    {
        private final RuntimeException failure;

        private RuntimeFailingAlterCatalog(RuntimeException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
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
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            return schemaOptionsFileStoreTable(Map.of(CoreOptions.BUCKET.key(), "4"));
        }

        @Override
        public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            throw failure;
        }
    }

    private static class CheckedFailingAlterCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingAlterCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
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
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            return schemaOptionsFileStoreTable(Map.of(CoreOptions.BUCKET.key(), "4"));
        }

        @Override
        public void alterTable(Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            throw new RuntimeException(failure);
        }
    }

    private static class CheckedFailingSchemaCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingSchemaCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
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
        public void createDatabase(String name, boolean ignoreIfExists)
        {
            assertThat(name).isEqualTo("schema");
            throw new RuntimeException(failure);
        }

        @Override
        public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
        {
            assertThat(properties).isEmpty();
            createDatabase(name, ignoreIfExists);
        }
    }

    private static class CheckedFailingDropSchemaCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingDropSchemaCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
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
        public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
        {
            assertThat(name).isEqualTo("schema");
            assertThat(ignoreIfNotExists).isFalse();
            assertThat(cascade).isFalse();
            throw new RuntimeException(failure);
        }
    }

    private static class CheckedFailingSchemaAuthorizationCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingSchemaAuthorizationCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
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
        public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
        {
            assertThat(name).isEqualTo("schema");
            assertThat(ignoreIfNotExists).isFalse();
            assertThat(changes).hasSize(2);
            assertThat(changes).allMatch(PropertyChange.SetProperty.class::isInstance);
            throw new RuntimeException(failure);
        }
    }

    private static class CheckedFailingCreateTableCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingCreateTableCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
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
        public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            assertThat(ignoreIfExists).isFalse();
            throw new RuntimeException(failure);
        }
    }

    private static class CheckedFailingRenameTableCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingRenameTableCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
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
        public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
        {
            assertThat(fromTable.getFullName()).isEqualTo("schema.table");
            assertThat(toTable.getFullName()).isEqualTo("schema.target");
            assertThat(ignoreIfNotExists).isFalse();
            throw new RuntimeException(failure);
        }
    }

    private static class RuntimeWrappedRenameFailureCatalog
            extends PaimonCatalog
    {
        private final Exception failure;

        private RuntimeWrappedRenameFailureCatalog(Exception failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
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
        public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
        {
            assertThat(fromTable.getFullName()).isEqualTo("schema.table");
            assertThat(toTable.getFullName()).isEqualTo("target_schema.target");
            assertThat(ignoreIfNotExists).isFalse();
            throw new RuntimeException(failure);
        }
    }

    private static class CheckedFailingDropTableCatalog
            extends PaimonCatalog
    {
        private final IOException failure;

        private CheckedFailingDropTableCatalog(IOException failure)
        {
            super(new Options(), unsupportedFileSystemFactory());
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
        public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
        {
            assertThat(identifier.getFullName()).isEqualTo("schema.table");
            assertThat(ignoreIfNotExists).isFalse();
            throw new RuntimeException(failure);
        }
    }

    private static class SchemaQueryCatalog
            extends PaimonCatalog
    {
        private SchemaQueryCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public Database getDatabase(String name)
                throws Catalog.DatabaseNotExistException
        {
            if (name.equals(SYSTEM_DATABASE_NAME)) {
                return Database.of(name);
            }
            if (name.equals("existing_schema")) {
                return Database.of(name);
            }
            throw new Catalog.DatabaseNotExistException(name);
        }

        @Override
        public List<String> listDatabases()
        {
            return List.of("alpha", "beta");
        }

        @Override
        public List<String> listTables(String databaseName)
                throws Catalog.DatabaseNotExistException
        {
            return switch (databaseName) {
                case "alpha" -> List.of("t1", "t2");
                case "beta" -> List.of("t3");
                case SYSTEM_DATABASE_NAME -> SystemTableLoader.loadGlobalTableNames();
                default -> List.of();
            };
        }

        @Override
        public List<String> listViews(String databaseName)
        {
            return switch (databaseName) {
                case "alpha" -> List.of("v1");
                case "beta" -> List.of("v2");
                default -> List.of();
            };
        }

        @Override
        public View getView(Identifier identifier)
        {
            return paimonView(identifier, List.of(DataTypes.FIELD(0, "id", DataTypes.BIGINT())));
        }
    }

    private static class SystemSchemaRejectingCatalog
            extends PaimonCatalog
    {
        private SystemSchemaRejectingCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public List<String> listDatabases()
        {
            return List.of("alpha");
        }

        @Override
        public List<String> listTables(String databaseName)
        {
            if (databaseName.equals(SYSTEM_DATABASE_NAME)) {
                throw new AssertionError("system schema tables must be provided by the connector");
            }
            assertThat(databaseName).isEqualTo("alpha");
            return List.of("t1");
        }

        @Override
        public List<String> listViews(String databaseName)
        {
            if (databaseName.equals(SYSTEM_DATABASE_NAME)) {
                throw new AssertionError("system schema views must not be queried while listing tables");
            }
            assertThat(databaseName).isEqualTo("alpha");
            return List.of();
        }
    }

    private static class SchemaPropertiesCatalog
            extends PaimonCatalog
    {
        private SchemaPropertiesCatalog()
        {
            super(new Options(), unsupportedFileSystemFactory());
        }

        @Override
        public void initSession(ConnectorSession connectorSession) {}

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            return this;
        }

        @Override
        public Database getDatabase(String name)
                throws Catalog.DatabaseNotExistException
        {
            return switch (name) {
                case "schema" -> Database.of(name, Map.of(
                        LOCATION_PROPERTY, "s3://warehouse/schema",
                        COMMENT_PROPERTY, "schema comment",
                        OWNER_PROPERTY, "schema_owner",
                        "unregistered-paimon-property", "hidden"), "schema comment");
                case "schema_with_role_owner" -> Database.of(name, Map.of(
                        OWNER_PROPERTY, "schema_role",
                        TRINO_SCHEMA_OWNER_TYPE_PROPERTY, PrincipalType.ROLE.name()), null);
                case "schema_with_invalid_owner_type" -> Database.of(name, Map.of(
                        OWNER_PROPERTY, "schema_owner",
                        TRINO_SCHEMA_OWNER_TYPE_PROPERTY, "GROUP"), null);
                case "schema_without_owner" -> Database.of(name, Map.of(
                        LOCATION_PROPERTY, "s3://warehouse/schema_without_owner"), null);
                case "schema_with_blank_owner" -> Database.of(name, Map.of(
                        OWNER_PROPERTY, " "), null);
                default -> throw new Catalog.DatabaseNotExistException(name);
            };
        }
    }

    private static TrinoFileSystemFactory unsupportedFileSystemFactory()
    {
        return _ -> {
            throw new UnsupportedOperationException("filesystem is not used by this test");
        };
    }
}
