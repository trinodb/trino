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

import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveApplyProjectionUtil;
import io.trino.plugin.hive.HiveApplyProjectionUtil.ProjectedColumnRepresentation;
import io.trino.plugin.hive.HiveWrittenPartitions;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.procedure.IcebergExpireSnapshotsHandle;
import io.trino.plugin.iceberg.procedure.IcebergOptimizeHandle;
import io.trino.plugin.iceberg.procedure.IcebergRemoveOrphanFilesHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableProcedureId;
import io.trino.plugin.iceberg.util.DataFileWithDeleteFiles;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.MaterializedViewNotFoundException;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.union;
import static com.google.common.collect.Streams.concat;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.HiveApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.hive.HiveApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.hive.util.HiveUtil.isStructuralType;
import static io.trino.plugin.iceberg.ConstraintExtractor.extractTupleDomain;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_UPDATE_ROW_ID_COLUMN_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_UPDATE_ROW_ID_COLUMN_NAME;
import static io.trino.plugin.iceberg.IcebergColumnHandle.pathColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.pathColumnMetadata;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.FILE_PATH;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getExpireSnapshotMinRetention;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getRemoveOrphanFilesMinRetention;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isAllowLegacySnapshotSyntax;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isStatisticsEnabled;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergUtil.canEnforceColumnConstraintInAllSpecs;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.getColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableProperties;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.getSnapshotIdAsOfTime;
import static io.trino.plugin.iceberg.IcebergUtil.getTableComment;
import static io.trino.plugin.iceberg.IcebergUtil.newCreateTableTransaction;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromMetadata;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.PartitionFields.toPartitionFields;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog.DEPENDS_ON_TABLES;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.EXPIRE_SNAPSHOTS;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.OPTIMIZE;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.REMOVE_ORPHAN_FILES;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.UuidType.UUID;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.apache.iceberg.ReachableFileUtil.metadataFileLocations;
import static org.apache.iceberg.ReachableFileUtil.versionHintLocation;
import static org.apache.iceberg.SnapshotSummary.DELETED_RECORDS_PROP;
import static org.apache.iceberg.SnapshotSummary.REMOVED_EQ_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.REMOVED_POS_DELETES_PROP;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL_DEFAULT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.WRITE_LOCATION_PROVIDER_IMPL;

public class IcebergMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(IcebergMetadata.class);
    private static final Pattern PATH_PATTERN = Pattern.compile("(.*)/[^/]+");
    private static final int OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION = 2;
    private static final int CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION = 2;
    private static final String RETENTION_THRESHOLD = "retention_threshold";
    public static final Set<String> UPDATABLE_TABLE_PROPERTIES = ImmutableSet.of(FILE_FORMAT_PROPERTY, FORMAT_VERSION_PROPERTY, PARTITIONING_PROPERTY);

    public static final String ORC_BLOOM_FILTER_COLUMNS_KEY = "orc.bloom.filter.columns";
    public static final String ORC_BLOOM_FILTER_FPP_KEY = "orc.bloom.filter.fpp";

    private final TypeManager typeManager;
    private final TypeOperators typeOperators;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final TrinoCatalog catalog;
    private final HdfsEnvironment hdfsEnvironment;

    private final Map<String, Long> snapshotIds = new ConcurrentHashMap<>();

    private Transaction transaction;

    public IcebergMetadata(
            TypeManager typeManager,
            TypeOperators typeOperators,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoCatalog catalog,
            HdfsEnvironment hdfsEnvironment)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return catalog.listNamespaces(session);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return catalog.loadNamespaceMetadata(session, schemaName.getSchemaName());
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, CatalogSchemaName schemaName)
    {
        return catalog.getNamespacePrincipal(session, schemaName.getSchemaName());
    }

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        throw new UnsupportedOperationException("This method is not supported because getTableHandle with versions is implemented instead");
    }

    @Override
    public IcebergTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read table with start version is not supported");
        }

        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        if (name.getTableType() != DATA) {
            // Pretend the table does not exist to produce better error message in case of table redirects to Hive
            return null;
        }

        BaseTable table;
        try {
            table = (BaseTable) catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), name.getTableName()));
        }
        catch (TableNotFoundException e) {
            return null;
        }

        if (name.getSnapshotId().isPresent() && endVersion.isPresent()) {
            throw new TrinoException(GENERIC_USER_ERROR, "Cannot specify end version both in table name and FOR clause");
        }

        Optional<Long> snapshotId = endVersion.map(version -> getSnapshotIdFromVersion(table, version))
                .or(() -> getSnapshotId(table, name.getSnapshotId(), isAllowLegacySnapshotSyntax(session)));

        Map<String, String> tableProperties = table.properties();
        String nameMappingJson = tableProperties.get(TableProperties.DEFAULT_NAME_MAPPING);
        return new IcebergTableHandle(
                tableName.getSchemaName(),
                name.getTableName(),
                name.getTableType(),
                snapshotId,
                SchemaParser.toJson(table.schema()),
                PartitionSpecParser.toJson(table.spec()),
                table.operations().current().formatVersion(),
                TupleDomain.all(),
                TupleDomain.all(),
                ImmutableSet.of(),
                Optional.ofNullable(nameMappingJson),
                table.location(),
                table.properties(),
                NO_RETRIES,
                ImmutableList.of(),
                false,
                Optional.empty());
    }

    private static long getSnapshotIdFromVersion(Table table, ConnectorTableVersion version)
    {
        io.trino.spi.type.Type versionType = version.getVersionType();
        switch (version.getPointerType()) {
            case TEMPORAL:
                long epochMillis;
                if (versionType instanceof TimestampWithTimeZoneType) {
                    epochMillis = ((TimestampWithTimeZoneType) versionType).isShort()
                            ? unpackMillisUtc((long) version.getVersion())
                            : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
                }
                else {
                    throw new TrinoException(NOT_SUPPORTED, "Unsupported type for temporal table version: " + versionType.getDisplayName());
                }
                return getSnapshotIdAsOfTime(table, epochMillis);

            case TARGET_ID:
                if (versionType != BIGINT) {
                    throw new TrinoException(NOT_SUPPORTED, "Unsupported type for table version: " + versionType.getDisplayName());
                }
                long snapshotId = (long) version.getVersion();
                if (table.snapshot(snapshotId) == null) {
                    throw new TrinoException(INVALID_ARGUMENTS, "Iceberg snapshot ID does not exists: " + snapshotId);
                }
                return snapshotId;
        }
        throw new TrinoException(NOT_SUPPORTED, "Version pointer type is not supported: " + version.getPointerType());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(session, tableName)
                .map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    private Optional<SystemTable> getRawSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        if (name.getTableType() == DATA) {
            return Optional.empty();
        }

        // load the base table for the system table
        Table table;
        try {
            table = catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), name.getTableName()));
        }
        catch (TableNotFoundException e) {
            return Optional.empty();
        }
        catch (UnknownTableTypeException e) {
            // avoid dealing with non Iceberg tables
            return Optional.empty();
        }

        SchemaTableName systemTableName = new SchemaTableName(tableName.getSchemaName(), name.getTableNameWithType());
        switch (name.getTableType()) {
            case DATA:
                // Handled above.
                break;
            case HISTORY:
                if (name.getSnapshotId().isPresent()) {
                    throw new TrinoException(NOT_SUPPORTED, "Snapshot ID not supported for history table: " + systemTableName);
                }
                return Optional.of(new HistoryTable(systemTableName, table));
            case SNAPSHOTS:
                if (name.getSnapshotId().isPresent()) {
                    throw new TrinoException(NOT_SUPPORTED, "Snapshot ID not supported for snapshots table: " + systemTableName);
                }
                return Optional.of(new SnapshotsTable(systemTableName, typeManager, table));
            case PARTITIONS:
                return Optional.of(new PartitionTable(systemTableName, typeManager, table, getSnapshotId(table, name.getSnapshotId(), isAllowLegacySnapshotSyntax(session))));
            case MANIFESTS:
                return Optional.of(new ManifestsTable(systemTableName, table, getSnapshotId(table, name.getSnapshotId(), isAllowLegacySnapshotSyntax(session))));
            case FILES:
                return Optional.of(new FilesTable(systemTableName, typeManager, table, getSnapshotId(table, name.getSnapshotId(), isAllowLegacySnapshotSyntax(session))));
            case PROPERTIES:
                return Optional.of(new PropertiesTable(systemTableName, table));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;

        if (table.getSnapshotId().isEmpty()) {
            // A table with missing snapshot id produces no splits, so we optimize here by returning
            // TupleDomain.none() as the predicate
            return new ConnectorTableProperties(TupleDomain.none(), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of());
        }

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        // Extract identity partition fields that are present in all partition specs, for creating the discrete predicates.
        Set<Integer> partitionSourceIds = identityPartitionColumnsInAllSpecs(icebergTable);

        TupleDomain<IcebergColumnHandle> enforcedPredicate = table.getEnforcedPredicate();

        DiscretePredicates discretePredicates = null;
        if (!partitionSourceIds.isEmpty()) {
            // Extract identity partition columns
            Map<Integer, IcebergColumnHandle> columns = getColumns(icebergTable.schema(), typeManager).stream()
                    .filter(column -> partitionSourceIds.contains(column.getId()))
                    .collect(toImmutableMap(IcebergColumnHandle::getId, identity()));

            Supplier<List<FileScanTask>> lazyFiles = Suppliers.memoize(() -> {
                TableScan tableScan = icebergTable.newScan()
                        .useSnapshot(table.getSnapshotId().get())
                        .filter(toIcebergExpression(enforcedPredicate))
                        .includeColumnStats();

                try (CloseableIterable<FileScanTask> iterator = tableScan.planFiles()) {
                    return ImmutableList.copyOf(iterator);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            Iterable<FileScanTask> files = () -> lazyFiles.get().iterator();

            Iterable<TupleDomain<ColumnHandle>> discreteTupleDomain = Iterables.transform(files, fileScan -> {
                // Extract partition values in the data file
                Map<Integer, Optional<String>> partitionColumnValueStrings = getPartitionKeys(fileScan);
                Map<ColumnHandle, NullableValue> partitionValues = partitionSourceIds.stream()
                        .filter(partitionColumnValueStrings::containsKey)
                        .collect(toImmutableMap(
                                columns::get,
                                columnId -> {
                                    IcebergColumnHandle column = columns.get(columnId);
                                    Object prestoValue = deserializePartitionValue(
                                            column.getType(),
                                            partitionColumnValueStrings.get(columnId).orElse(null),
                                            column.getName());

                                    return NullableValue.of(column.getType(), prestoValue);
                                }));

                return TupleDomain.fromFixedValues(partitionValues);
            });

            discretePredicates = new DiscretePredicates(
                    columns.values().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toImmutableList()),
                    discreteTupleDomain);
        }

        return new ConnectorTableProperties(
                // Using the predicate here directly avoids eagerly loading all partition values. Logically, this
                // still keeps predicate and discretePredicates evaluation the same on every row of the table. This
                // can be further optimized by intersecting with partition values at the cost of iterating
                // over all tableScan.planFiles() and caching partition values in table handle.
                enforcedPredicate.transformKeys(ColumnHandle.class::cast),
                // TODO: implement table partitioning
                Optional.empty(),
                Optional.empty(),
                Optional.ofNullable(discretePredicates),
                ImmutableList.of());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(session, ((IcebergTableHandle) table).getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (IcebergColumnHandle columnHandle : getColumns(icebergTable.schema(), typeManager)) {
            columnHandles.put(columnHandle.getName(), columnHandle);
        }
        columnHandles.put(FILE_PATH.getColumnName(), pathColumnHandle());
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .setComment(column.getComment())
                .build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> schemaTableNames;
        if (prefix.getTable().isEmpty()) {
            schemaTableNames = catalog.listTables(session, prefix.getSchema());
        }
        else {
            schemaTableNames = ImmutableList.of(prefix.toSchemaTableName());
        }
        return schemaTableNames.stream()
                .flatMap(tableName -> {
                    try {
                        if (redirectTable(session, tableName).isPresent()) {
                            return Stream.of(TableColumnsMetadata.forRedirectedTable(tableName));
                        }
                        return Stream.of(TableColumnsMetadata.forTable(tableName, getTableMetadata(session, tableName).getColumns()));
                    }
                    catch (TableNotFoundException e) {
                        // Table disappeared during listing operation
                        return Stream.empty();
                    }
                    catch (UnknownTableTypeException e) {
                        // Skip unsupported table type in case that the table redirects are not enabled
                        return Stream.empty();
                    }
                    catch (RuntimeException e) {
                        // Table can be being removed and this may cause all sorts of exceptions. Log, because we're catching broadly.
                        log.warn(e, "Failed to access metadata of table %s during streaming table columns for %s", tableName, prefix);
                        return Stream.empty();
                    }
                })
                .iterator();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        catalog.createNamespace(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        catalog.dropNamespace(session, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        catalog.renameNamespace(session, source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String schemaName, TrinoPrincipal principal)
    {
        catalog.setNamespacePrincipal(session, schemaName, principal);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Optional<ConnectorTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout, NO_RETRIES), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        catalog.updateTableComment(session, ((IcebergTableHandle) tableHandle).getSchemaTableName(), comment);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        catalog.updateViewComment(session, viewName, comment);
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Schema schema = schemaFromMetadata(tableMetadata.getColumns());
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));
        return getWriteLayout(schema, partitionSpec, false);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        verify(transaction == null, "transaction already set");
        transaction = newCreateTableTransaction(catalog, tableMetadata, session);
        String location = transaction.table().location();
        HdfsContext hdfsContext = new HdfsContext(session);
        try {
            Path path = new Path(location);
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, path);
            if (fileSystem.exists(path) && fileSystem.listFiles(path, true).hasNext()) {
                throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, format("" +
                        "Cannot create a table on a non-empty location: %s, set 'iceberg.unique-table-location=true' in your Iceberg catalog properties " +
                        "to use unique table locations for every table.", location));
            }
            return new IcebergWritableTableHandle(
                    tableMetadata.getTable(),
                    SchemaParser.toJson(transaction.table().schema()),
                    PartitionSpecParser.toJson(transaction.table().spec()),
                    getColumns(transaction.table().schema(), typeManager),
                    location,
                    getFileFormat(transaction.table()),
                    transaction.table().properties(),
                    retryMode);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed checking new table's location: " + location, e);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        if (fragments.isEmpty()) {
            // Commit the transaction if the table is being created without data
            transaction.newFastAppend().commit();
            transaction.commitTransaction();
            transaction = null;
            return Optional.empty();
        }

        return finishInsert(session, (IcebergWritableTableHandle) tableHandle, fragments, computedStatistics);
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        return getWriteLayout(icebergTable.schema(), icebergTable.spec(), false);
    }

    private Optional<ConnectorTableLayout> getWriteLayout(Schema tableSchema, PartitionSpec partitionSpec, boolean forceRepartitioning)
    {
        if (partitionSpec.isUnpartitioned()) {
            return Optional.empty();
        }

        Map<Integer, IcebergColumnHandle> columnById = getColumns(tableSchema, typeManager).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getId, identity()));

        List<IcebergColumnHandle> partitioningColumns = partitionSpec.fields().stream()
                .sorted(Comparator.comparing(PartitionField::sourceId))
                .map(field -> requireNonNull(columnById.get(field.sourceId()), () -> "Cannot find source column for partitioning field " + field))
                .distinct()
                .collect(toImmutableList());
        List<String> partitioningColumnNames = partitioningColumns.stream()
                .map(IcebergColumnHandle::getName)
                .collect(toImmutableList());

        if (!forceRepartitioning && partitionSpec.fields().stream().allMatch(field -> field.transform().isIdentity())) {
            // Do not set partitioningHandle, to let engine determine whether to repartition data or not, on stat-based basis.
            return Optional.of(new ConnectorTableLayout(partitioningColumnNames));
        }
        IcebergPartitioningHandle partitioningHandle = new IcebergPartitioningHandle(toPartitionFields(partitionSpec), partitioningColumns);
        return Optional.of(new ConnectorTableLayout(partitioningHandle, partitioningColumnNames));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        validateNotModifyingOldSnapshot(table, icebergTable);

        beginTransaction(icebergTable);

        return new IcebergWritableTableHandle(
                table.getSchemaTableName(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                getColumns(icebergTable.schema(), typeManager),
                icebergTable.location(),
                getFileFormat(icebergTable),
                icebergTable.properties(),
                retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        if (commitTasks.isEmpty()) {
            transaction = null;
            return Optional.empty();
        }

        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;
        Table icebergTable = transaction.table();
        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = transaction.newAppend();
        ImmutableSet.Builder<String> writtenFiles = ImmutableSet.builder();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.getPath())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withFormat(table.getFileFormat().toIceberg())
                    .withMetrics(task.getMetrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
            writtenFiles.add(task.getPath());
        }

        // try to leave as little garbage as possible behind
        if (table.getRetryMode() != NO_RETRIES) {
            cleanExtraOutputFiles(session, writtenFiles.build());
        }

        appendFiles.commit();
        transaction.commitTransaction();
        transaction = null;

        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::getPath)
                .collect(toImmutableList())));
    }

    private void cleanExtraOutputFiles(HdfsContext hdfsContext, String queryId, String location, Set<String> filesToKeep)
    {
        Deque<String> filesToDelete = new ArrayDeque<>();
        try {
            log.debug("Deleting failed attempt files from %s for query %s", location, queryId);
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, new Path(location));
            if (!fileSystem.exists(new Path(location))) {
                // directory may not exist if no files were actually written
                return;
            }

            // files within given partition are written flat into location; we need to list recursively
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(location), false);
            while (iterator.hasNext()) {
                Path file = iterator.next().getPath();
                if (isFileCreatedByQuery(file.getName(), queryId) && !filesToKeep.contains(location + "/" + file.getName())) {
                    filesToDelete.add(file.getName());
                }
            }

            if (filesToDelete.isEmpty()) {
                return;
            }

            log.info("Found %s files to delete and %s to retain in location %s for query %s", filesToDelete.size(), filesToKeep.size(), location, queryId);
            ImmutableList.Builder<String> deletedFilesBuilder = ImmutableList.builder();
            Iterator<String> filesToDeleteIterator = filesToDelete.iterator();
            while (filesToDeleteIterator.hasNext()) {
                String fileName = filesToDeleteIterator.next();
                log.debug("Deleting failed attempt file %s/%s for query %s", location, fileName, queryId);
                fileSystem.delete(new Path(location, fileName), false);
                deletedFilesBuilder.add(fileName);
                filesToDeleteIterator.remove();
            }

            List<String> deletedFiles = deletedFilesBuilder.build();
            if (!deletedFiles.isEmpty()) {
                log.info("Deleted failed attempt files %s from %s for query %s", deletedFiles, location, queryId);
            }
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Could not clean up extraneous output files; remaining files: %s", filesToDelete), e);
        }
    }

    private static boolean isFileCreatedByQuery(String fileName, String queryId)
    {
        verify(!queryId.contains("-"), "queryId(%s) should not contain hyphens", queryId);
        return fileName.startsWith(queryId + "-");
    }

    private static Set<String> getOutputFilesLocations(Set<String> writtenFiles)
    {
        return writtenFiles.stream()
                .map(IcebergMetadata::getLocation)
                .collect(toImmutableSet());
    }

    private static String getLocation(String path)
    {
        Matcher matcher = PATH_PATTERN.matcher(path);
        verify(matcher.matches(), "path %s does not match pattern", path);
        return matcher.group(1);
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            String procedureName,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) connectorTableHandle;

        IcebergTableProcedureId procedureId;
        try {
            procedureId = IcebergTableProcedureId.valueOf(procedureName);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown procedure '" + procedureName + "'");
        }

        switch (procedureId) {
            case OPTIMIZE:
                return getTableHandleForOptimize(session, tableHandle, executeProperties, retryMode);
            case EXPIRE_SNAPSHOTS:
                return getTableHandleForExpireSnapshots(session, tableHandle, executeProperties);
            case REMOVE_ORPHAN_FILES:
                return getTableHandleForRemoveOrphanFiles(session, tableHandle, executeProperties);
        }

        throw new IllegalArgumentException("Unknown procedure: " + procedureId);
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForOptimize(ConnectorSession session, IcebergTableHandle tableHandle, Map<String, Object> executeProperties, RetryMode retryMode)
    {
        DataSize maxScannedFileSize = (DataSize) executeProperties.get("file_size_threshold");
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                OPTIMIZE,
                new IcebergOptimizeHandle(
                        tableHandle.getSnapshotId().orElseThrow(),
                        SchemaParser.toJson(icebergTable.schema()),
                        PartitionSpecParser.toJson(icebergTable.spec()),
                        getColumns(icebergTable.schema(), typeManager),
                        getFileFormat(icebergTable),
                        icebergTable.properties(),
                        maxScannedFileSize,
                        retryMode != NO_RETRIES,
                        tableHandle.getEnforcedPredicate().isAll()),
                icebergTable.location()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForExpireSnapshots(ConnectorSession session, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        Duration retentionThreshold = (Duration) executeProperties.get(RETENTION_THRESHOLD);
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                EXPIRE_SNAPSHOTS,
                new IcebergExpireSnapshotsHandle(retentionThreshold),
                icebergTable.location()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForRemoveOrphanFiles(ConnectorSession session, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        Duration retentionThreshold = (Duration) executeProperties.get(RETENTION_THRESHOLD);
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                REMOVE_ORPHAN_FILES,
                new IcebergRemoveOrphanFilesHandle(retentionThreshold),
                icebergTable.location()));
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.getProcedureId()) {
            case OPTIMIZE:
                return getLayoutForOptimize(session, executeHandle);
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.getProcedureId() + "'");
    }

    private Optional<ConnectorTableLayout> getLayoutForOptimize(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        Table icebergTable = catalog.loadTable(session, executeHandle.getSchemaTableName());
        // from performance perspective it is better to have lower number of bigger files than other way around
        // thus we force repartitioning for optimize to achieve this
        return getWriteLayout(icebergTable.schema(), icebergTable.spec(), true);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(
            ConnectorSession session,
            ConnectorTableExecuteHandle tableExecuteHandle,
            ConnectorTableHandle updatedSourceTableHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        IcebergTableHandle table = (IcebergTableHandle) updatedSourceTableHandle;
        switch (executeHandle.getProcedureId()) {
            case OPTIMIZE:
                return beginOptimize(session, executeHandle, table);
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.getProcedureId() + "'");
    }

    private BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginOptimize(
            ConnectorSession session,
            IcebergTableExecuteHandle executeHandle,
            IcebergTableHandle table)
    {
        IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.getProcedureHandle();
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        validateNotModifyingOldSnapshot(table, icebergTable);

        int tableFormatVersion = ((BaseTable) icebergTable).operations().current().formatVersion();
        if (tableFormatVersion > OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION) {
            throw new TrinoException(NOT_SUPPORTED, format(
                    "%s is not supported for Iceberg table format version > %d. Table %s format version is %s.",
                    OPTIMIZE.name(),
                    OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION,
                    table.getSchemaTableName(),
                    tableFormatVersion));
        }

        beginTransaction(icebergTable);

        return new BeginTableExecuteResult<>(
                executeHandle,
                table.forOptimize(true, optimizeHandle.getMaxScannedFileSize()));
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.getProcedureId()) {
            case OPTIMIZE:
                finishOptimize(session, executeHandle, fragments, splitSourceInfo);
                return;
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.getProcedureId() + "'");
    }

    private void finishOptimize(ConnectorSession session, IcebergTableExecuteHandle executeHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.getProcedureHandle();
        Table icebergTable = transaction.table();

        // files to be deleted
        ImmutableSet.Builder<DataFile> scannedDataFilesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<DeleteFile> scannedDeleteFilesBuilder = ImmutableSet.builder();
        splitSourceInfo.stream().map(DataFileWithDeleteFiles.class::cast).forEach(dataFileWithDeleteFiles -> {
            scannedDataFilesBuilder.add(dataFileWithDeleteFiles.getDataFile());
            scannedDeleteFilesBuilder.addAll(filterDeleteFilesThatWereNotScannedDuringOptimize(dataFileWithDeleteFiles.getDeleteFiles(), optimizeHandle.isWholeTableScan()));
        });

        Set<DataFile> scannedDataFiles = scannedDataFilesBuilder.build();
        Set<DeleteFile> fullyAppliedDeleteFiles = scannedDeleteFilesBuilder.build();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        Set<DataFile> newFiles = new HashSet<>();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.getPath())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withFormat(optimizeHandle.getFileFormat().toIceberg())
                    .withMetrics(task.getMetrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            newFiles.add(builder.build());
        }

        if (scannedDataFiles.isEmpty() && fullyAppliedDeleteFiles.isEmpty() && newFiles.isEmpty()) {
            // Table scan turned out to be empty, nothing to commit
            transaction = null;
            return;
        }

        // try to leave as little garbage as possible behind
        if (optimizeHandle.isRetriesEnabled()) {
            cleanExtraOutputFiles(
                    session,
                    newFiles.stream()
                            .map(dataFile -> dataFile.path().toString())
                            .collect(toImmutableSet()));
        }

        RewriteFiles rewriteFiles = transaction.newRewrite();
        rewriteFiles.rewriteFiles(scannedDataFiles, fullyAppliedDeleteFiles, newFiles, ImmutableSet.of());
        // Table.snapshot method returns null if there is no matching snapshot
        Snapshot snapshot = requireNonNull(icebergTable.snapshot(optimizeHandle.getSnapshotId()), "snapshot is null");
        rewriteFiles.validateFromSnapshot(snapshot.snapshotId());
        rewriteFiles.commit();
        transaction.commitTransaction();
        transaction = null;
    }

    private static Set<DeleteFile> filterDeleteFilesThatWereNotScannedDuringOptimize(List<DeleteFile> deleteFiles, boolean isWholeTableScan)
    {
        // if whole table was scanned all delete files were read and applied
        // so it is safe to remove them
        if (isWholeTableScan) {
            return ImmutableSet.copyOf(deleteFiles);
        }
        return deleteFiles.stream()
                // equality delete files can be global so we can't clean them up unless we optimize whole table
                // position delete files cannot be global so it is safe to clean them if they were scanned
                .filter(deleteFile -> (deleteFile.content() == POSITION_DELETES) || deleteFile.partition() != null)
                .collect(toImmutableSet());
    }

    @Override
    public void executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.getProcedureId()) {
            case EXPIRE_SNAPSHOTS:
                executeExpireSnapshots(session, executeHandle);
                return;
            case REMOVE_ORPHAN_FILES:
                executeRemoveOrphanFiles(session, executeHandle);
                return;
            default:
                throw new IllegalArgumentException("Unknown procedure '" + executeHandle.getProcedureId() + "'");
        }
    }

    private void executeExpireSnapshots(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergExpireSnapshotsHandle expireSnapshotsHandle = (IcebergExpireSnapshotsHandle) executeHandle.getProcedureHandle();

        Table table = catalog.loadTable(session, executeHandle.getSchemaTableName());
        Duration retention = requireNonNull(expireSnapshotsHandle.getRetentionThreshold(), "retention is null");
        validateTableExecuteParameters(
                table,
                executeHandle.getSchemaTableName(),
                EXPIRE_SNAPSHOTS.name(),
                retention,
                getExpireSnapshotMinRetention(session),
                IcebergConfig.EXPIRE_SNAPSHOTS_MIN_RETENTION,
                IcebergSessionProperties.EXPIRE_SNAPSHOTS_MIN_RETENTION);

        long expireTimestampMillis = session.getStart().toEpochMilli() - retention.toMillis();
        expireSnapshots(table, expireTimestampMillis, session, executeHandle.getSchemaTableName());
    }

    private static void validateTableExecuteParameters(
            Table table,
            SchemaTableName schemaTableName,
            String procedureName,
            Duration retentionThreshold,
            Duration minRetention,
            String minRetentionParameterName,
            String sessionMinRetentionParameterName)
    {
        int tableFormatVersion = ((BaseTable) table).operations().current().formatVersion();
        if (tableFormatVersion > CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION) {
            // It is not known if future version won't bring any new kind of metadata or data files
            // because of the way procedures are implemented it is safer to fail here than to potentially remove
            // files that should stay there
            throw new TrinoException(NOT_SUPPORTED, format("%s is not supported for Iceberg table format version > %d. " +
                            "Table %s format version is %s.",
                    procedureName,
                    CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION,
                    schemaTableName,
                    tableFormatVersion));
        }
        Map<String, String> properties = table.properties();
        if (properties.containsKey(WRITE_LOCATION_PROVIDER_IMPL)) {
            throw new TrinoException(NOT_SUPPORTED, "Table " + schemaTableName + " specifies " + properties.get(WRITE_LOCATION_PROVIDER_IMPL) +
                    " as a location provider. Writing to Iceberg tables with custom location provider is not supported.");
        }

        Duration retention = requireNonNull(retentionThreshold, "retention is null");
        checkProcedureArgument(retention.compareTo(minRetention) >= 0,
                "Retention specified (%s) is shorter than the minimum retention configured in the system (%s). " +
                        "Minimum retention can be changed with %s configuration property or iceberg.%s session property",
                retention,
                minRetention,
                minRetentionParameterName,
                sessionMinRetentionParameterName);
    }

    private void expireSnapshots(Table table, long expireTimestamp, ConnectorSession session, SchemaTableName schemaTableName)
    {
        Set<String> originalFiles = buildSetOfValidFiles(table);
        table.expireSnapshots().expireOlderThan(expireTimestamp).cleanExpiredFiles(false).commit();
        Set<String> validFiles = buildSetOfValidFiles(table);
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), new Path(table.location()));
            Sets.SetView<String> filesToDelete = difference(originalFiles, validFiles);
            for (String filePath : filesToDelete) {
                log.debug("Deleting file %s while expiring snapshots %s", filePath, schemaTableName.getTableName());
                fileSystem.delete(new Path(filePath), false);
            }
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed accessing data for table: " + schemaTableName, e);
        }
    }

    private static Set<String> buildSetOfValidFiles(Table table)
    {
        List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
        Stream<String> dataFiles = snapshots.stream()
                .map(Snapshot::snapshotId)
                .flatMap(snapshotId -> stream(table.newScan().useSnapshot(snapshotId).planFiles()))
                .map(fileScanTask -> fileScanTask.file().path().toString());
        Stream<String> manifests = snapshots.stream()
                .flatMap(snapshot -> snapshot.allManifests().stream())
                .map(ManifestFile::path);
        Stream<String> manifestLists = snapshots.stream()
                .map(Snapshot::manifestListLocation);
        Stream<String> otherMetadataFiles = concat(
                metadataFileLocations(table, false).stream(),
                Stream.of(versionHintLocation(table)));
        return concat(dataFiles, manifests, manifestLists, otherMetadataFiles)
                .map(file -> URI.create(file).getPath())
                .collect(toImmutableSet());
    }

    public void executeRemoveOrphanFiles(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergRemoveOrphanFilesHandle removeOrphanFilesHandle = (IcebergRemoveOrphanFilesHandle) executeHandle.getProcedureHandle();

        Table table = catalog.loadTable(session, executeHandle.getSchemaTableName());
        Duration retention = requireNonNull(removeOrphanFilesHandle.getRetentionThreshold(), "retention is null");
        validateTableExecuteParameters(
                table,
                executeHandle.getSchemaTableName(),
                REMOVE_ORPHAN_FILES.name(),
                retention,
                getRemoveOrphanFilesMinRetention(session),
                IcebergConfig.REMOVE_ORPHAN_FILES_MIN_RETENTION,
                IcebergSessionProperties.REMOVE_ORPHAN_FILES_MIN_RETENTION);

        long expireTimestampMillis = session.getStart().toEpochMilli() - retention.toMillis();
        removeOrphanFiles(table, session, executeHandle.getSchemaTableName(), expireTimestampMillis);
        removeOrphanMetadataFiles(table, session, executeHandle.getSchemaTableName(), expireTimestampMillis);
    }

    private void removeOrphanFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, long expireTimestamp)
    {
        Set<String> validDataFilePaths = stream(table.snapshots())
                .map(Snapshot::snapshotId)
                .flatMap(snapshotId -> stream(table.newScan().useSnapshot(snapshotId).planFiles()))
                // compare only paths not to delete too many files, see https://github.com/apache/iceberg/pull/2890
                .map(fileScanTask -> URI.create(fileScanTask.file().path().toString()).getPath())
                .collect(toImmutableSet());
        Set<String> validDeleteFilePaths = stream(table.snapshots())
                .map(Snapshot::snapshotId)
                .flatMap(snapshotId -> stream(table.newScan().useSnapshot(snapshotId).planFiles()))
                .flatMap(fileScanTask -> fileScanTask.deletes().stream().map(deleteFile -> URI.create(deleteFile.path().toString()).getPath()))
                .collect(Collectors.toUnmodifiableSet());
        scanAndDeleteInvalidFiles(table, session, schemaTableName, expireTimestamp, union(validDataFilePaths, validDeleteFilePaths), "/data");
    }

    private void removeOrphanMetadataFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, long expireTimestamp)
    {
        ImmutableSet<String> manifests = stream(table.snapshots())
                .flatMap(snapshot -> snapshot.allManifests().stream())
                .map(ManifestFile::path)
                .collect(toImmutableSet());
        List<String> manifestLists = ReachableFileUtil.manifestListLocations(table);
        List<String> otherMetadataFiles = concat(
                metadataFileLocations(table, false).stream(),
                Stream.of(versionHintLocation(table)))
                .collect(toImmutableList());
        Set<String> validMetadataFiles = concat(manifests.stream(), manifestLists.stream(), otherMetadataFiles.stream())
                .map(path -> URI.create(path).getPath())
                .collect(toImmutableSet());
        scanAndDeleteInvalidFiles(table, session, schemaTableName, expireTimestamp, validMetadataFiles, "/metadata");
    }

    private void scanAndDeleteInvalidFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, long expireTimestamp, Set<String> validFiles, String subfolder)
    {
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), new Path(table.location()));
            RemoteIterator<LocatedFileStatus> allFiles = fileSystem.listFiles(new Path(table.location() + subfolder), true);
            while (allFiles.hasNext()) {
                LocatedFileStatus file = allFiles.next();
                if (file.isFile()) {
                    String normalizedPath = file.getPath().toUri().getPath();
                    if (file.getModificationTime() < expireTimestamp && !validFiles.contains(normalizedPath)) {
                        log.debug("Deleting %s file while removing orphan files %s", file.getPath().toString(), schemaTableName.getTableName());
                        fileSystem.delete(file.getPath(), false);
                    }
                    else {
                        log.debug("%s file retained while removing orphan files %s", file.getPath().toString(), schemaTableName.getTableName());
                    }
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed accessing data for table: " + schemaTableName, e);
        }
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(
                SchemaParser.fromJson(icebergTableHandle.getTableSchemaJson()),
                icebergTableHandle.getPartitionSpecJson());

        return Optional.of(new IcebergInputInfo(
                icebergTableHandle.getSnapshotId(),
                partitionSpec.isPartitioned(),
                getFileFormat(icebergTableHandle.getStorageProperties()).name()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        catalog.dropTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        catalog.renameTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName(), newTable);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        Set<String> unsupportedProperties = difference(properties.keySet(), UPDATABLE_TABLE_PROPERTIES);
        if (!unsupportedProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The following properties cannot be updated: " + String.join(", ", unsupportedProperties));
        }

        beginTransaction(icebergTable);
        UpdateProperties updateProperties = transaction.updateProperties();

        if (properties.containsKey(FILE_FORMAT_PROPERTY)) {
            IcebergFileFormat fileFormat = (IcebergFileFormat) properties.get(FILE_FORMAT_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The format property cannot be empty"));
            updateProperties.defaultFormat(fileFormat.toIceberg());
        }

        if (properties.containsKey(FORMAT_VERSION_PROPERTY)) {
            // UpdateProperties#commit will trigger any necessary metadata updates required for the new spec version
            int formatVersion = (int) properties.get(FORMAT_VERSION_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The format_version property cannot be empty"));
            updateProperties.set(FORMAT_VERSION, Integer.toString(formatVersion));
        }

        try {
            updateProperties.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set new property values", e);
        }

        if (properties.containsKey(PARTITIONING_PROPERTY)) {
            @SuppressWarnings("unchecked")
            List<String> partitionColumns = (List<String>) properties.get(PARTITIONING_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The partitioning property cannot be empty"));
            updatePartitioning(icebergTable, transaction, partitionColumns);
        }

        try {
            transaction.commitTransaction();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to commit new table properties", e);
        }
    }

    private static void updatePartitioning(Table icebergTable, Transaction transaction, List<String> partitionColumns)
    {
        UpdatePartitionSpec updatePartitionSpec = transaction.updateSpec();
        Set<PartitionField> existingPartitionFields = icebergTable.spec().fields().stream().collect(toImmutableSet());
        Schema schema = icebergTable.schema();
        if (partitionColumns.isEmpty()) {
            existingPartitionFields.stream()
                    .map(partitionField -> toIcebergTerm(schema, partitionField))
                    .forEach(updatePartitionSpec::removeField);
        }
        else {
            Set<PartitionField> partitionFields = ImmutableSet.copyOf(parsePartitionFields(schema, partitionColumns).fields());
            difference(existingPartitionFields, partitionFields).stream()
                    .map(PartitionField::name)
                    .forEach(updatePartitionSpec::removeField);
            difference(partitionFields, existingPartitionFields).stream()
                    .map(partitionField -> toIcebergTerm(schema, partitionField))
                    .forEach(updatePartitionSpec::addField);
        }

        try {
            updatePartitionSpec.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set new partitioning value", e);
        }
    }

    private static Term toIcebergTerm(Schema schema, PartitionField partitionField)
    {
        return Expressions.transform(schema.findColumnName(partitionField.sourceId()), partitionField.transform());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        icebergTable.updateSchema().addColumn(column.getName(), toIcebergType(column.getType()), column.getComment()).commit();
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        icebergTable.updateSchema().deleteColumn(handle.getName()).commit();
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) source;
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        icebergTable.updateSchema().renameColumn(columnHandle.getName(), target).commit();
    }

    /**
     * @throws TableNotFoundException when table cannot be found
     */
    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName table)
    {
        Table icebergTable = catalog.loadTable(session, table);

        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        columns.addAll(getColumnMetadatas(icebergTable));
        columns.add(pathColumnMetadata());

        return new ConnectorTableMetadata(table, columns.build(), getIcebergTableProperties(icebergTable), getTableComment(icebergTable));
    }

    private List<ColumnMetadata> getColumnMetadatas(Table table)
    {
        return table.schema().columns().stream()
                .map(column ->
                        ColumnMetadata.builder()
                                .setName(column.name())
                                .setType(toTrinoType(column.type(), typeManager))
                                .setNullable(column.isOptional())
                                .setComment(Optional.ofNullable(column.doc()))
                                .build())
                .collect(toImmutableList());
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.of(handle);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        verifyTableVersionForUpdate(table);

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        validateNotModifyingOldSnapshot(table, icebergTable);

        beginTransaction(icebergTable);
        return table.withRetryMode(retryMode);
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        finishWrite(session, (IcebergTableHandle) tableHandle, fragments, false);
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getColumnHandle(ROW_POSITION, typeManager);
    }

    @Override
    public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns, RetryMode retryMode)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        verifyTableVersionForUpdate(table);

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        validateNotModifyingOldSnapshot(table, icebergTable);

        beginTransaction(icebergTable);
        return table.withRetryMode(retryMode)
                .withUpdatedColumns(updatedColumns.stream()
                        .map(IcebergColumnHandle.class::cast)
                        .collect(toImmutableList()));
    }

    @Override
    public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        finishWrite(session, (IcebergTableHandle) tableHandle, fragments, true);
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        List<NestedField> unmodifiedColumns = new ArrayList<>();
        unmodifiedColumns.add(ROW_POSITION);

        // Include all the non-updated columns. These are needed when writing the new data file with updated column values.
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Set<Integer> updatedFields = updatedColumns.stream()
                .map(IcebergColumnHandle.class::cast)
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());
        for (NestedField column : SchemaParser.fromJson(table.getTableSchemaJson()).columns()) {
            if (!updatedFields.contains(column.fieldId())) {
                unmodifiedColumns.add(column);
            }
        }

        NestedField icebergRowIdField = NestedField.required(TRINO_UPDATE_ROW_ID_COLUMN_ID, TRINO_UPDATE_ROW_ID_COLUMN_NAME, StructType.of(unmodifiedColumns));
        return getColumnHandle(icebergRowIdField, typeManager);
    }

    private static void verifyTableVersionForUpdate(IcebergTableHandle table)
    {
        if (table.getFormatVersion() < 2) {
            throw new TrinoException(NOT_SUPPORTED, "Iceberg table updates require at least format version 2");
        }
    }

    private static void validateNotModifyingOldSnapshot(IcebergTableHandle table, Table icebergTable)
    {
        if (table.getSnapshotId().isPresent() && (table.getSnapshotId().get() != icebergTable.currentSnapshot().snapshotId())) {
            throw new TrinoException(NOT_SUPPORTED, "Modifying old snapshot is not supported in Iceberg.");
        }
    }

    private void finishWrite(ConnectorSession session, IcebergTableHandle table, Collection<Slice> fragments, boolean runUpdateValidations)
    {
        Table icebergTable = transaction.table();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        if (commitTasks.isEmpty()) {
            // Avoid recording "empty" write operation
            transaction = null;
            return;
        }

        Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());

        Map<String, List<CommitTaskData>> deletesByFilePath = commitTasks.stream()
                .filter(task -> task.getContent() == POSITION_DELETES)
                .collect(groupingBy(task -> task.getReferencedDataFile().orElseThrow()));
        Map<String, List<CommitTaskData>> fullyDeletedFiles = deletesByFilePath
                .entrySet().stream()
                .filter(entry -> fileIsFullyDeleted(entry.getValue()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!deletesByFilePath.keySet().equals(fullyDeletedFiles.keySet()) || commitTasks.stream().anyMatch(task -> task.getContent() == FileContent.DATA)) {
            RowDelta rowDelta = transaction.newRowDelta();
            table.getSnapshotId().map(icebergTable::snapshot).ifPresent(s -> rowDelta.validateFromSnapshot(s.snapshotId()));
            if (!table.getEnforcedPredicate().isAll()) {
                rowDelta.conflictDetectionFilter(toIcebergExpression(table.getEnforcedPredicate()));
            }

            IsolationLevel isolationLevel = IsolationLevel.fromName(icebergTable.properties().getOrDefault(DELETE_ISOLATION_LEVEL, DELETE_ISOLATION_LEVEL_DEFAULT));
            if (isolationLevel == IsolationLevel.SERIALIZABLE) {
                rowDelta.validateNoConflictingDataFiles();
            }

            if (runUpdateValidations) {
                // Ensure a row that is updated by this commit was not deleted by a separate commit
                rowDelta.validateDeletedFiles();
                rowDelta.validateNoConflictingDeleteFiles();
            }

            ImmutableSet.Builder<String> writtenFiles = ImmutableSet.builder();
            ImmutableSet.Builder<String> referencedDataFiles = ImmutableSet.builder();
            for (CommitTaskData task : commitTasks) {
                PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, task.getPartitionSpecJson());
                Type[] partitionColumnTypes = partitionSpec.fields().stream()
                        .map(field -> field.transform().getResultType(icebergTable.schema().findType(field.sourceId())))
                        .toArray(Type[]::new);
                switch (task.getContent()) {
                    case POSITION_DELETES:
                        if (fullyDeletedFiles.containsKey(task.getReferencedDataFile().orElseThrow())) {
                            continue;
                        }

                        FileMetadata.Builder deleteBuilder = FileMetadata.deleteFileBuilder(partitionSpec)
                                .withPath(task.getPath())
                                .withFormat(task.getFileFormat().toIceberg())
                                .ofPositionDeletes()
                                .withFileSizeInBytes(task.getFileSizeInBytes())
                                .withMetrics(task.getMetrics().metrics());

                        if (!partitionSpec.fields().isEmpty()) {
                            String partitionDataJson = task.getPartitionDataJson()
                                    .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                            deleteBuilder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                        }

                        rowDelta.addDeletes(deleteBuilder.build());
                        writtenFiles.add(task.getPath());
                        task.getReferencedDataFile().ifPresent(referencedDataFiles::add);
                        break;
                    case DATA:
                        DataFiles.Builder builder = DataFiles.builder(partitionSpec)
                                .withPath(task.getPath())
                                .withFormat(task.getFileFormat().toIceberg())
                                .withFileSizeInBytes(task.getFileSizeInBytes())
                                .withMetrics(task.getMetrics().metrics());

                        if (!icebergTable.spec().fields().isEmpty()) {
                            String partitionDataJson = task.getPartitionDataJson()
                                    .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                            builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                        }
                        rowDelta.addRows(builder.build());
                        writtenFiles.add(task.getPath());
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported task content: " + task.getContent());
                }
            }

            // try to leave as little garbage as possible behind
            if (table.getRetryMode() != NO_RETRIES) {
                cleanExtraOutputFiles(session, writtenFiles.build());
            }

            rowDelta.validateDataFilesExist(referencedDataFiles.build());
            try {
                rowDelta.commit();
            }
            catch (ValidationException e) {
                throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to commit Iceberg update to table: " + table.getSchemaTableName(), e);
            }
        }

        if (!fullyDeletedFiles.isEmpty()) {
            try {
                FileSystem fileSystem = hdfsEnvironment.getFileSystem(new HdfsContext(session), new Path(table.getTableLocation()));
                for (List<CommitTaskData> commitTasksToCleanUp : fullyDeletedFiles.values()) {
                    for (CommitTaskData commitTaskData : commitTasksToCleanUp) {
                        if (!fileSystem.delete(new Path(commitTaskData.getPath()), false)) {
                            log.warn("Failed to clean up uncommitted position delete file: %s", commitTaskData.getPath());
                        }
                    }
                }
            }
            catch (IOException e) {
                log.warn(e, "Failed to clean up uncommitted position delete files");
            }
        }

        try {
            if (!fullyDeletedFiles.isEmpty()) {
                DeleteFiles deleteFiles = transaction.newDelete();
                fullyDeletedFiles.keySet().forEach(deleteFiles::deleteFile);
                deleteFiles.commit();
            }
            transaction.commitTransaction();
        }
        catch (ValidationException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to commit Iceberg update to table: " + table.getSchemaTableName(), e);
        }
        transaction = null;
    }

    private static boolean fileIsFullyDeleted(List<CommitTaskData> positionDeletes)
    {
        checkArgument(!positionDeletes.isEmpty(), "Cannot call fileIsFullyDeletes with an empty list");
        String referencedDataFile = positionDeletes.get(0).getReferencedDataFile().orElseThrow();
        long fileRecordCount = positionDeletes.get(0).getFileRecordCount().orElseThrow();
        checkArgument(positionDeletes.stream().allMatch(positionDelete ->
                        positionDelete.getReferencedDataFile().orElseThrow().equals(referencedDataFile) &&
                                positionDelete.getFileRecordCount().orElseThrow() == fileRecordCount),
                "All position deletes must be for the same file and have the same fileRecordCount");
        long deletedRowCount = positionDeletes.stream()
                .map(CommitTaskData::getDeletedRowCount)
                .mapToLong(Optional::orElseThrow)
                .sum();
        checkState(deletedRowCount <= fileRecordCount, "Found more deleted rows than exist in the file");
        return fileRecordCount == deletedRowCount;
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        catalog.createView(session, viewName, definition, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        catalog.renameView(session, source, target);
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        catalog.setViewPrincipal(session, viewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.getViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return catalog.getView(session, viewName);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;

        Table icebergTable = catalog.loadTable(session, handle.getSchemaTableName());

        icebergTable.newDelete()
                .deleteFromRowFilter(toIcebergExpression(handle.getEnforcedPredicate()))
                .commit();

        Map<String, String> summary = icebergTable.currentSnapshot().summary();
        String deletedRowsStr = summary.get(DELETED_RECORDS_PROP);
        if (deletedRowsStr == null) {
            // TODO Iceberg should guarantee this is always present (https://github.com/apache/iceberg/issues/4647)
            return OptionalLong.empty();
        }
        long deletedRecords = Long.parseLong(deletedRowsStr);
        long removedPositionDeletes = Long.parseLong(summary.getOrDefault(REMOVED_POS_DELETES_PROP, "0"));
        long removedEqualityDeletes = Long.parseLong(summary.getOrDefault(REMOVED_EQ_DELETES_PROP, "0"));
        return OptionalLong.of(deletedRecords - removedPositionDeletes - removedEqualityDeletes);
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        ConstraintExtractor.ExtractionResult extractionResult = extractTupleDomain(constraint);
        TupleDomain<IcebergColumnHandle> predicate = extractionResult.getTupleDomain();
        if (predicate.isAll()) {
            return Optional.empty();
        }

        TupleDomain<IcebergColumnHandle> newEnforcedConstraint;
        TupleDomain<IcebergColumnHandle> newUnenforcedConstraint;
        TupleDomain<IcebergColumnHandle> remainingConstraint;
        if (predicate.isNone()) {
            // Engine does not pass none Constraint.summary. It can become none when combined with the expression and connector's domain knowledge.
            newEnforcedConstraint = TupleDomain.none();
            newUnenforcedConstraint = TupleDomain.all();
            remainingConstraint = TupleDomain.all();
        }
        else {
            Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

            Map<IcebergColumnHandle, Domain> unsupported = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> newEnforced = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> newUnenforced = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> domains = predicate.getDomains().orElseThrow(() -> new VerifyException("No domains"));
            domains.forEach((columnHandle, domain) -> {
                // Iceberg metadata columns can not be used to filter a table scan in Iceberg library
                // TODO (https://github.com/trinodb/trino/issues/8759) structural types cannot be used to filter a table scan in Iceberg library.
                if (isMetadataColumnId(columnHandle.getId()) || isStructuralType(columnHandle.getType()) ||
                        // Iceberg orders UUID values differently than Trino (perhaps due to https://bugs.openjdk.org/browse/JDK-7025832), so allow only IS NULL / IS NOT NULL checks
                        (columnHandle.getType() == UUID && !(domain.isOnlyNull() || domain.getValues().isAll()))) {
                    unsupported.put(columnHandle, domain);
                }
                else if (canEnforceColumnConstraintInAllSpecs(typeOperators, icebergTable, columnHandle, domain)) {
                    newEnforced.put(columnHandle, domain);
                }
                else {
                    newUnenforced.put(columnHandle, domain);
                }
            });

            newEnforcedConstraint = TupleDomain.withColumnDomains(newEnforced).intersect(table.getEnforcedPredicate());
            newUnenforcedConstraint = TupleDomain.withColumnDomains(newUnenforced).intersect(table.getUnenforcedPredicate());
            remainingConstraint = TupleDomain.withColumnDomains(newUnenforced).intersect(TupleDomain.withColumnDomains(unsupported));
        }

        if (newEnforcedConstraint.equals(table.getEnforcedPredicate())
                && newUnenforcedConstraint.equals(table.getUnenforcedPredicate())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                new IcebergTableHandle(
                        table.getSchemaName(),
                        table.getTableName(),
                        table.getTableType(),
                        table.getSnapshotId(),
                        table.getTableSchemaJson(),
                        table.getPartitionSpecJson(),
                        table.getFormatVersion(),
                        newUnenforcedConstraint,
                        newEnforcedConstraint,
                        table.getProjectedColumns(),
                        table.getNameMappingJson(),
                        table.getTableLocation(),
                        table.getStorageProperties(),
                        table.getRetryMode(),
                        table.getUpdatedColumns(),
                        table.isRecordScannedFiles(),
                        table.getMaxScannedFileSize()),
                remainingConstraint.transformKeys(ColumnHandle.class::cast),
                extractionResult.getRemainingExpression(),
                false));
    }

    private static Set<Integer> identityPartitionColumnsInAllSpecs(Table table)
    {
        // Extract identity partition column source ids common to ALL specs
        return table.spec().fields().stream()
                .filter(field -> field.transform().isIdentity())
                .filter(field -> table.specs().values().stream().allMatch(spec -> spec.fields().contains(field)))
                .map(PartitionField::sourceId)
                .collect(toImmutableSet());
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        if (!isProjectionPushdownEnabled(session)) {
            return Optional.empty();
        }

        // Create projected column representations for supported sub expressions. Simple column references and chain of
        // dereferences on a variable are supported right now.
        Set<ConnectorExpression> projectedExpressions = projections.stream()
                .flatMap(expression -> extractSupportedProjectedColumns(expression).stream())
                .collect(toImmutableSet());

        Map<ConnectorExpression, ProjectedColumnRepresentation> columnProjections = projectedExpressions.stream()
                .collect(toImmutableMap(identity(), HiveApplyProjectionUtil::createProjectedColumnRepresentation));

        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) handle;

        // all references are simple variables
        if (columnProjections.values().stream().allMatch(ProjectedColumnRepresentation::isVariable)) {
            Set<IcebergColumnHandle> projectedColumns = assignments.values().stream()
                    .map(IcebergColumnHandle.class::cast)
                    .collect(toImmutableSet());
            if (icebergTableHandle.getProjectedColumns().equals(projectedColumns)) {
                return Optional.empty();
            }
            List<Assignment> assignmentsList = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((IcebergColumnHandle) assignment.getValue()).getType()))
                    .collect(toImmutableList());

            return Optional.of(new ProjectionApplicationResult<>(
                    icebergTableHandle.withProjectedColumns(projectedColumns),
                    projections,
                    assignmentsList,
                    false));
        }

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<IcebergColumnHandle> projectedColumnsBuilder = ImmutableSet.builder();

        for (Map.Entry<ConnectorExpression, ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ProjectedColumnRepresentation projectedColumn = entry.getValue();

            IcebergColumnHandle baseColumnHandle = (IcebergColumnHandle) assignments.get(projectedColumn.getVariable().getName());
            IcebergColumnHandle projectedColumnHandle = createProjectedColumnHandle(baseColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType());
            String projectedColumnName = projectedColumnHandle.getQualifiedName();

            Variable projectedColumnVariable = new Variable(projectedColumnName, expression.getType());
            Assignment newAssignment = new Assignment(projectedColumnName, projectedColumnHandle, expression.getType());
            newAssignments.putIfAbsent(projectedColumnName, newAssignment);

            newVariablesBuilder.put(expression, projectedColumnVariable);
            projectedColumnsBuilder.add(projectedColumnHandle);
        }

        // Modify projections to refer to new variables
        Map<ConnectorExpression, Variable> newVariables = newVariablesBuilder.buildOrThrow();
        List<ConnectorExpression> newProjections = projections.stream()
                .map(expression -> replaceWithNewVariables(expression, newVariables))
                .collect(toImmutableList());

        List<Assignment> outputAssignments = newAssignments.values().stream().collect(toImmutableList());
        return Optional.of(new ProjectionApplicationResult<>(
                icebergTableHandle.withProjectedColumns(projectedColumnsBuilder.build()),
                newProjections,
                outputAssignments,
                false));
    }

    private static IcebergColumnHandle createProjectedColumnHandle(IcebergColumnHandle column, List<Integer> indices, io.trino.spi.type.Type projectedColumnType)
    {
        if (indices.isEmpty()) {
            return column;
        }
        ImmutableList.Builder<Integer> fullPath = ImmutableList.builder();
        fullPath.addAll(column.getPath());

        ColumnIdentity projectedColumnIdentity = column.getColumnIdentity();
        for (int index : indices) {
            // Position based lookup, not FieldId based
            projectedColumnIdentity = projectedColumnIdentity.getChildren().get(index);
            fullPath.add(projectedColumnIdentity.getId());
        }

        return new IcebergColumnHandle(
                column.getBaseColumnIdentity(),
                column.getBaseType(),
                fullPath.build(),
                projectedColumnType,
                Optional.empty());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }

        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, handle.getSchemaTableName());
        return TableStatisticsMaker.getTableStatistics(typeManager, handle, icebergTable);
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        catalog.setTablePrincipal(session, tableName, principal);
    }

    private Optional<Long> getSnapshotId(Table table, Optional<Long> snapshotId, boolean allowLegacySnapshotSyntax)
    {
        // table.name() is an encoded version of SchemaTableName
        return snapshotId
                .map(id ->
                        snapshotIds.computeIfAbsent(
                                table.name() + "@" + id,
                                ignored -> IcebergUtil.resolveSnapshotId(table, id, allowLegacySnapshotSyntax)))
                .or(() -> Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId));
    }

    Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return catalog.loadTable(session, schemaTableName);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        catalog.createMaterializedView(session, viewName, definition, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.dropMaterializedView(session, viewName);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return false;
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, RetryMode retryMode)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        beginTransaction(icebergTable);

        return new IcebergWritableTableHandle(
                table.getSchemaTableName(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                getColumns(icebergTable.schema(), typeManager),
                icebergTable.location(),
                getFileFormat(icebergTable),
                icebergTable.properties(),
                retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<ConnectorTableHandle> sourceTableHandles)
    {
        // delete before insert .. simulating overwrite
        executeDelete(session, tableHandle);

        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;

        Table icebergTable = transaction.table();
        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = transaction.newFastAppend();
        ImmutableSet.Builder<String> writtenFiles = ImmutableSet.builder();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.getPath())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withFormat(table.getFileFormat().toIceberg())
                    .withMetrics(task.getMetrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
            writtenFiles.add(task.getPath());
        }

        String dependencies = sourceTableHandles.stream()
                .map(handle -> (IcebergTableHandle) handle)
                .filter(handle -> handle.getSnapshotId().isPresent())
                .map(handle -> handle.getSchemaTableName() + "=" + handle.getSnapshotId().get())
                .distinct()
                .collect(joining(","));

        // try to leave as little garbage as possible behind
        if (table.getRetryMode() != NO_RETRIES) {
            cleanExtraOutputFiles(session, writtenFiles.build());
        }

        // Update the 'dependsOnTables' property that tracks tables on which the materialized view depends and the corresponding snapshot ids of the tables
        appendFiles.set(DEPENDS_ON_TABLES, dependencies);
        appendFiles.commit();

        transaction.commitTransaction();
        transaction = null;
        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::getPath)
                .collect(toImmutableList())));
    }

    private void cleanExtraOutputFiles(ConnectorSession session, Set<String> writtenFiles)
    {
        HdfsContext hdfsContext = new HdfsContext(session);
        Set<String> locations = getOutputFilesLocations(writtenFiles);
        for (String location : locations) {
            cleanExtraOutputFiles(hdfsContext, session.getQueryId(), location, writtenFiles);
        }
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listMaterializedViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViews = new HashMap<>();
        for (SchemaTableName name : listMaterializedViews(session, schemaName)) {
            try {
                getMaterializedView(session, name).ifPresent(view -> materializedViews.put(name, view));
            }
            catch (RuntimeException e) {
                // Materialized view can be being removed and this may cause all sorts of exceptions. Log, because we're catching broadly.
                log.warn(e, "Failed to access metadata of materialized view %s during listing", name);
            }
        }
        return materializedViews;
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return catalog.getMaterializedView(session, viewName);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        // TODO (https://github.com/trinodb/trino/issues/9594) support rename across schemas
        if (!source.getSchemaName().equals(target.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "Materialized View rename across schemas is not supported");
        }
        catalog.renameMaterializedView(session, source, target);
    }

    public Optional<TableToken> getTableToken(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        return Optional.ofNullable(icebergTable.currentSnapshot())
                .map(snapshot -> new TableToken(snapshot.snapshotId()));
    }

    public boolean isTableCurrent(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<TableToken> tableToken)
    {
        Optional<TableToken> currentToken = getTableToken(session, tableHandle);

        if (tableToken.isEmpty() || currentToken.isEmpty()) {
            return false;
        }

        return tableToken.get().getSnapshotId() == currentToken.get().getSnapshotId();
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName materializedViewName)
    {
        Map<String, Optional<TableToken>> refreshStateMap = getMaterializedViewToken(session, materializedViewName);
        if (refreshStateMap.isEmpty()) {
            return new MaterializedViewFreshness(false);
        }

        for (Map.Entry<String, Optional<TableToken>> entry : refreshStateMap.entrySet()) {
            List<String> strings = Splitter.on(".").splitToList(entry.getKey());
            if (strings.size() == 3) {
                strings = strings.subList(1, 3);
            }
            else if (strings.size() != 2) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, format("Invalid table name in '%s' property: %s'", DEPENDS_ON_TABLES, strings));
            }
            String schema = strings.get(0);
            String name = strings.get(1);
            SchemaTableName schemaTableName = new SchemaTableName(schema, name);
            IcebergTableHandle tableHandle = getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());

            if (tableHandle == null) {
                throw new MaterializedViewNotFoundException(materializedViewName);
            }
            if (!isTableCurrent(session, tableHandle, entry.getValue())) {
                return new MaterializedViewFreshness(false);
            }
        }
        return new MaterializedViewFreshness(true);
    }

    @Override
    public boolean supportsReportingWrittenBytes(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        return true;
    }

    @Override
    public boolean supportsReportingWrittenBytes(ConnectorSession session, SchemaTableName fullTableName, Map<String, Object> tableProperties)
    {
        return true;
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        catalog.updateColumnComment(session, ((IcebergTableHandle) tableHandle).getSchemaTableName(), ((IcebergColumnHandle) column).getColumnIdentity(), comment);
    }

    private Map<String, Optional<TableToken>> getMaterializedViewToken(ConnectorSession session, SchemaTableName name)
    {
        Map<String, Optional<TableToken>> viewToken = new HashMap<>();
        Optional<ConnectorMaterializedViewDefinition> materializedViewDefinition = getMaterializedView(session, name);
        if (materializedViewDefinition.isEmpty()) {
            return viewToken;
        }

        SchemaTableName storageTableName = materializedViewDefinition.get().getStorageTable()
                .map(CatalogSchemaTableName::getSchemaTableName)
                .orElseThrow(() -> new IllegalStateException("Storage table missing in definition of materialized view " + name));

        Table icebergTable = catalog.loadTable(session, storageTableName);
        String dependsOnTables = icebergTable.currentSnapshot().summary().getOrDefault(DEPENDS_ON_TABLES, "");
        if (!dependsOnTables.isEmpty()) {
            Map<String, String> tableToSnapshotIdMap = Splitter.on(',').withKeyValueSeparator('=').split(dependsOnTables);
            for (Map.Entry<String, String> entry : tableToSnapshotIdMap.entrySet()) {
                viewToken.put(entry.getKey(), Optional.of(new TableToken(Long.parseLong(entry.getValue()))));
            }
        }
        return viewToken;
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        return catalog.redirectTable(session, tableName);
    }

    private void beginTransaction(Table icebergTable)
    {
        verify(transaction == null, "transaction already set");
        transaction = icebergTable.newTransaction();
    }

    private static class TableToken
    {
        // Current Snapshot ID of the table
        private final long snapshotId;

        public TableToken(long snapshotId)
        {
            this.snapshotId = snapshotId;
        }

        public long getSnapshotId()
        {
            return snapshotId;
        }
    }
}
