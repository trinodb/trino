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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;
import com.google.common.base.Suppliers;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.TableInfo;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.base.filter.UtcConstraintExtractor;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.base.projection.ApplyProjectionUtil.ProjectedColumnRepresentation;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveWrittenPartitions;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.iceberg.aggregation.DataSketchStateSerializer;
import io.trino.plugin.iceberg.aggregation.IcebergThetaSketchForStats;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.procedure.IcebergAddFilesFromTableHandle;
import io.trino.plugin.iceberg.procedure.IcebergAddFilesHandle;
import io.trino.plugin.iceberg.procedure.IcebergDropExtendedStatsHandle;
import io.trino.plugin.iceberg.procedure.IcebergExpireSnapshotsHandle;
import io.trino.plugin.iceberg.procedure.IcebergOptimizeHandle;
import io.trino.plugin.iceberg.procedure.IcebergRemoveOrphanFilesHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableProcedureId;
import io.trino.plugin.iceberg.procedure.MigrationUtils.RecursiveDirectory;
import io.trino.plugin.iceberg.util.DataFileWithDeleteFiles;
import io.trino.spi.ErrorCode;
import io.trino.spi.RefreshType;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
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
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.difference;
import static io.trino.plugin.base.filter.UtcConstraintExtractor.extractTupleDomain;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.HiveMetadata.TRANSACTIONAL;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.ViewReaderUtil.isSomeKindOfAView;
import static io.trino.plugin.hive.util.HiveTypeUtil.getTypeSignature;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity;
import static io.trino.plugin.iceberg.ExpressionConverter.isConvertableToIcebergExpression;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergAnalyzeProperties.getColumnNames;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_MERGE_PARTITION_DATA;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_MERGE_PARTITION_SPEC_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_MERGE_ROW_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_ROW_ID_NAME;
import static io.trino.plugin.iceberg.IcebergColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.pathColumnHandle;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_MISSING_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_UNSUPPORTED_VIEW_DIALECT;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.FILE_MODIFIED_TIME;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.FILE_PATH;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getExpireSnapshotMinRetention;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getHiveCatalogName;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getQueryPartitionFilterRequiredSchemas;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getRemoveOrphanFilesMinRetention;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isCollectExtendedStatisticsOnWrite;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isExtendedStatisticsEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isIncrementalRefreshEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isMergeManifestsOnWrite;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isQueryPartitionFilterRequired;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isStatisticsEnabled;
import static io.trino.plugin.iceberg.IcebergTableName.isDataTable;
import static io.trino.plugin.iceberg.IcebergTableName.isIcebergTableName;
import static io.trino.plugin.iceberg.IcebergTableName.isMaterializedViewStorage;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameFrom;
import static io.trino.plugin.iceberg.IcebergTableProperties.DATA_LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.EXTRA_PROPERTIES_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.OBJECT_STORE_LAYOUT_ENABLED_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergTableProperties.getTableLocation;
import static io.trino.plugin.iceberg.IcebergUtil.buildPath;
import static io.trino.plugin.iceberg.IcebergUtil.canEnforceColumnConstraintInSpecs;
import static io.trino.plugin.iceberg.IcebergUtil.commit;
import static io.trino.plugin.iceberg.IcebergUtil.createColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.fileName;
import static io.trino.plugin.iceberg.IcebergUtil.firstSnapshot;
import static io.trino.plugin.iceberg.IcebergUtil.firstSnapshotAfter;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnMetadatas;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableProperties;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.getProjectedColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getSnapshotIdAsOfTime;
import static io.trino.plugin.iceberg.IcebergUtil.getTableComment;
import static io.trino.plugin.iceberg.IcebergUtil.getTopLevelColumns;
import static io.trino.plugin.iceberg.IcebergUtil.newCreateTableTransaction;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.verifyExtraProperties;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.PartitionFields.toPartitionFields;
import static io.trino.plugin.iceberg.SortFieldUtils.parseSortFields;
import static io.trino.plugin.iceberg.TableStatisticsReader.readNdvs;
import static io.trino.plugin.iceberg.TableStatisticsWriter.StatsUpdateMode.INCREMENTAL_UPDATE;
import static io.trino.plugin.iceberg.TableStatisticsWriter.StatsUpdateMode.REPLACE;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergTypeForNewColumn;
import static io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog.DEPENDS_ON_TABLES;
import static io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog.DEPENDS_ON_TABLE_FUNCTIONS;
import static io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog.TRINO_QUERY_START_TIME;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.ADD_FILES;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.ADD_FILES_FROM_TABLE;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.DROP_EXTENDED_STATS;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.EXPIRE_SNAPSHOTS;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.OPTIMIZE;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.REMOVE_ORPHAN_FILES;
import static io.trino.plugin.iceberg.procedure.MigrationUtils.addFiles;
import static io.trino.plugin.iceberg.procedure.MigrationUtils.addFilesFromTable;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.UNKNOWN;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Math.floorDiv;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static org.apache.iceberg.ReachableFileUtil.metadataFileLocations;
import static org.apache.iceberg.ReachableFileUtil.statisticsFilesLocations;
import static org.apache.iceberg.SnapshotSummary.DELETED_RECORDS_PROP;
import static org.apache.iceberg.SnapshotSummary.REMOVED_EQ_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.REMOVED_POS_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL_DEFAULT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_ENABLED;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_LOCATION_PROVIDER_IMPL;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.types.TypeUtil.indexParents;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;
import static org.apache.iceberg.util.SnapshotUtil.schemaFor;

public class IcebergMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(IcebergMetadata.class);
    private static final Pattern PATH_PATTERN = Pattern.compile("(.*)/[^/]+");
    private static final int OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION = 2;
    private static final int CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION = 2;
    private static final String RETENTION_THRESHOLD = "retention_threshold";
    private static final String UNKNOWN_SNAPSHOT_TOKEN = "UNKNOWN";
    public static final Set<String> UPDATABLE_TABLE_PROPERTIES = ImmutableSet.<String>builder()
            .add(EXTRA_PROPERTIES_PROPERTY)
            .add(FILE_FORMAT_PROPERTY)
            .add(FORMAT_VERSION_PROPERTY)
            .add(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY)
            .add(DATA_LOCATION_PROPERTY)
            .add(PARTITIONING_PROPERTY)
            .add(SORTED_BY_PROPERTY)
            .build();

    public static final String NUMBER_OF_DISTINCT_VALUES_NAME = "NUMBER_OF_DISTINCT_VALUES";
    private static final FunctionName NUMBER_OF_DISTINCT_VALUES_FUNCTION = new FunctionName(IcebergThetaSketchForStats.NAME);

    private static final Integer DELETE_BATCH_SIZE = 1000;
    public static final int GET_METADATA_BATCH_SIZE = 1000;
    private static final MapSplitter MAP_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("=");

    private final TypeManager typeManager;
    private final CatalogHandle trinoCatalogHandle;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final TrinoCatalog catalog;
    private final IcebergFileSystemFactory fileSystemFactory;
    private final TableStatisticsWriter tableStatisticsWriter;
    private final Optional<HiveMetastoreFactory> metastoreFactory;
    private final boolean addFilesProcedureEnabled;
    private final Predicate<String> allowedExtraProperties;

    private final Map<IcebergTableHandle, AtomicReference<TableStatistics>> tableStatisticsCache = new ConcurrentHashMap<>();

    private Transaction transaction;
    private Optional<Long> fromSnapshotForRefresh = Optional.empty();

    public IcebergMetadata(
            TypeManager typeManager,
            CatalogHandle trinoCatalogHandle,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoCatalog catalog,
            IcebergFileSystemFactory fileSystemFactory,
            TableStatisticsWriter tableStatisticsWriter,
            Optional<HiveMetastoreFactory> metastoreFactory,
            boolean addFilesProcedureEnabled,
            Predicate<String> allowedExtraProperties)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.trinoCatalogHandle = requireNonNull(trinoCatalogHandle, "trinoCatalogHandle is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableStatisticsWriter = requireNonNull(tableStatisticsWriter, "tableStatisticsWriter is null");
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.addFilesProcedureEnabled = addFilesProcedureEnabled;
        this.allowedExtraProperties = requireNonNull(allowedExtraProperties, "allowedExtraProperties is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return catalog.namespaceExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return catalog.listNamespaces(session);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        return catalog.loadNamespaceMetadata(session, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        return catalog.getNamespacePrincipal(session, schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read table with start version is not supported");
        }

        if (!isIcebergTableName(tableName.getTableName())) {
            return null;
        }

        if (isMaterializedViewStorage(tableName.getTableName())) {
            verify(endVersion.isEmpty(), "Materialized views do not support versioned queries");

            SchemaTableName materializedViewName = new SchemaTableName(tableName.getSchemaName(), tableNameFrom(tableName.getTableName()));
            if (getMaterializedView(session, materializedViewName).isEmpty()) {
                throw new TableNotFoundException(tableName);
            }

            BaseTable storageTable = catalog.getMaterializedViewStorageTable(session, materializedViewName)
                    .orElseThrow(() -> new TrinoException(TABLE_NOT_FOUND, "Storage table metadata not found for materialized view " + tableName));

            return tableHandleForCurrentSnapshot(tableName, storageTable);
        }

        if (!isDataTable(tableName.getTableName())) {
            // Pretend the table does not exist to produce better error message in case of table redirects to Hive
            return null;
        }

        BaseTable table;
        try {
            table = (BaseTable) catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), tableName.getTableName()));
        }
        catch (TableNotFoundException e) {
            return null;
        }
        catch (TrinoException e) {
            ErrorCode errorCode = e.getErrorCode();
            if (errorCode.equals(ICEBERG_MISSING_METADATA.toErrorCode())
                    || errorCode.equals(ICEBERG_INVALID_METADATA.toErrorCode())) {
                return new CorruptedIcebergTableHandle(tableName, e);
            }
            throw e;
        }

        if (endVersion.isPresent()) {
            long snapshotId = getSnapshotIdFromVersion(session, table, endVersion.get());
            return tableHandleForSnapshot(
                    tableName,
                    table,
                    Optional.of(snapshotId),
                    schemaFor(table, snapshotId),
                    Optional.empty());
        }
        return tableHandleForCurrentSnapshot(tableName, table);
    }

    private IcebergTableHandle tableHandleForCurrentSnapshot(SchemaTableName tableName, BaseTable table)
    {
        return tableHandleForSnapshot(
                tableName,
                table,
                Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId),
                table.schema(),
                Optional.of(table.spec()));
    }

    private IcebergTableHandle tableHandleForSnapshot(
            SchemaTableName tableName,
            BaseTable table,
            Optional<Long> tableSnapshotId,
            Schema tableSchema,
            Optional<PartitionSpec> partitionSpec)
    {
        Map<String, String> tableProperties = table.properties();
        return new IcebergTableHandle(
                trinoCatalogHandle,
                tableName.getSchemaName(),
                tableName.getTableName(),
                DATA,
                tableSnapshotId,
                SchemaParser.toJson(tableSchema),
                partitionSpec.map(PartitionSpecParser::toJson),
                table.operations().current().formatVersion(),
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(),
                Optional.ofNullable(tableProperties.get(TableProperties.DEFAULT_NAME_MAPPING)),
                table.location(),
                table.properties(),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.of(false));
    }

    private static long getSnapshotIdFromVersion(ConnectorSession session, Table table, ConnectorTableVersion version)
    {
        io.trino.spi.type.Type versionType = version.getVersionType();
        return switch (version.getPointerType()) {
            case TEMPORAL -> getTemporalSnapshotIdFromVersion(session, table, version, versionType);
            case TARGET_ID -> getTargetSnapshotIdFromVersion(table, version, versionType);
        };
    }

    private static long getTargetSnapshotIdFromVersion(Table table, ConnectorTableVersion version, io.trino.spi.type.Type versionType)
    {
        long snapshotId;
        if (versionType == BIGINT) {
            snapshotId = (long) version.getVersion();
        }
        else if (versionType instanceof VarcharType) {
            String refName = ((Slice) version.getVersion()).toStringUtf8();
            SnapshotRef ref = table.refs().get(refName);
            if (ref == null) {
                throw new TrinoException(INVALID_ARGUMENTS, "Cannot find snapshot with reference name: " + refName);
            }
            snapshotId = ref.snapshotId();
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported type for table version: " + versionType.getDisplayName());
        }

        if (table.snapshot(snapshotId) == null) {
            throw new TrinoException(INVALID_ARGUMENTS, "Iceberg snapshot ID does not exists: " + snapshotId);
        }
        return snapshotId;
    }

    private static long getTemporalSnapshotIdFromVersion(ConnectorSession session, Table table, ConnectorTableVersion version, io.trino.spi.type.Type versionType)
    {
        if (versionType.equals(DATE)) {
            // Retrieve the latest snapshot made before or at the beginning of the day of the specified date in the session's time zone
            long epochMillis = LocalDate.ofEpochDay((Long) version.getVersion())
                    .atStartOfDay()
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant()
                    .toEpochMilli();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        if (versionType instanceof TimestampType timestampVersionType) {
            long epochMicrosUtc = timestampVersionType.isShort()
                    ? (long) version.getVersion()
                    : ((LongTimestamp) version.getVersion()).getEpochMicros();
            long epochMillisUtc = floorDiv(epochMicrosUtc, MICROSECONDS_PER_MILLISECOND);
            long epochMillis = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillisUtc), ZoneOffset.UTC)
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant()
                    .toEpochMilli();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        if (versionType instanceof TimestampWithTimeZoneType timeZonedVersionType) {
            long epochMillis = timeZonedVersionType.isShort()
                    ? unpackMillisUtc((long) version.getVersion())
                    : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type for temporal table version: " + versionType.getDisplayName());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(session, tableName)
                .map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    private Optional<SystemTable> getRawSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        if (!isIcebergTableName(tableName.getTableName()) || isDataTable(tableName.getTableName()) || isMaterializedViewStorage(tableName.getTableName())) {
            return Optional.empty();
        }

        // Only when dealing with an actual system table proceed to retrieve the base table for the system table
        String name = tableNameFrom(tableName.getTableName());
        Table table;
        try {
            table = catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), name));
        }
        catch (TableNotFoundException e) {
            return Optional.empty();
        }
        catch (UnknownTableTypeException e) {
            // avoid dealing with non Iceberg tables
            return Optional.empty();
        }

        TableType tableType = IcebergTableName.tableTypeFrom(tableName.getTableName());
        return switch (tableType) {
            case DATA, MATERIALIZED_VIEW_STORAGE -> throw new VerifyException("Unexpected table type: " + tableType); // Handled above.
            case HISTORY -> Optional.of(new HistoryTable(tableName, table));
            case METADATA_LOG_ENTRIES -> Optional.of(new MetadataLogEntriesTable(tableName, table));
            case SNAPSHOTS -> Optional.of(new SnapshotsTable(tableName, typeManager, table));
            case PARTITIONS -> Optional.of(new PartitionsTable(tableName, typeManager, table, getCurrentSnapshotId(table)));
            case MANIFESTS -> Optional.of(new ManifestsTable(tableName, table, getCurrentSnapshotId(table)));
            case FILES -> Optional.of(new FilesTable(tableName, typeManager, table, getCurrentSnapshotId(table)));
            case PROPERTIES -> Optional.of(new PropertiesTable(tableName, table));
            case REFS -> Optional.of(new RefsTable(tableName, table));
        };
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;

        if (table.getSnapshotId().isEmpty()) {
            // A table with missing snapshot id produces no splits, so we optimize here by returning
            // TupleDomain.none() as the predicate
            return new ConnectorTableProperties(TupleDomain.none(), Optional.empty(), Optional.empty(), ImmutableList.of());
        }

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        // Extract identity partition fields that are present in all partition specs, for creating the discrete predicates.
        Set<Integer> partitionSourceIds = identityPartitionColumnsInAllSpecs(icebergTable);

        TupleDomain<IcebergColumnHandle> enforcedPredicate = table.getEnforcedPredicate();

        DiscretePredicates discretePredicates = null;
        if (!partitionSourceIds.isEmpty()) {
            // Extract identity partition columns
            Map<Integer, IcebergColumnHandle> columns = getProjectedColumns(icebergTable.schema(), typeManager, partitionSourceIds).stream()
                    .collect(toImmutableMap(IcebergColumnHandle::getId, identity()));

            Supplier<List<FileScanTask>> lazyFiles = Suppliers.memoize(() -> {
                TableScan tableScan = icebergTable.newScan()
                        .useSnapshot(table.getSnapshotId().get())
                        .filter(toIcebergExpression(enforcedPredicate));

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

                                    return new NullableValue(column.getType(), prestoValue);
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
                Optional.ofNullable(discretePredicates),
                ImmutableList.of());
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        if (table instanceof CorruptedIcebergTableHandle corruptedTableHandle) {
            return corruptedTableHandle.schemaTableName();
        }
        return ((IcebergTableHandle) table).getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        IcebergTableHandle tableHandle = checkValidTableHandle(table);
        // This method does not calculate column metadata for the projected columns
        checkArgument(tableHandle.getProjectedColumns().isEmpty(), "Unexpected projected columns");
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
        List<ColumnMetadata> columns = getColumnMetadatas(SchemaParser.fromJson(tableHandle.getTableSchemaJson()), typeManager);
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), columns, getIcebergTableProperties(icebergTable), getTableComment(icebergTable));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName).stream()
                .map(TableInfo::tableName)
                .toList();
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableMap.Builder<SchemaTableName, RelationType> result = ImmutableMap.builder();
        for (TableInfo info : catalog.listTables(session, schemaName)) {
            result.put(info.tableName(), info.extendedRelationType().toRelationType());
        }
        return result.buildKeepingLast();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (IcebergColumnHandle columnHandle : getTopLevelColumns(SchemaParser.fromJson(table.getTableSchemaJson()), typeManager)) {
            columnHandles.put(columnHandle.getName(), columnHandle);
        }
        columnHandles.put(FILE_PATH.getColumnName(), pathColumnHandle());
        columnHandles.put(FILE_MODIFIED_TIME.getColumnName(), fileModifiedTimeColumnHandle());
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .setNullable(column.isNullable())
                .setComment(column.getComment())
                .build();
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        if (isQueryPartitionFilterRequiredForTable(session, table) && table.getEnforcedPredicate().isAll() && !table.getForAnalyze().orElseThrow()) {
            Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());
            Optional<PartitionSpec> partitionSpec = table.getPartitionSpecJson()
                    .map(partitionSpecJson -> PartitionSpecParser.fromJson(schema, partitionSpecJson));
            if (partitionSpec.isEmpty() || partitionSpec.get().isUnpartitioned()) {
                return;
            }
            Set<Integer> columnsWithPredicates = new HashSet<>();
            table.getConstraintColumns().stream()
                    .map(IcebergColumnHandle::getId)
                    .forEach(columnsWithPredicates::add);
            table.getUnenforcedPredicate().getDomains().ifPresent(domain -> domain.keySet().stream()
                    .map(IcebergColumnHandle::getId)
                    .forEach(columnsWithPredicates::add));
            Set<Integer> partitionColumns = partitionSpec.get().fields().stream()
                    .filter(field -> !field.transform().isVoid())
                    .map(PartitionField::sourceId)
                    .collect(toImmutableSet());
            if (Collections.disjoint(columnsWithPredicates, partitionColumns)) {
                String partitionColumnNames = partitionSpec.get().fields().stream()
                        .filter(field -> !field.transform().isVoid())
                        .map(PartitionField::sourceId)
                        .map(id -> schema.idToName().get(id))
                        .collect(joining(", "));
                throw new TrinoException(
                        QUERY_REJECTED,
                        format("Filter required for %s on at least one of the partition columns: %s", table.getSchemaTableName(), partitionColumnNames));
            }
        }
    }

    private static boolean isQueryPartitionFilterRequiredForTable(ConnectorSession session, IcebergTableHandle table)
    {
        Set<String> requiredSchemas = getQueryPartitionFilterRequiredSchemas(session);
        // If query_partition_filter_required_schemas is empty then we would apply partition filter for all tables.
        return isQueryPartitionFilterRequired(session) &&
                (requiredSchemas.isEmpty() || requiredSchemas.contains(table.getSchemaName()));
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
            schemaTableNames = catalog.listTables(session, prefix.getSchema()).stream()
                    .map(TableInfo::tableName)
                    .collect(toImmutableList());
        }
        else {
            schemaTableNames = ImmutableList.of(prefix.toSchemaTableName());
        }

        return Lists.partition(schemaTableNames, GET_METADATA_BATCH_SIZE).stream()
                .map(tableBatch -> {
                    ImmutableList.Builder<TableColumnsMetadata> tableMetadatas = ImmutableList.builderWithExpectedSize(tableBatch.size());
                    Set<SchemaTableName> remainingTables = new HashSet<>(tableBatch.size());
                    for (SchemaTableName tableName : tableBatch) {
                        if (redirectTable(session, tableName).isPresent()) {
                            tableMetadatas.add(TableColumnsMetadata.forRedirectedTable(tableName));
                        }
                        else {
                            remainingTables.add(tableName);
                        }
                    }

                    Map<SchemaTableName, List<ColumnMetadata>> loaded = catalog.tryGetColumnMetadata(session, ImmutableList.copyOf(remainingTables));
                    loaded.forEach((tableName, columns) -> {
                        remainingTables.remove(tableName);
                        tableMetadatas.add(TableColumnsMetadata.forTable(tableName, columns));
                    });

                    for (SchemaTableName tableName : remainingTables) {
                        try {
                            Table icebergTable = catalog.loadTable(session, tableName);
                            List<ColumnMetadata> columns = getColumnMetadatas(icebergTable.schema(), typeManager);
                            tableMetadatas.add(TableColumnsMetadata.forTable(tableName, columns));
                        }
                        catch (TableNotFoundException e) {
                            // Table disappeared during listing operation
                        }
                        catch (UnknownTableTypeException e) {
                            // Skip unsupported table type in case that the table redirects are not enabled
                        }
                        catch (RuntimeException e) {
                            // Table can be being removed and this may cause all sorts of exceptions. Log, because we're catching broadly.
                            log.warn(e, "Failed to access metadata of table %s during streaming table columns for %s", tableName, prefix);
                        }
                    }
                    return tableMetadatas.build();
                })
                .flatMap(List::stream)
                .iterator();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return catalog.streamRelationColumns(session, schemaName, relationFilter, tableName -> redirectTable(session, tableName).isPresent())
                .orElseGet(() -> {
                    // Catalog does not support streamRelationColumns
                    return ConnectorMetadata.super.streamRelationColumns(session, schemaName, relationFilter);
                });
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return catalog.streamRelationComments(session, schemaName, relationFilter, tableName -> redirectTable(session, tableName).isPresent())
                .orElseGet(() -> {
                    // Catalog does not support streamRelationComments
                    return ConnectorMetadata.super.streamRelationComments(session, schemaName, relationFilter);
                });
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        catalog.createNamespace(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (cascade) {
            List<String> nestedNamespaces = getChildNamespaces(session, schemaName);
            if (!nestedNamespaces.isEmpty()) {
                throw new TrinoException(
                        ICEBERG_CATALOG_ERROR,
                        format("Cannot drop non-empty schema: %s, contains %s nested schema(s)", schemaName, Joiner.on(", ").join(nestedNamespaces)));
            }

            for (SchemaTableName materializedView : listMaterializedViews(session, Optional.of(schemaName))) {
                dropMaterializedView(session, materializedView);
            }
            for (SchemaTableName viewName : listViews(session, Optional.of(schemaName))) {
                dropView(session, viewName);
            }
            for (SchemaTableName tableName : listTables(session, Optional.of(schemaName))) {
                dropTable(session, getTableHandle(session, tableName, Optional.empty(), Optional.empty()));
            }
        }
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
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        Optional<ConnectorTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout, NO_RETRIES, saveMode == SaveMode.REPLACE), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        IcebergTableHandle handle = checkValidTableHandle(tableHandle);
        catalog.updateTableComment(session, handle.getSchemaTableName(), comment);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        catalog.updateViewComment(session, viewName, comment);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        catalog.updateViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void setMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        catalog.updateMaterializedViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Schema schema = schemaFromMetadata(tableMetadata.getColumns());
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));
        return getWriteLayout(schema, partitionSpec, false);
    }

    @Override
    public Optional<io.trino.spi.type.Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, io.trino.spi.type.Type type)
    {
        io.trino.spi.type.Type newType = coerceType(type);
        if (type.getTypeSignature().equals(newType.getTypeSignature())) {
            return Optional.empty();
        }
        return Optional.of(newType);
    }

    private io.trino.spi.type.Type coerceType(io.trino.spi.type.Type type)
    {
        if (type == TINYINT || type == SMALLINT) {
            return INTEGER;
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return TIMESTAMP_TZ_MICROS;
        }
        if (type instanceof TimestampType) {
            return TIMESTAMP_MICROS;
        }
        if (type instanceof TimeType) {
            return TIME_MICROS;
        }
        if (type instanceof CharType) {
            return VARCHAR;
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayType(coerceType(arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            return new MapType(coerceType(mapType.getKeyType()), coerceType(mapType.getValueType()), typeManager.getTypeOperators());
        }
        if (type instanceof RowType rowType) {
            return RowType.from(rowType.getFields().stream()
                    .map(field -> new RowType.Field(field.getName(), coerceType(field.getType())))
                    .collect(toImmutableList()));
        }
        return type;
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        verify(transaction == null, "transaction already set");
        String schemaName = tableMetadata.getTable().getSchemaName();
        if (!schemaExists(session, schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }

        String tableLocation = null;
        if (replace) {
            ConnectorTableHandle tableHandle = getTableHandle(session, tableMetadata.getTableSchema().getTable(), Optional.empty(), Optional.empty());
            if (tableHandle != null) {
                checkValidTableHandle(tableHandle);
                IcebergTableHandle table = (IcebergTableHandle) tableHandle;
                verifyTableVersionForUpdate(table);
                Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
                Optional<String> providedTableLocation = getTableLocation(tableMetadata.getProperties());
                if (providedTableLocation.isPresent() && !stripTrailingSlash(providedTableLocation.get()).equals(icebergTable.location())) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY, format("The provided location '%s' does not match the existing table location '%s'", providedTableLocation.get(), icebergTable.location()));
                }
                validateNotModifyingOldSnapshot(table, icebergTable);
                tableLocation = icebergTable.location();
            }
        }

        if (tableLocation == null) {
            tableLocation = getTableLocation(tableMetadata.getProperties())
                    .orElseGet(() -> catalog.defaultTableLocation(session, tableMetadata.getTable()));
        }
        transaction = newCreateTableTransaction(catalog, tableMetadata, session, replace, tableLocation, allowedExtraProperties);
        Location location = Location.of(transaction.table().location());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), transaction.table().io().properties());
        try {
            if (!replace && fileSystem.listFiles(location).hasNext()) {
                throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, format("" +
                        "Cannot create a table on a non-empty location: %s, set 'iceberg.unique-table-location=true' in your Iceberg catalog properties " +
                        "to use unique table locations for every table.", location));
            }
            return newWritableTableHandle(tableMetadata.getTable(), transaction.table(), retryMode);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed checking new table's location: " + location, e);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergWritableTableHandle icebergTableHandle = (IcebergWritableTableHandle) tableHandle;
        try {
            if (fragments.isEmpty()) {
                // Commit the transaction if the table is being created without data
                AppendFiles appendFiles = transaction.newFastAppend();
                commitUpdateAndTransaction(appendFiles, session, transaction, "create table");
                transaction = null;
                return Optional.empty();
            }

            return finishInsert(session, icebergTableHandle, ImmutableList.of(), fragments, computedStatistics);
        }
        catch (AlreadyExistsException e) {
            // May happen when table has been already created concurrently.
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("Table %s already exists", icebergTableHandle.name()), e);
        }
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(
                schema,
                table.getPartitionSpecJson().orElseThrow(() -> new VerifyException("Partition spec missing in the table handle")));
        return getWriteLayout(schema, partitionSpec, false);
    }

    private Optional<ConnectorTableLayout> getWriteLayout(Schema tableSchema, PartitionSpec partitionSpec, boolean forceRepartitioning)
    {
        if (partitionSpec.isUnpartitioned()) {
            return Optional.empty();
        }

        StructType schemaAsStruct = tableSchema.asStruct();
        Map<Integer, NestedField> indexById = TypeUtil.indexById(schemaAsStruct);
        Map<Integer, Integer> indexParents = indexParents(schemaAsStruct);
        Map<Integer, List<Integer>> indexPaths = indexById.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(buildPath(indexParents, entry.getKey()))));

        List<IcebergColumnHandle> partitioningColumns = partitionSpec.fields().stream()
                .sorted(Comparator.comparing(PartitionField::sourceId))
                .map(field -> {
                    boolean isBaseColumn = !indexParents.containsKey(field.sourceId());
                    int sourceId;
                    if (isBaseColumn) {
                        sourceId = field.sourceId();
                    }
                    else {
                        sourceId = getRootFieldId(indexParents, field.sourceId());
                    }
                    Type sourceType = tableSchema.findType(sourceId);
                    // The source column, must be a primitive type and cannot be contained in a map or list, but may be nested in a struct.
                    // https://iceberg.apache.org/spec/#partitioning
                    if (sourceType.isMapType()) {
                        throw new TrinoException(NOT_SUPPORTED, "Partitioning field [" + field.name() + "] cannot be contained in a map");
                    }
                    if (sourceType.isListType()) {
                        throw new TrinoException(NOT_SUPPORTED, "Partitioning field [" + field.name() + "] cannot be contained in a array");
                    }
                    verify(indexById.containsKey(sourceId), "Cannot find source column for partition field " + field);
                    return createColumnHandle(typeManager, sourceId, indexById, indexPaths);
                })
                .distinct()
                .collect(toImmutableList());
        List<String> partitioningColumnNames = partitioningColumns.stream()
                .map(column -> column.getName().toLowerCase(ENGLISH))
                .collect(toImmutableList());

        if (!forceRepartitioning && partitionSpec.fields().stream().allMatch(field -> field.transform().isIdentity())) {
            // Do not set partitioningHandle, to let engine determine whether to repartition data or not, on stat-based basis.
            return Optional.of(new ConnectorTableLayout(partitioningColumnNames));
        }
        IcebergPartitioningHandle partitioningHandle = new IcebergPartitioningHandle(toPartitionFields(partitionSpec), partitioningColumns);
        return Optional.of(new ConnectorTableLayout(partitioningHandle, partitioningColumnNames, true));
    }

    private static int getRootFieldId(Map<Integer, Integer> indexParents, int fieldId)
    {
        int rootFieldId = fieldId;
        while (indexParents.containsKey(rootFieldId)) {
            rootFieldId = indexParents.get(rootFieldId);
        }
        return rootFieldId;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        validateNotModifyingOldSnapshot(table, icebergTable);

        beginTransaction(icebergTable);

        return newWritableTableHandle(table.getSchemaTableName(), icebergTable, retryMode);
    }

    private List<String> getChildNamespaces(ConnectorSession session, String parentNamespace)
    {
        Optional<String> namespaceSeparator = catalog.getNamespaceSeparator();

        if (namespaceSeparator.isEmpty()) {
            return ImmutableList.of();
        }

        return catalog.listNamespaces(session).stream()
                .filter(namespace -> namespace.startsWith(parentNamespace + namespaceSeparator.get()))
                .collect(toImmutableList());
    }

    private IcebergWritableTableHandle newWritableTableHandle(SchemaTableName name, Table table, RetryMode retryMode)
    {
        return new IcebergWritableTableHandle(
                name,
                SchemaParser.toJson(table.schema()),
                transformValues(table.specs(), PartitionSpecParser::toJson),
                table.spec().specId(),
                getSupportedSortFields(table.schema(), table.sortOrder()),
                getProjectedColumns(table.schema(), typeManager),
                table.location(),
                getFileFormat(table),
                table.properties(),
                retryMode,
                table.io().properties());
    }

    private static List<TrinoSortField> getSupportedSortFields(Schema schema, SortOrder sortOrder)
    {
        if (!sortOrder.isSorted()) {
            return ImmutableList.of();
        }
        Set<Integer> baseColumnFieldIds = schema.columns().stream()
                .map(Types.NestedField::fieldId)
                .collect(toImmutableSet());

        ImmutableList.Builder<TrinoSortField> sortFields = ImmutableList.builder();
        for (SortField sortField : sortOrder.fields()) {
            if (!sortField.transform().isIdentity()) {
                continue;
            }
            if (!baseColumnFieldIds.contains(sortField.sourceId())) {
                continue;
            }

            sortFields.add(TrinoSortField.fromIceberg(sortField));
        }

        return sortFields.build();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
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
        Optional<Long> beforeWriteSnapshotId = Optional.ofNullable(icebergTable.currentSnapshot()).map(Snapshot::snapshotId);
        Schema schema = icebergTable.schema();
        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        schema.findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = isMergeManifestsOnWrite(session) ? transaction.newAppend() : transaction.newFastAppend();
        ImmutableSet.Builder<String> writtenFiles = ImmutableSet.builder();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.path())
                    .withFileSizeInBytes(task.fileSizeInBytes())
                    .withFormat(table.fileFormat().toIceberg())
                    .withMetrics(task.metrics().metrics());
            task.fileSplitOffsets().ifPresent(builder::withSplitOffsets);

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
            writtenFiles.add(task.path());
        }

        // try to leave as little garbage as possible behind
        if (table.retryMode() != NO_RETRIES) {
            cleanExtraOutputFiles(session, writtenFiles.build());
        }

        commitUpdateAndTransaction(appendFiles, session, transaction, "insert");
        // TODO (https://github.com/trinodb/trino/issues/15439) this may not exactly be the snapshot we committed, if there is another writer
        long newSnapshotId = transaction.table().currentSnapshot().snapshotId();
        transaction = null;

        // TODO (https://github.com/trinodb/trino/issues/15439): it would be good to publish data and stats atomically
        beforeWriteSnapshotId.ifPresent(previous ->
                verify(previous != newSnapshotId, "Failed to get new snapshot ID"));

        if (!computedStatistics.isEmpty()) {
            try {
                beginTransaction(catalog.loadTable(session, table.name()));
                Table reloadedTable = transaction.table();
                CollectedStatistics collectedStatistics = processComputedTableStatistics(reloadedTable, computedStatistics);
                StatisticsFile statisticsFile = tableStatisticsWriter.writeStatisticsFile(
                        session,
                        reloadedTable,
                        newSnapshotId,
                        INCREMENTAL_UPDATE,
                        collectedStatistics);
                transaction.updateStatistics()
                        .setStatistics(newSnapshotId, statisticsFile)
                        .commit();

                commitTransaction(transaction, "update statistics on insert");
            }
            catch (Exception e) {
                // Write was committed, so at this point we cannot fail the query
                // TODO (https://github.com/trinodb/trino/issues/15439): it would be good to publish data and stats atomically
                log.error(e, "Failed to save table statistics");
            }
            transaction = null;
        }

        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::path)
                .collect(toImmutableList())));
    }

    private void cleanExtraOutputFiles(ConnectorSession session, Set<String> writtenFiles)
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), transaction.table().io().properties());
        Set<String> locations = getOutputFilesLocations(writtenFiles);
        Set<String> fileNames = getOutputFilesFileNames(writtenFiles);
        for (String location : locations) {
            cleanExtraOutputFiles(fileSystem, session.getQueryId(), Location.of(location), fileNames);
        }
    }

    private static void cleanExtraOutputFiles(TrinoFileSystem fileSystem, String queryId, Location location, Set<String> fileNamesToKeep)
    {
        checkArgument(!queryId.contains("-"), "query ID should not contain hyphens: %s", queryId);

        Deque<String> filesToDelete = new ArrayDeque<>();
        try {
            log.debug("Deleting failed attempt files from %s for query %s", location, queryId);

            FileIterator iterator = fileSystem.listFiles(location);
            while (iterator.hasNext()) {
                FileEntry entry = iterator.next();
                String name = entry.location().fileName();
                if (name.startsWith(queryId + "-") && !fileNamesToKeep.contains(name)) {
                    filesToDelete.add(name);
                }
            }

            if (filesToDelete.isEmpty()) {
                return;
            }

            log.info("Found %s files to delete and %s to retain in location %s for query %s", filesToDelete.size(), fileNamesToKeep.size(), location, queryId);
            ImmutableList.Builder<String> deletedFilesBuilder = ImmutableList.builder();
            List<Location> deleteBatch = new ArrayList<>();
            for (String fileName : filesToDelete) {
                deletedFilesBuilder.add(fileName);

                deleteBatch.add(location.appendPath(fileName));
                if (deleteBatch.size() >= DELETE_BATCH_SIZE) {
                    log.debug("Deleting failed attempt files %s for query %s", deleteBatch, queryId);
                    fileSystem.deleteFiles(deleteBatch);
                    deleteBatch.clear();
                }
            }

            if (!deleteBatch.isEmpty()) {
                log.debug("Deleting failed attempt files %s for query %s", deleteBatch, queryId);
                fileSystem.deleteFiles(deleteBatch);
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

    private static Set<String> getOutputFilesLocations(Set<String> writtenFiles)
    {
        return writtenFiles.stream()
                .map(IcebergMetadata::getLocation)
                .collect(toImmutableSet());
    }

    private static Set<String> getOutputFilesFileNames(Set<String> writtenFiles)
    {
        return writtenFiles.stream()
                .map(IcebergUtil::fileName)
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
            ConnectorAccessControl accessControl,
            ConnectorTableHandle connectorTableHandle,
            String procedureName,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) connectorTableHandle;
        checkArgument(tableHandle.getTableType() == DATA, "Cannot execute table procedure %s on non-DATA table: %s", procedureName, tableHandle.getTableType());
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
        if (tableHandle.getSnapshotId().isPresent() && (tableHandle.getSnapshotId().get() != icebergTable.currentSnapshot().snapshotId())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot execute table procedure %s on old snapshot %s".formatted(procedureName, tableHandle.getSnapshotId().get()));
        }

        IcebergTableProcedureId procedureId;
        try {
            procedureId = IcebergTableProcedureId.valueOf(procedureName);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown procedure '" + procedureName + "'");
        }

        return switch (procedureId) {
            case OPTIMIZE -> getTableHandleForOptimize(tableHandle, icebergTable, executeProperties, retryMode);
            case DROP_EXTENDED_STATS -> getTableHandleForDropExtendedStats(session, tableHandle);
            case EXPIRE_SNAPSHOTS -> getTableHandleForExpireSnapshots(session, tableHandle, executeProperties);
            case REMOVE_ORPHAN_FILES -> getTableHandleForRemoveOrphanFiles(session, tableHandle, executeProperties);
            case ADD_FILES -> getTableHandleForAddFiles(session, accessControl, tableHandle, executeProperties);
            case ADD_FILES_FROM_TABLE -> getTableHandleForAddFilesFromTable(session, accessControl, tableHandle, executeProperties);
        };
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForOptimize(
            IcebergTableHandle tableHandle,
            Table icebergTable,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        DataSize maxScannedFileSize = (DataSize) executeProperties.get("file_size_threshold");

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                OPTIMIZE,
                new IcebergOptimizeHandle(
                        tableHandle.getSnapshotId(),
                        tableHandle.getTableSchemaJson(),
                        tableHandle.getPartitionSpecJson().orElseThrow(() -> new VerifyException("Partition spec missing in the table handle")),
                        getProjectedColumns(SchemaParser.fromJson(tableHandle.getTableSchemaJson()), typeManager),
                        icebergTable.sortOrder().fields().stream()
                                .map(TrinoSortField::fromIceberg)
                                .collect(toImmutableList()),
                        getFileFormat(tableHandle.getStorageProperties()),
                        tableHandle.getStorageProperties(),
                        maxScannedFileSize,
                        retryMode != NO_RETRIES),
                tableHandle.getTableLocation(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForDropExtendedStats(ConnectorSession session, IcebergTableHandle tableHandle)
    {
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                DROP_EXTENDED_STATS,
                new IcebergDropExtendedStatsHandle(),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForExpireSnapshots(ConnectorSession session, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        Duration retentionThreshold = (Duration) executeProperties.get(RETENTION_THRESHOLD);
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                EXPIRE_SNAPSHOTS,
                new IcebergExpireSnapshotsHandle(retentionThreshold),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForRemoveOrphanFiles(ConnectorSession session, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        Duration retentionThreshold = (Duration) executeProperties.get(RETENTION_THRESHOLD);
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                REMOVE_ORPHAN_FILES,
                new IcebergRemoveOrphanFilesHandle(retentionThreshold),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForAddFiles(ConnectorSession session, ConnectorAccessControl accessControl, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        if (!addFilesProcedureEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "add_files procedure is disabled");
        }

        accessControl.checkCanInsertIntoTable(null, tableHandle.getSchemaTableName());

        String location = (String) requireProcedureArgument(executeProperties, "location");
        HiveStorageFormat format = (HiveStorageFormat) requireProcedureArgument(executeProperties, "format");
        RecursiveDirectory recursiveDirectory = (RecursiveDirectory) executeProperties.getOrDefault("recursive_directory", "fail");

        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                ADD_FILES,
                new IcebergAddFilesHandle(location, format, recursiveDirectory),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForAddFilesFromTable(ConnectorSession session, ConnectorAccessControl accessControl, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        accessControl.checkCanInsertIntoTable(null, tableHandle.getSchemaTableName());

        String schemaName = (String) requireProcedureArgument(executeProperties, "schema_name");
        String tableName = (String) requireProcedureArgument(executeProperties, "table_name");
        @SuppressWarnings("unchecked")
        Map<String, String> partitionFilter = (Map<String, String>) executeProperties.get("partition_filter");
        RecursiveDirectory recursiveDirectory = (RecursiveDirectory) executeProperties.getOrDefault("recursive_directory", "fail");

        HiveMetastore metastore = metastoreFactory.orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "This catalog does not support add_files_from_table procedure"))
                .createMetastore(Optional.of(session.getIdentity()));
        SchemaTableName sourceName = new SchemaTableName(schemaName, tableName);
        io.trino.metastore.Table sourceTable = metastore.getTable(schemaName, tableName).orElseThrow(() -> new TableNotFoundException(sourceName));
        accessControl.checkCanSelectFromColumns(null, sourceName, Stream.concat(sourceTable.getDataColumns().stream(), sourceTable.getPartitionColumns().stream())
                .map(Column::getName)
                .collect(toImmutableSet()));

        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        checkProcedureArgument(
                icebergTable.schemas().size() >= sourceTable.getDataColumns().size(),
                "Target table should have at least %d columns but got %d", sourceTable.getDataColumns().size(), icebergTable.schemas().size());
        checkProcedureArgument(
                icebergTable.spec().fields().size() == sourceTable.getPartitionColumns().size(),
                "Numbers of partition columns should be equivalent. target: %d, source: %d", icebergTable.spec().fields().size(), sourceTable.getPartitionColumns().size());

        // TODO Add files from all partitions when partition filter is not provided
        checkProcedureArgument(
                sourceTable.getPartitionColumns().isEmpty() || partitionFilter != null,
                "partition_filter argument must be provided for partitioned tables");

        String transactionalProperty = sourceTable.getParameters().get(TRANSACTIONAL);
        if (parseBoolean(transactionalProperty)) {
            throw new TrinoException(NOT_SUPPORTED, "Adding files from transactional tables is unsupported");
        }
        if (!"MANAGED_TABLE".equalsIgnoreCase(sourceTable.getTableType()) && !"EXTERNAL_TABLE".equalsIgnoreCase(sourceTable.getTableType())) {
            throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support adding files from %s table type".formatted(sourceTable.getTableType()));
        }
        if (isSomeKindOfAView(sourceTable) || isIcebergTable(sourceTable) || isDeltaLakeTable(sourceTable) || isHudiTable(sourceTable)) {
            throw new TrinoException(NOT_SUPPORTED, "Adding files from non-Hive tables is unsupported");
        }
        if (sourceTable.getPartitionColumns().isEmpty() && partitionFilter != null && !partitionFilter.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Partition filter is not supported for non-partitioned tables");
        }

        Set<String> missingDataColumns = new HashSet<>();
        Stream.of(sourceTable.getDataColumns(), sourceTable.getPartitionColumns())
                .flatMap(List::stream)
                .forEach(sourceColumn -> {
                    Types.NestedField targetColumn = icebergTable.schema().caseInsensitiveFindField(sourceColumn.getName());
                    if (targetColumn == null) {
                        if (sourceTable.getPartitionColumns().contains(sourceColumn)) {
                            throw new TrinoException(COLUMN_NOT_FOUND, "Partition column '%s' does not exist".formatted(sourceColumn.getName()));
                        }
                        missingDataColumns.add(sourceColumn.getName());
                        return;
                    }
                    ColumnIdentity columnIdentity = createColumnIdentity(targetColumn);
                    org.apache.iceberg.types.Type sourceColumnType = toIcebergType(typeManager.getType(getTypeSignature(sourceColumn.getType(), DEFAULT_PRECISION)), columnIdentity);
                    if (!targetColumn.type().equals(sourceColumnType)) {
                        throw new TrinoException(TYPE_MISMATCH, "Target '%s' column is '%s' type, but got source '%s' type".formatted(targetColumn.name(), targetColumn.type(), sourceColumnType));
                    }
                });
        if (missingDataColumns.size() == sourceTable.getDataColumns().size()) {
            throw new TrinoException(COLUMN_NOT_FOUND, "All columns in the source table do not exist in the target table");
        }

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                ADD_FILES_FROM_TABLE,
                new IcebergAddFilesFromTableHandle(sourceTable, partitionFilter, recursiveDirectory),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private static Object requireProcedureArgument(Map<String, Object> properties, String name)
    {
        Object value = properties.get(name);
        checkProcedureArgument(value != null, "Required procedure argument '%s' is missing", name);
        return value;
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                return getLayoutForOptimize(session, executeHandle);
            case DROP_EXTENDED_STATS:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case ADD_FILES:
            case ADD_FILES_FROM_TABLE:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private Optional<ConnectorTableLayout> getLayoutForOptimize(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        Table icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
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
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                return beginOptimize(session, executeHandle, table);
            case DROP_EXTENDED_STATS:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case ADD_FILES:
            case ADD_FILES_FROM_TABLE:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginOptimize(
            ConnectorSession session,
            IcebergTableExecuteHandle executeHandle,
            IcebergTableHandle table)
    {
        IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.procedureHandle();
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
                table.forOptimize(true, optimizeHandle.maxScannedFileSize()));
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                finishOptimize(session, executeHandle, fragments, splitSourceInfo);
                return;
            case DROP_EXTENDED_STATS:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case ADD_FILES:
            case ADD_FILES_FROM_TABLE:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private void finishOptimize(ConnectorSession session, IcebergTableExecuteHandle executeHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.procedureHandle();
        Table icebergTable = transaction.table();
        Optional<Long> beforeWriteSnapshotId = getCurrentSnapshotId(icebergTable);

        // files to be deleted
        ImmutableSet.Builder<DataFile> scannedDataFilesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<DeleteFile> scannedDeleteFilesBuilder = ImmutableSet.builder();
        splitSourceInfo.stream().map(DataFileWithDeleteFiles.class::cast).forEach(dataFileWithDeleteFiles -> {
            scannedDataFilesBuilder.add(dataFileWithDeleteFiles.dataFile());
            scannedDeleteFilesBuilder.addAll(dataFileWithDeleteFiles.deleteFiles());
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
                    .withPath(task.path())
                    .withFileSizeInBytes(task.fileSizeInBytes())
                    .withFormat(optimizeHandle.fileFormat().toIceberg())
                    .withMetrics(task.metrics().metrics());
            task.fileSplitOffsets().ifPresent(builder::withSplitOffsets);

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            newFiles.add(builder.build());
        }

        if (optimizeHandle.snapshotId().isEmpty() || scannedDataFiles.isEmpty() && fullyAppliedDeleteFiles.isEmpty() && newFiles.isEmpty()) {
            // Either the table is empty, or the table scan turned out to be empty, nothing to commit
            transaction = null;
            return;
        }

        // try to leave as little garbage as possible behind
        if (optimizeHandle.retriesEnabled()) {
            cleanExtraOutputFiles(
                    session,
                    newFiles.stream()
                            .map(dataFile -> dataFile.path().toString())
                            .collect(toImmutableSet()));
        }

        RewriteFiles rewriteFiles = transaction.newRewrite();
        scannedDataFiles.forEach(rewriteFiles::deleteFile);
        fullyAppliedDeleteFiles.forEach(rewriteFiles::deleteFile);
        newFiles.forEach(rewriteFiles::addFile);

        // Table.snapshot method returns null if there is no matching snapshot
        Snapshot snapshot = requireNonNull(icebergTable.snapshot(optimizeHandle.snapshotId().get()), "snapshot is null");
        rewriteFiles.validateFromSnapshot(snapshot.snapshotId());
        commitUpdateAndTransaction(rewriteFiles, session, transaction, "optimize");

        // TODO (https://github.com/trinodb/trino/issues/15439) this may not exactly be the snapshot we committed, if there is another writer
        long newSnapshotId = transaction.table().currentSnapshot().snapshotId();
        transaction = null;

        // TODO (https://github.com/trinodb/trino/issues/15439): it would be good to publish data and stats atomically
        beforeWriteSnapshotId.ifPresent(previous ->
                verify(previous != newSnapshotId, "Failed to get new snapshot ID"));

        try {
            beginTransaction(catalog.loadTable(session, executeHandle.schemaTableName()));
            Table reloadedTable = transaction.table();
            StatisticsFile newStatsFile = tableStatisticsWriter.rewriteStatisticsFile(session, reloadedTable, newSnapshotId);

            transaction.updateStatistics()
                    .setStatistics(newSnapshotId, newStatsFile)
                    .commit();
            commitTransaction(transaction, "update statistics after optimize");
        }
        catch (Exception e) {
            // Write was committed, so at this point we cannot fail the query
            // TODO (https://github.com/trinodb/trino/issues/15439): it would be good to publish data and stats atomically
            log.error(e, "Failed to save table statistics");
        }
        transaction = null;
    }

    private static void commitUpdateAndTransaction(SnapshotUpdate<?> update, ConnectorSession session, Transaction transaction, String operation)
    {
        try {
            commit(update, session);
            commitTransaction(transaction, operation);
        }
        catch (UncheckedIOException | ValidationException | CommitFailedException | CommitStateUnknownException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Failed to commit during %s: %s", operation, firstNonNull(e.getMessage(), e)), e);
        }
    }

    private static void commitTransaction(Transaction transaction, String operation)
    {
        try {
            transaction.commitTransaction();
        }
        catch (ValidationException | CommitFailedException | CommitStateUnknownException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Failed to commit the transaction during %s: %s", operation, firstNonNull(e.getMessage(), e)), e);
        }
    }

    @Override
    public void executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case DROP_EXTENDED_STATS:
                executeDropExtendedStats(session, executeHandle);
                return;
            case EXPIRE_SNAPSHOTS:
                executeExpireSnapshots(session, executeHandle);
                return;
            case REMOVE_ORPHAN_FILES:
                executeRemoveOrphanFiles(session, executeHandle);
                return;
            case ADD_FILES:
                executeAddFiles(session, executeHandle);
                return;
            case ADD_FILES_FROM_TABLE:
                executeAddFilesFromTable(session, executeHandle);
                return;
            default:
                throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
        }
    }

    private void executeDropExtendedStats(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        checkArgument(executeHandle.procedureHandle() instanceof IcebergDropExtendedStatsHandle, "Unexpected procedure handle %s", executeHandle.procedureHandle());

        Table icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
        beginTransaction(icebergTable);
        UpdateStatistics updateStatistics = transaction.updateStatistics();
        for (StatisticsFile statisticsFile : icebergTable.statisticsFiles()) {
            updateStatistics.removeStatistics(statisticsFile.snapshotId());
        }
        updateStatistics.commit();
        commitTransaction(transaction, "drop extended stats");
        transaction = null;
    }

    private void executeExpireSnapshots(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergExpireSnapshotsHandle expireSnapshotsHandle = (IcebergExpireSnapshotsHandle) executeHandle.procedureHandle();

        Table table = catalog.loadTable(session, executeHandle.schemaTableName());
        Duration retention = requireNonNull(expireSnapshotsHandle.retentionThreshold(), "retention is null");
        validateTableExecuteParameters(
                table,
                executeHandle.schemaTableName(),
                EXPIRE_SNAPSHOTS.name(),
                retention,
                getExpireSnapshotMinRetention(session),
                IcebergConfig.EXPIRE_SNAPSHOTS_MIN_RETENTION,
                IcebergSessionProperties.EXPIRE_SNAPSHOTS_MIN_RETENTION);

        long expireTimestampMillis = session.getStart().toEpochMilli() - retention.toMillis();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), table.io().properties());
        List<Location> pathsToDelete = new ArrayList<>();
        // deleteFunction is not accessed from multiple threads unless .executeDeleteWith() is used
        Consumer<String> deleteFunction = path -> {
            pathsToDelete.add(Location.of(path));
            if (pathsToDelete.size() == DELETE_BATCH_SIZE) {
                try {
                    fileSystem.deleteFiles(pathsToDelete);
                    pathsToDelete.clear();
                }
                catch (IOException e) {
                    throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to delete files during snapshot expiration", e);
                }
            }
        };

        try {
            table.expireSnapshots()
                    .expireOlderThan(expireTimestampMillis)
                    .deleteWith(deleteFunction)
                    .commit();

            fileSystem.deleteFiles(pathsToDelete);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed to delete files during snapshot expiration", e);
        }
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

    public void executeRemoveOrphanFiles(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergRemoveOrphanFilesHandle removeOrphanFilesHandle = (IcebergRemoveOrphanFilesHandle) executeHandle.procedureHandle();

        Table table = catalog.loadTable(session, executeHandle.schemaTableName());
        Duration retention = requireNonNull(removeOrphanFilesHandle.retentionThreshold(), "retention is null");
        validateTableExecuteParameters(
                table,
                executeHandle.schemaTableName(),
                REMOVE_ORPHAN_FILES.name(),
                retention,
                getRemoveOrphanFilesMinRetention(session),
                IcebergConfig.REMOVE_ORPHAN_FILES_MIN_RETENTION,
                IcebergSessionProperties.REMOVE_ORPHAN_FILES_MIN_RETENTION);

        if (table.currentSnapshot() == null) {
            log.debug("Skipping remove_orphan_files procedure for empty table %s", table);
            return;
        }

        Instant expiration = session.getStart().minusMillis(retention.toMillis());
        removeOrphanFiles(table, session, executeHandle.schemaTableName(), expiration, executeHandle.fileIoProperties());
    }

    private void removeOrphanFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, Instant expiration, Map<String, String> fileIoProperties)
    {
        Set<String> processedManifestFilePaths = new HashSet<>();
        // Similarly to issues like https://github.com/trinodb/trino/issues/13759, equivalent paths may have different String
        // representations due to things like double slashes. Using file names may result in retaining files which could be removed.
        // However, in practice Iceberg metadata and data files have UUIDs in their names which makes this unlikely.
        ImmutableSet.Builder<String> validMetadataFileNames = ImmutableSet.builder();
        ImmutableSet.Builder<String> validDataFileNames = ImmutableSet.builder();

        for (Snapshot snapshot : table.snapshots()) {
            if (snapshot.manifestListLocation() != null) {
                validMetadataFileNames.add(fileName(snapshot.manifestListLocation()));
            }

            for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                if (!processedManifestFilePaths.add(manifest.path())) {
                    // Already read this manifest
                    continue;
                }

                validMetadataFileNames.add(fileName(manifest.path()));
                try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(table, manifest)) {
                    for (ContentFile<?> contentFile : manifestReader) {
                        validDataFileNames.add(fileName(contentFile.path().toString()));
                    }
                }
                catch (IOException e) {
                    throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
                }
            }
        }

        metadataFileLocations(table, false).stream()
                .map(IcebergUtil::fileName)
                .forEach(validMetadataFileNames::add);

        statisticsFilesLocations(table).stream()
                .map(IcebergUtil::fileName)
                .forEach(validMetadataFileNames::add);

        validMetadataFileNames.add("version-hint.text");

        scanAndDeleteInvalidFiles(table, session, schemaTableName, expiration, validDataFileNames.build(), "data", fileIoProperties);
        scanAndDeleteInvalidFiles(table, session, schemaTableName, expiration, validMetadataFileNames.build(), "metadata", fileIoProperties);
    }

    public void executeAddFiles(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergAddFilesHandle addFilesHandle = (IcebergAddFilesHandle) executeHandle.procedureHandle();
        Table table = catalog.loadTable(session, executeHandle.schemaTableName());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), table.io().properties());
        addFiles(
                session,
                fileSystem,
                catalog,
                executeHandle.schemaTableName(),
                addFilesHandle.location(),
                addFilesHandle.format(),
                addFilesHandle.recursiveDirectory());
    }

    public void executeAddFilesFromTable(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergAddFilesFromTableHandle addFilesHandle = (IcebergAddFilesFromTableHandle) executeHandle.procedureHandle();
        Table table = catalog.loadTable(session, executeHandle.schemaTableName());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), table.io().properties());
        addFilesFromTable(
                session,
                fileSystem,
                metastoreFactory.orElseThrow(),
                table,
                addFilesHandle.table(),
                addFilesHandle.partitionFilter(),
                addFilesHandle.recursiveDirectory());
    }

    private static ManifestReader<? extends ContentFile<?>> readerForManifest(Table table, ManifestFile manifest)
    {
        return switch (manifest.content()) {
            case DATA -> ManifestFiles.read(manifest, table.io());
            case DELETES -> ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs());
        };
    }

    private void scanAndDeleteInvalidFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, Instant expiration, Set<String> validFiles, String subfolder, Map<String, String> fileIoProperties)
    {
        try {
            List<Location> filesToDelete = new ArrayList<>();
            TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), fileIoProperties);
            FileIterator allFiles = fileSystem.listFiles(Location.of(table.location()).appendPath(subfolder));
            while (allFiles.hasNext()) {
                FileEntry entry = allFiles.next();
                if (entry.lastModified().isBefore(expiration) && !validFiles.contains(entry.location().fileName())) {
                    filesToDelete.add(entry.location());
                    if (filesToDelete.size() >= DELETE_BATCH_SIZE) {
                        log.debug("Deleting files while removing orphan files for table %s [%s]", schemaTableName, filesToDelete);
                        fileSystem.deleteFiles(filesToDelete);
                        filesToDelete.clear();
                    }
                }
                else {
                    log.debug("%s file retained while removing orphan files %s", entry.location(), schemaTableName.getTableName());
                }
            }
            if (!filesToDelete.isEmpty()) {
                log.debug("Deleting files while removing orphan files for table %s %s", schemaTableName, filesToDelete);
                fileSystem.deleteFiles(filesToDelete);
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
        Optional<Boolean> partitioned = icebergTableHandle.getPartitionSpecJson()
                .map(partitionSpecJson -> PartitionSpecParser.fromJson(SchemaParser.fromJson(icebergTableHandle.getTableSchemaJson()), partitionSpecJson).isPartitioned());

        return Optional.of(new IcebergInputInfo(
                icebergTableHandle.getSnapshotId(),
                partitioned,
                getFileFormat(icebergTableHandle.getStorageProperties()).name()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof CorruptedIcebergTableHandle corruptedTableHandle) {
            catalog.dropCorruptedTable(session, corruptedTableHandle.schemaTableName());
        }
        else {
            catalog.dropTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle handle = checkValidTableHandle(tableHandle);
        catalog.renameTable(session, handle.getSchemaTableName(), newTable);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        Set<String> unsupportedProperties = difference(properties.keySet(), UPDATABLE_TABLE_PROPERTIES);
        if (!unsupportedProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The following properties cannot be updated: " + String.join(", ", unsupportedProperties));
        }

        beginTransaction(icebergTable);
        UpdateProperties updateProperties = transaction.updateProperties();

        if (properties.containsKey(EXTRA_PROPERTIES_PROPERTY)) {
            //noinspection unchecked
            Map<String, String> extraProperties = (Map<String, String>) properties.get(EXTRA_PROPERTIES_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The extra_properties property cannot be empty"));
            verifyExtraProperties(properties.keySet(), extraProperties, allowedExtraProperties);
            extraProperties.forEach(updateProperties::set);
        }

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

        if (properties.containsKey(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY)) {
            boolean objectStoreEnabled = (boolean) properties.get(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The object_store_enabled property cannot be empty"));
            updateProperties.set(OBJECT_STORE_ENABLED, Boolean.toString(objectStoreEnabled));
        }

        if (properties.containsKey(DATA_LOCATION_PROPERTY)) {
            String dataLocation = (String) properties.get(DATA_LOCATION_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The data_location property cannot be empty"));
            boolean objectStoreEnabled = (boolean) properties.getOrDefault(
                    OBJECT_STORE_LAYOUT_ENABLED_PROPERTY,
                    Optional.of(Boolean.parseBoolean(icebergTable.properties().get(OBJECT_STORE_ENABLED)))).orElseThrow();
            if (!objectStoreEnabled) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Data location can only be set when object store layout is enabled");
            }
            updateProperties.set(WRITE_DATA_LOCATION, dataLocation);
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

        if (properties.containsKey(SORTED_BY_PROPERTY)) {
            @SuppressWarnings("unchecked")
            List<String> sortColumns = (List<String>) properties.get(SORTED_BY_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The sorted_by property cannot be empty"));
            ReplaceSortOrder replaceSortOrder = transaction.replaceSortOrder();
            parseSortFields(replaceSortOrder, sortColumns);
            try {
                replaceSortOrder.commit();
            }
            catch (RuntimeException e) {
                throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set the sorted_by property", e);
            }
        }

        commitTransaction(transaction, "set table properties");
    }

    private static void updatePartitioning(Table icebergTable, Transaction transaction, List<String> partitionColumns)
    {
        UpdatePartitionSpec updatePartitionSpec = transaction.updateSpec();
        Set<PartitionField> existingPartitionFields = ImmutableSet.copyOf(icebergTable.spec().fields());
        Schema schema = icebergTable.schema();
        if (partitionColumns.isEmpty()) {
            existingPartitionFields.stream()
                    .map(partitionField -> toIcebergTerm(schema, partitionField))
                    .forEach(updatePartitionSpec::removeField);
        }
        else {
            PartitionSpec partitionSpec = parsePartitionFields(schema, partitionColumns);
            Set<PartitionField> partitionFields = ImmutableSet.copyOf(partitionSpec.fields());
            difference(existingPartitionFields, partitionFields).stream()
                    .map(PartitionField::name)
                    .forEach(updatePartitionSpec::removeField);
            difference(partitionFields, existingPartitionFields)
                    .forEach(partitionField -> updatePartitionSpec.addField(partitionField.name(), toIcebergTerm(schema, partitionField)));
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
        // Spark doesn't support adding a NOT NULL column to Iceberg tables
        // Also, Spark throws an exception when reading the table if we add such columns and execute a rollback procedure
        // because they keep returning the latest table definition even after the rollback https://github.com/apache/iceberg/issues/5591
        // Even when a table is empty, this connector doesn't support adding not null columns to avoid the above Spark failure
        if (!column.isNullable()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding not null columns");
        }
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        // Start explicitly with highestFieldId + 2 to account for existing columns and the new one being
        // added - instead of relying on addColumn in iceberg library to assign Ids
        AtomicInteger nextFieldId = new AtomicInteger(icebergTable.schema().highestFieldId() + 2);
        try {
            icebergTable.updateSchema()
                    .addColumn(column.getName(), toIcebergTypeForNewColumn(column.getType(), nextFieldId), column.getComment())
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to add column: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void addField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> parentPath, String fieldName, io.trino.spi.type.Type type, boolean ignoreExisting)
    {
        // Iceberg disallows ambiguous field names in a table. e.g. (a row(b int), "a.b" int)
        String parentName = String.join(".", parentPath);

        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        NestedField parent = icebergTable.schema().caseInsensitiveFindField(parentName);

        String caseSensitiveParentName = icebergTable.schema().findColumnName(parent.fieldId());

        Types.StructType structType;
        if (parent.type().isListType()) {
            // list(struct...)
            structType = parent.type().asListType().elementType().asStructType();
        }
        else {
            // just struct
            structType = parent.type().asStructType();
        }

        NestedField field = structType.caseInsensitiveField(fieldName);
        if (field != null) {
            if (ignoreExisting) {
                return;
            }
            throw new TrinoException(COLUMN_ALREADY_EXISTS, "Field '%s' already exists".formatted(fieldName));
        }

        try {
            icebergTable.updateSchema()
                    .addColumn(caseSensitiveParentName, fieldName, toIcebergTypeForNewColumn(type, new AtomicInteger())) // Iceberg library assigns fresh id internally
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to add field: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        dropField(session, tableHandle, handle.getName());
    }

    @Override
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        // Iceberg disallows ambiguous field names in a table. e.g. (a row(b int), "a.b" int)
        String name = String.join(".", ImmutableList.<String>builder().add(handle.getName()).addAll(fieldPath).build());
        dropField(session, tableHandle, name);
    }

    private void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, String name)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        long fieldId = icebergTable.schema().findField(name).fieldId();
        boolean isPartitionColumn = icebergTable.spec().fields().stream()
                .anyMatch(field -> field.sourceId() == fieldId);
        if (isPartitionColumn) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop partition field: " + name);
        }
        int currentSpecId = icebergTable.spec().specId();
        boolean columnUsedInOlderPartitionSpecs = icebergTable.specs().entrySet().stream()
                .filter(spec -> spec.getValue().specId() != currentSpecId)
                .flatMap(spec -> spec.getValue().fields().stream())
                .anyMatch(field -> field.sourceId() == fieldId);
        if (columnUsedInOlderPartitionSpecs) {
            // After dropping a column which was used in older partition specs, insert/update/select fails on the table.
            // So restricting user to dropping that column. https://github.com/trinodb/trino/issues/15729
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop column which is used by an old partition spec: " + name);
        }
        try {
            icebergTable.updateSchema()
                    .deleteColumn(name)
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to drop column: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) source;
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        try {
            icebergTable.updateSchema()
                    .renameColumn(columnHandle.getName(), target)
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to rename column: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void renameField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, String target)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        String parentPath = String.join(".", fieldPath.subList(0, fieldPath.size() - 1));
        NestedField parent = icebergTable.schema().caseInsensitiveFindField(parentPath);

        String caseSensitiveParentName = icebergTable.schema().findColumnName(parent.fieldId());
        NestedField source = parent.type().asStructType().caseInsensitiveField(getLast(fieldPath));

        String sourcePath = caseSensitiveParentName + "." + source.name();
        try {
            icebergTable.updateSchema()
                    .renameColumn(sourcePath, target)
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to rename field: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, io.trino.spi.type.Type type)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;
        verify(column.isBaseColumn(), "Cannot change nested field types");

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        Type sourceType = icebergTable.schema().findType(column.getName());
        AtomicInteger nextFieldId = new AtomicInteger(1);
        Type newType = toIcebergTypeForNewColumn(type, nextFieldId);
        try {
            UpdateSchema schemaUpdate = icebergTable.updateSchema();
            buildUpdateSchema(column.getName(), sourceType, newType, schemaUpdate);
            schemaUpdate.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set column type: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    private static void buildUpdateSchema(String name, Type sourceType, Type newType, UpdateSchema schemaUpdate)
    {
        if (sourceType.equals(newType)) {
            return;
        }
        if (sourceType.isPrimitiveType() && newType.isPrimitiveType()) {
            schemaUpdate.updateColumn(name, newType.asPrimitiveType());
            return;
        }
        if (sourceType instanceof StructType sourceRowType && newType instanceof StructType newRowType) {
            // Add, update or delete fields
            List<NestedField> fields = Streams.concat(sourceRowType.fields().stream(), newRowType.fields().stream())
                    .distinct()
                    .collect(toImmutableList());
            for (NestedField field : fields) {
                if (fieldExists(sourceRowType, field.name()) && fieldExists(newRowType, field.name())) {
                    buildUpdateSchema(name + "." + field.name(), sourceRowType.fieldType(field.name()), newRowType.fieldType(field.name()), schemaUpdate);
                }
                else if (fieldExists(newRowType, field.name())) {
                    schemaUpdate.addColumn(name, field.name(), field.type());
                }
                else {
                    schemaUpdate.deleteColumn(name + "." + field.name());
                }
            }

            // Order fields based on the new column type
            String currentName = null;
            for (NestedField field : newRowType.fields()) {
                String path = name + "." + field.name();
                if (currentName == null) {
                    schemaUpdate.moveFirst(path);
                }
                else {
                    schemaUpdate.moveAfter(path, currentName);
                }
                currentName = path;
            }

            return;
        }
        throw new IllegalArgumentException("Cannot change type from %s to %s".formatted(sourceType, newType));
    }

    private static boolean fieldExists(StructType structType, String fieldName)
    {
        for (NestedField field : structType.fields()) {
            if (field.name().equals(fieldName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setFieldType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, io.trino.spi.type.Type type)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        String parentPath = String.join(".", fieldPath.subList(0, fieldPath.size() - 1));
        NestedField parent = icebergTable.schema().caseInsensitiveFindField(parentPath);

        String caseSensitiveParentName = icebergTable.schema().findColumnName(parent.fieldId());

        Types.StructType structType;
        if (parent.type().isListType()) {
            // list(struct...)
            structType = parent.type().asListType().elementType().asStructType();
            caseSensitiveParentName += ".element";
        }
        else {
            // just struct
            structType = parent.type().asStructType();
        }
        NestedField field = structType.caseInsensitiveField(getLast(fieldPath));

        // TODO: Add support for changing non-primitive field type
        if (!field.type().isPrimitiveType()) {
            throw new TrinoException(NOT_SUPPORTED, "Iceberg doesn't support changing field type from non-primitive types");
        }

        String name = caseSensitiveParentName + "." + field.name();
        // Pass dummy AtomicInteger. The field id will be discarded because the subsequent logic disallows non-primitive types.
        Type icebergType = toIcebergTypeForNewColumn(type, new AtomicInteger());
        if (!icebergType.isPrimitiveType()) {
            throw new TrinoException(NOT_SUPPORTED, "Iceberg doesn't support changing field type to non-primitive types");
        }
        try {
            icebergTable.updateSchema()
                    .updateColumn(name, icebergType.asPrimitiveType())
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set field type: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        verify(column.isBaseColumn(), "Cannot drop a not null constraint on nested fields");

        try {
            icebergTable.updateSchema()
                    .makeColumnOptional(column.getName())
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to drop a not null constraint: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!isExtendedStatisticsEnabled(session) || !isCollectExtendedStatisticsOnWrite(session)) {
            return TableStatisticsMetadata.empty();
        }

        ConnectorTableHandle tableHandle = getTableHandle(session, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        if (tableHandle == null) {
            // Assume new table (CTAS), collect NDV stats on all columns
            return getStatisticsCollectionMetadata(tableMetadata, Optional.empty(), availableColumnNames -> {});
        }
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        if (table.getSnapshotId().isEmpty()) {
            // Table has no data (empty, or wiped out). Collect NDV stats on all columns
            return getStatisticsCollectionMetadata(tableMetadata, Optional.empty(), availableColumnNames -> {});
        }

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        long snapshotId = table.getSnapshotId().orElseThrow();
        Snapshot snapshot = icebergTable.snapshot(snapshotId);
        String totalRecords = snapshot.summary().get(TOTAL_RECORDS_PROP);
        if (totalRecords != null && Long.parseLong(totalRecords) == 0) {
            // Table has no data (empty, or wiped out). Collect NDV stats on all columns
            return getStatisticsCollectionMetadata(tableMetadata, Optional.empty(), availableColumnNames -> {});
        }

        Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());
        List<IcebergColumnHandle> columns = getTopLevelColumns(schema, typeManager);
        Set<Integer> columnIds = columns.stream()
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());
        Map<Integer, Long> ndvs = readNdvs(icebergTable, snapshotId, columnIds, true);
        // Avoid collecting NDV stats on columns where we don't know the existing NDV count
        Set<String> columnsWithExtendedStatistics = columns.stream()
                .filter(column -> ndvs.containsKey(column.getId()))
                .map(IcebergColumnHandle::getName)
                .collect(toImmutableSet());
        return getStatisticsCollectionMetadata(tableMetadata, Optional.of(columnsWithExtendedStatistics), availableColumnNames -> {});
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        IcebergTableHandle handle = checkValidTableHandle(tableHandle);
        if (!isExtendedStatisticsEnabled(session)) {
            throw new TrinoException(NOT_SUPPORTED, "Analyze is not enabled. You can enable analyze using %s config or %s catalog session property".formatted(
                    IcebergConfig.EXTENDED_STATISTICS_CONFIG,
                    IcebergSessionProperties.EXTENDED_STATISTICS_ENABLED));
        }

        checkArgument(handle.getTableType() == DATA, "Cannot analyze non-DATA table: %s", handle.getTableType());

        if (handle.getSnapshotId().isEmpty()) {
            // No snapshot, table is empty
            return new ConnectorAnalyzeMetadata(tableHandle, TableStatisticsMetadata.empty());
        }

        ConnectorTableMetadata tableMetadata = getTableMetadata(session, handle);
        Optional<Set<String>> analyzeColumnNames = getColumnNames(analyzeProperties)
                .map(columnNames -> {
                    // validate that proper column names are passed via `columns` analyze property
                    if (columnNames.isEmpty()) {
                        throw new TrinoException(INVALID_ANALYZE_PROPERTY, "Cannot specify empty list of columns for analysis");
                    }
                    return columnNames;
                });

        return new ConnectorAnalyzeMetadata(
                handle.forAnalyze(),
                getStatisticsCollectionMetadata(
                        tableMetadata,
                        analyzeColumnNames,
                        availableColumnNames -> {
                            throw new TrinoException(
                                    INVALID_ANALYZE_PROPERTY,
                                    format("Invalid columns specified for analysis: %s", Sets.difference(analyzeColumnNames.orElseThrow(), availableColumnNames)));
                        }));
    }

    private TableStatisticsMetadata getStatisticsCollectionMetadata(
            ConnectorTableMetadata tableMetadata,
            Optional<Set<String>> selectedColumnNames,
            Consumer<Set<String>> unsatisfiableSelectedColumnsHandler)
    {
        Set<String> allScalarColumnNames = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .filter(column -> column.getType().getTypeParameters().isEmpty()) // is scalar type
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());

        selectedColumnNames.ifPresent(columnNames -> {
            if (!allScalarColumnNames.containsAll(columnNames)) {
                unsatisfiableSelectedColumnsHandler.accept(allScalarColumnNames);
            }
        });

        Set<ColumnStatisticMetadata> columnStatistics = tableMetadata.getColumns().stream()
                .filter(columnMetadata -> allScalarColumnNames.contains(columnMetadata.getName()))
                .filter(selectedColumnNames
                        .map(columnNames -> (Predicate<ColumnMetadata>) columnMetadata -> columnNames.contains(columnMetadata.getName()))
                        .orElse(columnMetadata -> true))
                .map(column -> new ColumnStatisticMetadata(column.getName(), NUMBER_OF_DISTINCT_VALUES_NAME, NUMBER_OF_DISTINCT_VALUES_FUNCTION))
                .collect(toImmutableSet());

        return new TableStatisticsMetadata(columnStatistics, ImmutableSet.of(), ImmutableList.of());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, handle.getSchemaTableName());
        beginTransaction(icebergTable);
        return handle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table table = transaction.table();
        if (handle.getSnapshotId().isEmpty()) {
            // No snapshot, table is empty
            verify(
                    computedStatistics.isEmpty(),
                    "Unexpected computed statistics that cannot be attached to a snapshot because none exists: %s",
                    computedStatistics);

            commitTransaction(transaction, "statistics collection");
            transaction = null;
            return;
        }
        long snapshotId = handle.getSnapshotId().orElseThrow();

        CollectedStatistics collectedStatistics = processComputedTableStatistics(table, computedStatistics);
        StatisticsFile statisticsFile = tableStatisticsWriter.writeStatisticsFile(
                session,
                table,
                snapshotId,
                REPLACE,
                collectedStatistics);
        transaction.updateStatistics()
                .setStatistics(snapshotId, statisticsFile)
                .commit();

        commitTransaction(transaction, "statistics collection");
        transaction = null;
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        TupleDomain<IcebergColumnHandle> medataColumnPredicate = table.getEnforcedPredicate().filter((column, domain) -> isMetadataColumnId(column.getId()));
        if (!medataColumnPredicate.isAll()) {
            return Optional.empty();
        }
        return Optional.of(handle);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        StructType type = StructType.of(ImmutableList.<NestedField>builder()
                .add(MetadataColumns.FILE_PATH)
                .add(MetadataColumns.ROW_POSITION)
                .add(NestedField.required(TRINO_MERGE_PARTITION_SPEC_ID, "partition_spec_id", IntegerType.get()))
                .add(NestedField.required(TRINO_MERGE_PARTITION_DATA, "partition_data", StringType.get()))
                .build());

        NestedField field = NestedField.required(TRINO_MERGE_ROW_ID, TRINO_ROW_ID_NAME, type);
        return getColumnHandle(field, typeManager);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return Optional.of(IcebergUpdateHandle.INSTANCE);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        verifyTableVersionForUpdate(table);

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        validateNotModifyingOldSnapshot(table, icebergTable);

        beginTransaction(icebergTable);

        IcebergWritableTableHandle insertHandle = newWritableTableHandle(table.getSchemaTableName(), icebergTable, retryMode);
        return new IcebergMergeTableHandle(table, insertHandle);
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergMergeTableHandle mergeHandle = (IcebergMergeTableHandle) mergeTableHandle;
        IcebergTableHandle handle = mergeHandle.getTableHandle();
        RetryMode retryMode = mergeHandle.getInsertTableHandle().retryMode();
        finishWrite(session, handle, fragments, retryMode);
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
            throw new TrinoException(NOT_SUPPORTED, "Modifying old snapshot is not supported in Iceberg");
        }
    }

    private void finishWrite(ConnectorSession session, IcebergTableHandle table, Collection<Slice> fragments, RetryMode retryMode)
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

        RowDelta rowDelta = transaction.newRowDelta();
        table.getSnapshotId().map(icebergTable::snapshot).ifPresent(s -> rowDelta.validateFromSnapshot(s.snapshotId()));
        TupleDomain<IcebergColumnHandle> dataColumnPredicate = table.getEnforcedPredicate().filter((column, domain) -> !isMetadataColumnId(column.getId()));
        if (!dataColumnPredicate.isAll()) {
            rowDelta.conflictDetectionFilter(toIcebergExpression(dataColumnPredicate));
        }
        IsolationLevel isolationLevel = IsolationLevel.fromName(icebergTable.properties().getOrDefault(DELETE_ISOLATION_LEVEL, DELETE_ISOLATION_LEVEL_DEFAULT));
        if (isolationLevel == IsolationLevel.SERIALIZABLE) {
            rowDelta.validateNoConflictingDataFiles();
        }

        // Ensure a row that is updated by this commit was not deleted by a separate commit
        rowDelta.validateDeletedFiles();
        rowDelta.validateNoConflictingDeleteFiles();

        ImmutableSet.Builder<String> writtenFiles = ImmutableSet.builder();
        ImmutableSet.Builder<String> referencedDataFiles = ImmutableSet.builder();
        for (CommitTaskData task : commitTasks) {
            PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, task.partitionSpecJson());
            Type[] partitionColumnTypes = partitionSpec.fields().stream()
                    .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
                    .toArray(Type[]::new);
            switch (task.content()) {
                case POSITION_DELETES -> {
                    FileMetadata.Builder deleteBuilder = FileMetadata.deleteFileBuilder(partitionSpec)
                            .withPath(task.path())
                            .withFormat(task.fileFormat().toIceberg())
                            .ofPositionDeletes()
                            .withFileSizeInBytes(task.fileSizeInBytes())
                            .withMetrics(task.metrics().metrics());
                    task.fileSplitOffsets().ifPresent(deleteBuilder::withSplitOffsets);
                    if (!partitionSpec.fields().isEmpty()) {
                        String partitionDataJson = task.partitionDataJson()
                                .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                        deleteBuilder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                    }
                    rowDelta.addDeletes(deleteBuilder.build());
                    writtenFiles.add(task.path());
                    task.referencedDataFile().ifPresent(referencedDataFiles::add);
                }
                case DATA -> {
                    DataFiles.Builder builder = DataFiles.builder(partitionSpec)
                            .withPath(task.path())
                            .withFormat(task.fileFormat().toIceberg())
                            .withFileSizeInBytes(task.fileSizeInBytes())
                            .withMetrics(task.metrics().metrics());
                    if (!icebergTable.spec().fields().isEmpty()) {
                        String partitionDataJson = task.partitionDataJson()
                                .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                        builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                    }
                    rowDelta.addRows(builder.build());
                    writtenFiles.add(task.path());
                }
                default -> throw new UnsupportedOperationException("Unsupported task content: " + task.content());
            }
        }

        // try to leave as little garbage as possible behind
        if (retryMode != NO_RETRIES) {
            cleanExtraOutputFiles(session, writtenFiles.build());
        }

        rowDelta.validateDataFilesExist(referencedDataFiles.build());
        commitUpdateAndTransaction(rowDelta, session, transaction, "write");
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        checkArgument(viewProperties.isEmpty(), "This connector does not support creating views with properties");
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
    public boolean isView(ConnectorSession session, SchemaTableName viewName)
    {
        try {
            return catalog.getView(session, viewName).isPresent();
        }
        catch (TrinoException e) {
            if (e.getErrorCode() == ICEBERG_UNSUPPORTED_VIEW_DIALECT.toErrorCode()) {
                return true;
            }
            throw e;
        }
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

        DeleteFiles deleteFiles = icebergTable.newDelete()
                .deleteFromRowFilter(toIcebergExpression(handle.getEnforcedPredicate()));
        commit(deleteFiles, session);

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

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        DeleteFiles deleteFiles = icebergTable.newDelete()
                .deleteFromRowFilter(alwaysTrue());
        commit(deleteFiles, session);
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }
        if (!table.getUnenforcedPredicate().isAll()) {
            return Optional.empty();
        }

        table = new IcebergTableHandle(
                table.getCatalog(),
                table.getSchemaName(),
                table.getTableName(),
                table.getTableType(),
                table.getSnapshotId(),
                table.getTableSchemaJson(),
                table.getPartitionSpecJson(),
                table.getFormatVersion(),
                table.getUnenforcedPredicate(), // known to be ALL
                table.getEnforcedPredicate(),
                OptionalLong.of(limit),
                table.getProjectedColumns(),
                table.getNameMappingJson(),
                table.getTableLocation(),
                table.getStorageProperties(),
                table.isRecordScannedFiles(),
                table.getMaxScannedFileSize(),
                table.getConstraintColumns(),
                table.getForAnalyze());

        return Optional.of(new LimitApplicationResult<>(table, false, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        UtcConstraintExtractor.ExtractionResult extractionResult = extractTupleDomain(constraint);
        TupleDomain<IcebergColumnHandle> predicate = extractionResult.tupleDomain()
                .transformKeys(IcebergColumnHandle.class::cast);
        if (predicate.isAll() && constraint.getPredicateColumns().isEmpty()) {
            return Optional.empty();
        }
        if (table.getLimit().isPresent()) {
            // TODO we probably can allow predicate pushdown after we accepted limit. Currently, this is theoretical because we don't enforce limit, so
            //  LimitNode remains above TableScan, and there is no "push filter through limit" optimization.
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

            Set<Integer> partitionSpecIds = table.getSnapshotId().map(
                            snapshot -> icebergTable.snapshot(snapshot).allManifests(icebergTable.io()).stream()
                                    .map(ManifestFile::partitionSpecId)
                                    .collect(toImmutableSet()))
                    // No snapshot, so no data. This case doesn't matter.
                    .orElseGet(() -> ImmutableSet.copyOf(icebergTable.specs().keySet()));

            Map<IcebergColumnHandle, Domain> unsupported = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> newEnforced = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> newUnenforced = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> domains = predicate.getDomains().orElseThrow(() -> new VerifyException("No domains"));
            domains.forEach((columnHandle, domain) -> {
                if (!isConvertableToIcebergExpression(domain)) {
                    unsupported.put(columnHandle, domain);
                }
                else if (canEnforceColumnConstraintInSpecs(typeManager.getTypeOperators(), icebergTable, partitionSpecIds, columnHandle, domain)) {
                    newEnforced.put(columnHandle, domain);
                }
                else if (isMetadataColumnId(columnHandle.getId())) {
                    if (columnHandle.isPathColumn() || columnHandle.isFileModifiedTimeColumn()) {
                        newEnforced.put(columnHandle, domain);
                    }
                    else {
                        unsupported.put(columnHandle, domain);
                    }
                }
                else {
                    newUnenforced.put(columnHandle, domain);
                }
            });

            newEnforcedConstraint = TupleDomain.withColumnDomains(newEnforced).intersect(table.getEnforcedPredicate());
            newUnenforcedConstraint = TupleDomain.withColumnDomains(newUnenforced).intersect(table.getUnenforcedPredicate());
            remainingConstraint = TupleDomain.withColumnDomains(newUnenforced).intersect(TupleDomain.withColumnDomains(unsupported));
        }

        Set<IcebergColumnHandle> newConstraintColumns = Streams.concat(
                        table.getConstraintColumns().stream(),
                        constraint.getPredicateColumns().orElseGet(ImmutableSet::of).stream()
                                .map(columnHandle -> (IcebergColumnHandle) columnHandle))
                .collect(toImmutableSet());

        if (newEnforcedConstraint.equals(table.getEnforcedPredicate())
                && newUnenforcedConstraint.equals(table.getUnenforcedPredicate())
                && newConstraintColumns.equals(table.getConstraintColumns())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                new IcebergTableHandle(
                        table.getCatalog(),
                        table.getSchemaName(),
                        table.getTableName(),
                        table.getTableType(),
                        table.getSnapshotId(),
                        table.getTableSchemaJson(),
                        table.getPartitionSpecJson(),
                        table.getFormatVersion(),
                        newUnenforcedConstraint,
                        newEnforcedConstraint,
                        table.getLimit(),
                        table.getProjectedColumns(),
                        table.getNameMappingJson(),
                        table.getTableLocation(),
                        table.getStorageProperties(),
                        table.isRecordScannedFiles(),
                        table.getMaxScannedFileSize(),
                        newConstraintColumns,
                        table.getForAnalyze()),
                remainingConstraint.transformKeys(ColumnHandle.class::cast),
                extractionResult.remainingExpression(),
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
                .collect(toImmutableMap(identity(), ApplyProjectionUtil::createProjectedColumnRepresentation));

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

        List<Assignment> outputAssignments = ImmutableList.copyOf(newAssignments.values());
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
                true,
                Optional.empty());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }

        IcebergTableHandle originalHandle = (IcebergTableHandle) tableHandle;
        // Certain table handle attributes are not applicable to select queries (which need stats).
        // If this changes, the caching logic may here may need to be revised.
        checkArgument(!originalHandle.isRecordScannedFiles(), "Unexpected scanned files recording set");
        checkArgument(originalHandle.getMaxScannedFileSize().isEmpty(), "Unexpected max scanned file size set");

        IcebergTableHandle cacheKey = new IcebergTableHandle(
                originalHandle.getCatalog(),
                originalHandle.getSchemaName(),
                originalHandle.getTableName(),
                originalHandle.getTableType(),
                originalHandle.getSnapshotId(),
                originalHandle.getTableSchemaJson(),
                originalHandle.getPartitionSpecJson(),
                originalHandle.getFormatVersion(),
                originalHandle.getUnenforcedPredicate(),
                originalHandle.getEnforcedPredicate(),
                OptionalLong.empty(), // limit is currently not included in stats and is not enforced by the connector
                ImmutableSet.of(), // projectedColumns are used to request statistics only for the required columns, but are not part of cache key
                originalHandle.getNameMappingJson(),
                originalHandle.getTableLocation(),
                originalHandle.getStorageProperties(),
                false, // recordScannedFiles does not affect stats
                originalHandle.getMaxScannedFileSize(),
                ImmutableSet.of(), // constraintColumns do not affect stats
                Optional.empty()); // forAnalyze does not affect stats
        return getIncrementally(
                tableStatisticsCache,
                cacheKey,
                currentStatistics -> currentStatistics.getColumnStatistics().keySet().containsAll(originalHandle.getProjectedColumns()),
                projectedColumns -> {
                    Table icebergTable = catalog.loadTable(session, originalHandle.getSchemaTableName());
                    return TableStatisticsReader.getTableStatistics(
                            typeManager,
                            session,
                            originalHandle,
                            projectedColumns,
                            icebergTable,
                            fileSystemFactory.create(session.getIdentity(), icebergTable.io().properties()));
                },
                originalHandle.getProjectedColumns());
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        catalog.setTablePrincipal(session, tableName, principal);
    }

    private Optional<Long> getCurrentSnapshotId(Table table)
    {
        return Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId);
    }

    Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return catalog.loadTable(session, schemaTableName);
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
    {
        catalog.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
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
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, RetryMode retryMode, RefreshType refreshType)
    {
        checkState(fromSnapshotForRefresh.isEmpty(), "From Snapshot must be empty at the start of MV refresh operation.");
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        beginTransaction(icebergTable);

        Optional<String> dependencies = Optional.ofNullable(icebergTable.currentSnapshot())
                .map(Snapshot::summary)
                .map(summary -> summary.get(DEPENDS_ON_TABLES));

        boolean shouldUseIncremental = isIncrementalRefreshEnabled(session)
                && refreshType == RefreshType.INCREMENTAL
                // there is a single source table
                && sourceTableHandles.size() == 1
                // and source table is an Iceberg table
                && getOnlyElement(sourceTableHandles) instanceof IcebergTableHandle handle
                // and source table is from the same catalog
                && handle.getCatalog().equals(trinoCatalogHandle)
                // and the source table's fromSnapshot is available in the MV snapshot summary
                && dependencies.isPresent() && !dependencies.get().equals(UNKNOWN_SNAPSHOT_TOKEN);

        if (shouldUseIncremental) {
            Map<String, String> sourceTableToSnapshot = MAP_SPLITTER.split(dependencies.get());
            checkState(sourceTableToSnapshot.size() == 1, "Expected %s to contain only single source table in snapshot summary", sourceTableToSnapshot);
            Map.Entry<String, String> sourceTable = getOnlyElement(sourceTableToSnapshot.entrySet());
            String[] schemaTable = sourceTable.getKey().split("\\.");
            IcebergTableHandle handle = (IcebergTableHandle) getOnlyElement(sourceTableHandles);
            SchemaTableName sourceSchemaTable = new SchemaTableName(schemaTable[0], schemaTable[1]);
            checkState(sourceSchemaTable.equals(handle.getSchemaTableName()), "Source table name %s doesn't match handle table name %s", sourceSchemaTable, handle.getSchemaTableName());
            fromSnapshotForRefresh = Optional.of(Long.parseLong(sourceTable.getValue()));
        }

        return newWritableTableHandle(table.getSchemaTableName(), icebergTable, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<ConnectorTableHandle> sourceTableHandles,
            List<String> sourceTableFunctions)
    {
        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;

        Table icebergTable = transaction.table();
        boolean isFullRefresh = fromSnapshotForRefresh.isEmpty();
        if (isFullRefresh) {
            // delete before insert .. simulating overwrite
            log.info("Performing full MV refresh for storage table: %s", table.name());
            transaction.newDelete()
                    .deleteFromRowFilter(Expressions.alwaysTrue())
                    .commit();
        }
        else {
            log.info("Performing incremental MV refresh for storage table: %s", table.name());
        }

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
                    .withPath(task.path())
                    .withFileSizeInBytes(task.fileSizeInBytes())
                    .withFormat(table.fileFormat().toIceberg())
                    .withMetrics(task.metrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
            writtenFiles.add(task.path());
        }

        String tableDependencies = sourceTableHandles.stream()
                .map(handle -> {
                    if (!(handle instanceof IcebergTableHandle icebergHandle)) {
                        return UNKNOWN_SNAPSHOT_TOKEN;
                    }
                    // Currently the catalogs are isolated in separate classloaders, and the above instanceof check is sufficient to know "our" handles.
                    // This isolation will be removed after we remove Hadoop dependencies, so check that this is "our" handle explicitly.
                    if (!trinoCatalogHandle.equals(icebergHandle.getCatalog())) {
                        return UNKNOWN_SNAPSHOT_TOKEN;
                    }
                    return icebergHandle.getSchemaTableName() + "=" + icebergHandle.getSnapshotId().map(Object.class::cast).orElse("");
                })
                .distinct()
                .collect(joining(","));

        // try to leave as little garbage as possible behind
        if (table.retryMode() != NO_RETRIES) {
            cleanExtraOutputFiles(session, writtenFiles.build());
        }

        // Update the 'dependsOnTables' property that tracks tables on which the materialized view depends and the corresponding snapshot ids of the tables
        appendFiles.set(DEPENDS_ON_TABLES, tableDependencies);
        appendFiles.set(DEPENDS_ON_TABLE_FUNCTIONS, Boolean.toString(!sourceTableFunctions.isEmpty()));
        appendFiles.set(TRINO_QUERY_START_TIME, session.getStart().toString());
        commitUpdateAndTransaction(appendFiles, session, transaction, "refresh materialized view");
        transaction = null;
        fromSnapshotForRefresh = Optional.empty();
        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::path)
                .collect(toImmutableList())));
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName).stream()
                .filter(info -> info.extendedRelationType() == TableInfo.ExtendedRelationType.TRINO_MATERIALIZED_VIEW)
                .map(TableInfo::tableName)
                .toList();
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
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        return catalog.getMaterializedViewProperties(session, viewName, definition);
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

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName materializedViewName)
    {
        Optional<ConnectorMaterializedViewDefinition> materializedViewDefinition = getMaterializedView(session, materializedViewName);
        if (materializedViewDefinition.isEmpty()) {
            // View not found, might have been concurrently deleted
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }

        SchemaTableName storageTableName = materializedViewDefinition.get().getStorageTable()
                .map(CatalogSchemaTableName::getSchemaTableName)
                .orElseThrow(() -> new IllegalStateException("Storage table missing in definition of materialized view " + materializedViewName));

        Table icebergTable = catalog.loadTable(session, storageTableName);
        Optional<Snapshot> currentSnapshot = Optional.ofNullable(icebergTable.currentSnapshot());
        String dependsOnTables = currentSnapshot
                .map(snapshot -> snapshot.summary().getOrDefault(DEPENDS_ON_TABLES, ""))
                .orElse("");
        boolean dependsOnTableFunctions = currentSnapshot
                .map(snapshot -> Boolean.valueOf(snapshot.summary().getOrDefault(DEPENDS_ON_TABLE_FUNCTIONS, "false")))
                .orElse(false);

        Optional<Instant> refreshTime = currentSnapshot.map(snapshot -> snapshot.summary().get(TRINO_QUERY_START_TIME))
                .map(Instant::parse)
                .or(() -> currentSnapshot.map(snapshot -> Instant.ofEpochMilli(snapshot.timestampMillis())));

        if (dependsOnTableFunctions) {
            // It can't be determined whether a value returned by table function is STALE or not
            return new MaterializedViewFreshness(UNKNOWN, refreshTime);
        }

        if (dependsOnTables.isEmpty()) {
            // Information missing. While it's "unknown" whether storage is stale, we return "stale".
            // Normally dependsOnTables may be missing only when there was no refresh yet.
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }

        boolean hasUnknownTables = false;
        boolean hasStaleIcebergTables = false;
        Optional<Long> firstTableChange = Optional.of(Long.MAX_VALUE);

        Iterable<String> tableToSnapshotIds = Splitter.on(',').split(dependsOnTables);
        for (String entry : tableToSnapshotIds) {
            if (entry.equals(UNKNOWN_SNAPSHOT_TOKEN)) {
                // This is a "federated" materialized view (spanning across connectors). Trust user's choice and assume "fresh or fresh enough".
                hasUnknownTables = true;
                firstTableChange = Optional.empty();
                continue;
            }
            List<String> keyValue = Splitter.on("=").splitToList(entry);
            if (keyValue.size() != 2) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, format("Invalid entry in '%s' property: %s'", DEPENDS_ON_TABLES, entry));
            }
            String tableName = keyValue.get(0);
            String value = keyValue.get(1);
            List<String> strings = Splitter.on(".").splitToList(tableName);
            if (strings.size() == 3) {
                strings = strings.subList(1, 3);
            }
            else if (strings.size() != 2) {
                throw new TrinoException(ICEBERG_INVALID_METADATA, format("Invalid table name in '%s' property: %s'", DEPENDS_ON_TABLES, strings));
            }
            String schema = strings.get(0);
            String name = strings.get(1);
            SchemaTableName schemaTableName = new SchemaTableName(schema, name);
            ConnectorTableHandle tableHandle = getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());

            if (tableHandle == null || tableHandle instanceof CorruptedIcebergTableHandle) {
                // Base table is gone or table is corrupted
                return new MaterializedViewFreshness(STALE, Optional.empty());
            }
            Optional<Long> snapshotAtRefresh;
            if (value.isEmpty()) {
                snapshotAtRefresh = Optional.empty();
            }
            else {
                snapshotAtRefresh = Optional.of(Long.parseLong(value));
            }
            switch (getTableChangeInfo(session, (IcebergTableHandle) tableHandle, snapshotAtRefresh)) {
                case NoTableChange() -> {
                    // Fresh
                }
                case FirstChangeSnapshot(Snapshot snapshot) -> {
                    hasStaleIcebergTables = true;
                    firstTableChange = firstTableChange
                            .map(epochMilli -> Math.min(epochMilli, snapshot.timestampMillis()));
                }
                case UnknownTableChange() -> {
                    hasStaleIcebergTables = true;
                    firstTableChange = Optional.empty();
                }
            }
        }

        Optional<Instant> lastFreshTime = firstTableChange
                .map(Instant::ofEpochMilli)
                .or(() -> refreshTime);
        if (hasStaleIcebergTables) {
            return new MaterializedViewFreshness(STALE, lastFreshTime);
        }
        if (hasUnknownTables) {
            return new MaterializedViewFreshness(UNKNOWN, lastFreshTime);
        }
        return new MaterializedViewFreshness(FRESH, Optional.empty());
    }

    private TableChangeInfo getTableChangeInfo(ConnectorSession session, IcebergTableHandle table, Optional<Long> snapshotAtRefresh)
    {
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        Snapshot currentSnapshot = icebergTable.currentSnapshot();

        if (snapshotAtRefresh.isEmpty()) {
            // Table had no snapshot at refresh time.
            if (currentSnapshot == null) {
                return new NoTableChange();
            }
            return firstSnapshot(icebergTable)
                    .<TableChangeInfo>map(FirstChangeSnapshot::new)
                    .orElse(new UnknownTableChange());
        }

        if (snapshotAtRefresh.get() == currentSnapshot.snapshotId()) {
            return new NoTableChange();
        }
        return firstSnapshotAfter(icebergTable, snapshotAtRefresh.get())
                .<TableChangeInfo>map(FirstChangeSnapshot::new)
                .orElse(new UnknownTableChange());
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        catalog.updateColumnComment(session, ((IcebergTableHandle) tableHandle).getSchemaTableName(), ((IcebergColumnHandle) column).getColumnIdentity(), comment);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<String> targetCatalogName = getHiveCatalogName(session);
        if (targetCatalogName.isEmpty()) {
            return Optional.empty();
        }
        return catalog.redirectTable(session, tableName, targetCatalogName.get());
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) connectorTableHandle;
        IcebergFileFormat storageFormat = getFileFormat(tableHandle.getStorageProperties());

        return storageFormat == IcebergFileFormat.ORC || storageFormat == IcebergFileFormat.PARQUET;
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(ConnectorSession session, SchemaTableName tableName, Map<String, Object> tableProperties)
    {
        return WriterScalingOptions.ENABLED;
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return WriterScalingOptions.ENABLED;
    }

    public Optional<Long> getIncrementalRefreshFromSnapshot()
    {
        return fromSnapshotForRefresh;
    }

    public void disableIncrementalRefresh()
    {
        fromSnapshotForRefresh = Optional.empty();
    }

    private static CollectedStatistics processComputedTableStatistics(Table table, Collection<ComputedStatistics> computedStatistics)
    {
        Map<String, Integer> columnNameToId = table.schema().columns().stream()
                .collect(toImmutableMap(nestedField -> nestedField.name().toLowerCase(ENGLISH), Types.NestedField::fieldId));

        ImmutableMap.Builder<Integer, CompactSketch> ndvSketches = ImmutableMap.builder();
        for (ComputedStatistics computedStatistic : computedStatistics) {
            verify(computedStatistic.getGroupingColumns().isEmpty() && computedStatistic.getGroupingValues().isEmpty(), "Unexpected grouping");
            verify(computedStatistic.getTableStatistics().isEmpty(), "Unexpected table statistics");
            for (Map.Entry<ColumnStatisticMetadata, Block> entry : computedStatistic.getColumnStatistics().entrySet()) {
                ColumnStatisticMetadata statisticMetadata = entry.getKey();
                if (statisticMetadata.getConnectorAggregationId().equals(NUMBER_OF_DISTINCT_VALUES_NAME)) {
                    Integer columnId = verifyNotNull(
                            columnNameToId.get(statisticMetadata.getColumnName()),
                            "Column not found in table: [%s]",
                            statisticMetadata.getColumnName());
                    CompactSketch sketch = DataSketchStateSerializer.deserialize(entry.getValue(), 0);
                    ndvSketches.put(columnId, sketch);
                }
                else {
                    throw new UnsupportedOperationException("Unsupported statistic: " + statisticMetadata);
                }
            }
        }

        return new CollectedStatistics(ndvSketches.buildOrThrow());
    }

    private void beginTransaction(Table icebergTable)
    {
        verify(transaction == null, "transaction already set");
        transaction = catalog.newTransaction(icebergTable);
    }

    private static IcebergTableHandle checkValidTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        if (tableHandle instanceof CorruptedIcebergTableHandle corruptedTableHandle) {
            throw corruptedTableHandle.createException();
        }
        return ((IcebergTableHandle) tableHandle);
    }

    private sealed interface TableChangeInfo
            permits NoTableChange, FirstChangeSnapshot, UnknownTableChange {}

    private record NoTableChange()
            implements TableChangeInfo {}

    private record FirstChangeSnapshot(Snapshot snapshot)
            implements TableChangeInfo
    {
        FirstChangeSnapshot
        {
            requireNonNull(snapshot, "snapshot is null");
        }
    }

    private record UnknownTableChange()
            implements TableChangeInfo {}

    private static TableStatistics getIncrementally(
            Map<IcebergTableHandle, AtomicReference<TableStatistics>> cache,
            IcebergTableHandle key,
            Predicate<TableStatistics> isSufficient,
            Function<Set<IcebergColumnHandle>, TableStatistics> columnStatisticsLoader,
            Set<IcebergColumnHandle> projectedColumns)
    {
        AtomicReference<TableStatistics> valueHolder = cache.computeIfAbsent(key, _ -> new AtomicReference<>());
        TableStatistics oldValue = valueHolder.get();
        if (oldValue != null && isSufficient.test(oldValue)) {
            return oldValue;
        }

        TableStatistics newValue;
        if (oldValue == null) {
            newValue = columnStatisticsLoader.apply(projectedColumns);
        }
        else {
            Sets.SetView<IcebergColumnHandle> missingColumns = difference(projectedColumns, oldValue.getColumnStatistics().keySet());
            newValue = columnStatisticsLoader.apply(missingColumns);
        }

        verifyNotNull(newValue, "loader returned null for %s", key);

        TableStatistics merged = mergeColumnStatistics(oldValue, newValue);
        if (!valueHolder.compareAndSet(oldValue, merged)) {
            // if the value changed in the valueHolder, we only add newly loaded value to be sure we have up-to-date value
            valueHolder.accumulateAndGet(newValue, IcebergMetadata::mergeColumnStatistics);
        }
        return merged;
    }

    private static TableStatistics mergeColumnStatistics(TableStatistics currentStats, TableStatistics newStats)
    {
        requireNonNull(newStats, "newStats is null");
        TableStatistics.Builder statisticsBuilder = TableStatistics.builder();
        if (currentStats != null) {
            currentStats.getColumnStatistics().forEach(statisticsBuilder::setColumnStatistics);
        }
        statisticsBuilder.setRowCount(newStats.getRowCount());
        newStats.getColumnStatistics().forEach(statisticsBuilder::setColumnStatistics);
        return statisticsBuilder.build();
    }
}
