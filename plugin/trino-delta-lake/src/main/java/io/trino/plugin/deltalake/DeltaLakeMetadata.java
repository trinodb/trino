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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.VerifyException;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.deltalake.DeltaLakeAnalyzeProperties.AnalyzeMode;
import io.trino.plugin.deltalake.expression.ParsingException;
import io.trino.plugin.deltalake.expression.SparkExpressionParser;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.deltalake.metastore.NotADeltaLakeTableException;
import io.trino.plugin.deltalake.procedure.DeltaLakeTableExecuteHandle;
import io.trino.plugin.deltalake.procedure.DeltaLakeTableProcedureId;
import io.trino.plugin.deltalake.procedure.DeltaTableOptimizeHandle;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.DeltaLakeColumnStatistics;
import io.trino.plugin.deltalake.statistics.DeltaLakeTableStatisticsProvider;
import io.trino.plugin.deltalake.statistics.ExtendedStatistics;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.CdcEntry;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeComputedStatistics;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry.Format;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionConflictException;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriter;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.security.AccessControlMetadata;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
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
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatisticType;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.HyperLogLogType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Sets.difference;
import static com.google.common.primitives.Ints.max;
import static io.trino.filesystem.Locations.appendPath;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.ProjectedColumnRepresentation;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.deltalake.DataFileInfo.DataFileType.DATA;
import static io.trino.plugin.deltalake.DeltaLakeAnalyzeProperties.AnalyzeMode.FULL_REFRESH;
import static io.trino.plugin.deltalake.DeltaLakeAnalyzeProperties.AnalyzeMode.INCREMENTAL;
import static io.trino.plugin.deltalake.DeltaLakeAnalyzeProperties.getColumnNames;
import static io.trino.plugin.deltalake.DeltaLakeAnalyzeProperties.getFilesModifiedAfterProperty;
import static io.trino.plugin.deltalake.DeltaLakeAnalyzeProperties.getRefreshMode;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.PATH_COLUMN_NAME;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.fileSizeColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.mergeRowIdColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeColumnHandle.pathColumnHandle;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.SYNTHESIZED;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_WRITE;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getHiveCatalogName;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isCollectExtendedStatisticsColumnStatisticsOnWrite;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isExtendedStatisticsEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isLegacyCreateTableWithExistingLocationEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isQueryPartitionFilterRequired;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isTableStatisticsEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSplitManager.partitionMatchesPredicate;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.CHANGE_DATA_FEED_ENABLED_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.CHECKPOINT_INTERVAL_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.COLUMN_MAPPING_MODE_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.LOCATION_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.getChangeDataFeedEnabled;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.getCheckpointInterval;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.getLocation;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.getPartitionedBy;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_PROPERTY;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_VALUE;
import static io.trino.plugin.deltalake.procedure.DeltaLakeTableProcedureId.OPTIMIZE;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.APPEND_ONLY_CONFIGURATION_KEY;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.COLUMN_MAPPING_PHYSICAL_NAME_CONFIGURATION_KEY;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode.ID;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode.NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode.NONE;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.MAX_COLUMN_ID_CONFIGURATION_KEY;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.changeDataFeedEnabled;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.deserializeType;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractPartitionColumns;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.generateColumnMetadata;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getCheckConstraints;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnComments;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnIdentities;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnInvariants;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnMappingMode;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnTypes;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnsMetadata;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnsNullability;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getExactColumnNames;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getGeneratedColumnExpressions;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getMaxColumnId;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.isAppendOnly;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeColumnType;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeSchemaAsJson;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeStatsAsJson;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.unsupportedReaderFeatures;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.validateType;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.verifySupportedColumnMapping;
import static io.trino.plugin.deltalake.transactionlog.MetadataEntry.DELTA_CHANGE_DATA_FEED_ENABLED_PROPERTY;
import static io.trino.plugin.deltalake.transactionlog.MetadataEntry.configurationForNewTable;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.getMandatoryCurrentVersion;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.StorageFormat.create;
import static io.trino.plugin.hive.util.HiveClassNames.HIVE_SEQUENCEFILE_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.LAZY_SIMPLE_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.SEQUENCEFILE_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveUtil.escapeTableName;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.predicate.Range.greaterThanOrEqual;
import static io.trino.spi.predicate.Range.lessThanOrEqual;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.ValueSet.ofRanges;
import static io.trino.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES_SUMMARY;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static io.trino.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.time.Instant.EPOCH;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Comparator.naturalOrder;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.partitioningBy;

public class DeltaLakeMetadata
        implements ConnectorMetadata
{
    public static final Logger LOG = Logger.get(DeltaLakeMetadata.class);

    public static final String PATH_PROPERTY = "path";
    public static final StorageFormat DELTA_STORAGE_FORMAT = create(
            LAZY_SIMPLE_SERDE_CLASS,
            SEQUENCEFILE_INPUT_FORMAT_CLASS,
            HIVE_SEQUENCEFILE_OUTPUT_FORMAT_CLASS);
    public static final String CREATE_TABLE_AS_OPERATION = "CREATE TABLE AS SELECT";
    public static final String CREATE_TABLE_OPERATION = "CREATE TABLE";
    public static final String ADD_COLUMN_OPERATION = "ADD COLUMNS";
    public static final String DROP_COLUMN_OPERATION = "DROP COLUMNS";
    public static final String RENAME_COLUMN_OPERATION = "RENAME COLUMN";
    public static final String INSERT_OPERATION = "WRITE";
    public static final String MERGE_OPERATION = "MERGE";
    public static final String UPDATE_OPERATION = "UPDATE"; // used by old Trino versions and Spark
    public static final String DELETE_OPERATION = "DELETE"; // used Trino for whole table/partition deletes as well as Spark
    public static final String TRUNCATE_OPERATION = "TRUNCATE";
    public static final String OPTIMIZE_OPERATION = "OPTIMIZE";
    public static final String SET_TBLPROPERTIES_OPERATION = "SET TBLPROPERTIES";
    public static final String CHANGE_COLUMN_OPERATION = "CHANGE COLUMN";
    public static final String ISOLATION_LEVEL = "WriteSerializable";

    public static final int DEFAULT_READER_VERSION = 1;
    public static final int DEFAULT_WRITER_VERSION = 2;
    // The highest reader and writer versions Trino supports
    private static final int MAX_READER_VERSION = 3;
    public static final int MAX_WRITER_VERSION = 6;
    private static final int CDF_SUPPORTED_WRITER_VERSION = 4;
    private static final int COLUMN_MAPPING_MODE_SUPPORTED_READER_VERSION = 2;
    private static final int COLUMN_MAPPING_MODE_SUPPORTED_WRITER_VERSION = 5;

    // Matches the dummy column Databricks stores in the metastore
    private static final List<Column> DUMMY_DATA_COLUMNS = ImmutableList.of(
            new Column("col", HiveType.toHiveType(new ArrayType(VarcharType.createUnboundedVarcharType())), Optional.empty()));
    private static final Set<ColumnStatisticType> SUPPORTED_STATISTICS_TYPE = ImmutableSet.<ColumnStatisticType>builder()
            .add(TOTAL_SIZE_IN_BYTES)
            .add(NUMBER_OF_DISTINCT_VALUES_SUMMARY)
            .add(MAX_VALUE)
            .add(MIN_VALUE)
            .add(NUMBER_OF_NON_NULL_VALUES)
            .build();
    private static final String ENABLE_NON_CONCURRENT_WRITES_CONFIGURATION_KEY = "delta.enable-non-concurrent-writes";
    public static final Set<String> UPDATABLE_TABLE_PROPERTIES = ImmutableSet.of(CHANGE_DATA_FEED_ENABLED_PROPERTY);

    public static final Set<String> CHANGE_DATA_FEED_COLUMN_NAMES = ImmutableSet.<String>builder()
            .add("_change_type")
            .add("_commit_version")
            .add("_commit_timestamp")
            .build();

    private static final String CHECK_CONSTRAINT_CONVERT_FAIL_EXPRESSION = "CAST(fail('Failed to convert Delta check constraints to Trino expression') AS boolean)";

    private final DeltaLakeMetastore metastore;
    private final TransactionLogAccess transactionLogAccess;
    private final DeltaLakeTableStatisticsProvider tableStatisticsProvider;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final AccessControlMetadata accessControlMetadata;
    private final TrinoViewHiveMetastore trinoViewHiveMetastore;
    private final CheckpointWriterManager checkpointWriterManager;
    private final long defaultCheckpointInterval;
    private final int domainCompactionThreshold;
    private final boolean unsafeWritesEnabled;
    private final JsonCodec<DataFileInfo> dataFileInfoCodec;
    private final JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec;
    private final TransactionLogWriterFactory transactionLogWriterFactory;
    private final String nodeVersion;
    private final String nodeId;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();
    private final DeltaLakeRedirectionsProvider deltaLakeRedirectionsProvider;
    private final CachingExtendedStatisticsAccess statisticsAccess;
    private final boolean deleteSchemaLocationsFallback;
    private final boolean useUniqueTableLocation;
    private final boolean allowManagedTableRename;

    public DeltaLakeMetadata(
            DeltaLakeMetastore metastore,
            TransactionLogAccess transactionLogAccess,
            DeltaLakeTableStatisticsProvider tableStatisticsProvider,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            AccessControlMetadata accessControlMetadata,
            TrinoViewHiveMetastore trinoViewHiveMetastore,
            int domainCompactionThreshold,
            boolean unsafeWritesEnabled,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec,
            TransactionLogWriterFactory transactionLogWriterFactory,
            NodeManager nodeManager,
            CheckpointWriterManager checkpointWriterManager,
            long defaultCheckpointInterval,
            boolean deleteSchemaLocationsFallback,
            DeltaLakeRedirectionsProvider deltaLakeRedirectionsProvider,
            CachingExtendedStatisticsAccess statisticsAccess,
            boolean useUniqueTableLocation,
            boolean allowManagedTableRename)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.tableStatisticsProvider = requireNonNull(tableStatisticsProvider, "tableStatisticsProvider is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.accessControlMetadata = requireNonNull(accessControlMetadata, "accessControlMetadata is null");
        this.trinoViewHiveMetastore = requireNonNull(trinoViewHiveMetastore, "trinoViewHiveMetastore is null");
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.unsafeWritesEnabled = unsafeWritesEnabled;
        this.dataFileInfoCodec = requireNonNull(dataFileInfoCodec, "dataFileInfoCodec is null");
        this.mergeResultJsonCodec = requireNonNull(mergeResultJsonCodec, "mergeResultJsonCodec is null");
        this.transactionLogWriterFactory = requireNonNull(transactionLogWriterFactory, "transactionLogWriterFactory is null");
        this.nodeVersion = nodeManager.getCurrentNode().getVersion();
        this.nodeId = nodeManager.getCurrentNode().getNodeIdentifier();
        this.checkpointWriterManager = requireNonNull(checkpointWriterManager, "checkpointWriterManager is null");
        this.defaultCheckpointInterval = defaultCheckpointInterval;
        this.deltaLakeRedirectionsProvider = requireNonNull(deltaLakeRedirectionsProvider, "deltaLakeRedirectionsProvider is null");
        this.statisticsAccess = requireNonNull(statisticsAccess, "statisticsAccess is null");
        this.deleteSchemaLocationsFallback = deleteSchemaLocationsFallback;
        this.useUniqueTableLocation = useUniqueTableLocation;
        this.allowManagedTableRename = allowManagedTableRename;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases().stream()
                .filter(schema -> !schema.equalsIgnoreCase("sys"))
                .collect(toImmutableList());
    }

    private static boolean isHiveTable(Table table)
    {
        return !isDeltaLakeTable(table);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");
        Optional<String> targetCatalogName = getHiveCatalogName(session);
        if (targetCatalogName.isEmpty()) {
            return Optional.empty();
        }
        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return Optional.empty();
        }

        // we need to chop off any "$partitions" and similar suffixes from table name while querying the metastore for the Table object
        int metadataMarkerIndex = tableName.getTableName().lastIndexOf('$');
        SchemaTableName tableNameBase = (metadataMarkerIndex == -1) ? tableName : schemaTableName(
                tableName.getSchemaName(),
                tableName.getTableName().substring(0, metadataMarkerIndex));

        Optional<Table> table = metastore.getRawMetastoreTable(tableNameBase.getSchemaName(), tableNameBase.getTableName());
        if (table.isEmpty() || VIRTUAL_VIEW.name().equals(table.get().getTableType())) {
            return Optional.empty();
        }
        if (isHiveTable(table.get())) {
            // After redirecting, use the original table name, with "$partitions" and similar suffixes
            return targetCatalogName.map(catalog -> new CatalogSchemaTableName(catalog, tableName));
        }
        return Optional.empty();
    }

    @Override
    public LocatedTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!DeltaLakeTableName.isDataTable(tableName.getTableName())) {
            // Pretend the table does not exist to produce better error message in case of table redirects to Hive
            return null;
        }
        Optional<DeltaMetastoreTable> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table.isEmpty()) {
            return null;
        }
        boolean managed = table.get().managed();

        String tableLocation = table.get().location();
        TableSnapshot tableSnapshot = getSnapshot(tableName, tableLocation, session);
        MetadataEntry metadataEntry;
        try {
            metadataEntry = transactionLogAccess.getMetadataEntry(tableSnapshot, session);
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(DELTA_LAKE_INVALID_SCHEMA.toErrorCode())) {
                return new CorruptedDeltaLakeTableHandle(tableName, managed, tableLocation, e);
            }
            throw e;
        }
        ProtocolEntry protocolEntry = transactionLogAccess.getProtocolEntry(session, tableSnapshot);
        if (protocolEntry.getMinReaderVersion() > MAX_READER_VERSION) {
            LOG.debug("Skip %s because the reader version is unsupported: %d", tableName, protocolEntry.getMinReaderVersion());
            return null;
        }
        Set<String> unsupportedReaderFeatures = unsupportedReaderFeatures(protocolEntry.getReaderFeatures().orElse(ImmutableSet.of()));
        if (!unsupportedReaderFeatures.isEmpty()) {
            LOG.debug("Skip %s because the table contains unsupported reader features: %s", tableName, unsupportedReaderFeatures);
            return null;
        }
        verifySupportedColumnMapping(getColumnMappingMode(metadataEntry));
        return new DeltaLakeTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                managed,
                tableLocation,
                metadataEntry,
                protocolEntry,
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                tableSnapshot.getVersion());
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new ConnectorTableProperties(
                ((DeltaLakeTableHandle) tableHandle).getEnforcedPartitionConstraint()
                        .transformKeys(ColumnHandle.class::cast),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        if (table instanceof CorruptedDeltaLakeTableHandle corruptedTableHandle) {
            return corruptedTableHandle.schemaTableName();
        }
        return ((DeltaLakeTableHandle) table).getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DeltaLakeTableHandle tableHandle = checkValidTableHandle(table);
        // This method does not calculate column metadata for the projected columns
        checkArgument(tableHandle.getProjectedColumns().isEmpty(), "Unexpected projected columns");
        MetadataEntry metadataEntry = tableHandle.getMetadataEntry();

        List<String> constraints = ImmutableList.<String>builder()
                .addAll(getCheckConstraints(metadataEntry).values())
                .addAll(getColumnInvariants(metadataEntry).values()) // The internal logic for column invariants in Delta Lake is same as check constraints
                .build();
        List<ColumnMetadata> columns = getTableColumnMetadata(metadataEntry);

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(LOCATION_PROPERTY, tableHandle.getLocation());
        List<String> partitionColumnNames = metadataEntry.getLowercasePartitionColumns();
        if (!partitionColumnNames.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionColumnNames);
        }

        Optional<Long> checkpointInterval = metadataEntry.getCheckpointInterval();
        checkpointInterval.ifPresent(value -> properties.put(CHECKPOINT_INTERVAL_PROPERTY, value));

        Optional<Boolean> changeDataFeedEnabled = metadataEntry.isChangeDataFeedEnabled();
        changeDataFeedEnabled.ifPresent(value -> properties.put(CHANGE_DATA_FEED_ENABLED_PROPERTY, value));

        ColumnMappingMode columnMappingMode = DeltaLakeSchemaSupport.getColumnMappingMode(metadataEntry);
        if (columnMappingMode != NONE) {
            properties.put(COLUMN_MAPPING_MODE_PROPERTY, columnMappingMode.name());
        }

        return new ConnectorTableMetadata(
                tableHandle.getSchemaTableName(),
                columns,
                properties.buildOrThrow(),
                Optional.ofNullable(metadataEntry.getDescription()),
                constraints.stream()
                        .map(constraint -> {
                            try {
                                return SparkExpressionParser.toTrinoExpression(constraint);
                            }
                            catch (ParsingException e) {
                                return CHECK_CONSTRAINT_CONVERT_FAIL_EXPRESSION;
                            }
                        })
                        .collect(toImmutableList()));
    }

    private List<ColumnMetadata> getTableColumnMetadata(MetadataEntry metadataEntry)
    {
        Map<String, String> columnComments = getColumnComments(metadataEntry);
        Map<String, Boolean> columnsNullability = getColumnsNullability(metadataEntry);
        Map<String, String> columnGenerations = getGeneratedColumnExpressions(metadataEntry);
        List<ColumnMetadata> columns = getColumns(metadataEntry).stream()
                .map(column -> getColumnMetadata(column, columnComments.get(column.getBaseColumnName()), columnsNullability.getOrDefault(column.getBaseColumnName(), true), columnGenerations.get(column.getBaseColumnName())))
                .collect(toImmutableList());
        return columns;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return schemaName.map(Collections::singletonList)
                .orElseGet(() -> listSchemaNames(session))
                .stream()
                .flatMap(schema -> metastore.getAllTables(schema).stream()
                        .map(table -> new SchemaTableName(schema, table)))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DeltaLakeTableHandle table = checkValidTableHandle(tableHandle);
        return table.getProjectedColumns()
                .map(projectColumns -> (Collection<DeltaLakeColumnHandle>) projectColumns)
                .orElseGet(() -> getColumns(table.getMetadataEntry())).stream()
                // This method does not calculate column name for the projected columns
                .peek(handle -> checkArgument(handle.isBaseColumn(), "Unsupported projected column: %s", handle))
                .collect(toImmutableMap(DeltaLakeColumnHandle::getBaseColumnName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) tableHandle;
        DeltaLakeColumnHandle column = (DeltaLakeColumnHandle) columnHandle;
        if (column.getProjectionInfo().isPresent()) {
            return getColumnMetadata(column, null, true, null);
        }
        return getColumnMetadata(
                column,
                getColumnComments(table.getMetadataEntry()).get(column.getBaseColumnName()),
                getColumnsNullability(table.getMetadataEntry()).getOrDefault(column.getBaseColumnName(), true),
                getGeneratedColumnExpressions(table.getMetadataEntry()).get(column.getBaseColumnName()));
    }

    /**
     * Provides partitioning scheme of table for query planner to decide how to
     * write to multiple partitions.
     */
    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        validateTableColumns(tableMetadata);

        List<String> partitionColumnNames = getPartitionedBy(tableMetadata.getProperties());

        if (partitionColumnNames.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new ConnectorTableLayout(partitionColumnNames));
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DeltaLakeTableHandle deltaLakeTableHandle = (DeltaLakeTableHandle) tableHandle;
        List<String> partitionColumnNames = deltaLakeTableHandle.getMetadataEntry().getLowercasePartitionColumns();

        if (partitionColumnNames.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new ConnectorTableLayout(partitionColumnNames));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTable()
                .map(ignored -> singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        return tables.stream()
                .flatMap(table -> {
                    try {
                        if (redirectTable(session, table).isPresent()) {
                            // put "redirect marker" for current table
                            return Stream.of(TableColumnsMetadata.forRedirectedTable(table));
                        }

                        Optional<DeltaMetastoreTable> metastoreTable = metastore.getTable(table.getSchemaName(), table.getTableName());
                        if (metastoreTable.isEmpty()) {
                            // this may happen when table is being deleted concurrently,
                            return Stream.of();
                        }
                        String tableLocation = metastoreTable.get().location();
                        MetadataEntry metadata = transactionLogAccess.getMetadataEntry(getSnapshot(table, tableLocation, session), session);
                        Map<String, String> columnComments = getColumnComments(metadata);
                        Map<String, Boolean> columnsNullability = getColumnsNullability(metadata);
                        Map<String, String> columnGenerations = getGeneratedColumnExpressions(metadata);
                        List<ColumnMetadata> columnMetadata = getColumns(metadata).stream()
                                .map(column -> getColumnMetadata(column, columnComments.get(column.getColumnName()), columnsNullability.getOrDefault(column.getBaseColumnName(), true), columnGenerations.get(column.getBaseColumnName())))
                                .collect(toImmutableList());
                        return Stream.of(TableColumnsMetadata.forTable(table, columnMetadata));
                    }
                    catch (NotADeltaLakeTableException e) {
                        return Stream.empty();
                    }
                    catch (RuntimeException e) {
                        // this may happen when table is being deleted concurrently, it still exists in metastore but TL is no longer present
                        // there can be several different exceptions thrown this is why all RTE are caught and ignored here
                        LOG.debug(e, "Ignored exception when trying to list columns from %s", table);
                        return Stream.empty();
                    }
                })
                .iterator();
    }

    private List<DeltaLakeColumnHandle> getColumns(MetadataEntry deltaMetadata)
    {
        ImmutableList.Builder<DeltaLakeColumnHandle> columns = ImmutableList.builder();
        extractSchema(deltaMetadata, typeManager).stream()
                .map(column -> toColumnHandle(column.getName(), column.getType(), column.getFieldId(), column.getPhysicalName(), column.getPhysicalColumnType(), deltaMetadata.getLowercasePartitionColumns()))
                .forEach(columns::add);
        columns.add(pathColumnHandle());
        columns.add(fileSizeColumnHandle());
        columns.add(fileModifiedTimeColumnHandle());
        return columns.build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DeltaLakeTableHandle handle = checkValidTableHandle(tableHandle);
        if (!isTableStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }
        return tableStatisticsProvider.getTableStatistics(session, handle, getSnapshot(handle.getSchemaTableName(), handle.getLocation(), session));
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        Optional<String> location = DeltaLakeSchemaProperties.getLocation(properties).map(locationUri -> {
            try {
                fileSystemFactory.create(session).directoryExists(Location.of(locationUri));
            }
            catch (IOException | IllegalArgumentException e) {
                throw new TrinoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + locationUri, e);
            }
            return locationUri;
        });

        String queryId = session.getQueryId();

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setLocation(location)
                .setOwnerType(Optional.of(owner.getType()))
                .setOwnerName(Optional.of(owner.getName()))
                .setParameters(ImmutableMap.of(PRESTO_QUERY_ID_NAME, queryId))
                .build();

        // Ensure the database has queryId set. This is relied on for exception handling
        verify(
                getQueryId(database).orElseThrow(() -> new IllegalArgumentException("Query id is not present")).equals(queryId),
                "Database '%s' does not have correct query id set",
                database.getDatabaseName());

        try {
            metastore.createDatabase(database);
        }
        catch (SchemaAlreadyExistsException e) {
            // Ignore SchemaAlreadyExistsException when database looks like created by us.
            // This may happen when an actually successful metastore create call is retried
            // e.g. because of a timeout on our side.
            Optional<Database> existingDatabase = metastore.getDatabase(schemaName);
            if (existingDatabase.isEmpty() || !isCreatedBy(existingDatabase.get(), queryId)) {
                throw e;
            }
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (cascade) {
            for (SchemaTableName viewName : listViews(session, Optional.of(schemaName))) {
                try {
                    dropView(session, viewName);
                }
                catch (ViewNotFoundException e) {
                    LOG.debug("View disappeared during DROP SCHEMA CASCADE: %s", viewName);
                }
            }
            for (SchemaTableName tableName : listTables(session, Optional.of(schemaName))) {
                ConnectorTableHandle table = getTableHandle(session, tableName, Optional.empty(), Optional.empty());
                if (table == null) {
                    LOG.debug("Table disappeared during DROP SCHEMA CASCADE: %s", tableName);
                    continue;
                }
                try {
                    dropTable(session, table);
                }
                catch (TableNotFoundException e) {
                    LOG.debug("Table disappeared during DROP SCHEMA CASCADE: %s", tableName);
                }
            }
        }

        Optional<String> location = metastore.getDatabase(schemaName)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName))
                .getLocation();

        // If we see files in the schema location, don't delete it.
        // If we see no files or can't see the location at all, use fallback.
        boolean deleteData = location.map(path -> {
            try {
                return !fileSystemFactory.create(session).listFiles(Location.of(path)).hasNext();
            }
            catch (IOException | RuntimeException e) {
                LOG.warn(e, "Could not check schema directory '%s'", path);
                return deleteSchemaLocationsFallback;
            }
        }).orElse(deleteSchemaLocationsFallback);

        metastore.dropDatabase(schemaName, deleteData);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Database schema = metastore.getDatabase(schemaName).orElseThrow(() -> new SchemaNotFoundException(schemaName));

        boolean external = true;
        String location = getLocation(tableMetadata.getProperties());
        if (location == null) {
            location = getTableLocation(schema, tableName);
            checkPathContainsNoFiles(session, Location.of(location));
            external = false;
        }
        Location deltaLogDirectory = Location.of(getTransactionLogDir(location));
        Optional<Long> checkpointInterval = getCheckpointInterval(tableMetadata.getProperties());
        Optional<Boolean> changeDataFeedEnabled = getChangeDataFeedEnabled(tableMetadata.getProperties());
        ColumnMappingMode columnMappingMode = DeltaLakeTableProperties.getColumnMappingMode(tableMetadata.getProperties());
        AtomicInteger fieldId = new AtomicInteger();

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            if (!fileSystem.listFiles(deltaLogDirectory).hasNext()) {
                validateTableColumns(tableMetadata);

                List<String> partitionColumns = getPartitionedBy(tableMetadata.getProperties());
                ImmutableList.Builder<String> columnNames = ImmutableList.builderWithExpectedSize(tableMetadata.getColumns().size());
                ImmutableMap.Builder<String, Object> columnTypes = ImmutableMap.builderWithExpectedSize(tableMetadata.getColumns().size());
                ImmutableMap.Builder<String, Map<String, Object>> columnsMetadata = ImmutableMap.builderWithExpectedSize(tableMetadata.getColumns().size());
                for (ColumnMetadata column : tableMetadata.getColumns()) {
                    columnNames.add(column.getName());
                    columnTypes.put(column.getName(), serializeColumnType(columnMappingMode, fieldId, column.getType()));
                    columnsMetadata.put(column.getName(), generateColumnMetadata(columnMappingMode, fieldId));
                }
                Map<String, String> columnComments = tableMetadata.getColumns().stream()
                        .filter(column -> column.getComment() != null)
                        .collect(toImmutableMap(ColumnMetadata::getName, ColumnMetadata::getComment));
                Map<String, Boolean> columnsNullability = tableMetadata.getColumns().stream()
                        .collect(toImmutableMap(ColumnMetadata::getName, ColumnMetadata::isNullable));
                OptionalInt maxFieldId = OptionalInt.empty();
                if (columnMappingMode == ID || columnMappingMode == NAME) {
                    maxFieldId = OptionalInt.of(fieldId.get());
                }

                TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriterWithoutTransactionIsolation(session, location);
                appendTableEntries(
                        0,
                        transactionLogWriter,
                        randomUUID().toString(),
                        columnNames.build(),
                        partitionColumns,
                        columnTypes.buildOrThrow(),
                        columnComments,
                        columnsNullability,
                        columnsMetadata.buildOrThrow(),
                        configurationForNewTable(checkpointInterval, changeDataFeedEnabled, columnMappingMode, maxFieldId),
                        CREATE_TABLE_OPERATION,
                        session,
                        tableMetadata.getComment(),
                        protocolEntryForNewTable(tableMetadata.getProperties()));

                setRollback(() -> deleteRecursivelyIfExists(fileSystem, deltaLogDirectory));
                transactionLogWriter.flush();
            }
            else {
                if (!isLegacyCreateTableWithExistingLocationEnabled(session)) {
                    throw new TrinoException(
                            NOT_SUPPORTED,
                            "Using CREATE TABLE with an existing table content is deprecated, instead use the system.register_table() procedure." +
                                    " The CREATE TABLE syntax can be temporarily re-enabled using the 'delta.legacy-create-table-with-existing-location.enabled' config property" +
                                    " or 'legacy_create_table_with_existing_location_enabled' session property.");
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Unable to access file system for: " + location, e);
        }

        Table table = buildTable(session, schemaTableName, location, external);

        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner().orElseThrow());
        // As a precaution, clear the caches
        statisticsAccess.invalidateCache(schemaTableName, Optional.of(location));
        transactionLogAccess.invalidateCache(schemaTableName, Optional.of(location));
        metastore.createTable(
                session,
                table,
                principalPrivileges);
    }

    public static Table buildTable(ConnectorSession session, SchemaTableName schemaTableName, String location, boolean isExternal)
    {
        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(schemaTableName.getSchemaName())
                .setTableName(schemaTableName.getTableName())
                .setOwner(Optional.of(session.getUser()))
                .setTableType(isExternal ? EXTERNAL_TABLE.name() : MANAGED_TABLE.name())
                .setDataColumns(DUMMY_DATA_COLUMNS)
                .setParameters(deltaTableProperties(session, location, isExternal));

        setDeltaStorageFormat(tableBuilder, location);
        return tableBuilder.build();
    }

    private static Map<String, String> deltaTableProperties(ConnectorSession session, String location, boolean external)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .put(LOCATION_PROPERTY, location)
                .put(TABLE_PROVIDER_PROPERTY, TABLE_PROVIDER_VALUE)
                // Set bogus table stats to prevent Hive 3.x from gathering these stats at table creation.
                // These stats are not useful by themselves and can take a long time to collect when creating a
                // table over a large data set.
                .put("numFiles", "-1")
                .put("totalSize", "-1");

        if (external) {
            // Mimicking the behavior of the Hive connector which sets both `Table#setTableType` and the "EXTERNAL" table property
            properties.put("EXTERNAL", "TRUE");
        }
        return properties.buildOrThrow();
    }

    private static void setDeltaStorageFormat(Table.Builder tableBuilder, String location)
    {
        tableBuilder.getStorageBuilder()
                // this mimics what Databricks is doing when creating a Delta table in the Hive metastore
                .setStorageFormat(DELTA_STORAGE_FORMAT)
                .setSerdeParameters(ImmutableMap.of(PATH_PROPERTY, location))
                .setLocation(location);
    }

    @Override
    public DeltaLakeOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        validateTableColumns(tableMetadata);

        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Database schema = metastore.getDatabase(schemaName).orElseThrow(() -> new SchemaNotFoundException(schemaName));
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

        boolean external = true;
        String location = getLocation(tableMetadata.getProperties());
        if (location == null) {
            location = getTableLocation(schema, tableName);
            external = false;
        }

        ColumnMappingMode columnMappingMode = DeltaLakeTableProperties.getColumnMappingMode(tableMetadata.getProperties());
        AtomicInteger fieldId = new AtomicInteger();

        Location finalLocation = Location.of(location);
        checkPathContainsNoFiles(session, finalLocation);
        setRollback(() -> deleteRecursivelyIfExists(fileSystemFactory.create(session), finalLocation));

        boolean usePhysicalName = columnMappingMode == ID || columnMappingMode == NAME;
        int columnSize = tableMetadata.getColumns().size();
        ImmutableList.Builder<String> columnNames = ImmutableList.builderWithExpectedSize(columnSize);
        ImmutableMap.Builder<String, Object> columnTypes = ImmutableMap.builderWithExpectedSize(columnSize);
        ImmutableMap.Builder<String, Boolean> columnNullabilities = ImmutableMap.builderWithExpectedSize(columnSize);
        ImmutableMap.Builder<String, Map<String, Object>> columnsMetadata = ImmutableMap.builderWithExpectedSize(columnSize);
        ImmutableList.Builder<DeltaLakeColumnHandle> columnHandles = ImmutableList.builderWithExpectedSize(columnSize);
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
            columnNullabilities.put(column.getName(), column.isNullable());

            Object serializedType = serializeColumnType(columnMappingMode, fieldId, column.getType());
            Type physicalType = deserializeType(typeManager, serializedType, usePhysicalName);
            columnTypes.put(column.getName(), serializedType);

            OptionalInt id;
            String physicalName;
            Map<String, Object> columnMetadata;
            switch (columnMappingMode) {
                case NONE -> {
                    id = OptionalInt.empty();
                    physicalName = column.getName();
                    columnMetadata = ImmutableMap.of();
                }
                case ID, NAME -> {
                    columnMetadata = generateColumnMetadata(columnMappingMode, fieldId);
                    id = OptionalInt.of(fieldId.get());
                    physicalName = (String) columnMetadata.get(COLUMN_MAPPING_PHYSICAL_NAME_CONFIGURATION_KEY);
                }
                default -> throw new IllegalArgumentException("Unexpected column mapping mode: " + columnMappingMode);
            }
            columnHandles.add(toColumnHandle(column.getName(), column.getType(), id, physicalName, physicalType, partitionedBy));
            columnsMetadata.put(column.getName(), columnMetadata);
        }

        String schemaString = serializeSchemaAsJson(
                columnNames.build(),
                columnTypes.buildOrThrow(),
                ImmutableMap.of(),
                columnNullabilities.buildOrThrow(),
                columnsMetadata.buildOrThrow());

        OptionalInt maxFieldId = OptionalInt.empty();
        if (columnMappingMode == ID || columnMappingMode == NAME) {
            maxFieldId = OptionalInt.of(fieldId.get());
        }

        return new DeltaLakeOutputTableHandle(
                schemaName,
                tableName,
                columnHandles.build(),
                location,
                getCheckpointInterval(tableMetadata.getProperties()),
                external,
                tableMetadata.getComment(),
                getChangeDataFeedEnabled(tableMetadata.getProperties()),
                schemaString,
                columnMappingMode,
                maxFieldId,
                protocolEntryForNewTable(tableMetadata.getProperties()));
    }

    private Optional<String> getSchemaLocation(Database database)
    {
        Optional<String> schemaLocation = database.getLocation();
        if (schemaLocation.isEmpty() || schemaLocation.get().isEmpty()) {
            return Optional.empty();
        }

        return schemaLocation;
    }

    private String getTableLocation(Database schema, String tableName)
    {
        String schemaLocation = getSchemaLocation(schema)
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "The 'location' property must be specified either for the table or the schema"));
        String tableNameLocationComponent = escapeTableName(tableName);
        if (useUniqueTableLocation) {
            tableNameLocationComponent += "-" + randomUUID().toString().replace("-", "");
        }
        return appendPath(schemaLocation, tableNameLocationComponent);
    }

    private void checkPathContainsNoFiles(ConnectorSession session, Location targetPath)
    {
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            if (fileSystem.listFiles(targetPath).hasNext()) {
                throw new TrinoException(NOT_SUPPORTED, "Target location cannot contain any files: " + targetPath);
            }
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Unable to access file system for: " + targetPath, e);
        }
    }

    private void validateTableColumns(ConnectorTableMetadata tableMetadata)
    {
        checkPartitionColumns(tableMetadata.getColumns(), getPartitionedBy(tableMetadata.getProperties()));
        checkColumnTypes(tableMetadata.getColumns());
        if (getChangeDataFeedEnabled(tableMetadata.getProperties()).orElse(false)) {
            Set<String> conflicts = Sets.intersection(tableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(toImmutableSet()), CHANGE_DATA_FEED_COLUMN_NAMES);
            if (!conflicts.isEmpty()) {
                throw new TrinoException(NOT_SUPPORTED, "Unable to use %s when change data feed is enabled".formatted(conflicts));
            }
        }
    }

    private static void checkPartitionColumns(List<ColumnMetadata> columns, List<String> partitionColumnNames)
    {
        Set<String> columnNames = columns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());
        List<String> invalidPartitionNames = partitionColumnNames.stream()
                .filter(partitionColumnName -> !columnNames.contains(partitionColumnName))
                .collect(toImmutableList());

        if (columns.stream().filter(column -> partitionColumnNames.contains(column.getName()))
                .anyMatch(column -> column.getType() instanceof ArrayType || column.getType() instanceof MapType || column.getType() instanceof RowType)) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Using array, map or row type on partitioned columns is unsupported");
        }

        if (!invalidPartitionNames.isEmpty()) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Table property 'partition_by' contained column names which do not exist: " + invalidPartitionNames);
        }
        if (columns.size() == partitionColumnNames.size()) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Using all columns for partition columns is unsupported");
        }
    }

    private void checkColumnTypes(List<ColumnMetadata> columnMetadata)
    {
        for (ColumnMetadata column : columnMetadata) {
            Type type = column.getType();
            validateType(type);
        }
    }

    private static void deleteRecursivelyIfExists(TrinoFileSystem fileSystem, Location path)
    {
        try {
            fileSystem.deleteDirectory(path);
        }
        catch (IOException e) {
            LOG.warn(e, "IOException while trying to delete '%s'", path);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DeltaLakeOutputTableHandle handle = (DeltaLakeOutputTableHandle) tableHandle;

        String schemaName = handle.getSchemaName();
        String tableName = handle.getTableName();
        String location = handle.getLocation();

        List<DataFileInfo> dataFileInfos = fragments.stream()
                .map(Slice::getBytes)
                .map(dataFileInfoCodec::fromJson)
                .collect(toImmutableList());

        SchemaTableName schemaTableName = schemaTableName(schemaName, tableName);
        Table table = buildTable(session, schemaTableName, location, handle.isExternal());
        // Ensure the table has queryId set. This is relied on for exception handling
        String queryId = session.getQueryId();
        verify(
                getQueryId(table).orElseThrow(() -> new IllegalArgumentException("Query id is not present")).equals(queryId),
                "Table '%s' does not have correct query id set",
                table);

        ColumnMappingMode columnMappingMode = handle.getColumnMappingMode();
        String schemaString = handle.getSchemaString();
        List<String> columnNames = handle.getInputColumns().stream().map(DeltaLakeColumnHandle::getBaseColumnName).collect(toImmutableList());
        List<String> physicalPartitionNames = handle.getInputColumns().stream()
                .filter(column -> column.getColumnType() == PARTITION_KEY)
                .map(DeltaLakeColumnHandle::getBasePhysicalColumnName)
                .collect(toImmutableList());
        try {
            // For CTAS there is no risk of multiple writers racing. Using writer without transaction isolation so we are not limiting support for CTAS to
            // filesystems for which we have proper implementations of TransactionLogSynchronizers.
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriterWithoutTransactionIsolation(session, handle.getLocation());

            appendTableEntries(
                    0,
                    transactionLogWriter,
                    randomUUID().toString(),
                    schemaString,
                    handle.getPartitionedBy(),
                    configurationForNewTable(handle.getCheckpointInterval(), handle.getChangeDataFeedEnabled(), columnMappingMode, handle.getMaxColumnId()),
                    CREATE_TABLE_AS_OPERATION,
                    session,
                    handle.getComment(),
                    handle.getProtocolEntry());
            appendAddFileEntries(transactionLogWriter, dataFileInfos, physicalPartitionNames, columnNames, true);
            transactionLogWriter.flush();

            if (isCollectExtendedStatisticsColumnStatisticsOnWrite(session) && !computedStatistics.isEmpty()) {
                Optional<Instant> maxFileModificationTime = dataFileInfos.stream()
                        .map(DataFileInfo::getCreationTime)
                        .max(Long::compare)
                        .map(Instant::ofEpochMilli);
                Map<String, String> physicalColumnMapping = DeltaLakeSchemaSupport.getColumnMetadata(schemaString, typeManager, columnMappingMode).stream()
                        .map(e -> Map.entry(e.getName(), e.getPhysicalName()))
                        .collect(toImmutableMap(Entry::getKey, Entry::getValue));

                updateTableStatistics(
                        session,
                        Optional.empty(),
                        schemaTableName,
                        location,
                        maxFileModificationTime,
                        computedStatistics,
                        columnNames,
                        Optional.of(physicalColumnMapping));
            }

            PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner().orElseThrow());

            // As a precaution, clear the caches
            statisticsAccess.invalidateCache(schemaTableName, Optional.of(location));
            transactionLogAccess.invalidateCache(schemaTableName, Optional.of(location));
            try {
                metastore.createTable(session, table, principalPrivileges);
            }
            catch (TableAlreadyExistsException e) {
                // Ignore TableAlreadyExistsException when table looks like created by us.
                // This may happen when an actually successful metastore create call is retried
                // e.g. because of a timeout on our side.
                Optional<Table> existingTable = metastore.getRawMetastoreTable(schemaName, tableName);
                if (existingTable.isEmpty() || !isCreatedBy(existingTable.get(), queryId)) {
                    throw e;
                }
            }
        }
        catch (Exception e) {
            // Remove the transaction log entry if the table creation fails
            try {
                Location transactionLogDir = Location.of(getTransactionLogDir(location));
                fileSystemFactory.create(session).deleteDirectory(transactionLogDir);
            }
            catch (IOException ioException) {
                // Nothing to do, the IOException is probably the same reason why the initial write failed
                LOG.error(ioException, "Transaction log cleanup failed during CREATE TABLE rollback");
            }
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Failed to write Delta Lake transaction log entry", e);
        }

        return Optional.empty();
    }

    private static boolean isCreatedBy(Database database, String queryId)
    {
        Optional<String> databaseQueryId = getQueryId(database);
        return databaseQueryId.isPresent() && databaseQueryId.get().equals(queryId);
    }

    private static boolean isCreatedBy(Table table, String queryId)
    {
        Optional<String> tableQueryId = getQueryId(table);
        return tableQueryId.isPresent() && tableQueryId.get().equals(queryId);
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        DeltaLakeTableHandle handle = checkValidTableHandle(tableHandle);
        checkSupportedWriterVersion(handle);
        ColumnMappingMode columnMappingMode = getColumnMappingMode(handle.getMetadataEntry());
        if (columnMappingMode != ID && columnMappingMode != NAME && columnMappingMode != NONE) {
            throw new TrinoException(NOT_SUPPORTED, "Setting a table comment with column mapping %s is not supported".formatted(columnMappingMode));
        }

        ConnectorTableMetadata tableMetadata = getTableMetadata(session, handle);

        try {
            long commitVersion = handle.getReadVersion() + 1;

            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, handle.getLocation());
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    handle.getMetadataEntry().getId(),
                    handle.getMetadataEntry().getSchemaString(),
                    getPartitionedBy(tableMetadata.getProperties()),
                    handle.getMetadataEntry().getConfiguration(),
                    SET_TBLPROPERTIES_OPERATION,
                    session,
                    comment,
                    handle.getProtocolEntry());
            transactionLogWriter.flush();
        }
        catch (Exception e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Unable to comment on table: %s.%s", handle.getSchemaName(), handle.getTableName()), e);
        }
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        DeltaLakeTableHandle deltaLakeTableHandle = (DeltaLakeTableHandle) tableHandle;
        DeltaLakeColumnHandle deltaLakeColumnHandle = (DeltaLakeColumnHandle) column;
        verify(deltaLakeColumnHandle.isBaseColumn(), "Unexpected dereference: %s", column);
        checkSupportedWriterVersion(deltaLakeTableHandle);
        ColumnMappingMode columnMappingMode = getColumnMappingMode(deltaLakeTableHandle.getMetadataEntry());
        if (columnMappingMode != ID && columnMappingMode != NAME && columnMappingMode != NONE) {
            throw new TrinoException(NOT_SUPPORTED, "Setting a column comment with column mapping %s is not supported".formatted(columnMappingMode));
        }

        try {
            long commitVersion = deltaLakeTableHandle.getReadVersion() + 1;

            ImmutableMap.Builder<String, String> columnComments = ImmutableMap.builder();
            columnComments.putAll(getColumnComments(deltaLakeTableHandle.getMetadataEntry()).entrySet().stream()
                    .filter(e -> !e.getKey().equals(deltaLakeColumnHandle.getBaseColumnName()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
            comment.ifPresent(s -> columnComments.put(deltaLakeColumnHandle.getBaseColumnName(), s));

            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, deltaLakeTableHandle.getLocation());
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    deltaLakeTableHandle.getMetadataEntry().getId(),
                    getExactColumnNames(deltaLakeTableHandle.getMetadataEntry()),
                    deltaLakeTableHandle.getMetadataEntry().getOriginalPartitionColumns(),
                    getColumnTypes(deltaLakeTableHandle.getMetadataEntry()),
                    columnComments.buildOrThrow(),
                    getColumnsNullability(deltaLakeTableHandle.getMetadataEntry()),
                    getColumnsMetadata(deltaLakeTableHandle.getMetadataEntry()),
                    deltaLakeTableHandle.getMetadataEntry().getConfiguration(),
                    CHANGE_COLUMN_OPERATION,
                    session,
                    Optional.ofNullable(deltaLakeTableHandle.getMetadataEntry().getDescription()),
                    deltaLakeTableHandle.getProtocolEntry());
            transactionLogWriter.flush();
        }
        catch (Exception e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Unable to add '%s' column comment for: %s.%s", deltaLakeColumnHandle.getBaseColumnName(), deltaLakeTableHandle.getSchemaName(), deltaLakeTableHandle.getTableName()), e);
        }
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        trinoViewHiveMetastore.updateViewComment(session, viewName, comment);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        trinoViewHiveMetastore.updateViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata newColumnMetadata)
    {
        DeltaLakeTableHandle handle = checkValidTableHandle(tableHandle);
        checkSupportedWriterVersion(handle);
        ColumnMappingMode columnMappingMode = getColumnMappingMode(handle.getMetadataEntry());
        if (changeDataFeedEnabled(handle.getMetadataEntry()) && CHANGE_DATA_FEED_COLUMN_NAMES.contains(newColumnMetadata.getName())) {
            throw new TrinoException(NOT_SUPPORTED, "Column name %s is forbidden when change data feed is enabled".formatted(newColumnMetadata.getName()));
        }

        if (!newColumnMetadata.isNullable() && !transactionLogAccess.getActiveFiles(getSnapshot(handle.getSchemaTableName(), handle.getLocation(), session), session).isEmpty()) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Unable to add NOT NULL column '%s' for non-empty table: %s.%s", newColumnMetadata.getName(), handle.getSchemaName(), handle.getTableName()));
        }

        try {
            long commitVersion = handle.getReadVersion() + 1;

            AtomicInteger maxColumnId = switch (columnMappingMode) {
                case NONE -> new AtomicInteger();
                case ID, NAME -> new AtomicInteger(getMaxColumnId(handle.getMetadataEntry()));
                default -> throw new IllegalArgumentException("Unexpected column mapping mode: " + columnMappingMode);
            };

            List<String> columnNames = ImmutableList.<String>builder()
                    .addAll(getExactColumnNames(handle.getMetadataEntry()))
                    .add(newColumnMetadata.getName())
                    .build();
            ImmutableMap.Builder<String, String> columnComments = ImmutableMap.builder();
            columnComments.putAll(getColumnComments(handle.getMetadataEntry()));
            if (newColumnMetadata.getComment() != null) {
                columnComments.put(newColumnMetadata.getName(), newColumnMetadata.getComment());
            }
            ImmutableMap.Builder<String, Boolean> columnsNullability = ImmutableMap.builder();
            columnsNullability.putAll(getColumnsNullability(handle.getMetadataEntry()));
            columnsNullability.put(newColumnMetadata.getName(), newColumnMetadata.isNullable());
            Map<String, Object> columnTypes = ImmutableMap.<String, Object>builderWithExpectedSize(columnNames.size())
                    .putAll(getColumnTypes(handle.getMetadataEntry()))
                    .put(Map.entry(newColumnMetadata.getName(), serializeColumnType(columnMappingMode, maxColumnId, newColumnMetadata.getType())))
                    .buildOrThrow();

            ImmutableMap.Builder<String, Map<String, Object>> columnMetadata = ImmutableMap.builder();
            columnMetadata.putAll(getColumnsMetadata(handle.getMetadataEntry()));
            columnMetadata.put(newColumnMetadata.getName(), generateColumnMetadata(columnMappingMode, maxColumnId));

            Map<String, String> configuration = new HashMap<>(handle.getMetadataEntry().getConfiguration());
            if (columnMappingMode == ID || columnMappingMode == NAME) {
                checkArgument(maxColumnId.get() > 0, "maxColumnId must be larger than 0: %s", maxColumnId);
                configuration.put(MAX_COLUMN_ID_CONFIGURATION_KEY, String.valueOf(maxColumnId.get()));
            }

            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, handle.getLocation());
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    handle.getMetadataEntry().getId(),
                    serializeSchemaAsJson(
                            columnNames,
                            columnTypes,
                            columnComments.buildOrThrow(),
                            columnsNullability.buildOrThrow(),
                            columnMetadata.buildOrThrow()),
                    handle.getMetadataEntry().getOriginalPartitionColumns(),
                    configuration,
                    ADD_COLUMN_OPERATION,
                    session,
                    Optional.ofNullable(handle.getMetadataEntry().getDescription()),
                    handle.getProtocolEntry());
            transactionLogWriter.flush();
        }
        catch (Exception e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Unable to add '%s' column for: %s.%s", newColumnMetadata.getName(), handle.getSchemaName(), handle.getTableName()), e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) tableHandle;
        DeltaLakeColumnHandle deltaLakeColumn = (DeltaLakeColumnHandle) columnHandle;
        verify(deltaLakeColumn.isBaseColumn(), "Unexpected dereference: %s", deltaLakeColumn);
        String dropColumnName = deltaLakeColumn.getBaseColumnName();
        MetadataEntry metadataEntry = table.getMetadataEntry();

        checkSupportedWriterVersion(table);
        ColumnMappingMode columnMappingMode = getColumnMappingMode(metadataEntry);
        if (columnMappingMode != ColumnMappingMode.NAME && columnMappingMode != ColumnMappingMode.ID) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop column from table using column mapping mode " + columnMappingMode);
        }

        long commitVersion = table.getReadVersion() + 1;
        List<String> partitionColumns = metadataEntry.getOriginalPartitionColumns();
        if (partitionColumns.contains(dropColumnName)) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop partition column: " + dropColumnName);
        }

        // Use equalsIgnoreCase because the remote column name can contain uppercase characters
        // Creating a table with ambiguous names (e.g. "a" and "A") is disallowed, so this should be safe
        List<DeltaLakeColumnMetadata> columns = extractSchema(metadataEntry, typeManager);
        List<String> columnNames = getExactColumnNames(metadataEntry).stream()
                .filter(name -> !name.equalsIgnoreCase(dropColumnName))
                .collect(toImmutableList());
        if (columns.size() == columnNames.size()) {
            throw new ColumnNotFoundException(table.schemaTableName(), dropColumnName);
        }
        if (columnNames.size() == partitionColumns.size()) {
            throw new TrinoException(NOT_SUPPORTED, "Dropping the last non-partition column is unsupported");
        }
        Map<String, String> lowerCaseToExactColumnNames = getExactColumnNames(metadataEntry).stream()
                .collect(toImmutableMap(name -> name.toLowerCase(ENGLISH), name -> name));
        Map<String, String> physicalColumnNameMapping = columns.stream()
                .collect(toImmutableMap(DeltaLakeColumnMetadata::getName, DeltaLakeColumnMetadata::getPhysicalName));

        Map<String, Object> columnTypes = filterKeys(getColumnTypes(metadataEntry), name -> !name.equalsIgnoreCase(dropColumnName));
        Map<String, String> columnComments = filterKeys(getColumnComments(metadataEntry), name -> !name.equalsIgnoreCase(dropColumnName));
        Map<String, Boolean> columnsNullability = filterKeys(getColumnsNullability(metadataEntry), name -> !name.equalsIgnoreCase(dropColumnName));
        Map<String, Map<String, Object>> columnMetadata = filterKeys(getColumnsMetadata(metadataEntry), name -> !name.equalsIgnoreCase(dropColumnName));
        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, table.getLocation());
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    metadataEntry.getId(),
                    columnNames,
                    partitionColumns,
                    columnTypes,
                    columnComments,
                    columnsNullability,
                    columnMetadata,
                    metadataEntry.getConfiguration(),
                    DROP_COLUMN_OPERATION,
                    session,
                    Optional.ofNullable(metadataEntry.getDescription()),
                    table.getProtocolEntry());
            transactionLogWriter.flush();
        }
        catch (Exception e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Unable to drop '%s' column from: %s.%s", dropColumnName, table.getSchemaName(), table.getTableName()), e);
        }

        try {
            statisticsAccess.readExtendedStatistics(session, table.getSchemaTableName(), table.getLocation()).ifPresent(existingStatistics -> {
                ExtendedStatistics statistics = new ExtendedStatistics(
                        existingStatistics.getAlreadyAnalyzedModifiedTimeMax(),
                        existingStatistics.getColumnStatistics().entrySet().stream()
                                .filter(stats -> !stats.getKey().equalsIgnoreCase(toPhysicalColumnName(dropColumnName, lowerCaseToExactColumnNames, Optional.of(physicalColumnNameMapping))))
                                .collect(toImmutableMap(Entry::getKey, Entry::getValue)),
                        existingStatistics.getAnalyzedColumns()
                                .map(analyzedColumns -> analyzedColumns.stream().filter(column -> !column.equalsIgnoreCase(dropColumnName)).collect(toImmutableSet())));
                statisticsAccess.updateExtendedStatistics(session, table.getSchemaTableName(), table.getLocation(), statistics);
            });
        }
        catch (Exception e) {
            LOG.warn(e, "Failed to update extended statistics when dropping %s column from %s table", dropColumnName, table.schemaTableName());
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, String newColumnName)
    {
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) tableHandle;
        DeltaLakeColumnHandle deltaLakeColumn = (DeltaLakeColumnHandle) columnHandle;
        verify(deltaLakeColumn.isBaseColumn(), "Unexpected dereference: %s", deltaLakeColumn);
        String sourceColumnName = deltaLakeColumn.getBaseColumnName();

        checkSupportedWriterVersion(table);
        if (changeDataFeedEnabled(table.getMetadataEntry())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot rename column when change data feed is enabled");
        }

        MetadataEntry metadataEntry = table.getMetadataEntry();
        ColumnMappingMode columnMappingMode = getColumnMappingMode(metadataEntry);
        if (columnMappingMode != ColumnMappingMode.NAME && columnMappingMode != ColumnMappingMode.ID) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot rename column in table using column mapping mode " + columnMappingMode);
        }

        long commitVersion = table.getReadVersion() + 1;

        // Use equalsIgnoreCase because the remote column name can contain uppercase characters
        // Creating a table with ambiguous names (e.g. "a" and "A") is disallowed, so this should be safe
        List<String> partitionColumns = metadataEntry.getOriginalPartitionColumns().stream()
                .map(columnName -> columnName.equalsIgnoreCase(sourceColumnName) ? newColumnName : columnName)
                .collect(toImmutableList());

        List<String> columnNames = getExactColumnNames(metadataEntry).stream()
                .map(name -> name.equalsIgnoreCase(sourceColumnName) ? newColumnName : name)
                .collect(toImmutableList());
        Map<String, Object> columnTypes = getColumnTypes(metadataEntry).entrySet().stream()
                .map(column -> column.getKey().equalsIgnoreCase(sourceColumnName) ? Map.entry(newColumnName, column.getValue()) : column)
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
        Map<String, String> columnComments = getColumnComments(metadataEntry).entrySet().stream()
                .map(column -> column.getKey().equalsIgnoreCase(sourceColumnName) ? Map.entry(newColumnName, column.getValue()) : column)
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Boolean> columnsNullability = getColumnsNullability(metadataEntry).entrySet().stream()
                .map(column -> column.getKey().equalsIgnoreCase(sourceColumnName) ? Map.entry(newColumnName, column.getValue()) : column)
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Map<String, Object>> columnMetadata = getColumnsMetadata(metadataEntry).entrySet().stream()
                .map(column -> column.getKey().equalsIgnoreCase(sourceColumnName) ? Map.entry(newColumnName, column.getValue()) : column)
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, table.getLocation());
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    metadataEntry.getId(),
                    columnNames,
                    partitionColumns,
                    columnTypes,
                    columnComments,
                    columnsNullability,
                    columnMetadata,
                    metadataEntry.getConfiguration(),
                    RENAME_COLUMN_OPERATION,
                    session,
                    Optional.ofNullable(metadataEntry.getDescription()),
                    table.getProtocolEntry());
            transactionLogWriter.flush();
            // Don't update extended statistics because it uses physical column names internally
        }
        catch (Exception e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Unable to rename '%s' column for: %s.%s", sourceColumnName, table.getSchemaName(), table.getTableName()), e);
        }
    }

    private void appendTableEntries(
            long commitVersion,
            TransactionLogWriter transactionLogWriter,
            String tableId,
            List<String> columnNames,
            List<String> partitionColumnNames,
            Map<String, Object> columnTypes,
            Map<String, String> columnComments,
            Map<String, Boolean> columnNullability,
            Map<String, Map<String, Object>> columnMetadata,
            Map<String, String> configuration,
            String operation,
            ConnectorSession session,
            Optional<String> comment,
            ProtocolEntry protocolEntry)
    {
        appendTableEntries(
                commitVersion,
                transactionLogWriter,
                tableId,
                serializeSchemaAsJson(columnNames, columnTypes, columnComments, columnNullability, columnMetadata),
                partitionColumnNames,
                configuration,
                operation,
                session,
                comment,
                protocolEntry);
    }

    private void appendTableEntries(
            long commitVersion,
            TransactionLogWriter transactionLogWriter,
            String tableId,
            String schemaString,
            List<String> partitionColumnNames,
            Map<String, String> configuration,
            String operation,
            ConnectorSession session,
            Optional<String> comment,
            ProtocolEntry protocolEntry)
    {
        long createdTime = System.currentTimeMillis();
        transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, commitVersion, createdTime, operation, 0));

        transactionLogWriter.appendProtocolEntry(protocolEntry);

        transactionLogWriter.appendMetadataEntry(
                new MetadataEntry(
                        tableId,
                        null,
                        comment.orElse(null),
                        new Format("parquet", ImmutableMap.of()),
                        schemaString,
                        partitionColumnNames,
                        ImmutableMap.copyOf(configuration),
                        createdTime));
    }

    private static void appendAddFileEntries(TransactionLogWriter transactionLogWriter, List<DataFileInfo> dataFileInfos, List<String> partitionColumnNames, List<String> originalColumnNames, boolean dataChange)
            throws JsonProcessingException
    {
        Map<String, String> toOriginalColumnNames = originalColumnNames.stream()
                .collect(toImmutableMap(name -> name.toLowerCase(ENGLISH), identity()));
        for (DataFileInfo info : dataFileInfos) {
            // using Hashmap because partition values can be null
            Map<String, String> partitionValues = new HashMap<>();
            for (int i = 0; i < partitionColumnNames.size(); i++) {
                partitionValues.put(partitionColumnNames.get(i), info.getPartitionValues().get(i));
            }

            Optional<Map<String, Object>> minStats = toOriginalColumnNames(info.getStatistics().getMinValues(), toOriginalColumnNames);
            Optional<Map<String, Object>> maxStats = toOriginalColumnNames(info.getStatistics().getMaxValues(), toOriginalColumnNames);
            Optional<Map<String, Object>> nullStats = toOriginalColumnNames(info.getStatistics().getNullCount(), toOriginalColumnNames);
            DeltaLakeJsonFileStatistics statisticsWithExactNames = new DeltaLakeJsonFileStatistics(info.getStatistics().getNumRecords(), minStats, maxStats, nullStats);

            partitionValues = unmodifiableMap(partitionValues);

            transactionLogWriter.appendAddFileEntry(
                    new AddFileEntry(
                            toUriFormat(info.getPath()), // Paths are RFC 2396 URI encoded https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
                            partitionValues,
                            info.getSize(),
                            info.getCreationTime(),
                            dataChange,
                            Optional.of(serializeStatsAsJson(statisticsWithExactNames)),
                            Optional.empty(),
                            ImmutableMap.of()));
        }
    }

    private static Optional<Map<String, Object>> toOriginalColumnNames(Optional<Map<String, Object>> statistics, Map</* lowercase*/ String, String> lowerCaseToExactColumnNames)
    {
        return statistics.map(statsMap -> statsMap.entrySet().stream()
                .collect(toImmutableMap(
                        // Lowercase column names because io.trino.parquet.reader.MetadataReader lowercase the path
                        stats -> lowerCaseToExactColumnNames.getOrDefault(stats.getKey().toLowerCase(ENGLISH), stats.getKey()),
                        Entry::getValue)));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) tableHandle;
        checkWriteAllowed(session, table);
        checkWriteSupported(table);

        List<DeltaLakeColumnHandle> inputColumns = columns.stream()
                .map(handle -> (DeltaLakeColumnHandle) handle)
                .collect(toImmutableList());

        ConnectorTableMetadata tableMetadata = getTableMetadata(session, table);

        // This check acts as a safeguard in cases where the input columns may differ from the table metadata case-sensitively
        checkAllColumnsPassedOnInsert(tableMetadata, inputColumns);

        return createInsertHandle(session, retryMode, table, inputColumns);
    }

    private DeltaLakeInsertTableHandle createInsertHandle(ConnectorSession session, RetryMode retryMode, DeltaLakeTableHandle table, List<DeltaLakeColumnHandle> inputColumns)
    {
        String tableLocation = table.getLocation();
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            return new DeltaLakeInsertTableHandle(
                    table.getSchemaTableName(),
                    tableLocation,
                    table.getMetadataEntry(),
                    inputColumns,
                    getMandatoryCurrentVersion(fileSystem, tableLocation),
                    retryMode != NO_RETRIES);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private void checkAllColumnsPassedOnInsert(ConnectorTableMetadata tableMetadata, List<DeltaLakeColumnHandle> insertColumns)
    {
        List<String> allColumnNames = tableMetadata.getColumns().stream()
                .filter(not(ColumnMetadata::isHidden))
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        List<String> insertColumnNames = insertColumns.stream()
                // Lowercase because the above allColumnNames uses lowercase
                .map(column -> column.getBaseColumnName().toLowerCase(ENGLISH))
                .collect(toImmutableList());

        checkArgument(allColumnNames.equals(insertColumnNames), "Not all table columns passed on INSERT; table columns=%s; insert columns=%s", allColumnNames, insertColumnNames);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DeltaLakeInsertTableHandle handle = (DeltaLakeInsertTableHandle) insertHandle;

        List<DataFileInfo> dataFileInfos = fragments.stream()
                .map(Slice::getBytes)
                .map(dataFileInfoCodec::fromJson)
                .collect(toImmutableList());

        if (handle.isRetriesEnabled()) {
            cleanExtraOutputFiles(session, Location.of(handle.getLocation()), dataFileInfos);
        }

        boolean writeCommitted = false;
        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, handle.getLocation());

            long createdTime = Instant.now().toEpochMilli();

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            long commitVersion = getMandatoryCurrentVersion(fileSystem, handle.getLocation()) + 1;
            if (commitVersion != handle.getReadVersion() + 1) {
                throw new TransactionConflictException(format("Conflicting concurrent writes found. Expected transaction log version: %s, actual version: %s",
                        handle.getReadVersion(),
                        commitVersion - 1));
            }
            Optional<Long> checkpointInterval = handle.getMetadataEntry().getCheckpointInterval();
            // it is not obvious why we need to persist this readVersion
            transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, commitVersion, createdTime, INSERT_OPERATION, handle.getReadVersion()));

            ColumnMappingMode columnMappingMode = getColumnMappingMode(handle.getMetadataEntry());
            List<String> partitionColumns = getPartitionColumns(
                    handle.getMetadataEntry().getOriginalPartitionColumns(),
                    handle.getInputColumns(),
                    columnMappingMode);
            List<String> exactColumnNames = getExactColumnNames(handle.getMetadataEntry());
            appendAddFileEntries(transactionLogWriter, dataFileInfos, partitionColumns, exactColumnNames, true);

            transactionLogWriter.flush();
            writeCommitted = true;
            writeCheckpointIfNeeded(session, handle.getTableName(), handle.getLocation(), checkpointInterval, commitVersion);

            if (isCollectExtendedStatisticsColumnStatisticsOnWrite(session) && !computedStatistics.isEmpty() && !dataFileInfos.isEmpty()) {
                // TODO (https://github.com/trinodb/trino/issues/16088) Add synchronization when version conflict for INSERT is resolved.
                Optional<Instant> maxFileModificationTime = dataFileInfos.stream()
                        .map(DataFileInfo::getCreationTime)
                        .max(Long::compare)
                        .map(Instant::ofEpochMilli);
                updateTableStatistics(
                        session,
                        Optional.empty(),
                        handle.getTableName(),
                        handle.getLocation(),
                        maxFileModificationTime,
                        computedStatistics,
                        exactColumnNames,
                        Optional.of(extractSchema(handle.getMetadataEntry(), typeManager).stream()
                                .collect(toImmutableMap(DeltaLakeColumnMetadata::getName, DeltaLakeColumnMetadata::getPhysicalName))));
            }
        }
        catch (Exception e) {
            if (!writeCommitted) {
                // TODO perhaps it should happen in a background thread (https://github.com/trinodb/trino/issues/12011)
                cleanupFailedWrite(session, handle.getLocation(), dataFileInfos);
            }
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Failed to write Delta Lake transaction log entry", e);
        }

        return Optional.empty();
    }

    private static List<String> getPartitionColumns(List<String> originalPartitionColumns, List<DeltaLakeColumnHandle> dataColumns, ColumnMappingMode columnMappingMode)
    {
        return switch (columnMappingMode) {
            case NAME, ID -> getPartitionColumnsForNameOrIdMapping(originalPartitionColumns, dataColumns);
            case NONE -> originalPartitionColumns;
            case UNKNOWN -> throw new TrinoException(NOT_SUPPORTED, "Unsupported column mapping mode");
        };
    }

    private static List<String> getPartitionColumnsForNameOrIdMapping(List<String> originalPartitionColumns, List<DeltaLakeColumnHandle> dataColumns)
    {
        Map<String, DeltaLakeColumnHandle> nameToDataColumns = dataColumns.stream()
                .collect(toImmutableMap(DeltaLakeColumnHandle::getColumnName, Function.identity()));
        return originalPartitionColumns.stream()
                .map(columnName -> {
                    DeltaLakeColumnHandle dataColumn = nameToDataColumns.get(columnName);
                    // During writes we want to preserve original case of partition columns, if the name is not different from the physical name
                    if (dataColumn.getBasePhysicalColumnName().equalsIgnoreCase(columnName)) {
                        return columnName;
                    }
                    return dataColumn.getBasePhysicalColumnName();
                })
                .collect(toImmutableList());
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return mergeRowIdColumnHandle();
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return Optional.of(DeltaLakeUpdateHandle.INSTANCE);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        DeltaLakeTableHandle handle = (DeltaLakeTableHandle) tableHandle;
        if (isAppendOnly(handle.getMetadataEntry())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot modify rows from a table with '" + APPEND_ONLY_CONFIGURATION_KEY + "' set to true");
        }
        checkWriteAllowed(session, handle);
        checkWriteSupported(handle);

        List<DeltaLakeColumnHandle> inputColumns = getColumns(handle.getMetadataEntry()).stream()
                .filter(column -> column.getColumnType() != SYNTHESIZED)
                .collect(toImmutableList());

        DeltaLakeInsertTableHandle insertHandle = createInsertHandle(session, retryMode, handle, inputColumns);

        return new DeltaLakeMergeTableHandle(handle, insertHandle);
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        DeltaLakeMergeTableHandle mergeHandle = (DeltaLakeMergeTableHandle) mergeTableHandle;
        DeltaLakeTableHandle handle = mergeHandle.getTableHandle();

        List<DeltaLakeMergeResult> mergeResults = fragments.stream()
                .map(Slice::getBytes)
                .map(mergeResultJsonCodec::fromJson)
                .collect(toImmutableList());

        List<String> oldFiles = mergeResults.stream()
                .map(DeltaLakeMergeResult::getOldFile)
                .flatMap(Optional::stream)
                .collect(toImmutableList());

        List<DataFileInfo> allFiles = mergeResults.stream()
                .map(DeltaLakeMergeResult::getNewFile)
                .flatMap(Optional::stream)
                .collect(toImmutableList());

        Map<Boolean, List<DataFileInfo>> split = allFiles.stream()
                .collect(partitioningBy(dataFile -> dataFile.getDataFileType() == DATA));

        List<DataFileInfo> newFiles = ImmutableList.copyOf(split.get(true));
        List<DataFileInfo> cdcFiles = ImmutableList.copyOf(split.get(false));

        if (mergeHandle.getInsertTableHandle().isRetriesEnabled()) {
            cleanExtraOutputFiles(session, Location.of(handle.getLocation()), allFiles);
        }

        Optional<Long> checkpointInterval = handle.getMetadataEntry().getCheckpointInterval();

        String tableLocation = handle.getLocation();
        boolean writeCommitted = false;
        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, tableLocation);

            long createdTime = Instant.now().toEpochMilli();

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            long currentVersion = getMandatoryCurrentVersion(fileSystem, tableLocation);
            if (currentVersion != handle.getReadVersion()) {
                throw new TransactionConflictException(format("Conflicting concurrent writes found. Expected transaction log version: %s, actual version: %s", handle.getReadVersion(), currentVersion));
            }
            long commitVersion = currentVersion + 1;

            transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, commitVersion, createdTime, MERGE_OPERATION, handle.getReadVersion()));
            // TODO: Delta writes another field "operationMetrics" (https://github.com/trinodb/trino/issues/12005)

            long writeTimestamp = Instant.now().toEpochMilli();

            ColumnMappingMode columnMappingMode = getColumnMappingMode(handle.getMetadataEntry());
            List<String> partitionColumns = getPartitionColumns(
                    handle.getMetadataEntry().getOriginalPartitionColumns(),
                    mergeHandle.getInsertTableHandle().getInputColumns(),
                    columnMappingMode);

            if (!cdcFiles.isEmpty()) {
                appendCdcFilesInfos(transactionLogWriter, cdcFiles, partitionColumns);
            }

            for (String file : oldFiles) {
                transactionLogWriter.appendRemoveFileEntry(new RemoveFileEntry(file, writeTimestamp, true));
            }

            appendAddFileEntries(transactionLogWriter, newFiles, partitionColumns, getExactColumnNames(handle.getMetadataEntry()), true);

            transactionLogWriter.flush();
            writeCommitted = true;

            writeCheckpointIfNeeded(session, new SchemaTableName(handle.getSchemaName(), handle.getTableName()), handle.getLocation(), checkpointInterval, commitVersion);
        }
        catch (IOException | RuntimeException e) {
            if (!writeCommitted) {
                // TODO perhaps it should happen in a background thread (https://github.com/trinodb/trino/issues/12011)
                cleanupFailedWrite(session, tableLocation, allFiles);
            }
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Failed to write Delta Lake transaction log entry", e);
        }
    }

    private static void appendCdcFilesInfos(
            TransactionLogWriter transactionLogWriter,
            List<DataFileInfo> cdcFilesInfos,
            List<String> partitionColumnNames)
    {
        for (DataFileInfo info : cdcFilesInfos) {
            // using Hashmap because partition values can be null
            Map<String, String> partitionValues = new HashMap<>();
            for (int i = 0; i < partitionColumnNames.size(); i++) {
                partitionValues.put(partitionColumnNames.get(i), info.getPartitionValues().get(i));
            }
            partitionValues = unmodifiableMap(partitionValues);

            transactionLogWriter.appendCdcEntry(
                    new CdcEntry(
                            toUriFormat(info.getPath()),
                            partitionValues,
                            info.getSize()));
        }
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            String procedureName,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        DeltaLakeTableHandle tableHandle = checkValidTableHandle(connectorTableHandle);

        DeltaLakeTableProcedureId procedureId;
        try {
            procedureId = DeltaLakeTableProcedureId.valueOf(procedureName);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown procedure '" + procedureName + "'");
        }

        return switch (procedureId) {
            case OPTIMIZE -> getTableHandleForOptimize(tableHandle, executeProperties, retryMode);
        };
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForOptimize(DeltaLakeTableHandle tableHandle, Map<String, Object> executeProperties, RetryMode retryMode)
    {
        DataSize maxScannedFileSize = (DataSize) executeProperties.get("file_size_threshold");

        List<DeltaLakeColumnHandle> columns = getColumns(tableHandle.getMetadataEntry()).stream()
                .filter(column -> column.getColumnType() != SYNTHESIZED)
                .collect(toImmutableList());

        return Optional.of(new DeltaLakeTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                OPTIMIZE,
                new DeltaTableOptimizeHandle(
                        tableHandle.getMetadataEntry(),
                        columns,
                        tableHandle.getMetadataEntry().getOriginalPartitionColumns(),
                        maxScannedFileSize,
                        Optional.empty(),
                        retryMode != NO_RETRIES),
                tableHandle.getLocation()));
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        DeltaLakeTableExecuteHandle executeHandle = (DeltaLakeTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.getProcedureId()) {
            case OPTIMIZE:
                return getLayoutForOptimize(executeHandle);
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.getProcedureId() + "'");
    }

    private Optional<ConnectorTableLayout> getLayoutForOptimize(DeltaLakeTableExecuteHandle executeHandle)
    {
        DeltaTableOptimizeHandle optimizeHandle = (DeltaTableOptimizeHandle) executeHandle.getProcedureHandle();
        List<String> partitionColumnNames = optimizeHandle.getMetadataEntry().getLowercasePartitionColumns();
        if (partitionColumnNames.isEmpty()) {
            return Optional.empty();
        }
        Map<String, DeltaLakeColumnHandle> columnsByName = optimizeHandle.getTableColumns().stream()
                .collect(toImmutableMap(columnHandle -> columnHandle.getColumnName(), identity()));
        ImmutableList.Builder<DeltaLakeColumnHandle> partitioningColumns = ImmutableList.builder();
        for (String columnName : partitionColumnNames) {
            partitioningColumns.add(columnsByName.get(columnName));
        }
        DeltaLakePartitioningHandle partitioningHandle = new DeltaLakePartitioningHandle(partitioningColumns.build());
        return Optional.of(new ConnectorTableLayout(partitioningHandle, partitionColumnNames, true));
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(
            ConnectorSession session,
            ConnectorTableExecuteHandle tableExecuteHandle,
            ConnectorTableHandle updatedSourceTableHandle)
    {
        DeltaLakeTableExecuteHandle executeHandle = (DeltaLakeTableExecuteHandle) tableExecuteHandle;
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) updatedSourceTableHandle;
        switch (executeHandle.getProcedureId()) {
            case OPTIMIZE:
                return beginOptimize(session, executeHandle, table);
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.getProcedureId() + "'");
    }

    private BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginOptimize(
            ConnectorSession session,
            DeltaLakeTableExecuteHandle executeHandle,
            DeltaLakeTableHandle table)
    {
        DeltaTableOptimizeHandle optimizeHandle = (DeltaTableOptimizeHandle) executeHandle.getProcedureHandle();

        checkWriteAllowed(session, table);
        checkSupportedWriterVersion(table);

        return new BeginTableExecuteResult<>(
                executeHandle.withProcedureHandle(optimizeHandle.withCurrentVersion(table.getReadVersion())),
                table.forOptimize(true, optimizeHandle.getMaxScannedFileSize()));
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        DeltaLakeTableExecuteHandle executeHandle = (DeltaLakeTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.getProcedureId()) {
            case OPTIMIZE:
                finishOptimize(session, executeHandle, fragments, splitSourceInfo);
                return;
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.getProcedureId() + "'");
    }

    private void finishOptimize(ConnectorSession session, DeltaLakeTableExecuteHandle executeHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        DeltaTableOptimizeHandle optimizeHandle = (DeltaTableOptimizeHandle) executeHandle.getProcedureHandle();
        long readVersion = optimizeHandle.getCurrentVersion().orElseThrow(() -> new IllegalArgumentException("currentVersion not set"));
        String tableLocation = executeHandle.getTableLocation();

        // paths to be deleted
        Set<String> scannedPaths = splitSourceInfo.stream()
                .map(String.class::cast)
                .collect(toImmutableSet());

        // files to be added
        List<DataFileInfo> dataFileInfos = fragments.stream()
                .map(Slice::getBytes)
                .map(dataFileInfoCodec::fromJson)
                .collect(toImmutableList());

        if (optimizeHandle.isRetriesEnabled()) {
            cleanExtraOutputFiles(session, Location.of(executeHandle.getTableLocation()), dataFileInfos);
        }

        boolean writeCommitted = false;
        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, tableLocation);

            long createdTime = Instant.now().toEpochMilli();
            long commitVersion = readVersion + 1;
            transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, commitVersion, createdTime, OPTIMIZE_OPERATION, readVersion));
            // TODO: Delta writes another field "operationMetrics" that I haven't
            //   seen before. It contains delete/update metrics. Investigate/include it.

            long writeTimestamp = Instant.now().toEpochMilli();

            for (String scannedPath : scannedPaths) {
                String relativePath = relativePath(tableLocation, scannedPath);
                transactionLogWriter.appendRemoveFileEntry(new RemoveFileEntry(relativePath, writeTimestamp, false));
            }

            // Note: during writes we want to preserve original case of partition columns
            List<String> partitionColumns = getPartitionColumns(
                    optimizeHandle.getMetadataEntry().getOriginalPartitionColumns(),
                    optimizeHandle.getTableColumns(),
                    getColumnMappingMode(optimizeHandle.getMetadataEntry()));
            appendAddFileEntries(transactionLogWriter, dataFileInfos, partitionColumns, getExactColumnNames(optimizeHandle.getMetadataEntry()), false);

            transactionLogWriter.flush();
            writeCommitted = true;
            Optional<Long> checkpointInterval = Optional.of(1L); // force checkpoint
            writeCheckpointIfNeeded(session, executeHandle.getSchemaTableName(), executeHandle.getTableLocation(), checkpointInterval, commitVersion);
        }
        catch (Exception e) {
            if (!writeCommitted) {
                // TODO perhaps it should happen in a background thread (https://github.com/trinodb/trino/issues/12011)
                cleanupFailedWrite(session, tableLocation, dataFileInfos);
            }
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Failed to write Delta Lake transaction log entry", e);
        }
    }

    private void checkWriteAllowed(ConnectorSession session, DeltaLakeTableHandle table)
    {
        if (!allowWrite(session, table)) {
            String fileSystem = Location.of(table.getLocation()).scheme().orElse("unknown");
            throw new TrinoException(
                    NOT_SUPPORTED,
                    format("Writes are not enabled on the %1$s filesystem in order to avoid eventual data corruption which may be caused by concurrent data modifications on the table. " +
                            "Writes to the %1$s filesystem can be however enabled with the '%2$s' configuration property.", fileSystem, ENABLE_NON_CONCURRENT_WRITES_CONFIGURATION_KEY));
        }
    }

    private boolean allowWrite(ConnectorSession session, DeltaLakeTableHandle tableHandle)
    {
        try {
            String tableMetadataDirectory = getTransactionLogDir(tableHandle.getLocation());
            boolean requiresOptIn = transactionLogWriterFactory.newWriter(session, tableMetadataDirectory).isUnsafe();
            return !requiresOptIn || unsafeWritesEnabled;
        }
        catch (TrinoException e) {
            if (e.getErrorCode() == NOT_SUPPORTED.toErrorCode()) {
                return false;
            }
            throw e;
        }
    }

    private void checkWriteSupported(DeltaLakeTableHandle handle)
    {
        checkSupportedWriterVersion(handle);
        checkUnsupportedGeneratedColumns(handle.getMetadataEntry());
        ColumnMappingMode columnMappingMode = getColumnMappingMode(handle.getMetadataEntry());
        if (!(columnMappingMode == NONE || columnMappingMode == ColumnMappingMode.NAME || columnMappingMode == ColumnMappingMode.ID)) {
            throw new TrinoException(NOT_SUPPORTED, "Writing with column mapping %s is not supported".formatted(columnMappingMode));
        }
        if (getColumnIdentities(handle.getMetadataEntry()).values().stream().anyMatch(identity -> identity)) {
            throw new TrinoException(NOT_SUPPORTED, "Writing to tables with identity columns is not supported");
        }
        // TODO: Check writer-features
    }

    private void checkUnsupportedGeneratedColumns(MetadataEntry metadataEntry)
    {
        Map<String, String> columnGeneratedExpressions = getGeneratedColumnExpressions(metadataEntry);
        if (!columnGeneratedExpressions.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Writing to tables with generated columns is not supported");
        }
    }

    private void checkSupportedWriterVersion(DeltaLakeTableHandle handle)
    {
        int requiredWriterVersion = handle.getProtocolEntry().getMinWriterVersion();
        if (requiredWriterVersion > MAX_WRITER_VERSION) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    format("Table %s requires Delta Lake writer version %d which is not supported", handle.getSchemaTableName(), requiredWriterVersion));
        }
    }

    private TableSnapshot getSnapshot(SchemaTableName schemaTableName, String tableLocation, ConnectorSession session)
    {
        try {
            return transactionLogAccess.loadSnapshot(schemaTableName, tableLocation, session);
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Error getting snapshot for " + schemaTableName, e);
        }
    }

    private ProtocolEntry protocolEntryForNewTable(Map<String, Object> properties)
    {
        int readerVersion = DEFAULT_READER_VERSION;
        int writerVersion = DEFAULT_WRITER_VERSION;
        Optional<Boolean> changeDataFeedEnabled = getChangeDataFeedEnabled(properties);
        if (changeDataFeedEnabled.isPresent() && changeDataFeedEnabled.get()) {
            // Enabling cdf (change data feed) requires setting the writer version to 4
            writerVersion = CDF_SUPPORTED_WRITER_VERSION;
        }
        ColumnMappingMode columnMappingMode = DeltaLakeTableProperties.getColumnMappingMode(properties);
        if (columnMappingMode == ID || columnMappingMode == NAME) {
            // TODO Add 'columnMapping' feature to reader and writer features when supporting writer version 7
            readerVersion = max(readerVersion, COLUMN_MAPPING_MODE_SUPPORTED_READER_VERSION);
            writerVersion = max(writerVersion, COLUMN_MAPPING_MODE_SUPPORTED_WRITER_VERSION);
        }
        return new ProtocolEntry(readerVersion, writerVersion, Optional.empty(), Optional.empty());
    }

    private void writeCheckpointIfNeeded(ConnectorSession session, SchemaTableName table, String tableLocation, Optional<Long> checkpointInterval, long newVersion)
    {
        try {
            // We are writing checkpoint synchronously. It should not be long lasting operation for tables where transaction log is not humongous.
            // Tables with really huge transaction logs would behave poorly in read flow already.
            TableSnapshot snapshot = getSnapshot(table, tableLocation, session);
            long lastCheckpointVersion = snapshot.getLastCheckpointVersion().orElse(0L);
            if (newVersion - lastCheckpointVersion < checkpointInterval.orElse(defaultCheckpointInterval)) {
                return;
            }

            // TODO: There is a race possibility here(https://github.com/trinodb/trino/issues/12004),
            // which may result in us not writing checkpoints at exactly the planned frequency.
            // The snapshot obtained above may already be on a version higher than `newVersion` because some other transaction could have just been committed.
            // This does not pose correctness issue but may be confusing if someone looks into transaction log.
            // To fix that we should allow for getting snapshot for given version.
            if (snapshot.getVersion() > newVersion) {
                LOG.info("Snapshot for table %s already at version %s when checkpoint requested for version %s", table, snapshot.getVersion(), newVersion);
            }

            checkpointWriterManager.writeCheckpoint(session, snapshot);
        }
        catch (Exception e) {
            // We can't fail here as transaction was already committed, in case of INSERT this could result
            // in inserting data twice if client saw an error and decided to retry
            LOG.error(e, "Failed to write checkpoint for table %s for version %s", table, newVersion);
        }
    }

    private void cleanupFailedWrite(ConnectorSession session, String tableLocation, List<DataFileInfo> dataFiles)
    {
        Location location = Location.of(tableLocation);
        List<Location> filesToDelete = dataFiles.stream()
                .map(DataFileInfo::getPath)
                .map(location::appendPath)
                .collect(toImmutableList());
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            fileSystem.deleteFiles(filesToDelete);
        }
        catch (Exception e) {
            // Can be safely ignored since a VACUUM from DeltaLake will take care of such orphaned files
            LOG.warn(e, "Failed cleanup of leftover files from failed write, files are: %s", filesToDelete);
        }
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        boolean isPartitioned = !((DeltaLakeTableHandle) table).getMetadataEntry().getLowercasePartitionColumns().isEmpty();
        return Optional.of(new DeltaLakeInputInfo(isPartitioned));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LocatedTableHandle handle = (LocatedTableHandle) tableHandle;
        boolean deleteData = handle.managed();
        metastore.dropTable(session, handle.schemaTableName(), handle.location(), deleteData);
        if (deleteData) {
            try {
                fileSystemFactory.create(session).deleteDirectory(Location.of(handle.location()));
            }
            catch (IOException e) {
                throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, format("Failed to delete directory %s of the table %s", handle.location(), handle.schemaTableName()), e);
            }
        }
        // As a precaution, clear the caches
        statisticsAccess.invalidateCache(handle.schemaTableName(), Optional.of(handle.location()));
        transactionLogAccess.invalidateCache(handle.schemaTableName(), Optional.of(handle.location()));
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        DeltaLakeTableHandle handle = checkValidTableHandle(tableHandle);
        DeltaMetastoreTable table = metastore.getTable(handle.getSchemaName(), handle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));
        if (table.managed() && !allowManagedTableRename) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming managed tables is not allowed with current metastore configuration");
        }
        metastore.renameTable(session, handle.getSchemaTableName(), newTableName);
    }

    private CommitInfoEntry getCommitInfoEntry(
            ConnectorSession session,
            long commitVersion,
            long createdTime,
            String operation,
            long readVersion)
    {
        return new CommitInfoEntry(
                commitVersion,
                createdTime,
                session.getUser(),
                session.getUser(),
                operation,
                ImmutableMap.of("queryId", session.getQueryId()),
                null,
                null,
                "trino-" + nodeVersion + "-" + nodeId,
                readVersion,
                ISOLATION_LEVEL,
                Optional.of(true));
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        DeltaLakeTableHandle handle = checkValidTableHandle(tableHandle);
        Set<String> unsupportedProperties = difference(properties.keySet(), UPDATABLE_TABLE_PROPERTIES);
        if (!unsupportedProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The following properties cannot be updated: " + String.join(", ", unsupportedProperties));
        }

        ProtocolEntry currentProtocolEntry = handle.getProtocolEntry();

        long createdTime = Instant.now().toEpochMilli();

        int requiredWriterVersion = currentProtocolEntry.getMinWriterVersion();
        Optional<MetadataEntry> metadataEntry = Optional.empty();
        if (properties.containsKey(CHANGE_DATA_FEED_ENABLED_PROPERTY)) {
            boolean changeDataFeedEnabled = (Boolean) properties.get(CHANGE_DATA_FEED_ENABLED_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The change_data_feed_enabled property cannot be empty"));
            if (changeDataFeedEnabled) {
                Set<String> columnNames = getColumns(handle.getMetadataEntry()).stream().map(DeltaLakeColumnHandle::getBaseColumnName).collect(toImmutableSet());
                Set<String> conflicts = Sets.intersection(columnNames, CHANGE_DATA_FEED_COLUMN_NAMES);
                if (!conflicts.isEmpty()) {
                    throw new TrinoException(NOT_SUPPORTED, "Unable to enable change data feed because table contains %s columns".formatted(conflicts));
                }
                requiredWriterVersion = max(requiredWriterVersion, CDF_SUPPORTED_WRITER_VERSION);
            }
            Map<String, String> configuration = new HashMap<>(handle.getMetadataEntry().getConfiguration());
            configuration.put(DELTA_CHANGE_DATA_FEED_ENABLED_PROPERTY, String.valueOf(changeDataFeedEnabled));
            metadataEntry = Optional.of(buildMetadataEntry(handle.getMetadataEntry(), configuration, createdTime));
        }

        long readVersion = handle.getReadVersion();
        long commitVersion = readVersion + 1;

        Optional<ProtocolEntry> protocolEntry = Optional.empty();
        if (requiredWriterVersion != currentProtocolEntry.getMinWriterVersion()) {
            protocolEntry = Optional.of(new ProtocolEntry(currentProtocolEntry.getMinReaderVersion(), requiredWriterVersion, currentProtocolEntry.getReaderFeatures(), currentProtocolEntry.getWriterFeatures()));
        }

        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, handle.getLocation());
            transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, commitVersion, createdTime, SET_TBLPROPERTIES_OPERATION, readVersion));
            protocolEntry.ifPresent(transactionLogWriter::appendProtocolEntry);

            metadataEntry.ifPresent(transactionLogWriter::appendMetadataEntry);

            transactionLogWriter.flush();
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Failed to write Delta Lake transaction log entry", e);
        }
    }

    private MetadataEntry buildMetadataEntry(MetadataEntry metadataEntry, Map<String, String> configuration, long createdTime)
    {
        return new MetadataEntry(
                metadataEntry.getId(),
                metadataEntry.getName(),
                metadataEntry.getDescription(),
                metadataEntry.getFormat(),
                metadataEntry.getSchemaString(),
                metadataEntry.getOriginalPartitionColumns(),
                configuration,
                createdTime);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        if (isHiveSystemSchema(schemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "Schema properties are not supported for system schema: " + schemaName);
        }
        return metastore.getDatabase(schemaName)
                .map(DeltaLakeSchemaProperties::fromDatabase)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        trinoViewHiveMetastore.createView(session, viewName, definition, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        trinoViewHiveMetastore.dropView(viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return trinoViewHiveMetastore.listViews(schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return trinoViewHiveMetastore.getViews(schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return trinoViewHiveMetastore.getView(viewName);
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<TrinoPrincipal> grantor)
    {
        accessControlMetadata.createRole(session, role, grantor.map(HivePrincipal::from));
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        accessControlMetadata.dropRole(session, role);
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return accessControlMetadata.listRoles(session);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, TrinoPrincipal principal)
    {
        return ImmutableSet.copyOf(accessControlMetadata.listRoleGrants(session, HivePrincipal.from(principal)));
    }

    @Override
    public void grantRoles(ConnectorSession session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean withAdminOption, Optional<TrinoPrincipal> grantor)
    {
        accessControlMetadata.grantRoles(session, roles, HivePrincipal.from(grantees), withAdminOption, grantor.map(HivePrincipal::from));
    }

    @Override
    public void revokeRoles(ConnectorSession session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOptionFor, Optional<TrinoPrincipal> grantor)
    {
        accessControlMetadata.revokeRoles(session, roles, HivePrincipal.from(grantees), adminOptionFor, grantor.map(HivePrincipal::from));
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, TrinoPrincipal principal)
    {
        return accessControlMetadata.listApplicableRoles(session, HivePrincipal.from(principal));
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        return accessControlMetadata.listEnabledRoles(session);
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        accessControlMetadata.grantTablePrivileges(session, schemaTableName, privileges, HivePrincipal.from(grantee), grantOption);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        accessControlMetadata.revokeTablePrivileges(session, schemaTableName, privileges, HivePrincipal.from(grantee), grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix schemaTablePrefix)
    {
        return accessControlMetadata.listTablePrivileges(session, listTables(session, schemaTablePrefix));
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        SchemaTableName tableName = prefix.toSchemaTableName();
        return metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .map(table -> ImmutableList.of(tableName))
                .orElse(ImmutableList.of());
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    private static String toUriFormat(String path)
    {
        verify(!path.startsWith("/") && !path.contains(":/"), "unexpected path: %s", path);
        try {
            return new URI(null, null, path, null).toString();
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid path: " + path, e);
        }
    }

    static String relativePath(String basePath, String path)
    {
        String basePathDirectory = basePath.endsWith("/") ? basePath : basePath + "/";
        checkArgument(path.startsWith(basePathDirectory) && (path.length() > basePathDirectory.length()),
                "path [%s] must be a subdirectory of basePath [%s]", path, basePath);
        return toUriFormat(path.substring(basePathDirectory.length()));
    }

    public void rollback()
    {
        // The actions are responsible for cleanup in case operation is aborted.
        // So far this is used by CTAS flow which does not require an explicit commit operation therefore
        // DeltaLakeMetadata does not define a commit() method.
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) handle;
        SchemaTableName tableName = tableHandle.getSchemaTableName();

        Set<DeltaLakeColumnHandle> partitionColumns = ImmutableSet.copyOf(extractPartitionColumns(tableHandle.getMetadataEntry(), typeManager));
        Map<ColumnHandle, Domain> constraintDomains = constraint.getSummary().getDomains().orElseThrow(() -> new IllegalArgumentException("constraint summary is NONE"));

        ImmutableMap.Builder<DeltaLakeColumnHandle, Domain> enforceableDomains = ImmutableMap.builder();
        ImmutableMap.Builder<DeltaLakeColumnHandle, Domain> unenforceableDomains = ImmutableMap.builder();
        ImmutableSet.Builder<DeltaLakeColumnHandle> constraintColumns = ImmutableSet.builder();
        // We need additional field to track partition columns used in queries as enforceDomains seem to be not catching
        // cases when partition columns is used within complex filter as 'partitionColumn % 2 = 0'
        constraint.getPredicateColumns().stream()
                .flatMap(Collection::stream)
                .map(DeltaLakeColumnHandle.class::cast)
                .forEach(constraintColumns::add);
        for (Entry<ColumnHandle, Domain> domainEntry : constraintDomains.entrySet()) {
            DeltaLakeColumnHandle column = (DeltaLakeColumnHandle) domainEntry.getKey();
            if (!partitionColumns.contains(column)) {
                unenforceableDomains.put(column, domainEntry.getValue());
            }
            else {
                enforceableDomains.put(column, domainEntry.getValue());
            }
            constraintColumns.add(column);
        }

        TupleDomain<DeltaLakeColumnHandle> newEnforcedConstraint = TupleDomain.withColumnDomains(enforceableDomains.buildOrThrow());
        TupleDomain<DeltaLakeColumnHandle> newUnenforcedConstraint = TupleDomain.withColumnDomains(unenforceableDomains.buildOrThrow());
        DeltaLakeTableHandle newHandle = new DeltaLakeTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                tableHandle.isManaged(),
                tableHandle.getLocation(),
                tableHandle.getMetadataEntry(),
                tableHandle.getProtocolEntry(),
                // Do not simplify the enforced constraint, the connector is guaranteeing the constraint will be applied as is.
                // The unenforced constraint will still be checked by the engine.
                tableHandle.getEnforcedPartitionConstraint()
                        .intersect(newEnforcedConstraint),
                tableHandle.getNonPartitionConstraint()
                        .intersect(newUnenforcedConstraint)
                        .simplify(domainCompactionThreshold),
                Sets.union(tableHandle.getConstraintColumns(), constraintColumns.build()),
                tableHandle.getWriteType(),
                tableHandle.getProjectedColumns(),
                tableHandle.getUpdatedColumns(),
                tableHandle.getUpdateRowIdColumns(),
                Optional.empty(),
                false,
                Optional.empty(),
                tableHandle.getReadVersion());

        if (tableHandle.getEnforcedPartitionConstraint().equals(newHandle.getEnforcedPartitionConstraint()) &&
                tableHandle.getNonPartitionConstraint().equals(newHandle.getNonPartitionConstraint()) &&
                tableHandle.getConstraintColumns().equals(newHandle.getConstraintColumns())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                newUnenforcedConstraint.transformKeys(ColumnHandle.class::cast),
                false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        DeltaLakeTableHandle deltaLakeTableHandle = (DeltaLakeTableHandle) tableHandle;

        // Create projected column representations for supported sub expressions. Simple column references and chain of
        // dereferences on a variable are supported right now.
        Set<ConnectorExpression> projectedExpressions = projections.stream()
                .flatMap(expression -> extractSupportedProjectedColumns(expression).stream())
                .collect(toImmutableSet());

        Map<ConnectorExpression, ProjectedColumnRepresentation> columnProjections = projectedExpressions.stream()
                .collect(toImmutableMap(Function.identity(), ApplyProjectionUtil::createProjectedColumnRepresentation));

        // all references are simple variables
        if (!isProjectionPushdownEnabled(session)
                || columnProjections.values().stream().allMatch(ProjectedColumnRepresentation::isVariable)) {
            Set<DeltaLakeColumnHandle> projectedColumns = assignments.values().stream()
                    .map(DeltaLakeColumnHandle.class::cast)
                    .collect(toImmutableSet());
            // Check if column was projected already in previous call
            if (deltaLakeTableHandle.getProjectedColumns().isPresent()
                    && deltaLakeTableHandle.getProjectedColumns().get().equals(projectedColumns)) {
                return Optional.empty();
            }

            List<Assignment> newColumnAssignments = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((DeltaLakeColumnHandle) assignment.getValue()).getBaseType()))
                    .collect(toImmutableList());

            return Optional.of(new ProjectionApplicationResult<>(
                    deltaLakeTableHandle.withProjectedColumns(projectedColumns),
                    projections,
                    newColumnAssignments,
                    false));
        }

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<DeltaLakeColumnHandle> projectedColumnsBuilder = ImmutableSet.builder();

        for (Map.Entry<ConnectorExpression, ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ProjectedColumnRepresentation projectedColumn = entry.getValue();

            DeltaLakeColumnHandle projectedColumnHandle;
            String projectedColumnName;

            // See if input already contains a columnhandle for this projected column, avoid creating duplicates.
            Optional<String> existingColumn = find(assignments, projectedColumn);

            if (existingColumn.isPresent()) {
                projectedColumnName = existingColumn.get();
                projectedColumnHandle = (DeltaLakeColumnHandle) assignments.get(projectedColumnName);
            }
            else {
                // Create a new column handle
                DeltaLakeColumnHandle oldColumnHandle = (DeltaLakeColumnHandle) assignments.get(projectedColumn.getVariable().getName());
                projectedColumnHandle = projectColumn(oldColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType(), getColumnMappingMode(deltaLakeTableHandle.getMetadataEntry()));
                projectedColumnName = projectedColumnHandle.getQualifiedPhysicalName();
            }

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
                deltaLakeTableHandle.withProjectedColumns(projectedColumnsBuilder.build()),
                newProjections,
                outputAssignments,
                false));
    }

    private static DeltaLakeColumnHandle projectColumn(DeltaLakeColumnHandle column, List<Integer> indices, Type projectedColumnType, ColumnMappingMode columnMappingMode)
    {
        if (indices.isEmpty()) {
            return column;
        }
        Optional<DeltaLakeColumnProjectionInfo> existingProjectionInfo = column.getProjectionInfo();
        ImmutableList.Builder<String> dereferenceNames = ImmutableList.builder();
        ImmutableList.Builder<Integer> dereferenceIndices = ImmutableList.builder();

        if (!column.isBaseColumn()) {
            dereferenceNames.addAll(existingProjectionInfo.get().getDereferencePhysicalNames());
            dereferenceIndices.addAll(existingProjectionInfo.get().getDereferenceIndices());
        }

        Type columnType = switch (columnMappingMode) {
            case ID, NAME -> column.getBasePhysicalType();
            case NONE -> column.getBaseType();
            default -> throw new TrinoException(NOT_SUPPORTED, "Projecting columns with column mapping %s is not supported".formatted(columnMappingMode));
        };

        for (int index : dereferenceIndices.build()) {
            RowType.Field field = ((RowType) columnType).getFields().get(index);
            columnType = field.getType();
        }

        for (int index : indices) {
            RowType.Field field = ((RowType) columnType).getFields().get(index);
            dereferenceNames.add(field.getName().orElseThrow());
            columnType = field.getType();
        }
        dereferenceIndices.addAll(indices);

        DeltaLakeColumnProjectionInfo projectionInfo = new DeltaLakeColumnProjectionInfo(
                projectedColumnType,
                dereferenceIndices.build(),
                dereferenceNames.build());

        return new DeltaLakeColumnHandle(
                column.getBaseColumnName(),
                column.getBaseType(),
                column.getBaseFieldId(),
                column.getBasePhysicalColumnName(),
                column.getBasePhysicalType(),
                REGULAR,
                Optional.of(projectionInfo));
    }

    /**
     * Returns the assignment key corresponding to the column represented by {@param projectedColumn} in the {@param assignments}, if one exists.
     * The variable in the {@param projectedColumn} can itself be a representation of another projected column. For example,
     * say a projected column representation has variable "x" and a dereferenceIndices=[0]. "x" can in-turn map to a projected
     * column handle with base="a" and [1, 2] as dereference indices. Then the method searches for a column handle in
     * {@param assignments} with base="a" and dereferenceIndices=[1, 2, 0].
     */
    private static Optional<String> find(Map<String, ColumnHandle> assignments, ProjectedColumnRepresentation projectedColumn)
    {
        DeltaLakeColumnHandle variableColumn = (DeltaLakeColumnHandle) assignments.get(projectedColumn.getVariable().getName());

        requireNonNull(variableColumn, "variableColumn is null");

        String baseColumnName = variableColumn.getBaseColumnName();

        List<Integer> variableColumnIndices = variableColumn.getProjectionInfo()
                .map(DeltaLakeColumnProjectionInfo::getDereferenceIndices)
                .orElse(ImmutableList.of());

        List<Integer> projectionIndices = ImmutableList.<Integer>builder()
                .addAll(variableColumnIndices)
                .addAll(projectedColumn.getDereferenceIndices())
                .build();

        for (Map.Entry<String, ColumnHandle> entry : assignments.entrySet()) {
            DeltaLakeColumnHandle column = (DeltaLakeColumnHandle) entry.getValue();
            if (column.getBaseColumnName().equals(baseColumnName) &&
                    column.getProjectionInfo()
                            .map(DeltaLakeColumnProjectionInfo::getDereferenceIndices)
                            .orElse(ImmutableList.of())
                            .equals(projectionIndices)) {
                return Optional.of(entry.getKey());
            }
        }

        return Optional.empty();
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        DeltaLakeTableHandle deltaLakeTableHandle = (DeltaLakeTableHandle) handle;

        if (isQueryPartitionFilterRequired(session)) {
            List<String> partitionColumns = deltaLakeTableHandle.getMetadataEntry().getOriginalPartitionColumns();
            if (!partitionColumns.isEmpty()) {
                if (deltaLakeTableHandle.getAnalyzeHandle().isPresent()) {
                    throw new TrinoException(
                            QUERY_REJECTED,
                            "ANALYZE statement can not be performed on partitioned tables because filtering is required on at least one partition. However, the partition filtering check can be disabled with the catalog session property 'query_partition_filter_required'.");
                }
                Set<String> referencedColumns =
                        deltaLakeTableHandle.getConstraintColumns().stream()
                                .map(DeltaLakeColumnHandle::getBaseColumnName)
                                .collect(toImmutableSet());
                if (Collections.disjoint(referencedColumns, partitionColumns)) {
                    throw new TrinoException(
                            QUERY_REJECTED,
                            format("Filter required on %s for at least one partition column: %s", deltaLakeTableHandle.getSchemaTableName(), String.join(", ", partitionColumns)));
                }
            }
        }
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return deltaLakeRedirectionsProvider.getTableScanRedirection(session, (DeltaLakeTableHandle) tableHandle);
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        if (!isExtendedStatisticsEnabled(session)) {
            throw new TrinoException(NOT_SUPPORTED, format(
                    "ANALYZE not supported if extended statistics are disabled. Enable via %s config property or %s session property.",
                    DeltaLakeConfig.EXTENDED_STATISTICS_ENABLED,
                    DeltaLakeSessionProperties.EXTENDED_STATISTICS_ENABLED));
        }

        DeltaLakeTableHandle handle = checkValidTableHandle(tableHandle);
        MetadataEntry metadata = handle.getMetadataEntry();

        Optional<Instant> filesModifiedAfterFromProperties = getFilesModifiedAfterProperty(analyzeProperties);
        AnalyzeMode analyzeMode = getRefreshMode(analyzeProperties);

        Optional<ExtendedStatistics> statistics = Optional.empty();
        if (analyzeMode == INCREMENTAL) {
            statistics = statisticsAccess.readExtendedStatistics(session, handle.getSchemaTableName(), handle.getLocation());
        }

        Optional<Instant> alreadyAnalyzedModifiedTimeMax = statistics.map(ExtendedStatistics::getAlreadyAnalyzedModifiedTimeMax);

        // determine list of files we want to read based on what caller requested via files_modified_after and what files were already analyzed in the past
        Optional<Instant> filesModifiedAfter = Optional.empty();
        if (filesModifiedAfterFromProperties.isPresent() || alreadyAnalyzedModifiedTimeMax.isPresent()) {
            filesModifiedAfter = Optional.of(Comparators.max(
                    filesModifiedAfterFromProperties.orElse(EPOCH),
                    alreadyAnalyzedModifiedTimeMax.orElse(EPOCH)));
        }

        List<DeltaLakeColumnMetadata> columnsMetadata = extractSchema(metadata, typeManager);
        Set<String> allColumnNames = columnsMetadata.stream().map(columnMetadata -> columnMetadata.getName().toLowerCase(ENGLISH)).collect(Collectors.toSet());
        Optional<Set<String>> analyzeColumnNames = getColumnNames(analyzeProperties);
        if (analyzeColumnNames.isPresent()) {
            Set<String> columnNames = analyzeColumnNames.get();
            // validate that proper column names are passed via `columns` analyze property
            if (columnNames.isEmpty()) {
                throw new TrinoException(INVALID_ANALYZE_PROPERTY, "Cannot specify empty list of columns for analysis");
            }

            if (!allColumnNames.containsAll(columnNames)) {
                throw new TrinoException(
                        INVALID_ANALYZE_PROPERTY,
                        format("Invalid columns specified for analysis: %s", Sets.difference(columnNames, allColumnNames)));
            }
        }

        // verify that we do not extend set of analyzed columns
        Optional<Set<String>> oldAnalyzeColumnNames = statistics.flatMap(ExtendedStatistics::getAnalyzedColumns);
        if (oldAnalyzeColumnNames.isPresent()) {
            if (analyzeColumnNames.isEmpty() || !oldAnalyzeColumnNames.get().containsAll(analyzeColumnNames.get())) {
                throw new TrinoException(INVALID_ANALYZE_PROPERTY, format(
                        "List of columns to be analyzed must be a subset of previously used: %s. To extend list of analyzed columns drop table statistics",
                        oldAnalyzeColumnNames.get()));
            }
        }

        AnalyzeHandle analyzeHandle = new AnalyzeHandle(statistics.isEmpty() ? FULL_REFRESH : INCREMENTAL, filesModifiedAfter, analyzeColumnNames);
        DeltaLakeTableHandle newHandle = new DeltaLakeTableHandle(
                handle.getSchemaTableName().getSchemaName(),
                handle.getSchemaTableName().getTableName(),
                handle.isManaged(),
                handle.getLocation(),
                metadata,
                handle.getProtocolEntry(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(analyzeHandle),
                handle.getReadVersion());

        TableStatisticsMetadata statisticsMetadata = getStatisticsCollectionMetadata(
                columnsMetadata.stream().map(DeltaLakeColumnMetadata::getColumnMetadata).collect(toImmutableList()),
                analyzeColumnNames.orElse(allColumnNames),
                statistics.isPresent(),
                false);

        return new ConnectorAnalyzeMetadata(newHandle, statisticsMetadata);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!isCollectExtendedStatisticsColumnStatisticsOnWrite(session)) {
            return TableStatisticsMetadata.empty();
        }

        Set<String> allColumnNames = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());

        Optional<Set<String>> analyzeColumnNames = Optional.empty();
        String tableLocation = getLocation(tableMetadata.getProperties());
        Optional<ExtendedStatistics> existingStatistics = Optional.empty();
        if (tableLocation != null) {
            existingStatistics = statisticsAccess.readExtendedStatistics(session, tableMetadata.getTable(), tableLocation);
            analyzeColumnNames = existingStatistics.flatMap(ExtendedStatistics::getAnalyzedColumns);
        }

        return getStatisticsCollectionMetadata(
                tableMetadata.getColumns(),
                analyzeColumnNames.orElse(allColumnNames),
                existingStatistics.isPresent(),
                true);
    }

    private TableStatisticsMetadata getStatisticsCollectionMetadata(
            List<ColumnMetadata> tableColumns,
            Set<String> analyzeColumnNames,
            boolean extendedStatisticsExists,
            boolean isCollectionOnWrite)
    {
        // Collect file statistics only when performing ANALYZE on a table without extended statistics
        boolean collectFileStatistics = !extendedStatisticsExists && !isCollectionOnWrite;

        ImmutableSet.Builder<ColumnStatisticMetadata> columnStatistics = ImmutableSet.builder();
        tableColumns.stream()
                .filter(DeltaLakeMetadata::shouldCollectExtendedStatistics)
                .filter(columnMetadata -> analyzeColumnNames.contains(columnMetadata.getName()))
                .forEach(columnMetadata -> {
                    if (!(columnMetadata.getType() instanceof FixedWidthType)) {
                        columnStatistics.add(new ColumnStatisticMetadata(columnMetadata.getName(), TOTAL_SIZE_IN_BYTES));
                    }
                    columnStatistics.add(new ColumnStatisticMetadata(columnMetadata.getName(), NUMBER_OF_DISTINCT_VALUES_SUMMARY));
                    if (collectFileStatistics) {
                        // TODO: (https://github.com/trinodb/trino/issues/17055) Collect file level stats for VARCHAR type
                        if (!columnMetadata.getType().equals(VARCHAR)
                                && !columnMetadata.getType().equals(BOOLEAN)
                                && !columnMetadata.getType().equals(VARBINARY)) {
                            columnStatistics.add(new ColumnStatisticMetadata(columnMetadata.getName(), MIN_VALUE));
                            columnStatistics.add(new ColumnStatisticMetadata(columnMetadata.getName(), MAX_VALUE));
                        }
                        columnStatistics.add(new ColumnStatisticMetadata(columnMetadata.getName(), NUMBER_OF_NON_NULL_VALUES));
                    }
                });

        if (!isCollectionOnWrite) {
            // collect max(file modification time) for sake of incremental ANALYZE
            // File modified time does not need to be collected as a statistics because it gets derived directly from files being written
            columnStatistics.add(new ColumnStatisticMetadata(FILE_MODIFIED_TIME_COLUMN_NAME, MAX_VALUE));
        }

        Set<TableStatisticType> tableStatistics = ImmutableSet.of();
        List<String> groupingColumns = ImmutableList.of();
        if (collectFileStatistics) {
            tableStatistics = ImmutableSet.of(TableStatisticType.ROW_COUNT);
            groupingColumns = ImmutableList.of(PATH_COLUMN_NAME);
        }

        return new TableStatisticsMetadata(
                columnStatistics.build(),
                tableStatistics,
                groupingColumns);
    }

    private static boolean shouldCollectExtendedStatistics(ColumnMetadata columnMetadata)
    {
        if (columnMetadata.isHidden()) {
            return false;
        }
        Type type = columnMetadata.getType();
        if (type instanceof MapType || type instanceof RowType || type instanceof ArrayType) {
            return false;
        }
        return true;
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // nothing to be done here
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle table, Collection<ComputedStatistics> computedStatistics)
    {
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) table;
        AnalyzeHandle analyzeHandle = tableHandle.getAnalyzeHandle().orElseThrow(() -> new IllegalArgumentException("analyzeHandle not set"));
        if (analyzeHandle.getAnalyzeMode() == FULL_REFRESH) {
            // TODO: Populate stats for incremental ANALYZE https://github.com/trinodb/trino/issues/18110
            generateMissingFileStatistics(session, tableHandle, computedStatistics);
        }
        Optional<Instant> maxFileModificationTime = getMaxFileModificationTime(computedStatistics);
        Map<String, String> physicalColumnNameMapping = extractSchema(tableHandle.getMetadataEntry(), typeManager).stream()
                        .collect(toImmutableMap(DeltaLakeColumnMetadata::getName, DeltaLakeColumnMetadata::getPhysicalName));
        updateTableStatistics(
                session,
                Optional.of(analyzeHandle),
                tableHandle.getSchemaTableName(),
                tableHandle.getLocation(),
                maxFileModificationTime,
                computedStatistics,
                getExactColumnNames(tableHandle.getMetadataEntry()),
                Optional.of(physicalColumnNameMapping));
    }

    private void generateMissingFileStatistics(ConnectorSession session, DeltaLakeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        Map<String, AddFileEntry> addFileEntriesWithNoStats = transactionLogAccess.getActiveFiles(
                        getSnapshot(tableHandle.getSchemaTableName(), tableHandle.getLocation(), session), session)
                .stream()
                .filter(addFileEntry -> addFileEntry.getStats().isEmpty()
                        || addFileEntry.getStats().get().getNumRecords().isEmpty()
                        || addFileEntry.getStats().get().getMaxValues().isEmpty()
                        || addFileEntry.getStats().get().getMinValues().isEmpty()
                        || addFileEntry.getStats().get().getNullCount().isEmpty())
                .filter(addFileEntry -> !URI.create(addFileEntry.getPath()).isAbsolute()) // TODO: Support absolute paths https://github.com/trinodb/trino/issues/18277
                // Statistics returns whole path to file build in DeltaLakeSplitManager, so we need to create corresponding map key for AddFileEntry.
                .collect(toImmutableMap(addFileEntry -> DeltaLakeSplitManager.buildSplitPath(Location.of(tableHandle.getLocation()), addFileEntry).toString(), identity()));

        if (addFileEntriesWithNoStats.isEmpty()) {
            return;
        }

        Map</* lowercase */ String, DeltaLakeColumnHandle> lowercaseToColumnsHandles = getColumns(tableHandle.getMetadataEntry()).stream()
                .filter(column -> column.getColumnType() == REGULAR)
                .collect(toImmutableMap(columnHandle -> columnHandle.getBaseColumnName().toLowerCase(ENGLISH), identity()));

        List<AddFileEntry> updatedAddFileEntries = computedStatistics.stream()
                .map(statistics -> {
                    // Grouping by `PATH_COLUMN_NAME`.
                    String filePathFromStatistics = VARCHAR.getSlice(statistics.getGroupingValues().get(0), 0).toStringUtf8();
                    // Check if collected statistics are for files without stats.
                    // If AddFileEntry is present in addFileEntriesWithNoStats means that it does not have statistics so prepare updated entry.
                    // If null is returned from addFileEntriesWithNoStats means that statistics are present, and we don't need to do anything.
                    AddFileEntry addFileEntry = addFileEntriesWithNoStats.get(filePathFromStatistics);
                    if (addFileEntry != null) {
                        return Optional.of(prepareUpdatedAddFileEntry(statistics, addFileEntry, lowercaseToColumnsHandles));
                    }
                    return Optional.<AddFileEntry>empty();
                })
                .flatMap(Optional::stream)
                .collect(toImmutableList());

        if (updatedAddFileEntries.isEmpty()) {
            return;
        }
        try {
            long createdTime = Instant.now().toEpochMilli();
            long readVersion = tableHandle.getReadVersion();
            long commitVersion = readVersion + 1;
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, tableHandle.getLocation());
            transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, commitVersion, createdTime, OPTIMIZE_OPERATION, readVersion));
            updatedAddFileEntries.forEach(transactionLogWriter::appendAddFileEntry);
            transactionLogWriter.flush();
        }
        catch (Throwable e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Unable to access file system for: " + tableHandle.getLocation(), e);
        }
    }

    private AddFileEntry prepareUpdatedAddFileEntry(ComputedStatistics stats, AddFileEntry addFileEntry, Map</* lowercase */ String, DeltaLakeColumnHandle> lowercaseToColumnsHandles)
    {
        DeltaLakeJsonFileStatistics deltaLakeJsonFileStatistics = DeltaLakeComputedStatistics.toDeltaLakeJsonFileStatistics(stats, lowercaseToColumnsHandles);
        try {
            return new AddFileEntry(
                    addFileEntry.getPath(),
                    addFileEntry.getPartitionValues(), // preserve original case without canonicalization
                    addFileEntry.getSize(),
                    addFileEntry.getModificationTime(),
                    false,
                    Optional.of(serializeStatsAsJson(deltaLakeJsonFileStatistics)),
                    Optional.empty(),
                    addFileEntry.getTags());
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Statistics serialization error", e);
        }
    }

    private void updateTableStatistics(
            ConnectorSession session,
            Optional<AnalyzeHandle> analyzeHandle,
            SchemaTableName schemaTableName,
            String location,
            Optional<Instant> maxFileModificationTime,
            Collection<ComputedStatistics> computedStatistics,
            List<String> originalColumnNames,
            Optional<Map<String, String>> physicalColumnNameMapping)
    {
        Optional<ExtendedStatistics> oldStatistics = Optional.empty();
        boolean loadExistingStats = analyzeHandle.isEmpty() || analyzeHandle.get().getAnalyzeMode() == INCREMENTAL;
        if (loadExistingStats) {
            oldStatistics = statisticsAccess.readExtendedStatistics(session, schemaTableName, location);
        }

        // more elaborate logic for handling statistics model evaluation may need to be introduced in the future
        // for now let's have a simple check rejecting update
        oldStatistics.ifPresent(statistics ->
                checkArgument(
                        statistics.getModelVersion() == ExtendedStatistics.CURRENT_MODEL_VERSION,
                        "Existing table statistics are incompatible, run the drop statistics procedure on this table before re-analyzing"));

        Map<String, String> lowerCaseToExactColumnNames = originalColumnNames.stream()
                .collect(toImmutableMap(name -> name.toLowerCase(ENGLISH), identity()));

        Map<String, DeltaLakeColumnStatistics> oldColumnStatistics = oldStatistics.map(ExtendedStatistics::getColumnStatistics)
                .orElseGet(ImmutableMap::of);
        Map<String, DeltaLakeColumnStatistics> newColumnStatistics = toDeltaLakeColumnStatistics(computedStatistics);

        Map<String, DeltaLakeColumnStatistics> mergedColumnStatistics = newColumnStatistics.entrySet().stream()
                .map(entry -> {
                    String columnName = entry.getKey();
                    String physicalColumnName = toPhysicalColumnName(columnName, lowerCaseToExactColumnNames, physicalColumnNameMapping);
                    return Map.entry(physicalColumnName, entry.getValue());
                })
                .collect(toImmutableMap(
                        Entry::getKey,
                        entry -> {
                            String columnName = entry.getKey();
                            DeltaLakeColumnStatistics newStats = entry.getValue();
                            DeltaLakeColumnStatistics oldStats = oldColumnStatistics.get(columnName);
                            return oldStats == null
                                    ? newStats
                                    : oldStats.update(newStats);
                        }));

        // We do not want to hinder our future calls to ANALYZE if one of the files we analyzed have modification time far in the future.
        // Therefore we cap the value we store in extended_stats.json to current_time as observed on Trino coordinator.
        Instant finalAlreadyAnalyzedModifiedTimeMax = Instant.now();
        if (maxFileModificationTime.isPresent()) {
            finalAlreadyAnalyzedModifiedTimeMax = Comparators.min(maxFileModificationTime.get(), finalAlreadyAnalyzedModifiedTimeMax);
        }
        // also ensure that we are not traveling back in time
        if (oldStatistics.isPresent()) {
            finalAlreadyAnalyzedModifiedTimeMax = Comparators.max(oldStatistics.get().getAlreadyAnalyzedModifiedTimeMax(), finalAlreadyAnalyzedModifiedTimeMax);
        }

        Optional<Set<String>> analyzedColumns = analyzeHandle.flatMap(AnalyzeHandle::getColumns);
        // If update is invoked by other command than ANALYZE, statistics should preserve previous columns set.
        if (analyzeHandle.isEmpty()) {
            analyzedColumns = oldStatistics.flatMap(ExtendedStatistics::getAnalyzedColumns);
        }

        analyzedColumns.ifPresent(analyzeColumns -> {
            Set<String> analyzePhysicalColumns = analyzeColumns.stream()
                    .map(columnName -> toPhysicalColumnName(columnName, lowerCaseToExactColumnNames, physicalColumnNameMapping))
                    .collect(toImmutableSet());
            if (!mergedColumnStatistics.keySet().equals(analyzePhysicalColumns)) {
                // sanity validation
                throw new IllegalStateException(format("Unexpected columns in in mergedColumnStatistics %s; expected %s", mergedColumnStatistics.keySet(), analyzePhysicalColumns));
            }
        });

        ExtendedStatistics mergedExtendedStatistics = new ExtendedStatistics(
                finalAlreadyAnalyzedModifiedTimeMax,
                mergedColumnStatistics,
                analyzedColumns);

        statisticsAccess.updateExtendedStatistics(session, schemaTableName, location, mergedExtendedStatistics);
    }

    private static String toPhysicalColumnName(String columnName, Map</* lowercase*/ String, String> lowerCaseToExactColumnNames, Optional<Map<String, String>> physicalColumnNameMapping)
    {
        String originalColumnName = lowerCaseToExactColumnNames.get(columnName.toLowerCase(ENGLISH));
        checkArgument(originalColumnName != null, "%s doesn't contain '%s'", lowerCaseToExactColumnNames.keySet(), columnName);
        if (physicalColumnNameMapping.isPresent()) {
            String physicalColumnName = physicalColumnNameMapping.get().get(originalColumnName);
            return requireNonNull(physicalColumnName, () -> "%s doesn't exist in %s".formatted(columnName, physicalColumnNameMapping));
        }
        return originalColumnName;
    }

    private void cleanExtraOutputFiles(ConnectorSession session, Location baseLocation, List<DataFileInfo> validDataFiles)
    {
        Set<Location> writtenFilePaths = validDataFiles.stream()
                .map(dataFileInfo -> baseLocation.appendPath(dataFileInfo.getPath()))
                .collect(toImmutableSet());

        cleanExtraOutputFiles(session, writtenFilePaths);
    }

    private void cleanExtraOutputFiles(ConnectorSession session, Set<Location> validWrittenFilePaths)
    {
        Set<Location> fileLocations = validWrittenFilePaths.stream()
                .map(Location::parentDirectory)
                .collect(toImmutableSet());

        for (Location location : fileLocations) {
            cleanExtraOutputFiles(session, session.getQueryId(), location, validWrittenFilePaths);
        }
    }

    private void cleanExtraOutputFiles(ConnectorSession session, String queryId, Location location, Set<Location> filesToKeep)
    {
        Deque<Location> filesToDelete = new ArrayDeque<>();
        try {
            LOG.debug("Deleting failed attempt files from %s for query %s", location, queryId);
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);

            // files within given partition are written flat into location; we need to list recursively
            FileIterator iterator = fileSystem.listFiles(location);
            while (iterator.hasNext()) {
                Location file = iterator.next().location();
                if (!file.parentDirectory().equals(location)) {
                    // we do not want recursive listing
                    continue;
                }
                if (isFileCreatedByQuery(file, queryId) && !filesToKeep.contains(file)) {
                    filesToDelete.add(file);
                }
            }

            if (filesToDelete.isEmpty()) {
                return;
            }

            LOG.info("Found %s files to delete and %s to retain in location %s for query %s", filesToDelete.size(), filesToKeep.size(), location, queryId);
            fileSystem.deleteFiles(filesToDelete);
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, "Failed to clean up extraneous output files", e);
        }
    }

    private static boolean isFileCreatedByQuery(Location file, String queryId)
    {
        verify(!queryId.contains("-"), "queryId(%s) should not contain hyphens", queryId);
        return file.fileName().startsWith(queryId + "-");
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(tableName).map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    private Optional<SystemTable> getRawSystemTable(SchemaTableName systemTableName)
    {
        Optional<DeltaLakeTableType> tableType = DeltaLakeTableName.tableTypeFrom(systemTableName.getTableName());
        if (tableType.isEmpty() || tableType.get() == DeltaLakeTableType.DATA) {
            return Optional.empty();
        }

        String tableName = DeltaLakeTableName.tableNameFrom(systemTableName.getTableName());
        Optional<DeltaMetastoreTable> table;
        try {
            table = metastore.getTable(systemTableName.getSchemaName(), tableName);
        }
        catch (NotADeltaLakeTableException e) {
            return Optional.empty();
        }
        if (table.isEmpty()) {
            return Optional.empty();
        }

        String tableLocation = table.get().location();

        return switch (tableType.get()) {
            case DATA -> throw new VerifyException("Unexpected DATA table type"); // Handled above.
            case HISTORY -> Optional.of(new DeltaLakeHistoryTable(
                    systemTableName,
                    tableLocation,
                    fileSystemFactory,
                    transactionLogAccess,
                    typeManager));
            case PROPERTIES -> Optional.of(new DeltaLakePropertiesTable(systemTableName, tableLocation, transactionLogAccess));
        };
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

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        executeDelete(session, checkValidTableHandle(tableHandle), TRUNCATE_OPERATION);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) handle;
        if (tableHandle.getMetadataEntry().isChangeDataFeedEnabled().orElse(false)) {
            // For tables with CDF enabled the DELETE operation can't be performed only on metadata files
            return Optional.empty();
        }

        return Optional.of(tableHandle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return executeDelete(session, handle, DELETE_OPERATION);
    }

    private OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle, String operation)
    {
        DeltaLakeTableHandle tableHandle = (DeltaLakeTableHandle) handle;
        if (isAppendOnly(tableHandle.getMetadataEntry())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot modify rows from a table with '" + APPEND_ONLY_CONFIGURATION_KEY + "' set to true");
        }
        checkWriteAllowed(session, tableHandle);
        checkWriteSupported(tableHandle);

        String tableLocation = tableHandle.location();
        List<AddFileEntry> activeFiles = getAddFileEntriesMatchingEnforcedPartitionConstraint(session, tableHandle);

        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, tableLocation);

            long writeTimestamp = Instant.now().toEpochMilli();
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            long currentVersion = getMandatoryCurrentVersion(fileSystem, tableLocation);
            if (currentVersion != tableHandle.getReadVersion()) {
                throw new TransactionConflictException(format("Conflicting concurrent writes found. Expected transaction log version: %s, actual version: %s", tableHandle.getReadVersion(), currentVersion));
            }
            long commitVersion = currentVersion + 1;
            transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, commitVersion, writeTimestamp, operation, tableHandle.getReadVersion()));

            long deletedRecords = 0L;
            boolean allDeletedFilesStatsPresent = true;
            for (AddFileEntry addFileEntry : activeFiles) {
                transactionLogWriter.appendRemoveFileEntry(new RemoveFileEntry(addFileEntry.getPath(), writeTimestamp, true));

                Optional<Long> fileRecords = addFileEntry.getStats().flatMap(DeltaLakeFileStatistics::getNumRecords);
                allDeletedFilesStatsPresent &= fileRecords.isPresent();
                deletedRecords += fileRecords.orElse(0L);
            }

            transactionLogWriter.flush();
            writeCheckpointIfNeeded(session, tableHandle.getSchemaTableName(), tableHandle.location(), tableHandle.getMetadataEntry().getCheckpointInterval(), commitVersion);
            return allDeletedFilesStatsPresent ? OptionalLong.of(deletedRecords) : OptionalLong.empty();
        }
        catch (Exception e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Failed to write Delta Lake transaction log entry", e);
        }
    }

    private List<AddFileEntry> getAddFileEntriesMatchingEnforcedPartitionConstraint(ConnectorSession session, DeltaLakeTableHandle tableHandle)
    {
        TableSnapshot tableSnapshot = getSnapshot(tableHandle.getSchemaTableName(), tableHandle.getLocation(), session);
        List<AddFileEntry> validDataFiles = transactionLogAccess.getActiveFiles(tableSnapshot, session);
        TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint = tableHandle.getEnforcedPartitionConstraint();
        if (enforcedPartitionConstraint.isAll()) {
            return validDataFiles;
        }
        Map<DeltaLakeColumnHandle, Domain> enforcedDomains = enforcedPartitionConstraint.getDomains().orElseThrow();
        return validDataFiles.stream()
                .filter(addAction -> partitionMatchesPredicate(addAction.getCanonicalPartitionValues(), enforcedDomains))
                .collect(toImmutableList());
    }

    private static Map<String, DeltaLakeColumnStatistics> toDeltaLakeColumnStatistics(Collection<ComputedStatistics> computedStatistics)
    {
        return computedStatistics.stream()
                .map(statistics -> createColumnToComputedStatisticsMap(statistics.getColumnStatistics()).entrySet().stream()
                        .collect(toImmutableMap(Entry::getKey, entry -> createDeltaLakeColumnStatistics(entry.getValue()))))
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(toImmutableMap(Entry::getKey, Entry::getValue, DeltaLakeColumnStatistics::update));
    }

    private static Map<String, Map<ColumnStatisticType, Block>> createColumnToComputedStatisticsMap(Map<ColumnStatisticMetadata, Block> computedStatistics)
    {
        ImmutableTable.Builder<String, ColumnStatisticType, Block> result = ImmutableTable.builder();
        computedStatistics.forEach((metadata, block) -> {
            if (metadata.getColumnName().equals(FILE_MODIFIED_TIME_COLUMN_NAME)) {
                return;
            }
            if (!SUPPORTED_STATISTICS_TYPE.contains(metadata.getStatisticType())) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected statistics collection: " + metadata);
            }

            result.put(metadata.getColumnName(), metadata.getStatisticType(), block);
        });
        return result.buildOrThrow().rowMap();
    }

    private static DeltaLakeColumnStatistics createDeltaLakeColumnStatistics(Map<ColumnStatisticType, Block> computedStatistics)
    {
        OptionalLong totalSize = OptionalLong.empty();
        if (computedStatistics.containsKey(TOTAL_SIZE_IN_BYTES)) {
            totalSize = getLongValue(computedStatistics.get(TOTAL_SIZE_IN_BYTES));
        }
        HyperLogLog ndvSummary = getHyperLogLogForNdv(computedStatistics.get(NUMBER_OF_DISTINCT_VALUES_SUMMARY));
        return DeltaLakeColumnStatistics.create(totalSize, ndvSummary);
    }

    private static OptionalLong getLongValue(Block block)
    {
        if (block.isNull(0)) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(BIGINT.getLong(block, 0));
    }

    private static HyperLogLog getHyperLogLogForNdv(Block block)
    {
        if (block.isNull(0)) {
            return HyperLogLog.newInstance(4096); // number of buckets used by $approx_set
        }
        Slice serializedSummary = (Slice) blockToNativeValue(HyperLogLogType.HYPER_LOG_LOG, block);
        return HyperLogLog.newInstance(serializedSummary);
    }

    private static Optional<Instant> getMaxFileModificationTime(Collection<ComputedStatistics> computedStatistics)
    {
        return computedStatistics.stream()
                .map(ComputedStatistics::getColumnStatistics)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .filter(entry -> entry.getKey().getColumnName().equals(FILE_MODIFIED_TIME_COLUMN_NAME))
                .flatMap(entry -> {
                    ColumnStatisticMetadata columnStatisticMetadata = entry.getKey();
                    if (columnStatisticMetadata.getStatisticType() != MAX_VALUE) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unexpected statistics collection: " + columnStatisticMetadata);
                    }
                    if (entry.getValue().isNull(0)) {
                        return Stream.of();
                    }
                    return Stream.of(Instant.ofEpochMilli(unpackMillisUtc(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS.getLong(entry.getValue(), 0))));
                })
                .max(naturalOrder());
    }

    public DeltaLakeMetastore getMetastore()
    {
        return metastore;
    }

    private static ColumnMetadata getColumnMetadata(DeltaLakeColumnHandle column, @Nullable String comment, boolean nullability, @Nullable String generation)
    {
        String columnName;
        Type columnType;
        if (column.isBaseColumn()) {
            columnName = column.getBaseColumnName();
            columnType = column.getBaseType();
        }
        else {
            DeltaLakeColumnProjectionInfo projectionInfo = column.getProjectionInfo().get();
            columnName = column.getQualifiedPhysicalName();
            columnType = projectionInfo.getType();
        }
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .setHidden(column.getColumnType() == SYNTHESIZED)
                .setComment(Optional.ofNullable(comment))
                .setNullable(nullability)
                .setExtraInfo(generation == null ? Optional.empty() : Optional.of("generated: " + generation))
                .build();
    }

    public static DeltaLakeTableHandle checkValidTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        if (tableHandle instanceof CorruptedDeltaLakeTableHandle corruptedTableHandle) {
            throw corruptedTableHandle.createException();
        }
        return ((DeltaLakeTableHandle) tableHandle);
    }

    public static TupleDomain<DeltaLakeColumnHandle> createStatisticsPredicate(
            AddFileEntry addFileEntry,
            List<DeltaLakeColumnMetadata> schema,
            List<String> canonicalPartitionColumns)
    {
        return addFileEntry.getStats()
                .map(deltaLakeFileStatistics -> withColumnDomains(
                        schema.stream()
                                .filter(column -> canUseInPredicate(column.getColumnMetadata()))
                                .collect(toImmutableMap(
                                        column -> DeltaLakeMetadata.toColumnHandle(column.getName(), column.getType(), column.getFieldId(), column.getPhysicalName(), column.getPhysicalColumnType(), canonicalPartitionColumns),
                                        column -> buildColumnDomain(column, deltaLakeFileStatistics, canonicalPartitionColumns)))))
                .orElseGet(TupleDomain::all);
    }

    private static boolean canUseInPredicate(ColumnMetadata column)
    {
        Type type = column.getType();
        return type.equals(TINYINT)
                || type.equals(SMALLINT)
                || type.equals(INTEGER)
                || type.equals(BIGINT)
                || type.equals(REAL)
                || type.equals(DOUBLE)
                || type.equals(BOOLEAN)
                || type.equals(DATE)
                || type instanceof TimestampWithTimeZoneType
                || type instanceof DecimalType
                || type.equals(VARCHAR);
    }

    private static Domain buildColumnDomain(DeltaLakeColumnMetadata column, DeltaLakeFileStatistics stats, List<String> canonicalPartitionColumns)
    {
        Optional<Long> nullCount = stats.getNullCount(column.getPhysicalName());
        if (nullCount.isEmpty()) {
            // No stats were collected for this column; this can happen in 2 scenarios:
            // 1. The column didn't exist in the schema when the data file was created
            // 2. The column does exist in the file, but Spark property 'delta.dataSkippingNumIndexedCols'
            //    was used to limit the number of columns for which stats are collected
            // Since we don't know which scenario we're dealing with, we can't make a decision to prune.
            return Domain.all(column.getType());
        }
        if (stats.getNumRecords().equals(nullCount)) {
            return Domain.onlyNull(column.getType());
        }

        boolean hasNulls = nullCount.get() > 0;
        DeltaLakeColumnHandle deltaLakeColumnHandle = toColumnHandle(column.getName(), column.getType(), column.getFieldId(), column.getPhysicalName(), column.getPhysicalColumnType(), canonicalPartitionColumns);
        Optional<Object> minValue = stats.getMinColumnValue(deltaLakeColumnHandle);
        if (minValue.isPresent() && isFloatingPointNaN(column.getType(), minValue.get())) {
            return allValues(column.getType(), hasNulls);
        }
        if (isNotFinite(minValue, column.getType())) {
            minValue = Optional.empty();
        }
        Optional<Object> maxValue = stats.getMaxColumnValue(deltaLakeColumnHandle);
        if (maxValue.isPresent() && isFloatingPointNaN(column.getType(), maxValue.get())) {
            return allValues(column.getType(), hasNulls);
        }
        if (isNotFinite(maxValue, column.getType())) {
            maxValue = Optional.empty();
        }
        if (minValue.isPresent() && maxValue.isPresent()) {
            return Domain.create(
                    ofRanges(range(column.getType(), minValue.get(), true, maxValue.get(), true)),
                    hasNulls);
        }
        if (minValue.isPresent()) {
            return Domain.create(ofRanges(greaterThanOrEqual(column.getType(), minValue.get())), hasNulls);
        }

        return maxValue
                .map(value -> Domain.create(ofRanges(lessThanOrEqual(column.getType(), value)), hasNulls))
                .orElseGet(() -> Domain.all(column.getType()));
    }

    private static boolean isNotFinite(Optional<Object> value, Type type)
    {
        if (type.equals(DOUBLE)) {
            return value
                    .map(Double.class::cast)
                    .filter(val -> !Double.isFinite(val))
                    .isPresent();
        }
        if (type.equals(REAL)) {
            return value
                    .map(Long.class::cast)
                    .map(Math::toIntExact)
                    .map(Float::intBitsToFloat)
                    .filter(val -> !Float.isFinite(val))
                    .isPresent();
        }
        return false;
    }

    private static Domain allValues(Type type, boolean includeNull)
    {
        if (includeNull) {
            return Domain.all(type);
        }
        return Domain.notNull(type);
    }

    private static DeltaLakeColumnHandle toColumnHandle(String originalName, Type type, OptionalInt fieldId, String physicalName, Type physicalType, Collection<String> partitionColumns)
    {
        boolean isPartitionKey = partitionColumns.stream().anyMatch(partition -> partition.equalsIgnoreCase(originalName));
        return new DeltaLakeColumnHandle(
                originalName,
                type,
                fieldId,
                physicalName,
                physicalType,
                isPartitionKey ? PARTITION_KEY : REGULAR,
                Optional.empty());
    }

    private static Optional<String> getQueryId(Database database)
    {
        return Optional.ofNullable(database.getParameters().get(PRESTO_QUERY_ID_NAME));
    }

    private static Optional<String> getQueryId(Table table)
    {
        return Optional.ofNullable(table.getParameters().get(PRESTO_QUERY_ID_NAME));
    }
}
