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
import com.google.common.base.Throwables;
import com.google.common.base.VerifyException;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.base.filter.UtcConstraintExtractor;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.deltalake.DeltaLakeAnalyzeProperties.AnalyzeMode;
import io.trino.plugin.deltalake.DeltaLakeTable.DeltaLakeColumn;
import io.trino.plugin.deltalake.expression.ParsingException;
import io.trino.plugin.deltalake.expression.SparkExpressionParser;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler.TableUpdateInfo;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore;
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
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeComputedStatistics;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.UnsupportedTypeException;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionConflictException;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionFailedException;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriter;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.security.AccessControlMetadata;
import io.trino.spi.ErrorCode;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.ConnectorAccessControl;
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
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
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
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.HyperLogLogType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.alwaysFalse;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.difference;
import static com.google.common.primitives.Ints.max;
import static io.trino.filesystem.Locations.appendPath;
import static io.trino.filesystem.Locations.areDirectoryLocationsEquivalent;
import static io.trino.hive.formats.HiveClassNames.HIVE_SEQUENCEFILE_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.LAZY_SIMPLE_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.SEQUENCEFILE_INPUT_FORMAT_CLASS;
import static io.trino.metastore.StorageFormat.create;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.base.filter.UtcConstraintExtractor.extractTupleDomain;
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
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_DATA;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_WRITE;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getHiveCatalogName;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isCollectExtendedStatisticsColumnStatisticsOnWrite;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isExtendedStatisticsEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isQueryPartitionFilterRequired;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isStoreTableMetadataInMetastoreEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isTableStatisticsEnabled;
import static io.trino.plugin.deltalake.DeltaLakeSplitManager.partitionMatchesPredicate;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.CHANGE_DATA_FEED_ENABLED_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.CHECKPOINT_INTERVAL_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.COLUMN_MAPPING_MODE_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.DELETION_VECTORS_ENABLED_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.LOCATION_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.getChangeDataFeedEnabled;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.getCheckpointInterval;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.getColumnMappingMode;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.getDeletionVectorsEnabled;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.getLocation;
import static io.trino.plugin.deltalake.DeltaLakeTableProperties.getPartitionedBy;
import static io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler.containsSchemaString;
import static io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler.getLastTransactionVersion;
import static io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler.isSameTransactionVersion;
import static io.trino.plugin.deltalake.metastore.DeltaLakeTableMetadataScheduler.tableMetadataParameters;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_PROPERTY;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.TABLE_PROVIDER_VALUE;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.convertToDeltaMetastoreTable;
import static io.trino.plugin.deltalake.metastore.HiveMetastoreBackedDeltaLakeMetastore.verifyDeltaLakeTable;
import static io.trino.plugin.deltalake.procedure.DeltaLakeTableProcedureId.OPTIMIZE;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.APPEND_ONLY_CONFIGURATION_KEY;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.COLUMN_MAPPING_PHYSICAL_NAME_CONFIGURATION_KEY;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode.ID;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode.NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode.NONE;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.DELETION_VECTORS_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.IsolationLevel;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.MAX_COLUMN_ID_CONFIGURATION_KEY;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.TIMESTAMP_NTZ_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.changeDataFeedEnabled;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.deserializeType;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.enabledUniversalFormats;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractPartitionColumns;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.generateColumnMetadata;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnIdentities;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnMappingMode;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getExactColumnNames;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getGeneratedColumnExpressions;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getIsolationLevel;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getMaxColumnId;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.isAppendOnly;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.isDeletionVectorEnabled;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeColumnType;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeSchemaAsJson;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeStatsAsJson;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.unsupportedReaderFeatures;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.unsupportedWriterFeatures;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.validateType;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.verifySupportedColumnMapping;
import static io.trino.plugin.deltalake.transactionlog.MetadataEntry.DELTA_CHANGE_DATA_FEED_ENABLED_PROPERTY;
import static io.trino.plugin.deltalake.transactionlog.MetadataEntry.configurationForNewTable;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.getMandatoryCurrentVersion;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.METADATA;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.plugin.hive.util.HiveUtil.escapeTableName;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
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
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
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
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toUnmodifiableSet;

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
    public static final String CREATE_OR_REPLACE_TABLE_AS_OPERATION = "CREATE OR REPLACE TABLE AS SELECT";
    public static final String CREATE_TABLE_OPERATION = "CREATE TABLE";
    public static final String CREATE_OR_REPLACE_TABLE_OPERATION = "CREATE OR REPLACE TABLE";
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
    public static final int DEFAULT_READER_VERSION = 1;
    public static final int DEFAULT_WRITER_VERSION = 2;
    // The highest reader and writer versions Trino supports
    private static final int MAX_READER_VERSION = 3;
    public static final int MAX_WRITER_VERSION = 7;
    private static final int CDF_SUPPORTED_WRITER_VERSION = 4;
    private static final int COLUMN_MAPPING_MODE_SUPPORTED_READER_VERSION = 2;
    private static final int COLUMN_MAPPING_MODE_SUPPORTED_WRITER_VERSION = 5;
    private static final int TIMESTAMP_NTZ_SUPPORTED_READER_VERSION = 3;
    private static final int TIMESTAMP_NTZ_SUPPORTED_WRITER_VERSION = 7;
    private static final int DELETION_VECTORS_SUPPORTED_READER_VERSION = 3;
    private static final int DELETION_VECTORS_SUPPORTED_WRITER_VERSION = 7;
    private static final RetryPolicy<Object> TRANSACTION_CONFLICT_RETRY_POLICY = RetryPolicy.builder()
            .handleIf(throwable -> Throwables.getCausalChain(throwable).stream().anyMatch(TransactionConflictException.class::isInstance))
            .withDelay(Duration.ofMillis(400))
            .withJitter(Duration.ofMillis(200))
            .withMaxRetries(5)
            .onRetry(event -> LOG.debug(event.getLastException(), "Commit failed on attempt %d, will retry.", event.getAttemptCount()))
            .build();

    // Matches the dummy column Databricks stores in the metastore
    private static final List<Column> DUMMY_DATA_COLUMNS = ImmutableList.of(
            new Column("col", toHiveType(new ArrayType(VarcharType.createUnboundedVarcharType())), Optional.empty(), Map.of()));
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
    private final DeltaLakeTableMetadataScheduler metadataScheduler;
    private final Map<SchemaTableName, TableUpdateInfo> tableUpdateInfos = new ConcurrentHashMap<>();
    private final Map<SchemaTableName, Long> latestTableVersions = new ConcurrentHashMap<>();
    private final Map<QueriedTable, TableSnapshot> queriedSnapshots = new ConcurrentHashMap<>();

    private record QueriedTable(SchemaTableName schemaTableName, long version)
    {
        QueriedTable
        {
            requireNonNull(schemaTableName, "schemaTableName is null");
        }
    }

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
            DeltaLakeTableMetadataScheduler metadataScheduler,
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
        this.metadataScheduler = requireNonNull(metadataScheduler, "metadataScheduler is null");
        this.useUniqueTableLocation = useUniqueTableLocation;
        this.allowManagedTableRename = allowManagedTableRename;
    }

    public TableSnapshot getSnapshot(ConnectorSession session, SchemaTableName table, String tableLocation, Optional<Long> atVersion)
    {
        Optional<Long> version = atVersion.or(() -> Optional.ofNullable(latestTableVersions.get(table)));

        if (version.isPresent()) {
            QueriedTable queriedTable = new QueriedTable(table, version.get());
            if (queriedSnapshots.containsKey(queriedTable)) {
                return queriedSnapshots.get(queriedTable);
            }
        }

        try {
            TableSnapshot snapshot = transactionLogAccess.loadSnapshot(session, table, tableLocation, version);
            // Lack of concurrency for given query is currently guaranteed by DeltaLakeMetadata
            checkState(latestTableVersions.put(table, snapshot.getVersion()) == null || atVersion.isPresent(), "latestTableVersions changed concurrently for %s", table);
            queriedSnapshots.put(new QueriedTable(table, snapshot.getVersion()), snapshot);
            return snapshot;
        }
        catch (IOException | RuntimeException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Error getting snapshot for " + table, e);
        }
    }

    private static long getVersion(TrinoFileSystem fileSystem, String tableLocation, ConnectorTableVersion version)
    {
        return switch (version.getPointerType()) {
            // TODO https://github.com/trinodb/trino/issues/21024 Add support for reading tables with temporal versions
            case TEMPORAL -> throw new TrinoException(NOT_SUPPORTED, "This connector does not support reading tables with TIMESTAMP AS OF");
            case TARGET_ID -> getTargetVersion(fileSystem, tableLocation, version);
        };
    }

    private static long getTargetVersion(TrinoFileSystem fileSystem, String tableLocation, ConnectorTableVersion version)
    {
        long snapshotId;
        if (version.getVersionType() == SMALLINT || version.getVersionType() == TINYINT || version.getVersionType() == INTEGER || version.getVersionType() == BIGINT) {
            snapshotId = (long) version.getVersion();
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported type for table version: " + version.getVersionType().getDisplayName());
        }

        try {
            if (!fileSystem.newInputFile(getTransactionLogJsonEntryPath(getTransactionLogDir(tableLocation), snapshotId)).exists()) {
                throw new TrinoException(INVALID_ARGUMENTS, "Delta Lake snapshot ID does not exists: " + snapshotId);
            }
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_ARGUMENTS, "Delta Lake snapshot ID does not exists: " + snapshotId);
        }

        return snapshotId;
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
    public LocatedTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read table with start version is not supported");
        }

        requireNonNull(tableName, "tableName is null");
        if (!DeltaLakeTableName.isDataTable(tableName.getTableName())) {
            // Pretend the table does not exist to produce better error message in case of table redirects to Hive
            return null;
        }
        Optional<Table> metastoreTable = metastore.getRawMetastoreTable(tableName.getSchemaName(), tableName.getTableName());
        if (metastoreTable.isEmpty()) {
            return null;
        }
        DeltaMetastoreTable table = convertToDeltaMetastoreTable(metastoreTable.get());
        boolean managed = table.managed();

        String tableLocation = table.location();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TableSnapshot tableSnapshot = getSnapshot(session, tableName, tableLocation, endVersion.map(version -> getVersion(fileSystem, tableLocation, version)));

        Map<Class<?>, Object> logEntries;
        try {
            logEntries = transactionLogAccess.getTransactionLogEntries(
                    session,
                    tableSnapshot,
                    ImmutableSet.of(METADATA, PROTOCOL),
                    entryStream -> entryStream
                            .filter(entry -> entry.getMetaData() != null || entry.getProtocol() != null)
                            .map(entry -> firstNonNull(entry.getMetaData(), entry.getProtocol())));
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(DELTA_LAKE_INVALID_SCHEMA.toErrorCode())) {
                return new CorruptedDeltaLakeTableHandle(tableName, managed, tableLocation, e);
            }
            throw e;
        }
        MetadataEntry metadataEntry = (MetadataEntry) logEntries.get(MetadataEntry.class);
        if (metadataEntry == null) {
            return new CorruptedDeltaLakeTableHandle(tableName, managed, tableLocation, new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Metadata not found in transaction log for " + tableSnapshot.getTable()));
        }
        ProtocolEntry protocolEntry = (ProtocolEntry) logEntries.get(ProtocolEntry.class);
        if (protocolEntry == null) {
            return new CorruptedDeltaLakeTableHandle(tableName, managed, tableLocation, new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Protocol not found in transaction log for " + tableSnapshot.getTable()));
        }
        if (protocolEntry.minReaderVersion() > MAX_READER_VERSION) {
            LOG.debug("Skip %s because the reader version is unsupported: %d", tableName, protocolEntry.minReaderVersion());
            return null;
        }
        Set<String> unsupportedReaderFeatures = unsupportedReaderFeatures(protocolEntry.readerFeatures().orElse(ImmutableSet.of()));
        if (!unsupportedReaderFeatures.isEmpty()) {
            LOG.debug("Skip %s because the table contains unsupported reader features: %s", tableName, unsupportedReaderFeatures);
            return null;
        }
        verifySupportedColumnMapping(getColumnMappingMode(metadataEntry, protocolEntry));
        if (metadataScheduler.canStoreTableMetadata(session, metadataEntry.getSchemaString(), Optional.ofNullable(metadataEntry.getDescription())) &&
                endVersion.isEmpty() &&
                !isSameTransactionVersion(metastoreTable.get(), tableSnapshot)) {
            tableUpdateInfos.put(tableName, new TableUpdateInfo(session, tableSnapshot.getVersion(), metadataEntry.getSchemaString(), Optional.ofNullable(metadataEntry.getDescription())));
        }
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
                tableSnapshot.getVersion(),
                endVersion.isPresent());
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
    public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, Type type)
    {
        Type newType = coerceType(type);
        if (type.getTypeSignature().equals(newType.getTypeSignature())) {
            return Optional.empty();
        }
        return Optional.of(newType);
    }

    private Type coerceType(Type type)
    {
        if (type instanceof TimestampType) {
            return TIMESTAMP_MICROS;
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
        ProtocolEntry protocolEntry = tableHandle.getProtocolEntry();

        List<ColumnMetadata> columns = getTableColumnMetadata(tableHandle.getMetadataEntry(), tableHandle.getProtocolEntry());
        DeltaLakeTable deltaTable = DeltaLakeTable.builder(metadataEntry, protocolEntry).build();

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(LOCATION_PROPERTY, tableHandle.getLocation());
        List<String> partitionColumnNames = metadataEntry.getLowercasePartitionColumns();
        if (!partitionColumnNames.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionColumnNames);
        }

        Optional<Long> checkpointInterval = metadataEntry.getCheckpointInterval();
        checkpointInterval.ifPresent(value -> properties.put(CHECKPOINT_INTERVAL_PROPERTY, value));

        changeDataFeedEnabled(metadataEntry, protocolEntry)
                .ifPresent(value -> properties.put(CHANGE_DATA_FEED_ENABLED_PROPERTY, value));

        if (isDeletionVectorEnabled(metadataEntry, protocolEntry)) {
            properties.put(DELETION_VECTORS_ENABLED_PROPERTY, true);
        }

        ColumnMappingMode columnMappingMode = getColumnMappingMode(metadataEntry, protocolEntry);
        if (columnMappingMode != NONE) {
            properties.put(COLUMN_MAPPING_MODE_PROPERTY, columnMappingMode.name());
        }

        return new ConnectorTableMetadata(
                tableHandle.getSchemaTableName(),
                columns,
                properties.buildOrThrow(),
                Optional.ofNullable(metadataEntry.getDescription()),
                deltaTable.constraints().stream()
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

    private List<ColumnMetadata> getTableColumnMetadata(MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        DeltaLakeTable deltaTable = DeltaLakeTable.builder(metadataEntry, protocolEntry).build();
        return getColumns(metadataEntry, protocolEntry).stream()
                .map(column -> getColumnMetadata(deltaTable, column))
                .collect(toImmutableList());
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
                .orElseGet(() -> getColumns(table.getMetadataEntry(), table.getProtocolEntry())).stream()
                // This method does not calculate column name for the projected columns
                .peek(handle -> checkArgument(handle.isBaseColumn(), "Unsupported projected column: %s", handle))
                .collect(toImmutableMap(DeltaLakeColumnHandle::baseColumnName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) tableHandle;
        DeltaLakeTable deltaTable = DeltaLakeTable.builder(table.getMetadataEntry(), table.getProtocolEntry()).build();
        return getColumnMetadata(deltaTable, (DeltaLakeColumnHandle) columnHandle);
    }

    private static ColumnMetadata getColumnMetadata(DeltaLakeTable deltaTable, DeltaLakeColumnHandle column)
    {
        if (column.projectionInfo().isPresent() || column.columnType() == SYNTHESIZED) {
            return getColumnMetadata(column, null, true, Optional.empty());
        }
        DeltaLakeColumn deltaColumn = deltaTable.findColumn(column.baseColumnName());
        return getColumnMetadata(
                column,
                deltaColumn.comment(),
                deltaColumn.nullable(),
                deltaColumn.generationExpression());
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
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, ConnectorViewDefinition> viewDefinitions = getViews(session, schemaName);
        ImmutableList.Builder<RelationCommentMetadata> commentMetadataBuilder = ImmutableList.builderWithExpectedSize(viewDefinitions.size());
        ImmutableSet.Builder<SchemaTableName> viewNamesBuilder = ImmutableSet.builderWithExpectedSize(viewDefinitions.size());
        for (Entry<SchemaTableName, ConnectorViewDefinition> viewDefinitionEntry : viewDefinitions.entrySet()) {
            RelationCommentMetadata relationCommentMetadata = RelationCommentMetadata.forRelation(viewDefinitionEntry.getKey(), viewDefinitionEntry.getValue().getComment());
            commentMetadataBuilder.add(relationCommentMetadata);
            viewNamesBuilder.add(relationCommentMetadata.name());
        }
        List<RelationCommentMetadata> views = commentMetadataBuilder.build();
        Set<SchemaTableName> viewNames = viewNamesBuilder.build();

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);

        Stream<RelationCommentMetadata> tables = listTables(session, schemaName).stream()
                .filter(tableName -> !viewNames.contains(tableName))
                .collect(collectingAndThen(toUnmodifiableSet(), relationFilter)).stream()
                .map(tableName -> getRelationCommentMetadata(session, fileSystem, tableName))
                .filter(Objects::nonNull);

        Set<SchemaTableName> availableViews = relationFilter.apply(viewNames);
        return Streams.concat(views.stream().filter(commentMetadata -> availableViews.contains(commentMetadata.name())), tables)
                .iterator();
    }

    private RelationCommentMetadata getRelationCommentMetadata(ConnectorSession session, TrinoFileSystem fileSystem, SchemaTableName tableName)
    {
        if (redirectTable(session, tableName).isPresent()) {
            return RelationCommentMetadata.forRedirectedTable(tableName);
        }

        try {
            Optional<Table> metastoreTable = metastore.getRawMetastoreTable(tableName.getSchemaName(), tableName.getTableName());
            if (metastoreTable.isEmpty()) {
                // this may happen when table is being deleted concurrently
                return null;
            }

            Table table = metastoreTable.get();
            verifyDeltaLakeTable(table);

            String tableLocation = HiveMetastoreBackedDeltaLakeMetastore.getTableLocation(table);
            if (canUseTableParametersFromMetastore(session, fileSystem, table, tableLocation)) {
                // Don't check TABLE_COMMENT existence because it's not stored in case of null comment
                return RelationCommentMetadata.forRelation(tableName, Optional.ofNullable(table.getParameters().get(TABLE_COMMENT)));
            }

            TableSnapshot snapshot = getSnapshot(session, tableName, tableLocation, Optional.empty());
            MetadataEntry metadata = transactionLogAccess.getMetadataEntry(session, snapshot);
            return RelationCommentMetadata.forRelation(tableName, Optional.ofNullable(metadata.getDescription()));
        }
        catch (RuntimeException e) {
            boolean suppressed = false;
            if (e instanceof TrinoException trinoException) {
                ErrorCode errorCode = trinoException.getErrorCode();
                suppressed = errorCode.equals(UNSUPPORTED_TABLE_TYPE.toErrorCode()) ||
                        // e.g. table deleted concurrently
                        errorCode.equals(TABLE_NOT_FOUND.toErrorCode()) ||
                        errorCode.equals(NOT_FOUND.toErrorCode()) ||
                        // e.g. Delta table being deleted concurrently resulting in failure to load metadata from filesystem
                        errorCode.getType() == EXTERNAL;
            }
            if (suppressed) {
                LOG.debug("Failed to get metadata for table: %s", tableName);
            }
            else {
                // getTableHandle or getTableMetadata failed call may fail if table disappeared during listing or is unsupported
                LOG.warn("Failed to get metadata for table: %s", tableName);
            }
            // Since the getTableHandle did not return null (i.e. succeeded or failed), we assume the table would be returned by listTables
            return RelationCommentMetadata.forRelation(tableName, Optional.empty());
        }
    }

    private static boolean canUseTableParametersFromMetastore(ConnectorSession session, TrinoFileSystem fileSystem, Table table, String tableLocation)
    {
        if (!isStoreTableMetadataInMetastoreEnabled(session)) {
            return false;
        }

        return getLastTransactionVersion(table)
                .map(version -> isLatestVersion(fileSystem, tableLocation, version))
                .orElse(false);
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
                .map(_ -> singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);

        return tables.stream()
                .flatMap(tableName -> {
                    try {
                        if (redirectTable(session, tableName).isPresent()) {
                            // put "redirect marker" for current table
                            return Stream.of(TableColumnsMetadata.forRedirectedTable(tableName));
                        }

                        Optional<Table> metastoreTable = metastore.getRawMetastoreTable(tableName.getSchemaName(), tableName.getTableName());
                        if (metastoreTable.isEmpty()) {
                            // this may happen when table is being deleted concurrently,
                            return Stream.of();
                        }

                        Table table = metastoreTable.get();
                        verifyDeltaLakeTable(table);

                        String tableLocation = HiveMetastoreBackedDeltaLakeMetastore.getTableLocation(table);
                        if (containsSchemaString(table) && canUseTableParametersFromMetastore(session, fileSystem, table, tableLocation)) {
                            List<ColumnMetadata> columnsMetadata = metadataScheduler.getColumnsMetadata(table);
                            return Stream.of(TableColumnsMetadata.forTable(tableName, columnsMetadata));
                        }
                        // Don't store cache in streamTableColumns method for avoiding too many update calls

                        TableSnapshot snapshot = transactionLogAccess.loadSnapshot(session, tableName, tableLocation, Optional.empty());
                        MetadataEntry metadata = transactionLogAccess.getMetadataEntry(session, snapshot);
                        ProtocolEntry protocol = transactionLogAccess.getProtocolEntry(session, snapshot);
                        List<ColumnMetadata> columnMetadata = getTableColumnMetadata(metadata, protocol);
                        return Stream.of(TableColumnsMetadata.forTable(tableName, columnMetadata));
                    }
                    catch (NotADeltaLakeTableException | IOException e) {
                        return Stream.empty();
                    }
                    catch (RuntimeException e) {
                        // this may happen when table is being deleted concurrently, it still exists in metastore but TL is no longer present
                        // there can be several different exceptions thrown this is why all RTE are caught and ignored here
                        LOG.debug(e, "Ignored exception when trying to list columns from %s", tableName);
                        return Stream.empty();
                    }
                })
                .iterator();
    }

    private static boolean isLatestVersion(TrinoFileSystem fileSystem, String tableLocation, long version)
    {
        String transactionLogDir = getTransactionLogDir(tableLocation);
        Location transactionLogJsonEntryPath = getTransactionLogJsonEntryPath(transactionLogDir, version);
        Location nextTransactionLogJsonEntryPath = getTransactionLogJsonEntryPath(transactionLogDir, version + 1);
        try {
            return !fileSystem.newInputFile(nextTransactionLogJsonEntryPath).exists() &&
                    fileSystem.newInputFile(transactionLogJsonEntryPath).exists();
        }
        catch (IOException e) {
            LOG.debug(e, "Failed to check table location: %s", tableLocation);
            return false;
        }
    }

    private List<DeltaLakeColumnHandle> getColumns(MetadataEntry deltaMetadata, ProtocolEntry protocolEntry)
    {
        ImmutableList.Builder<DeltaLakeColumnHandle> columns = ImmutableList.builder();
        extractSchema(deltaMetadata, protocolEntry, typeManager).stream()
                .map(column -> toColumnHandle(column.name(), column.type(), column.fieldId(), column.physicalName(), column.physicalColumnType(), deltaMetadata.getLowercasePartitionColumns()))
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
        return tableStatisticsProvider.getTableStatistics(session, handle, getSnapshot(session, handle));
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
                .setParameters(ImmutableMap.of(TRINO_QUERY_ID_NAME, queryId))
                .build();

        // Ensure the database has queryId set. This is relied on for exception handling
        verify(
                getQueryId(database).orElseThrow(() -> new IllegalArgumentException("Query id is not present")).equals(queryId),
                "Database '%s' does not have correct query id set",
                database.getDatabaseName());

        metastore.createDatabase(database);
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
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Database schema = metastore.getDatabase(schemaName).orElseThrow(() -> new SchemaNotFoundException(schemaName));

        boolean external = true;
        String location = getLocation(tableMetadata.getProperties());
        ConnectorTableHandle connectorTableHandle = getTableHandle(session, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        DeltaLakeTableHandle tableHandle = null;
        if (connectorTableHandle != null) {
            tableHandle = checkValidTableHandle(connectorTableHandle);
        }
        boolean replaceExistingTable = tableHandle != null && saveMode == SaveMode.REPLACE;
        if (replaceExistingTable) {
            ConnectorTableMetadata existingTableMetadata = getTableMetadata(session, tableHandle);
            validateTableForReplaceOperation(tableHandle, existingTableMetadata, tableMetadata);

            String currentLocation = getLocation(existingTableMetadata.getProperties());
            if (location != null && !areDirectoryLocationsEquivalent(Location.of(location), Location.of(currentLocation))) {
                throw new TrinoException(GENERIC_USER_ERROR, format("The provided location '%s' does not match the existing table location '%s'", location, currentLocation));
            }
            location = currentLocation;
            external = !tableHandle.isManaged();
        }
        if (location == null) {
            location = getTableLocation(schema, tableName);
            checkPathContainsNoFiles(session, Location.of(location));
            external = false;
        }
        long commitVersion = 0;
        Location deltaLogDirectory = Location.of(getTransactionLogDir(location));
        Optional<Long> checkpointInterval = getCheckpointInterval(tableMetadata.getProperties());
        Optional<Boolean> changeDataFeedEnabled = getChangeDataFeedEnabled(tableMetadata.getProperties());
        ColumnMappingMode columnMappingMode = getColumnMappingMode(tableMetadata.getProperties());
        boolean deletionVectorsEnabled = getDeletionVectorsEnabled(tableMetadata.getProperties());
        AtomicInteger fieldId = new AtomicInteger();

        validateTableColumns(tableMetadata);
        boolean containsTimestampType = false;
        DeltaLakeTable.Builder deltaTable = DeltaLakeTable.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            deltaTable.addColumn(
                    column.getName(),
                    serializeColumnType(columnMappingMode, fieldId, column.getType()),
                    column.isNullable(),
                    column.getComment(),
                    generateColumnMetadata(columnMappingMode, fieldId));
            if (!containsTimestampType) {
                containsTimestampType = containsTimestampType(column.getType());
            }
        }

        OptionalInt maxFieldId = OptionalInt.empty();
        if (columnMappingMode == ID || columnMappingMode == NAME) {
            maxFieldId = OptionalInt.of(fieldId.get());
        }

        String schemaString = serializeSchemaAsJson(deltaTable.build());
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            boolean transactionLogFileExists = fileSystem.listFiles(deltaLogDirectory).hasNext();

            if (!replaceExistingTable && transactionLogFileExists) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "Using CREATE [OR REPLACE] TABLE with an existing table content is disallowed, instead use the system.register_table() procedure.");
            }
            else {
                TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriterWithoutTransactionIsolation(session, location);
                ProtocolEntry protocolEntry;
                if (replaceExistingTable) {
                    commitVersion = getMandatoryCurrentVersion(fileSystem, location, tableHandle.getReadVersion()) + 1;
                    transactionLogWriter = transactionLogWriterFactory.newWriter(session, location);
                    try (Stream<AddFileEntry> activeFiles = transactionLogAccess.getActiveFiles(
                            session,
                            getSnapshot(session, tableHandle),
                            tableHandle.getMetadataEntry(),
                            tableHandle.getProtocolEntry(),
                            tableHandle.getEnforcedPartitionConstraint(),
                            tableHandle.getProjectedColumns().orElse(ImmutableSet.of()))) {
                        Iterator<AddFileEntry> addFileEntryIterator = activeFiles.iterator();
                        while (addFileEntryIterator.hasNext()) {
                            long writeTimestamp = Instant.now().toEpochMilli();
                            AddFileEntry addFileEntry = addFileEntryIterator.next();
                            transactionLogWriter.appendRemoveFileEntry(new RemoveFileEntry(addFileEntry.getPath(), addFileEntry.getPartitionValues(), writeTimestamp, true, Optional.empty()));
                        }
                    }
                    protocolEntry = protocolEntryForTable(tableHandle.getProtocolEntry(), containsTimestampType, tableMetadata.getProperties());
                    statisticsAccess.deleteExtendedStatistics(session, schemaTableName, location);
                }
                else {
                    setRollback(() -> deleteRecursivelyIfExists(fileSystem, deltaLogDirectory));
                    protocolEntry = protocolEntryForNewTable(containsTimestampType, tableMetadata.getProperties());
                }

                appendTableEntries(
                        commitVersion,
                        transactionLogWriter,
                        saveMode == SaveMode.REPLACE ? CREATE_OR_REPLACE_TABLE_OPERATION : CREATE_TABLE_OPERATION,
                        session,
                        protocolEntry,
                        MetadataEntry.builder()
                                .setDescription(tableMetadata.getComment())
                                .setSchemaString(serializeSchemaAsJson(deltaTable.build()))
                                .setPartitionColumns(getPartitionedBy(tableMetadata.getProperties()))
                                .setConfiguration(configurationForNewTable(checkpointInterval, changeDataFeedEnabled, deletionVectorsEnabled, columnMappingMode, maxFieldId)));

                transactionLogWriter.flush();

                if (replaceExistingTable) {
                    writeCheckpointIfNeeded(session, schemaTableName, location, tableHandle.getReadVersion(), checkpointInterval, commitVersion);
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Unable to access file system for: " + location, e);
        }

        Table table = buildTable(session, schemaTableName, location, external, tableMetadata.getComment(), commitVersion, schemaString);

        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner().orElseThrow());
        // As a precaution, clear the caches
        statisticsAccess.invalidateCache(schemaTableName, Optional.of(location));
        transactionLogAccess.invalidateCache(schemaTableName, Optional.of(location));
        if (replaceExistingTable) {
            metastore.replaceTable(table, principalPrivileges);
        }
        else {
            metastore.createTable(table, principalPrivileges);
        }
    }

    public Table buildTable(ConnectorSession session, SchemaTableName schemaTableName, String location, boolean isExternal, Optional<String> tableComment, long version, String schemaString)
    {
        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(schemaTableName.getSchemaName())
                .setTableName(schemaTableName.getTableName())
                .setOwner(Optional.of(session.getUser()))
                .setTableType(isExternal ? EXTERNAL_TABLE.name() : MANAGED_TABLE.name())
                .setDataColumns(DUMMY_DATA_COLUMNS)
                .setParameters(deltaTableProperties(session, location, isExternal, tableComment, version, schemaString));

        setDeltaStorageFormat(tableBuilder, location);
        return tableBuilder.build();
    }

    private Map<String, String> deltaTableProperties(ConnectorSession session, String location, boolean external, Optional<String> tableComment, long version, String schemaString)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put(TRINO_QUERY_ID_NAME, session.getQueryId())
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
        if (metadataScheduler.canStoreTableMetadata(session, schemaString, tableComment)) {
            properties.putAll(tableMetadataParameters(version, schemaString, tableComment));
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
    public DeltaLakeOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout,
            RetryMode retryMode,
            boolean replace)
    {
        validateTableColumns(tableMetadata);

        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Database schema = metastore.getDatabase(schemaName).orElseThrow(() -> new SchemaNotFoundException(schemaName));

        ConnectorTableHandle connectorTableHandle = getTableHandle(session, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        DeltaLakeTableHandle handle = null;
        if (connectorTableHandle != null) {
            handle = checkValidTableHandle(connectorTableHandle);
        }
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

        boolean replaceExistingTable = handle != null && replace;

        boolean external = true;
        String location = getLocation(tableMetadata.getProperties());
        if (replaceExistingTable) {
            ConnectorTableMetadata existingTableMetadata = getTableMetadata(session, handle);
            validateTableForReplaceOperation(handle, existingTableMetadata, tableMetadata);

            String currentLocation = getLocation(existingTableMetadata.getProperties());
            if (location != null && !areDirectoryLocationsEquivalent(Location.of(location), Location.of(currentLocation))) {
                throw new TrinoException(GENERIC_USER_ERROR, format("The provided location '%s' does not match the existing table location '%s'", location, currentLocation));
            }
            location = currentLocation;
            external = !handle.isManaged();
        }
        if (location == null) {
            location = getTableLocation(schema, tableName);
            external = false;
        }

        ColumnMappingMode columnMappingMode = getColumnMappingMode(tableMetadata.getProperties());
        AtomicInteger fieldId = new AtomicInteger();

        Location finalLocation = Location.of(location);

        boolean usePhysicalName = columnMappingMode == ID || columnMappingMode == NAME;
        boolean containsTimestampType = false;
        int columnSize = tableMetadata.getColumns().size();
        DeltaLakeTable.Builder deltaTable = DeltaLakeTable.builder();
        ImmutableList.Builder<DeltaLakeColumnHandle> columnHandles = ImmutableList.builderWithExpectedSize(columnSize);
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            containsTimestampType |= containsTimestampType(column.getType());
            Object serializedType = serializeColumnType(columnMappingMode, fieldId, column.getType());
            Type physicalType;
            try {
                physicalType = deserializeType(typeManager, serializedType, usePhysicalName);
            }
            catch (UnsupportedTypeException e) {
                throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + column.getType());
            }

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
            deltaTable.addColumn(column.getName(), serializedType, column.isNullable(), column.getComment(), columnMetadata);
        }

        OptionalInt maxFieldId = OptionalInt.empty();
        if (columnMappingMode == ID || columnMappingMode == NAME) {
            maxFieldId = OptionalInt.of(fieldId.get());
        }

        OptionalLong readVersion = OptionalLong.empty();
        ProtocolEntry protocolEntry;

        if (replaceExistingTable) {
            protocolEntry = protocolEntryForTable(handle.getProtocolEntry(), containsTimestampType, tableMetadata.getProperties());
            readVersion = OptionalLong.of(handle.getReadVersion());
        }
        else {
            checkPathContainsNoFiles(session, finalLocation);
            setRollback(() -> deleteRecursivelyIfExists(fileSystemFactory.create(session), finalLocation));
            protocolEntry = protocolEntryForNewTable(containsTimestampType, tableMetadata.getProperties());
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
                getDeletionVectorsEnabled(tableMetadata.getProperties()),
                serializeSchemaAsJson(deltaTable.build()),
                columnMappingMode,
                maxFieldId,
                replace,
                readVersion,
                protocolEntry);
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

    private void validateTableForReplaceOperation(DeltaLakeTableHandle tableHandle, ConnectorTableMetadata existingTableMetadata, ConnectorTableMetadata newTableMetadata)
    {
        if (isAppendOnly(tableHandle.getMetadataEntry(), tableHandle.getProtocolEntry())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot replace a table when '" + APPEND_ONLY_CONFIGURATION_KEY + "' is set to true");
        }

        if (getChangeDataFeedEnabled(existingTableMetadata.getProperties()).orElse(false) ||
                getChangeDataFeedEnabled(newTableMetadata.getProperties()).orElse(false)) {
            throw new TrinoException(NOT_SUPPORTED, "CREATE OR REPLACE is not supported for tables with change data feed enabled");
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
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, "Table property 'partitioned_by' contained column names which do not exist: " + invalidPartitionNames);
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

    private static boolean containsTimestampType(Type type)
    {
        if (type instanceof ArrayType arrayType) {
            return containsTimestampType(arrayType.getElementType());
        }
        if (type instanceof MapType mapType) {
            return containsTimestampType(mapType.getKeyType()) || containsTimestampType(mapType.getValueType());
        }
        if (type instanceof RowType rowType) {
            return rowType.getFields().stream().anyMatch(field -> containsTimestampType(field.getType()));
        }
        checkArgument(type.getTypeParameters().isEmpty(), "Unexpected type parameters for type %s", type);
        return type instanceof TimestampType;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DeltaLakeOutputTableHandle handle = (DeltaLakeOutputTableHandle) tableHandle;

        String schemaName = handle.schemaName();
        String tableName = handle.tableName();
        String location = handle.location();

        List<DataFileInfo> dataFileInfos = fragments.stream()
                .map(Slice::getBytes)
                .map(dataFileInfoCodec::fromJson)
                .collect(toImmutableList());

        SchemaTableName schemaTableName = schemaTableName(schemaName, tableName);

        ColumnMappingMode columnMappingMode = handle.columnMappingMode();
        String schemaString = handle.schemaString();
        List<String> columnNames = handle.inputColumns().stream().map(DeltaLakeColumnHandle::baseColumnName).collect(toImmutableList());
        List<String> physicalPartitionNames = handle.inputColumns().stream()
                .filter(column -> column.columnType() == PARTITION_KEY)
                .map(DeltaLakeColumnHandle::basePhysicalColumnName)
                .collect(toImmutableList());
        boolean writeCommitted = false;
        try {
            TransactionLogWriter transactionLogWriter;
            long commitVersion = 0;
            if (handle.readVersion().isEmpty()) {
                // For CTAS there is no risk of multiple writers racing. Using writer without transaction isolation so we are not limiting support for CTAS to
                // filesystems for which we have proper implementations of TransactionLogSynchronizers.
                transactionLogWriter = transactionLogWriterFactory.newWriterWithoutTransactionIsolation(session, handle.location());
            }
            else {
                TrinoFileSystem fileSystem = fileSystemFactory.create(session);
                commitVersion = getMandatoryCurrentVersion(fileSystem, handle.location(), handle.readVersion().getAsLong()) + 1;
                if (commitVersion != handle.readVersion().getAsLong() + 1) {
                    throw new TransactionConflictException(format("Conflicting concurrent writes found. Expected transaction log version: %s, actual version: %s",
                            handle.readVersion().getAsLong(),
                            commitVersion - 1));
                }
                transactionLogWriter = transactionLogWriterFactory.newWriter(session, handle.location());
            }
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    handle.replace() ? CREATE_OR_REPLACE_TABLE_AS_OPERATION : CREATE_TABLE_AS_OPERATION,
                    session,
                    handle.protocolEntry(),
                    MetadataEntry.builder()
                            .setDescription(handle.comment())
                            .setSchemaString(schemaString)
                            .setPartitionColumns(handle.partitionedBy())
                            .setConfiguration(configurationForNewTable(handle.checkpointInterval(), handle.changeDataFeedEnabled(), handle.deletionVectorsEnabled(), columnMappingMode, handle.maxColumnId())));
            appendAddFileEntries(transactionLogWriter, dataFileInfos, physicalPartitionNames, columnNames, true);
            if (handle.readVersion().isPresent()) {
                long writeTimestamp = Instant.now().toEpochMilli();
                DeltaLakeTableHandle deltaLakeTableHandle = (DeltaLakeTableHandle) getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());
                try (Stream<AddFileEntry> activeFiles = transactionLogAccess.getActiveFiles(
                        session,
                        getSnapshot(session, deltaLakeTableHandle),
                        deltaLakeTableHandle.getMetadataEntry(),
                        deltaLakeTableHandle.getProtocolEntry(),
                        deltaLakeTableHandle.getEnforcedPartitionConstraint(),
                        deltaLakeTableHandle.getProjectedColumns().orElse(ImmutableSet.of()))) {
                    Iterator<AddFileEntry> addFileEntryIterator = activeFiles.iterator();
                    while (addFileEntryIterator.hasNext()) {
                        AddFileEntry addFileEntry = addFileEntryIterator.next();
                        transactionLogWriter.appendRemoveFileEntry(new RemoveFileEntry(addFileEntry.getPath(), addFileEntry.getPartitionValues(), writeTimestamp, true, Optional.empty()));
                    }
                }
            }
            transactionLogWriter.flush();
            writeCommitted = true;

            if (handle.replace() && handle.readVersion().isPresent()) {
                writeCheckpointIfNeeded(session, schemaTableName, handle.location(), handle.readVersion().getAsLong(), handle.checkpointInterval(), commitVersion);
            }

            if (isCollectExtendedStatisticsColumnStatisticsOnWrite(session) && !computedStatistics.isEmpty()) {
                Optional<Instant> maxFileModificationTime = dataFileInfos.stream()
                        .map(DataFileInfo::creationTime)
                        .max(Long::compare)
                        .map(Instant::ofEpochMilli);
                Map<String, String> physicalColumnMapping = DeltaLakeSchemaSupport.getColumnMetadata(schemaString, typeManager, columnMappingMode, handle.partitionedBy()).stream()
                        .map(e -> Map.entry(e.name(), e.physicalName()))
                        .collect(toImmutableMap(Entry::getKey, Entry::getValue));

                updateTableStatistics(
                        session,
                        Optional.empty(),
                        schemaTableName,
                        location,
                        maxFileModificationTime,
                        computedStatistics,
                        columnNames,
                        Optional.of(physicalColumnMapping),
                        true);
            }

            Table table = buildTable(session, schemaTableName, location, handle.external(), handle.comment(), commitVersion, handle.schemaString());
            PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner().orElseThrow());

            // As a precaution, clear the caches
            statisticsAccess.invalidateCache(schemaTableName, Optional.of(location));
            transactionLogAccess.invalidateCache(schemaTableName, Optional.of(location));
            if (handle.readVersion().isPresent()) {
                metastore.replaceTable(table, principalPrivileges);
            }
            else {
                metastore.createTable(table, principalPrivileges);
            }
        }
        catch (Exception e) {
            // Remove the transaction log entry if the table creation fails
            if (!writeCommitted) {
                // TODO perhaps it should happen in a background thread (https://github.com/trinodb/trino/issues/12011)
                cleanupFailedWrite(session, handle.location(), dataFileInfos);
            }
            if (handle.readVersion().isEmpty()) {
                Location transactionLogDir = Location.of(getTransactionLogDir(location));
                try {
                    fileSystemFactory.create(session).deleteDirectory(transactionLogDir);
                }
                catch (IOException ioException) {
                    // Nothing to do, the IOException is probably the same reason why the initial write failed
                    LOG.error(ioException, "Transaction log cleanup failed during CREATE TABLE rollback");
                }
            }
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Failed to write Delta Lake transaction log entry", e);
        }

        return Optional.empty();
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        DeltaLakeTableHandle handle = checkValidTableHandle(tableHandle);
        checkSupportedWriterVersion(handle);
        ColumnMappingMode columnMappingMode = getColumnMappingMode(handle.getMetadataEntry(), handle.getProtocolEntry());
        if (columnMappingMode != ID && columnMappingMode != NAME && columnMappingMode != NONE) {
            throw new TrinoException(NOT_SUPPORTED, "Setting a table comment with column mapping %s is not supported".formatted(columnMappingMode));
        }
        MetadataEntry metadataEntry = handle.getMetadataEntry();
        ProtocolEntry protocolEntry = handle.getProtocolEntry();
        checkUnsupportedWriterFeatures(protocolEntry);

        try {
            long commitVersion = handle.getReadVersion() + 1;

            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, handle.getLocation());
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    SET_TBLPROPERTIES_OPERATION,
                    session,
                    protocolEntry,
                    MetadataEntry.builder(handle.getMetadataEntry())
                            .setDescription(comment));
            transactionLogWriter.flush();
            enqueueUpdateInfo(session, handle.getSchemaName(), handle.getTableName(), commitVersion, metadataEntry.getSchemaString(), comment);
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
        ColumnMappingMode columnMappingMode = getColumnMappingMode(deltaLakeTableHandle.getMetadataEntry(), deltaLakeTableHandle.getProtocolEntry());
        if (columnMappingMode != ID && columnMappingMode != NAME && columnMappingMode != NONE) {
            throw new TrinoException(NOT_SUPPORTED, "Setting a column comment with column mapping %s is not supported".formatted(columnMappingMode));
        }
        ProtocolEntry protocolEntry = deltaLakeTableHandle.getProtocolEntry();
        checkUnsupportedWriterFeatures(protocolEntry);

        try {
            long commitVersion = deltaLakeTableHandle.getReadVersion() + 1;

            DeltaLakeTable deltaTable = DeltaLakeTable.builder(deltaLakeTableHandle.getMetadataEntry(), deltaLakeTableHandle.getProtocolEntry())
                    .setColumnComment(deltaLakeColumnHandle.baseColumnName(), comment.orElse(null))
                    .build();
            String schemaString = serializeSchemaAsJson(deltaTable);

            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, deltaLakeTableHandle.getLocation());
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    CHANGE_COLUMN_OPERATION,
                    session,
                    protocolEntry,
                    MetadataEntry.builder(deltaLakeTableHandle.getMetadataEntry())
                            .setSchemaString(schemaString));
            transactionLogWriter.flush();
            enqueueUpdateInfo(
                    session,
                    deltaLakeTableHandle.getSchemaName(),
                    deltaLakeTableHandle.getTableName(),
                    commitVersion,
                    schemaString,
                    Optional.ofNullable(deltaLakeTableHandle.getMetadataEntry().getDescription()));
        }
        catch (Exception e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Unable to add '%s' column comment for: %s.%s", deltaLakeColumnHandle.baseColumnName(), deltaLakeTableHandle.getSchemaName(), deltaLakeTableHandle.getTableName()), e);
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
        ProtocolEntry protocolEntry = handle.getProtocolEntry();
        checkSupportedWriterVersion(handle);
        ColumnMappingMode columnMappingMode = getColumnMappingMode(handle.getMetadataEntry(), protocolEntry);
        if (changeDataFeedEnabled(handle.getMetadataEntry(), protocolEntry).orElse(false) && CHANGE_DATA_FEED_COLUMN_NAMES.contains(newColumnMetadata.getName())) {
            throw new TrinoException(NOT_SUPPORTED, "Column name %s is forbidden when change data feed is enabled".formatted(newColumnMetadata.getName()));
        }
        checkUnsupportedWriterFeatures(protocolEntry);

        if (!newColumnMetadata.isNullable()) {
            boolean tableHasDataFiles;
            try (Stream<AddFileEntry> addFileEntries = transactionLogAccess.getActiveFiles(
                    session,
                    getSnapshot(session, handle),
                    handle.getMetadataEntry(),
                    handle.getProtocolEntry(),
                    TupleDomain.all(),
                    alwaysFalse())) {
                tableHasDataFiles = addFileEntries.findAny().isPresent();
            }
            if (tableHasDataFiles) {
                throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Unable to add NOT NULL column '%s' for non-empty table: %s.%s", newColumnMetadata.getName(), handle.getSchemaName(), handle.getTableName()));
            }
        }

        try {
            long commitVersion = handle.getReadVersion() + 1;

            AtomicInteger maxColumnId = switch (columnMappingMode) {
                case NONE -> new AtomicInteger();
                case ID, NAME -> new AtomicInteger(getMaxColumnId(handle.getMetadataEntry()));
                default -> throw new IllegalArgumentException("Unexpected column mapping mode: " + columnMappingMode);
            };

            DeltaLakeTable deltaTable = DeltaLakeTable.builder(handle.getMetadataEntry(), handle.getProtocolEntry())
                    .addColumn(
                            newColumnMetadata.getName(),
                            serializeColumnType(columnMappingMode, maxColumnId, newColumnMetadata.getType()),
                            newColumnMetadata.isNullable(),
                            newColumnMetadata.getComment(),
                            generateColumnMetadata(columnMappingMode, maxColumnId))
                    .build();
            String schemaString = serializeSchemaAsJson(deltaTable);
            Map<String, String> configuration = new HashMap<>(handle.getMetadataEntry().getConfiguration());
            if (columnMappingMode == ID || columnMappingMode == NAME) {
                checkArgument(maxColumnId.get() > 0, "maxColumnId must be larger than 0: %s", maxColumnId);
                configuration.put(MAX_COLUMN_ID_CONFIGURATION_KEY, String.valueOf(maxColumnId.get()));
            }

            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, handle.getLocation());
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    ADD_COLUMN_OPERATION,
                    session,
                    buildProtocolEntryForNewColumn(protocolEntry, newColumnMetadata.getType()),
                    MetadataEntry.builder(handle.getMetadataEntry())
                            .setSchemaString(schemaString)
                            .setConfiguration(configuration));
            transactionLogWriter.flush();
            enqueueUpdateInfo(
                    session,
                    handle.getSchemaName(),
                    handle.getTableName(),
                    commitVersion,
                    schemaString,
                    Optional.ofNullable(handle.getMetadataEntry().getDescription()));
        }
        catch (Exception e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Unable to add '%s' column for: %s.%s %s", newColumnMetadata.getName(), handle.getSchemaName(), handle.getTableName(), firstNonNull(e.getMessage(), e)), e);
        }
    }

    private ProtocolEntry buildProtocolEntryForNewColumn(ProtocolEntry protocolEntry, Type type)
    {
        if (!containsTimestampType(type)) {
            return protocolEntry;
        }

        return new ProtocolEntry(
                max(protocolEntry.minReaderVersion(), TIMESTAMP_NTZ_SUPPORTED_READER_VERSION),
                max(protocolEntry.minWriterVersion(), TIMESTAMP_NTZ_SUPPORTED_WRITER_VERSION),
                Optional.of(ImmutableSet.<String>builder()
                        .addAll(protocolEntry.readerFeatures().orElse(ImmutableSet.of()))
                        .add(TIMESTAMP_NTZ_FEATURE_NAME)
                        .build()),
                Optional.of(ImmutableSet.<String>builder()
                        .addAll(protocolEntry.writerFeatures().orElse(ImmutableSet.of()))
                        .add(TIMESTAMP_NTZ_FEATURE_NAME)
                        .build()));
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) tableHandle;
        DeltaLakeColumnHandle deltaLakeColumn = (DeltaLakeColumnHandle) columnHandle;
        verify(deltaLakeColumn.isBaseColumn(), "Unexpected dereference: %s", deltaLakeColumn);
        String dropColumnName = deltaLakeColumn.baseColumnName();
        MetadataEntry metadataEntry = table.getMetadataEntry();
        ProtocolEntry protocolEntry = table.getProtocolEntry();
        checkUnsupportedWriterFeatures(protocolEntry);

        checkSupportedWriterVersion(table);
        ColumnMappingMode columnMappingMode = getColumnMappingMode(metadataEntry, protocolEntry);
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
        List<DeltaLakeColumnMetadata> columns = extractSchema(metadataEntry, protocolEntry, typeManager);
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
                .collect(toImmutableMap(DeltaLakeColumnMetadata::name, DeltaLakeColumnMetadata::physicalName));

        DeltaLakeTable deltaTable = DeltaLakeTable.builder(metadataEntry, protocolEntry)
                .removeColumn(dropColumnName)
                .build();
        if (deltaTable.columns().size() == partitionColumns.size()) {
            throw new TrinoException(NOT_SUPPORTED, "Dropping the last non-partition column is unsupported");
        }

        String schemaString = serializeSchemaAsJson(deltaTable);
        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, table.getLocation());
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    DROP_COLUMN_OPERATION,
                    session,
                    protocolEntry,
                    MetadataEntry.builder(metadataEntry)
                            .setSchemaString(schemaString));
            transactionLogWriter.flush();
            enqueueUpdateInfo(session, table.getSchemaName(), table.getTableName(), commitVersion, schemaString, Optional.ofNullable(metadataEntry.getDescription()));
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
        String sourceColumnName = deltaLakeColumn.baseColumnName();
        ProtocolEntry protocolEntry = table.getProtocolEntry();
        checkUnsupportedWriterFeatures(protocolEntry);

        checkSupportedWriterVersion(table);
        if (changeDataFeedEnabled(table.getMetadataEntry(), protocolEntry).orElse(false)) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot rename column when change data feed is enabled");
        }

        MetadataEntry metadataEntry = table.getMetadataEntry();
        ColumnMappingMode columnMappingMode = getColumnMappingMode(metadataEntry, protocolEntry);
        if (columnMappingMode != ColumnMappingMode.NAME && columnMappingMode != ColumnMappingMode.ID) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot rename column in table using column mapping mode " + columnMappingMode);
        }

        long commitVersion = table.getReadVersion() + 1;

        // Use equalsIgnoreCase because the remote column name can contain uppercase characters
        // Creating a table with ambiguous names (e.g. "a" and "A") is disallowed, so this should be safe
        List<String> partitionColumns = metadataEntry.getOriginalPartitionColumns().stream()
                .map(columnName -> columnName.equalsIgnoreCase(sourceColumnName) ? newColumnName : columnName)
                .collect(toImmutableList());

        DeltaLakeTable deltaTable = DeltaLakeTable.builder(metadataEntry, protocolEntry)
                .renameColumn(sourceColumnName, newColumnName)
                .build();
        String schemaString = serializeSchemaAsJson(deltaTable);
        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, table.getLocation());
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    RENAME_COLUMN_OPERATION,
                    session,
                    protocolEntry,
                    MetadataEntry.builder(metadataEntry)
                            .setSchemaString(schemaString)
                            .setPartitionColumns(partitionColumns));
            transactionLogWriter.flush();
            enqueueUpdateInfo(session, table.getSchemaName(), table.getTableName(), commitVersion, schemaString, Optional.ofNullable(metadataEntry.getDescription()));
            // Don't update extended statistics because it uses physical column names internally
        }
        catch (Exception e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Unable to rename '%s' column for: %s.%s", sourceColumnName, table.getSchemaName(), table.getTableName()), e);
        }
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        DeltaLakeTableHandle table = (DeltaLakeTableHandle) tableHandle;
        DeltaLakeColumnHandle column = (DeltaLakeColumnHandle) columnHandle;
        verify(column.isBaseColumn(), "Unexpected dereference: %s", column);
        String columnName = column.baseColumnName();
        MetadataEntry metadataEntry = table.getMetadataEntry();
        ProtocolEntry protocolEntry = table.getProtocolEntry();

        checkUnsupportedWriterFeatures(protocolEntry);
        checkSupportedWriterVersion(table);

        DeltaLakeTable deltaTable = DeltaLakeTable.builder(metadataEntry, protocolEntry)
                .dropNotNullConstraint(columnName)
                .build();
        long commitVersion = table.getReadVersion() + 1;
        String schemaString = serializeSchemaAsJson(deltaTable);
        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, table.getLocation());
            appendTableEntries(
                    commitVersion,
                    transactionLogWriter,
                    CHANGE_COLUMN_OPERATION,
                    session,
                    protocolEntry,
                    MetadataEntry.builder(metadataEntry)
                            .setSchemaString(schemaString));
            transactionLogWriter.flush();
            enqueueUpdateInfo(session, table.getSchemaName(), table.getTableName(), commitVersion, schemaString, Optional.ofNullable(metadataEntry.getDescription()));
        }
        catch (Exception e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, format("Unable to drop not null constraint from '%s' column in: %s", columnName, table.getSchemaTableName()), e);
        }
    }

    private void appendTableEntries(
            long commitVersion,
            TransactionLogWriter transactionLogWriter,
            String operation,
            ConnectorSession session,
            ProtocolEntry protocolEntry,
            MetadataEntry.Builder metadataEntry)
    {
        long createdTime = System.currentTimeMillis();
        transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, IsolationLevel.WRITESERIALIZABLE, commitVersion, createdTime, operation, 0, true));

        transactionLogWriter.appendProtocolEntry(protocolEntry);
        transactionLogWriter.appendMetadataEntry(metadataEntry.setCreatedTime(createdTime).build());
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
                partitionValues.put(partitionColumnNames.get(i), info.partitionValues().get(i));
            }

            Optional<Map<String, Object>> minStats = toOriginalColumnNames(info.statistics().getMinValues(), toOriginalColumnNames);
            Optional<Map<String, Object>> maxStats = toOriginalColumnNames(info.statistics().getMaxValues(), toOriginalColumnNames);
            Optional<Map<String, Object>> nullStats = toOriginalColumnNames(info.statistics().getNullCount(), toOriginalColumnNames);
            DeltaLakeJsonFileStatistics statisticsWithExactNames = new DeltaLakeJsonFileStatistics(info.statistics().getNumRecords(), minStats, maxStats, nullStats);

            partitionValues = unmodifiableMap(partitionValues);

            transactionLogWriter.appendAddFileEntry(
                    new AddFileEntry(
                            toUriFormat(info.path()), // Paths are RFC 2396 URI encoded https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
                            partitionValues,
                            info.size(),
                            info.creationTime(),
                            dataChange,
                            Optional.of(serializeStatsAsJson(statisticsWithExactNames)),
                            Optional.empty(),
                            ImmutableMap.of(),
                            info.deletionVector()));
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

        return createInsertHandle(retryMode, table, inputColumns);
    }

    private DeltaLakeInsertTableHandle createInsertHandle(RetryMode retryMode, DeltaLakeTableHandle table, List<DeltaLakeColumnHandle> inputColumns)
    {
        String tableLocation = table.getLocation();
        return new DeltaLakeInsertTableHandle(
                table.getSchemaTableName(),
                tableLocation,
                table.getMetadataEntry(),
                table.getProtocolEntry(),
                inputColumns,
                table.getReadVersion(),
                retryMode != NO_RETRIES);
    }

    private void checkAllColumnsPassedOnInsert(ConnectorTableMetadata tableMetadata, List<DeltaLakeColumnHandle> insertColumns)
    {
        List<String> allColumnNames = tableMetadata.getColumns().stream()
                .filter(not(ColumnMetadata::isHidden))
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        List<String> insertColumnNames = insertColumns.stream()
                // Lowercase because the above allColumnNames uses lowercase
                .map(column -> column.baseColumnName().toLowerCase(ENGLISH))
                .collect(toImmutableList());

        checkArgument(allColumnNames.equals(insertColumnNames), "Not all table columns passed on INSERT; table columns=%s; insert columns=%s", allColumnNames, insertColumnNames);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DeltaLakeInsertTableHandle handle = (DeltaLakeInsertTableHandle) insertHandle;

        List<DataFileInfo> dataFileInfos = fragments.stream()
                .map(Slice::getBytes)
                .map(dataFileInfoCodec::fromJson)
                .collect(toImmutableList());

        if (handle.retriesEnabled()) {
            cleanExtraOutputFiles(session, Location.of(handle.location()), dataFileInfos);
        }

        boolean writeCommitted = false;
        try {
            IsolationLevel isolationLevel = getIsolationLevel(handle.metadataEntry());
            AtomicReference<Long> readVersion = new AtomicReference<>(handle.readVersion());
            long commitVersion = Failsafe.with(TRANSACTION_CONFLICT_RETRY_POLICY)
                    .get(context -> commitInsertOperation(session, handle, sourceTableHandles, isolationLevel, dataFileInfos, readVersion, context.getAttemptCount()));
            writeCommitted = true;
            writeCheckpointIfNeeded(session, handle.tableName(), handle.location(), handle.readVersion(), handle.metadataEntry().getCheckpointInterval(), commitVersion);
            enqueueUpdateInfo(session, handle.tableName().getSchemaName(), handle.tableName().getTableName(), commitVersion, handle.metadataEntry().getSchemaString(), Optional.ofNullable(handle.metadataEntry().getDescription()));

            if (isCollectExtendedStatisticsColumnStatisticsOnWrite(session) && !computedStatistics.isEmpty() && !dataFileInfos.isEmpty()) {
                // TODO (https://github.com/trinodb/trino/issues/16088) Add synchronization when version conflict for INSERT is resolved.
                Optional<Instant> maxFileModificationTime = dataFileInfos.stream()
                        .map(DataFileInfo::creationTime)
                        .max(Long::compare)
                        .map(Instant::ofEpochMilli);
                updateTableStatistics(
                        session,
                        Optional.empty(),
                        handle.tableName(),
                        handle.location(),
                        maxFileModificationTime,
                        computedStatistics,
                        getExactColumnNames(handle.metadataEntry()),
                        Optional.of(extractSchema(handle.metadataEntry(), handle.protocolEntry(), typeManager).stream()
                                .collect(toImmutableMap(DeltaLakeColumnMetadata::name, DeltaLakeColumnMetadata::physicalName))),
                        true);
            }
        }
        catch (Exception e) {
            if (!writeCommitted) {
                // TODO perhaps it should happen in a background thread (https://github.com/trinodb/trino/issues/12011)
                cleanupFailedWrite(session, handle.location(), dataFileInfos);
            }
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Failed to write Delta Lake transaction log entry", e);
        }

        return Optional.empty();
    }

    private long commitInsertOperation(
            ConnectorSession session,
            DeltaLakeInsertTableHandle handle,
            List<ConnectorTableHandle> sourceTableHandles,
            IsolationLevel isolationLevel,
            List<DataFileInfo> dataFileInfos,
            AtomicReference<Long> readVersion,
            int attemptCount)
            throws IOException
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        long currentVersion = getMandatoryCurrentVersion(fileSystem, handle.location(), readVersion.get());

        List<DeltaLakeTableHandle> sameAsTargetSourceTableHandles = getSameAsTargetSourceTableHandles(sourceTableHandles, handle.tableName());
        checkForConcurrentTransactionConflicts(session, fileSystem, sameAsTargetSourceTableHandles, isolationLevel, currentVersion, readVersion, handle.location(), attemptCount);
        long commitVersion = currentVersion + 1;
        writeTransactionLogForInsertOperation(session, handle, sameAsTargetSourceTableHandles.isEmpty(), isolationLevel, dataFileInfos, commitVersion, currentVersion);
        return commitVersion;
    }

    private List<DeltaLakeTableHandle> getSameAsTargetSourceTableHandles(
            List<ConnectorTableHandle> sourceTableHandles,
            SchemaTableName schemaTableName)
    {
        return sourceTableHandles.stream()
                .filter(sourceTableHandle -> sourceTableHandle instanceof DeltaLakeTableHandle)
                .map(DeltaLakeTableHandle.class::cast)
                .filter(tableHandle -> schemaTableName.equals(tableHandle.getSchemaTableName())
                        // disregard time travel table handles
                        && !tableHandle.isTimeTravel())
                .collect(toImmutableList());
    }

    private void checkForConcurrentTransactionConflicts(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            List<DeltaLakeTableHandle> sameAsTargetSourceTableHandles,
            IsolationLevel isolationLevel,
            long currentVersion,
            AtomicReference<Long> readVersion,
            String tableLocation,
            int attemptCount)
    {
        long readVersionValue = readVersion.get();
        if (currentVersion > readVersionValue) {
            String transactionLogDirectory = getTransactionLogDir(tableLocation);
            for (long version = readVersionValue + 1; version <= currentVersion; version++) {
                List<DeltaLakeTransactionLogEntry> transactionLogEntries;
                try {
                    long finalVersion = version;
                    transactionLogEntries = getEntriesFromJson(version, transactionLogDirectory, fileSystem)
                            .orElseThrow(() -> new TrinoException(DELTA_LAKE_BAD_DATA, "Delta Lake log entries are missing for version " + finalVersion));
                }
                catch (IOException e) {
                    throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, "Failed to access table metadata", e);
                }
                DeltaLakeCommitSummary commitSummary = new DeltaLakeCommitSummary(version, transactionLogEntries);
                checkNoMetadataUpdates(commitSummary);
                checkNoProtocolUpdates(commitSummary);

                switch (isolationLevel) {
                    case WRITESERIALIZABLE -> {
                        if (!sameAsTargetSourceTableHandles.isEmpty()) {
                            List<TupleDomain<DeltaLakeColumnHandle>> enforcedSourcePartitionConstraints = sameAsTargetSourceTableHandles.stream()
                                    .map(DeltaLakeTableHandle::getEnforcedPartitionConstraint)
                                    .collect(toImmutableList());
                            TupleDomain<DeltaLakeColumnHandle> enforcedSourcePartitionConstraintsUnion = TupleDomain.columnWiseUnion(enforcedSourcePartitionConstraints);

                            checkIfCommittedAddedFilesConflictWithCurrentOperation(enforcedSourcePartitionConstraintsUnion, commitSummary);
                            checkIfCommittedRemovedFilesConflictWithCurrentOperation(enforcedSourcePartitionConstraintsUnion, commitSummary);
                        }
                    }
                    case SERIALIZABLE -> throw new TransactionFailedException("Conflicting concurrent writes with the current operation on Serializable isolation level");
                }

                LOG.debug("Completed checking for conflicts in the query %s for target table version: %s Attempt: %s ", session.getQueryId(), commitSummary.getVersion(), attemptCount);
            }

            // Avoid re-reading already processed transaction log entries in case of retries
            readVersion.set(currentVersion);
        }
    }

    private static void checkNoProtocolUpdates(DeltaLakeCommitSummary commitSummary)
    {
        if (commitSummary.getProtocol().isPresent()) {
            throw new TransactionFailedException("Conflicting concurrent writes found. Protocol changed by concurrent write operation");
        }
    }

    private static void checkNoMetadataUpdates(DeltaLakeCommitSummary commitSummary)
    {
        if (!commitSummary.getMetadataUpdates().isEmpty()) {
            throw new TransactionFailedException("Conflicting concurrent writes found. Metadata changed by concurrent write operation");
        }
    }

    private static void checkIfCommittedAddedFilesConflictWithCurrentOperation(TupleDomain<DeltaLakeColumnHandle> enforcedSourcePartitionConstraints, DeltaLakeCommitSummary commitSummary)
    {
        Set<Map<String, Optional<String>>> addedFilesCanonicalPartitionValues = commitSummary.getIsBlindAppend().orElse(false)
                // Do not conflict with blind appends. Blind appends can be placed before or after the current operation
                // when backtracking which serializable sequence of operations led to the current state of the table.
                ? Set.of()
                : commitSummary.getAddedFilesCanonicalPartitionValues();

        if (addedFilesCanonicalPartitionValues.isEmpty()) {
            return;
        }

        boolean readWholeTable = enforcedSourcePartitionConstraints.isAll();
        if (readWholeTable) {
            throw new TransactionFailedException("Conflicting concurrent writes found. Data files added in the modified table by concurrent write operation.");
        }

        Map<DeltaLakeColumnHandle, Domain> enforcedDomains = enforcedSourcePartitionConstraints.getDomains().orElseThrow();
        boolean conflictingAddFilesFound = addedFilesCanonicalPartitionValues.stream()
                .anyMatch(canonicalPartitionValues -> partitionMatchesPredicate(canonicalPartitionValues, enforcedDomains));
        if (conflictingAddFilesFound) {
            throw new TransactionFailedException("Conflicting concurrent writes found. Data files were added in the modified table by another concurrent write operation.");
        }
    }

    private static void checkIfCommittedRemovedFilesConflictWithCurrentOperation(TupleDomain<DeltaLakeColumnHandle> enforcedSourcePartitionConstraints, DeltaLakeCommitSummary commitSummary)
    {
        if (commitSummary.getIsBlindAppend().orElse(false)) {
            // Do not conflict with blind appends. Blind appends can be placed before or after the current operation
            // when backtracking which serializable sequence of operations led to the current state of the table.
            checkState(commitSummary.getRemovedFilesCanonicalPartitionValues().isEmpty(), "Blind append transaction %s cannot contain removed files", commitSummary.getVersion());
            checkState(!commitSummary.isContainsRemoveFileWithoutPartitionValues(), "Blind append transaction %s cannot contain removed files", commitSummary.getVersion());
            return;
        }
        if (commitSummary.isContainsRemoveFileWithoutPartitionValues()) {
            // Can't perform reconciliation between disjoint partitions when it is not clear which partitions are affected by the winning commit.
            throw new TransactionFailedException("Conflicting concurrent writes found. Data files removed in the modified table by another concurrent write operation.");
        }

        if (commitSummary.getRemovedFilesCanonicalPartitionValues().isEmpty()) {
            return;
        }

        boolean readWholeTable = enforcedSourcePartitionConstraints.isAll();
        if (readWholeTable) {
            throw new TransactionFailedException("Conflicting concurrent writes found. Data files removed in the modified table by another concurrent write operation.");
        }

        Map<DeltaLakeColumnHandle, Domain> enforcedDomains = enforcedSourcePartitionConstraints.getDomains().orElseThrow();
        boolean conflictingRemoveFilesFound = commitSummary.getRemovedFilesCanonicalPartitionValues().stream()
                .anyMatch(canonicalPartitionValues -> partitionMatchesPredicate(canonicalPartitionValues, enforcedDomains));
        if (conflictingRemoveFilesFound) {
            throw new TransactionFailedException("Conflicting concurrent writes found. Data files were removed from the modified table by another concurrent write operation.");
        }
    }

    private void writeTransactionLogForInsertOperation(
            ConnectorSession session,
            DeltaLakeInsertTableHandle insertTableHandle,
            boolean isBlindAppend,
            IsolationLevel isolationLevel,
            List<DataFileInfo> dataFileInfos,
            long commitVersion,
            long currentVersion)
            throws IOException
    {
        // it is not obvious why we need to persist this readVersion
        TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, insertTableHandle.location());
        transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, isolationLevel, commitVersion, Instant.now().toEpochMilli(), INSERT_OPERATION, currentVersion, isBlindAppend));

        ColumnMappingMode columnMappingMode = getColumnMappingMode(insertTableHandle.metadataEntry(), insertTableHandle.protocolEntry());
        List<String> partitionColumns = getPartitionColumns(
                insertTableHandle.metadataEntry().getOriginalPartitionColumns(),
                insertTableHandle.inputColumns(),
                columnMappingMode);
        List<String> exactColumnNames = getExactColumnNames(insertTableHandle.metadataEntry());
        appendAddFileEntries(transactionLogWriter, dataFileInfos, partitionColumns, exactColumnNames, true);

        transactionLogWriter.flush();
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
                .collect(toImmutableMap(DeltaLakeColumnHandle::columnName, Function.identity()));
        return originalPartitionColumns.stream()
                .map(columnName -> {
                    DeltaLakeColumnHandle dataColumn = nameToDataColumns.get(columnName);
                    // During writes we want to preserve original case of partition columns, if the name is not different from the physical name
                    if (dataColumn.basePhysicalColumnName().equalsIgnoreCase(columnName)) {
                        return columnName;
                    }
                    return dataColumn.basePhysicalColumnName();
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
        if (isAppendOnly(handle.getMetadataEntry(), handle.getProtocolEntry())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot modify rows from a table with '" + APPEND_ONLY_CONFIGURATION_KEY + "' set to true");
        }
        checkWriteAllowed(session, handle);
        checkWriteSupported(handle);

        List<DeltaLakeColumnHandle> inputColumns = getColumns(handle.getMetadataEntry(), handle.getProtocolEntry()).stream()
                .filter(column -> column.columnType() != SYNTHESIZED)
                .collect(toImmutableList());

        DeltaLakeInsertTableHandle insertHandle = createInsertHandle(retryMode, handle, inputColumns);

        Map<String, DeletionVectorEntry> deletionVectors = loadDeletionVectors(session, handle);
        return new DeltaLakeMergeTableHandle(handle, insertHandle, deletionVectors);
    }

    private Map<String, DeletionVectorEntry> loadDeletionVectors(ConnectorSession session, DeltaLakeTableHandle handle)
    {
        if (!isDeletionVectorEnabled(handle.getMetadataEntry(), handle.getProtocolEntry())) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<String, DeletionVectorEntry> deletionVectors = ImmutableMap.builder();
        try (Stream<AddFileEntry> activeFiles = transactionLogAccess.getActiveFiles(
                session,
                getSnapshot(session, handle),
                handle.getMetadataEntry(),
                handle.getProtocolEntry(),
                handle.getEnforcedPartitionConstraint(),
                handle.getProjectedColumns().orElse(ImmutableSet.of()))) {
            Iterator<AddFileEntry> addFileEntryIterator = activeFiles.iterator();
            while (addFileEntryIterator.hasNext()) {
                AddFileEntry addFileEntry = addFileEntryIterator.next();
                addFileEntry.getDeletionVector().ifPresent(deletionVector -> deletionVectors.put(addFileEntry.getPath(), deletionVector));
            }
        }
        // The latest deletion vector contains all the past deleted rows
        return deletionVectors.buildKeepingLast();
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DeltaLakeMergeTableHandle mergeHandle = (DeltaLakeMergeTableHandle) mergeTableHandle;
        DeltaLakeTableHandle handle = mergeHandle.tableHandle();

        List<DeltaLakeMergeResult> mergeResults = fragments.stream()
                .map(Slice::getBytes)
                .map(mergeResultJsonCodec::fromJson)
                .collect(toImmutableList());

        List<DataFileInfo> allFiles = mergeResults.stream()
                .map(DeltaLakeMergeResult::newFile)
                .flatMap(Optional::stream)
                .collect(toImmutableList());

        if (mergeHandle.insertTableHandle().retriesEnabled()) {
            cleanExtraOutputFiles(session, Location.of(handle.getLocation()), allFiles);
        }

        Optional<Long> checkpointInterval = handle.getMetadataEntry().getCheckpointInterval();

        String tableLocation = handle.getLocation();
        boolean writeCommitted = false;
        try {
            IsolationLevel isolationLevel = getIsolationLevel(handle.getMetadataEntry());
            AtomicReference<Long> readVersion = new AtomicReference<>(handle.getReadVersion());
            long commitVersion = Failsafe.with(TRANSACTION_CONFLICT_RETRY_POLICY)
                    .get(context -> commitMergeOperation(session, mergeHandle, mergeResults, sourceTableHandles, isolationLevel, allFiles, readVersion, context.getAttemptCount()));
            writeCommitted = true;
            enqueueUpdateInfo(
                    session,
                    handle.getSchemaName(),
                    handle.getTableName(),
                    commitVersion,
                    handle.getMetadataEntry().getSchemaString(),
                    Optional.ofNullable(handle.getMetadataEntry().getDescription()));

            writeCheckpointIfNeeded(session, handle.getSchemaTableName(), handle.getLocation(), handle.getReadVersion(), checkpointInterval, commitVersion);
        }
        catch (RuntimeException e) {
            if (!writeCommitted) {
                // TODO perhaps it should happen in a background thread (https://github.com/trinodb/trino/issues/12011)
                cleanupFailedWrite(session, tableLocation, allFiles);
            }
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Failed to write Delta Lake transaction log entry", e);
        }
    }

    private long commitMergeOperation(
            ConnectorSession session,
            DeltaLakeMergeTableHandle mergeHandle,
            List<DeltaLakeMergeResult> mergeResults,
            List<ConnectorTableHandle> sourceTableHandles,
            IsolationLevel isolationLevel,
            List<DataFileInfo> allFiles,
            AtomicReference<Long> readVersion,
            int attemptCount)
            throws IOException
    {
        Map<Boolean, List<DataFileInfo>> split = allFiles.stream()
                .collect(partitioningBy(dataFile -> dataFile.dataFileType() == DATA));

        List<DataFileInfo> newFiles = ImmutableList.copyOf(split.get(true));
        List<DataFileInfo> cdcFiles = ImmutableList.copyOf(split.get(false));

        DeltaLakeTableHandle handle = mergeHandle.tableHandle();
        String tableLocation = handle.getLocation();

        TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, tableLocation);

        long createdTime = Instant.now().toEpochMilli();

        List<DeltaLakeTableHandle> sameAsTargetSourceTableHandles = getSameAsTargetSourceTableHandles(sourceTableHandles, handle.getSchemaTableName());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        long currentVersion = getMandatoryCurrentVersion(fileSystem, tableLocation, readVersion.get());
        checkForConcurrentTransactionConflicts(session, fileSystem, sameAsTargetSourceTableHandles, isolationLevel, currentVersion, readVersion, handle.getLocation(), attemptCount);
        long commitVersion = currentVersion + 1;

        transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, isolationLevel, commitVersion, createdTime, MERGE_OPERATION, handle.getReadVersion(), sameAsTargetSourceTableHandles.isEmpty()));
        // TODO: Delta writes another field "operationMetrics" (https://github.com/trinodb/trino/issues/12005)

        long writeTimestamp = Instant.now().toEpochMilli();

        ColumnMappingMode columnMappingMode = getColumnMappingMode(handle.getMetadataEntry(), handle.getProtocolEntry());
        List<String> partitionColumns = getPartitionColumns(
                handle.getMetadataEntry().getOriginalPartitionColumns(),
                mergeHandle.insertTableHandle().inputColumns(),
                columnMappingMode);

        if (!cdcFiles.isEmpty()) {
            appendCdcFilesInfos(transactionLogWriter, cdcFiles, partitionColumns);
        }

        for (DeltaLakeMergeResult mergeResult : mergeResults) {
            if (mergeResult.oldFile().isEmpty()) {
                continue;
            }
            transactionLogWriter.appendRemoveFileEntry(new RemoveFileEntry(toUriFormat(mergeResult.oldFile().get()), createPartitionValuesMap(partitionColumns, mergeResult.partitionValues()), writeTimestamp, true, mergeResult.oldDeletionVector()));
        }

        appendAddFileEntries(transactionLogWriter, newFiles, partitionColumns, getExactColumnNames(handle.getMetadataEntry()), true);

        transactionLogWriter.flush();
        return commitVersion;
    }

    private static Map<String, String> createPartitionValuesMap(List<String> partitionColumnNames, List<String> partitionValues)
    {
        checkArgument(partitionColumnNames.size() == partitionValues.size(), "partitionColumnNames and partitionValues sizes don't match");
        // using Hashmap because partition values can be null
        Map<String, String> partitionValuesMap = new HashMap<>();
        for (int i = 0; i < partitionColumnNames.size(); i++) {
            partitionValuesMap.put(partitionColumnNames.get(i), partitionValues.get(i));
        }
        return unmodifiableMap(partitionValuesMap);
    }

    private static Map<String, String> createPartitionValuesMap(Map<String, Optional<String>> canonicalPartitionValues)
    {
        // using Hashmap because partition values can be null
        Map<String, String> partitionValuesMap = new HashMap<>();
        for (Map.Entry<String, Optional<String>> entry : canonicalPartitionValues.entrySet()) {
            partitionValuesMap.put(entry.getKey(), entry.getValue().orElse(null));
        }
        return unmodifiableMap(partitionValuesMap);
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
                partitionValues.put(partitionColumnNames.get(i), info.partitionValues().get(i));
            }
            partitionValues = unmodifiableMap(partitionValues);

            transactionLogWriter.appendCdcEntry(
                    new CdcEntry(
                            toUriFormat(info.path()),
                            partitionValues,
                            info.size()));
        }
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
        DeltaLakeTableHandle tableHandle = checkValidTableHandle(connectorTableHandle);
        checkUnsupportedWriterFeatures(tableHandle.getProtocolEntry());

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

        List<DeltaLakeColumnHandle> columns = getColumns(tableHandle.getMetadataEntry(), tableHandle.getProtocolEntry()).stream()
                .filter(column -> column.columnType() != SYNTHESIZED)
                .collect(toImmutableList());

        return Optional.of(new DeltaLakeTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                OPTIMIZE,
                new DeltaTableOptimizeHandle(
                        tableHandle.getMetadataEntry(),
                        tableHandle.getProtocolEntry(),
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
        return switch (executeHandle.procedureId()) {
            case OPTIMIZE -> getLayoutForOptimize(executeHandle);
        };
    }

    private Optional<ConnectorTableLayout> getLayoutForOptimize(DeltaLakeTableExecuteHandle executeHandle)
    {
        DeltaTableOptimizeHandle optimizeHandle = (DeltaTableOptimizeHandle) executeHandle.procedureHandle();
        List<String> partitionColumnNames = optimizeHandle.getMetadataEntry().getLowercasePartitionColumns();
        if (partitionColumnNames.isEmpty()) {
            return Optional.empty();
        }
        Map<String, DeltaLakeColumnHandle> columnsByName = optimizeHandle.getTableColumns().stream()
                .collect(toImmutableMap(DeltaLakeColumnHandle::columnName, identity()));
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
        return switch (executeHandle.procedureId()) {
            case OPTIMIZE -> beginOptimize(session, executeHandle, table);
        };
    }

    private BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginOptimize(
            ConnectorSession session,
            DeltaLakeTableExecuteHandle executeHandle,
            DeltaLakeTableHandle table)
    {
        DeltaTableOptimizeHandle optimizeHandle = (DeltaTableOptimizeHandle) executeHandle.procedureHandle();

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
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                finishOptimize(session, executeHandle, fragments, splitSourceInfo);
                return;
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private void finishOptimize(ConnectorSession session, DeltaLakeTableExecuteHandle executeHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        DeltaTableOptimizeHandle optimizeHandle = (DeltaTableOptimizeHandle) executeHandle.procedureHandle();
        long readVersion = optimizeHandle.getCurrentVersion().orElseThrow(() -> new IllegalArgumentException("currentVersion not set"));
        String tableLocation = executeHandle.tableLocation();

        // paths to be deleted
        Set<DeltaLakeScannedDataFile> scannnedDataFiles = splitSourceInfo.stream()
                .map(DeltaLakeScannedDataFile.class::cast)
                .collect(toImmutableSet());

        // files to be added
        List<DataFileInfo> dataFileInfos = fragments.stream()
                .map(Slice::getBytes)
                .map(dataFileInfoCodec::fromJson)
                .collect(toImmutableList());

        if (optimizeHandle.isRetriesEnabled()) {
            cleanExtraOutputFiles(session, Location.of(executeHandle.tableLocation()), dataFileInfos);
        }

        boolean writeCommitted = false;
        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, tableLocation);

            long createdTime = Instant.now().toEpochMilli();
            long commitVersion = readVersion + 1;
            transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, IsolationLevel.WRITESERIALIZABLE, commitVersion, createdTime, OPTIMIZE_OPERATION, readVersion, false));
            // TODO: Delta writes another field "operationMetrics" that I haven't
            //   seen before. It contains delete/update metrics. Investigate/include it.

            long writeTimestamp = Instant.now().toEpochMilli();

            for (DeltaLakeScannedDataFile scannedFile : scannnedDataFiles) {
                String relativePath = relativePath(tableLocation, scannedFile.path());
                Map<String, Optional<String>> canonicalPartitionValues = scannedFile.partitionKeys();
                transactionLogWriter.appendRemoveFileEntry(new RemoveFileEntry(
                        toUriFormat(relativePath),
                        createPartitionValuesMap(canonicalPartitionValues),
                        writeTimestamp,
                        false,
                        Optional.empty()));
            }

            // Note: during writes we want to preserve original case of partition columns
            List<String> partitionColumns = getPartitionColumns(
                    optimizeHandle.getMetadataEntry().getOriginalPartitionColumns(),
                    optimizeHandle.getTableColumns(),
                    getColumnMappingMode(optimizeHandle.getMetadataEntry(), optimizeHandle.getProtocolEntry()));
            appendAddFileEntries(transactionLogWriter, dataFileInfos, partitionColumns, getExactColumnNames(optimizeHandle.getMetadataEntry()), false);

            transactionLogWriter.flush();
            writeCommitted = true;
            enqueueUpdateInfo(
                    session,
                    executeHandle.schemaTableName().getSchemaName(),
                    executeHandle.schemaTableName().getTableName(),
                    commitVersion,
                    optimizeHandle.getMetadataEntry().getSchemaString(),
                    Optional.ofNullable(optimizeHandle.getMetadataEntry().getDescription()));
            Optional<Long> checkpointInterval = Optional.of(1L); // force checkpoint
            writeCheckpointIfNeeded(
                    session,
                    executeHandle.schemaTableName(),
                    executeHandle.tableLocation(),
                    optimizeHandle.getCurrentVersion().orElseThrow(),
                    checkpointInterval,
                    commitVersion);
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
        ColumnMappingMode columnMappingMode = getColumnMappingMode(handle.getMetadataEntry(), handle.getProtocolEntry());
        if (!(columnMappingMode == NONE || columnMappingMode == ColumnMappingMode.NAME || columnMappingMode == ColumnMappingMode.ID)) {
            throw new TrinoException(NOT_SUPPORTED, "Writing with column mapping %s is not supported".formatted(columnMappingMode));
        }
        if (getColumnIdentities(handle.getMetadataEntry(), handle.getProtocolEntry()).values().stream().anyMatch(identity -> identity)) {
            throw new TrinoException(NOT_SUPPORTED, "Writing to tables with identity columns is not supported");
        }
        checkUnsupportedUniversalFormat(handle.getMetadataEntry());
        checkUnsupportedWriterFeatures(handle.getProtocolEntry());
    }

    public static void checkUnsupportedUniversalFormat(MetadataEntry metadataEntry)
    {
        List<String> universalFormats = enabledUniversalFormats(metadataEntry);
        if (!universalFormats.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported universal formats: " + universalFormats);
        }
    }

    private static void checkUnsupportedWriterFeatures(ProtocolEntry protocolEntry)
    {
        Set<String> unsupportedWriterFeatures = unsupportedWriterFeatures(protocolEntry.writerFeatures().orElse(ImmutableSet.of()));
        if (!unsupportedWriterFeatures.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported writer features: " + unsupportedWriterFeatures);
        }
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
        int requiredWriterVersion = handle.getProtocolEntry().minWriterVersion();
        if (requiredWriterVersion > MAX_WRITER_VERSION) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    format("Table %s requires Delta Lake writer version %d which is not supported", handle.getSchemaTableName(), requiredWriterVersion));
        }
    }

    private TableSnapshot getSnapshot(ConnectorSession session, DeltaLakeTableHandle table)
    {
        return getSnapshot(session, table.getSchemaTableName(), table.getLocation(), Optional.of(table.getReadVersion()));
    }

    private ProtocolEntry protocolEntryForNewTable(boolean containsTimestampType, Map<String, Object> properties)
    {
        return protocolEntry(DEFAULT_READER_VERSION, DEFAULT_WRITER_VERSION, containsTimestampType, properties);
    }

    private ProtocolEntry protocolEntryForTable(ProtocolEntry existingProtocolEntry, boolean containsTimestampType, Map<String, Object> properties)
    {
        return protocolEntry(existingProtocolEntry.minReaderVersion(), existingProtocolEntry.minWriterVersion(), containsTimestampType, properties);
    }

    private ProtocolEntry protocolEntry(int readerVersion, int writerVersion, boolean containsTimestampType, Map<String, Object> properties)
    {
        Set<String> readerFeatures = new HashSet<>();
        Set<String> writerFeatures = new HashSet<>();
        Optional<Boolean> changeDataFeedEnabled = getChangeDataFeedEnabled(properties);
        if (changeDataFeedEnabled.isPresent() && changeDataFeedEnabled.get()) {
            // Enabling cdf (change data feed) requires setting the writer version to 4
            writerVersion = CDF_SUPPORTED_WRITER_VERSION;
        }
        ColumnMappingMode columnMappingMode = getColumnMappingMode(properties);
        if (columnMappingMode == ID || columnMappingMode == NAME) {
            readerVersion = max(readerVersion, COLUMN_MAPPING_MODE_SUPPORTED_READER_VERSION);
            writerVersion = max(writerVersion, COLUMN_MAPPING_MODE_SUPPORTED_WRITER_VERSION);
        }
        if (containsTimestampType) {
            readerVersion = max(readerVersion, TIMESTAMP_NTZ_SUPPORTED_READER_VERSION);
            writerVersion = max(writerVersion, TIMESTAMP_NTZ_SUPPORTED_WRITER_VERSION);
            readerFeatures.add(TIMESTAMP_NTZ_FEATURE_NAME);
            writerFeatures.add(TIMESTAMP_NTZ_FEATURE_NAME);
        }
        if (getDeletionVectorsEnabled(properties)) {
            readerVersion = max(readerVersion, DELETION_VECTORS_SUPPORTED_READER_VERSION);
            writerVersion = max(writerVersion, DELETION_VECTORS_SUPPORTED_WRITER_VERSION);
            readerFeatures.add(DELETION_VECTORS_FEATURE_NAME);
            writerFeatures.add(DELETION_VECTORS_FEATURE_NAME);
        }
        return new ProtocolEntry(
                readerVersion,
                writerVersion,
                readerFeatures.isEmpty() ? Optional.empty() : Optional.of(readerFeatures),
                writerFeatures.isEmpty() ? Optional.empty() : Optional.of(writerFeatures));
    }

    private void writeCheckpointIfNeeded(
            ConnectorSession session,
            SchemaTableName table,
            String tableLocation,
            long readVersion,
            Optional<Long> checkpointInterval,
            long newVersion)
    {
        try {
            // We are writing checkpoint synchronously. It should not be long lasting operation for tables where transaction log is not humongous.
            // Tables with really huge transaction logs would behave poorly in read flow already.
            TableSnapshot snapshot = getSnapshot(session, table, tableLocation, Optional.of(readVersion));
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

            TableSnapshot updatedSnapshot = snapshot.getUpdatedSnapshot(fileSystemFactory.create(session), Optional.of(newVersion)).orElseThrow();
            checkpointWriterManager.writeCheckpoint(session, updatedSnapshot);
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
                .map(DataFileInfo::path)
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
        DeltaLakeTableHandle handle = (DeltaLakeTableHandle) table;
        boolean isPartitioned = !handle.getMetadataEntry().getLowercasePartitionColumns().isEmpty();
        return Optional.of(new DeltaLakeInputInfo(isPartitioned, handle.getReadVersion()));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        LocatedTableHandle handle = (LocatedTableHandle) tableHandle;
        boolean deleteData = handle.managed();
        metastore.dropTable(handle.schemaTableName(), handle.location(), deleteData);
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
        metastore.renameTable(handle.getSchemaTableName(), newTableName);
    }

    private CommitInfoEntry getCommitInfoEntry(
            ConnectorSession session,
            IsolationLevel isolationLevel,
            long commitVersion,
            long createdTime,
            String operation,
            long readVersion,
            boolean isBlindAppend)
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
                isolationLevel.getValue(),
                Optional.of(isBlindAppend));
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

        int requiredWriterVersion = currentProtocolEntry.minWriterVersion();
        Optional<MetadataEntry> metadataEntry = Optional.empty();
        if (properties.containsKey(CHANGE_DATA_FEED_ENABLED_PROPERTY)) {
            boolean changeDataFeedEnabled = (Boolean) properties.get(CHANGE_DATA_FEED_ENABLED_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The change_data_feed_enabled property cannot be empty"));
            if (changeDataFeedEnabled) {
                Set<String> columnNames = getColumns(handle.getMetadataEntry(), handle.getProtocolEntry()).stream().map(DeltaLakeColumnHandle::baseColumnName).collect(toImmutableSet());
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
        if (requiredWriterVersion != currentProtocolEntry.minWriterVersion()) {
            protocolEntry = Optional.of(new ProtocolEntry(currentProtocolEntry.minReaderVersion(), requiredWriterVersion, currentProtocolEntry.readerFeatures(), currentProtocolEntry.writerFeatures()));
        }

        try {
            TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, handle.getLocation());
            transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, IsolationLevel.WRITESERIALIZABLE, commitVersion, createdTime, SET_TBLPROPERTIES_OPERATION, readVersion, true));
            protocolEntry.ifPresent(transactionLogWriter::appendProtocolEntry);

            metadataEntry.ifPresent(transactionLogWriter::appendMetadataEntry);

            transactionLogWriter.flush();
            enqueueUpdateInfo(
                    session,
                    handle.getSchemaName(),
                    handle.getTableName(),
                    commitVersion,
                    metadataEntry.orElseThrow().getSchemaString(),
                    Optional.ofNullable(metadataEntry.orElseThrow().getDescription()));
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
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        checkArgument(viewProperties.isEmpty(), "This connector does not support creating views with properties");
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
        return path.substring(basePathDirectory.length());
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

        checkArgument(constraint.getSummary().getDomains().isPresent(), "constraint summary is NONE");

        UtcConstraintExtractor.ExtractionResult extractionResult = extractTupleDomain(constraint);
        TupleDomain<ColumnHandle> predicate = extractionResult.tupleDomain();

        if (predicate.isAll() && constraint.getPredicateColumns().isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<DeltaLakeColumnHandle> newEnforcedConstraint;
        TupleDomain<DeltaLakeColumnHandle> newUnenforcedConstraint;
        Set<DeltaLakeColumnHandle> newConstraintColumns;
        if (predicate.isNone()) {
            // Engine does not pass none Constraint.summary. It can become none when combined with the expression and connector's domain knowledge.
            newEnforcedConstraint = TupleDomain.none();
            newUnenforcedConstraint = TupleDomain.all();
            newConstraintColumns = constraint.getPredicateColumns().stream()
                    .flatMap(Collection::stream)
                    .map(DeltaLakeColumnHandle.class::cast)
                    .collect(toImmutableSet());
        }
        else {
            Set<DeltaLakeColumnHandle> partitionColumns = ImmutableSet.copyOf(extractPartitionColumns(tableHandle.getMetadataEntry(), tableHandle.getProtocolEntry(), typeManager));
            Map<ColumnHandle, Domain> constraintDomains = predicate.getDomains().orElseThrow();

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

            newEnforcedConstraint = TupleDomain.withColumnDomains(enforceableDomains.buildOrThrow());
            newUnenforcedConstraint = TupleDomain.withColumnDomains(unenforceableDomains.buildOrThrow());
            newConstraintColumns = constraintColumns.build();
        }

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
                Sets.union(tableHandle.getConstraintColumns(), newConstraintColumns),
                tableHandle.getWriteType(),
                tableHandle.getProjectedColumns(),
                tableHandle.getUpdatedColumns(),
                tableHandle.getUpdateRowIdColumns(),
                Optional.empty(),
                false,
                false,
                Optional.empty(),
                tableHandle.getReadVersion(),
                tableHandle.isTimeTravel());

        if (tableHandle.getEnforcedPartitionConstraint().equals(newHandle.getEnforcedPartitionConstraint()) &&
                tableHandle.getNonPartitionConstraint().equals(newHandle.getNonPartitionConstraint()) &&
                tableHandle.getConstraintColumns().equals(newHandle.getConstraintColumns())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                newUnenforcedConstraint.transformKeys(ColumnHandle.class::cast),
                extractionResult.remainingExpression(),
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
                            ((DeltaLakeColumnHandle) assignment.getValue()).baseType()))
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
                projectedColumnHandle = projectColumn(oldColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType(), getColumnMappingMode(deltaLakeTableHandle.getMetadataEntry(), deltaLakeTableHandle.getProtocolEntry()));
                projectedColumnName = projectedColumnHandle.qualifiedPhysicalName();
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
        Optional<DeltaLakeColumnProjectionInfo> existingProjectionInfo = column.projectionInfo();
        ImmutableList.Builder<String> dereferenceNames = ImmutableList.builder();
        ImmutableList.Builder<Integer> dereferenceIndices = ImmutableList.builder();

        if (!column.isBaseColumn()) {
            dereferenceNames.addAll(existingProjectionInfo.orElseThrow().getDereferencePhysicalNames());
            dereferenceIndices.addAll(existingProjectionInfo.orElseThrow().getDereferenceIndices());
        }

        Type columnType = switch (columnMappingMode) {
            case ID, NAME -> column.basePhysicalType();
            case NONE -> column.baseType();
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
                column.baseColumnName(),
                column.baseType(),
                column.baseFieldId(),
                column.basePhysicalColumnName(),
                column.basePhysicalType(),
                REGULAR,
                Optional.of(projectionInfo));
    }

    /**
     * Returns the assignment key corresponding to the column represented by projectedColumn in the assignments, if one exists.
     * The variable in the projectedColumn can itself be a representation of another projected column. For example,
     * say a projected column representation has variable "x" and a dereferenceIndices=[0]. "x" can in-turn map to a projected
     * column handle with base="a" and [1, 2] as dereference indices. Then the method searches for a column handle in
     * assignments with base="a" and dereferenceIndices=[1, 2, 0].
     */
    private static Optional<String> find(Map<String, ColumnHandle> assignments, ProjectedColumnRepresentation projectedColumn)
    {
        DeltaLakeColumnHandle variableColumn = (DeltaLakeColumnHandle) assignments.get(projectedColumn.getVariable().getName());

        requireNonNull(variableColumn, "variableColumn is null");

        String baseColumnName = variableColumn.baseColumnName();

        List<Integer> variableColumnIndices = variableColumn.projectionInfo()
                .map(DeltaLakeColumnProjectionInfo::getDereferenceIndices)
                .orElse(ImmutableList.of());

        List<Integer> projectionIndices = ImmutableList.<Integer>builder()
                .addAll(variableColumnIndices)
                .addAll(projectedColumn.getDereferenceIndices())
                .build();

        for (Map.Entry<String, ColumnHandle> entry : assignments.entrySet()) {
            DeltaLakeColumnHandle column = (DeltaLakeColumnHandle) entry.getValue();
            if (column.baseColumnName().equals(baseColumnName) &&
                    column.projectionInfo()
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
                                .map(DeltaLakeColumnHandle::baseColumnName)
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

        List<DeltaLakeColumnMetadata> columnsMetadata = extractSchema(metadata, handle.getProtocolEntry(), typeManager);
        Set<String> allColumnNames = columnsMetadata.stream().map(columnMetadata -> columnMetadata.name().toLowerCase(ENGLISH)).collect(Collectors.toSet());
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
                handle.getReadVersion(),
                handle.isTimeTravel());
        TableStatisticsMetadata statisticsMetadata = getStatisticsCollectionMetadata(
                columnsMetadata.stream().map(DeltaLakeColumnMetadata::columnMetadata).collect(toImmutableList()),
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
        if (analyzeHandle.analyzeMode() == FULL_REFRESH) {
            // TODO: Populate stats for incremental ANALYZE https://github.com/trinodb/trino/issues/18110
            generateMissingFileStatistics(session, tableHandle, computedStatistics);
        }
        Optional<Instant> maxFileModificationTime = getMaxFileModificationTime(computedStatistics);
        Map<String, String> physicalColumnNameMapping = extractSchema(tableHandle.getMetadataEntry(), tableHandle.getProtocolEntry(), typeManager).stream()
                .collect(toImmutableMap(DeltaLakeColumnMetadata::name, DeltaLakeColumnMetadata::physicalName));
        updateTableStatistics(
                session,
                Optional.of(analyzeHandle),
                tableHandle.getSchemaTableName(),
                tableHandle.getLocation(),
                maxFileModificationTime,
                computedStatistics,
                getExactColumnNames(tableHandle.getMetadataEntry()),
                Optional.of(physicalColumnNameMapping),
                false);
    }

    private void generateMissingFileStatistics(ConnectorSession session, DeltaLakeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        Map<String, AddFileEntry> addFileEntriesWithNoStats;
        try (Stream<AddFileEntry> activeFiles = transactionLogAccess.getActiveFiles(
                session,
                getSnapshot(session, tableHandle),
                tableHandle.getMetadataEntry(),
                tableHandle.getProtocolEntry(),
                TupleDomain.all(),
                alwaysTrue())) {
            addFileEntriesWithNoStats = activeFiles.filter(addFileEntry -> addFileEntry.getStats().isEmpty()
                            || addFileEntry.getStats().get().getNumRecords().isEmpty()
                            || addFileEntry.getStats().get().getMaxValues().isEmpty()
                            || addFileEntry.getStats().get().getMinValues().isEmpty()
                            || addFileEntry.getStats().get().getNullCount().isEmpty())
                    .filter(addFileEntry -> !URI.create(addFileEntry.getPath()).isAbsolute()) // TODO: Support absolute paths https://github.com/trinodb/trino/issues/18277
                    // Statistics returns whole path to file build in DeltaLakeSplitManager, so we need to create corresponding map key for AddFileEntry.
                    .collect(toImmutableMap(addFileEntry -> DeltaLakeSplitManager.buildSplitPath(Location.of(tableHandle.getLocation()), addFileEntry).toString(), identity()));
        }
        if (addFileEntriesWithNoStats.isEmpty()) {
            return;
        }

        Map</* lowercase */ String, DeltaLakeColumnHandle> lowercaseToColumnsHandles = getColumns(tableHandle.getMetadataEntry(), tableHandle.getProtocolEntry()).stream()
                .filter(column -> column.columnType() == REGULAR)
                .collect(toImmutableMap(columnHandle -> columnHandle.baseColumnName().toLowerCase(ENGLISH), identity()));

        List<AddFileEntry> updatedAddFileEntries = computedStatistics.stream()
                .map(statistics -> {
                    // Grouping by `PATH_COLUMN_NAME`.
                    String filePathFromStatistics = VARCHAR.getSlice(getOnlyElement(statistics.getGroupingValues()), 0).toStringUtf8();
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
            transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, IsolationLevel.WRITESERIALIZABLE, commitVersion, createdTime, OPTIMIZE_OPERATION, readVersion, true));
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
                    addFileEntry.getTags(),
                    addFileEntry.getDeletionVector());
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
            Optional<Map<String, String>> physicalColumnNameMapping,
            boolean ignoreFailure)
    {
        Optional<ExtendedStatistics> oldStatistics = Optional.empty();
        boolean loadExistingStats = analyzeHandle.isEmpty() || analyzeHandle.get().analyzeMode() == INCREMENTAL;
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

        Optional<Set<String>> analyzedColumns = analyzeHandle.flatMap(AnalyzeHandle::columns);
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

        try {
            statisticsAccess.updateExtendedStatistics(session, schemaTableName, location, mergedExtendedStatistics);
        }
        catch (Exception e) {
            if (ignoreFailure) {
                // We can't fail here as transaction was already committed
                LOG.error(e, "Failed to write extended statistics for the table %s", schemaTableName);
            }
            else {
                throw e;
            }
        }
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
                .map(dataFileInfo -> baseLocation.appendPath(dataFileInfo.path()))
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
        return getRawSystemTable(session, tableName).map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    private Optional<SystemTable> getRawSystemTable(ConnectorSession session, SchemaTableName systemTableName)
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
            case PARTITIONS -> Optional.of(new DeltaLakePartitionsTable(session, systemTableName, tableLocation, transactionLogAccess, typeManager));
        };
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // delta lake supports only a columnar (parquet) storage format
        return true;
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
        if (changeDataFeedEnabled(tableHandle.getMetadataEntry(), tableHandle.getProtocolEntry()).orElse(false)) {
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
        if (isAppendOnly(tableHandle.getMetadataEntry(), tableHandle.getProtocolEntry())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot modify rows from a table with '" + APPEND_ONLY_CONFIGURATION_KEY + "' set to true");
        }
        checkWriteAllowed(session, tableHandle);
        checkWriteSupported(tableHandle);

        try {
            IsolationLevel isolationLevel = getIsolationLevel(tableHandle.getMetadataEntry());
            AtomicReference<Long> readVersion = new AtomicReference<>(tableHandle.getReadVersion());
            CommitDeleteOperationResult commitDeleteOperationResult = Failsafe.with(TRANSACTION_CONFLICT_RETRY_POLICY)
                    .get(context -> commitDeleteOperation(session, tableHandle, operation, isolationLevel, readVersion, context.getAttemptCount()));

            writeCheckpointIfNeeded(
                    session,
                    tableHandle.getSchemaTableName(),
                    tableHandle.location(),
                    tableHandle.getReadVersion(),
                    tableHandle.getMetadataEntry().getCheckpointInterval(),
                    commitDeleteOperationResult.commitVersion());
            enqueueUpdateInfo(
                    session,
                    tableHandle.getSchemaName(),
                    tableHandle.getTableName(),
                    commitDeleteOperationResult.commitVersion,
                    tableHandle.getMetadataEntry().getSchemaString(),
                    Optional.ofNullable(tableHandle.getMetadataEntry().getDescription()));
            return commitDeleteOperationResult.deletedRecords();
        }
        catch (Exception e) {
            throw new TrinoException(DELTA_LAKE_BAD_WRITE, "Failed to write Delta Lake transaction log entry", e);
        }
    }

    private CommitDeleteOperationResult commitDeleteOperation(
            ConnectorSession session,
            DeltaLakeTableHandle tableHandle,
            String operation,
            IsolationLevel isolationLevel,
            AtomicReference<Long> readVersion,
            int attemptCount)
            throws IOException
    {
        String tableLocation = tableHandle.location();

        TransactionLogWriter transactionLogWriter = transactionLogWriterFactory.newWriter(session, tableLocation);

        long writeTimestamp = Instant.now().toEpochMilli();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        long currentVersion = getMandatoryCurrentVersion(fileSystem, tableLocation, readVersion.get());
        checkForConcurrentTransactionConflicts(session, fileSystem, ImmutableList.of(tableHandle), isolationLevel, currentVersion, readVersion, tableHandle.getLocation(), attemptCount);
        long commitVersion = currentVersion + 1;
        transactionLogWriter.appendCommitInfoEntry(getCommitInfoEntry(session, isolationLevel, commitVersion, writeTimestamp, operation, tableHandle.getReadVersion(), false));

        long deletedRecords = 0L;
        boolean allDeletedFilesStatsPresent = true;
        try (Stream<AddFileEntry> activeFiles = getAddFileEntriesMatchingEnforcedPartitionConstraint(session, tableHandle)) {
            Iterator<AddFileEntry> addFileEntryIterator = activeFiles.iterator();
            while (addFileEntryIterator.hasNext()) {
                AddFileEntry addFileEntry = addFileEntryIterator.next();
                transactionLogWriter.appendRemoveFileEntry(new RemoveFileEntry(addFileEntry.getPath(), addFileEntry.getPartitionValues(), writeTimestamp, true, Optional.empty()));

                Optional<Long> fileRecords = addFileEntry.getStats().flatMap(DeltaLakeFileStatistics::getNumRecords);
                allDeletedFilesStatsPresent &= fileRecords.isPresent();
                deletedRecords += fileRecords.orElse(0L);
            }
        }

        transactionLogWriter.flush();
        return new CommitDeleteOperationResult(commitVersion, allDeletedFilesStatsPresent ? OptionalLong.of(deletedRecords) : OptionalLong.empty());
    }

    private record CommitDeleteOperationResult(long commitVersion, OptionalLong deletedRecords)
    {
        CommitDeleteOperationResult
        {
            requireNonNull(deletedRecords, "deletedRecords is null");
        }
    }

    private void enqueueUpdateInfo(ConnectorSession session, String schemaName, String tableName, long version, String schemaString, Optional<String> tableComment)
    {
        if (!metadataScheduler.canStoreTableMetadata(session, schemaString, tableComment)) {
            return;
        }
        tableUpdateInfos.put(new SchemaTableName(schemaName, tableName), new TableUpdateInfo(session, version, schemaString, tableComment));
    }

    public void commit()
    {
        metadataScheduler.putAll(tableUpdateInfos);
        tableUpdateInfos.clear();
    }

    private Stream<AddFileEntry> getAddFileEntriesMatchingEnforcedPartitionConstraint(ConnectorSession session, DeltaLakeTableHandle tableHandle)
    {
        TableSnapshot tableSnapshot = getSnapshot(session, tableHandle);
        Stream<AddFileEntry> validDataFiles = transactionLogAccess.getActiveFiles(
                session,
                tableSnapshot,
                tableHandle.getMetadataEntry(),
                tableHandle.getProtocolEntry(),
                tableHandle.getEnforcedPartitionConstraint(),
                tableHandle.getProjectedColumns().orElse(ImmutableSet.of()));
        TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint = tableHandle.getEnforcedPartitionConstraint();
        if (enforcedPartitionConstraint.isAll()) {
            return validDataFiles;
        }
        Map<DeltaLakeColumnHandle, Domain> enforcedDomains = enforcedPartitionConstraint.getDomains().orElseThrow();
        return validDataFiles
                .filter(addAction -> partitionMatchesPredicate(addAction.getCanonicalPartitionValues(), enforcedDomains));
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

    private static ColumnMetadata getColumnMetadata(DeltaLakeColumnHandle column, @Nullable String comment, boolean nullability, Optional<String> generationExpression)
    {
        String columnName;
        Type columnType;
        if (column.isBaseColumn()) {
            columnName = column.baseColumnName();
            columnType = column.baseType();
        }
        else {
            DeltaLakeColumnProjectionInfo projectionInfo = column.projectionInfo().orElseThrow();
            columnName = column.qualifiedPhysicalName();
            columnType = projectionInfo.getType();
        }
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .setHidden(column.columnType() == SYNTHESIZED)
                .setComment(Optional.ofNullable(comment))
                .setNullable(nullability)
                .setExtraInfo(generationExpression.map(expression -> "generated: " + expression))
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
                                .filter(column -> canUseInPredicate(column.columnMetadata()))
                                .collect(toImmutableMap(
                                        column -> DeltaLakeMetadata.toColumnHandle(column.name(), column.type(), column.fieldId(), column.physicalName(), column.physicalColumnType(), canonicalPartitionColumns),
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
        Optional<Long> nullCount = stats.getNullCount(column.physicalName());
        if (nullCount.isEmpty()) {
            // No stats were collected for this column; this can happen in 2 scenarios:
            // 1. The column didn't exist in the schema when the data file was created
            // 2. The column does exist in the file, but Spark property 'delta.dataSkippingNumIndexedCols'
            //    was used to limit the number of columns for which stats are collected
            // Since we don't know which scenario we're dealing with, we can't make a decision to prune.
            return Domain.all(column.type());
        }
        if (stats.getNumRecords().equals(nullCount)) {
            return Domain.onlyNull(column.type());
        }

        boolean hasNulls = nullCount.get() > 0;
        DeltaLakeColumnHandle deltaLakeColumnHandle = toColumnHandle(column.name(), column.type(), column.fieldId(), column.physicalName(), column.physicalColumnType(), canonicalPartitionColumns);
        Optional<Object> minValue = stats.getMinColumnValue(deltaLakeColumnHandle);
        if (minValue.isPresent() && isFloatingPointNaN(column.type(), minValue.get())) {
            return allValues(column.type(), hasNulls);
        }
        if (isNotFinite(minValue, column.type())) {
            minValue = Optional.empty();
        }
        Optional<Object> maxValue = stats.getMaxColumnValue(deltaLakeColumnHandle);
        if (maxValue.isPresent() && isFloatingPointNaN(column.type(), maxValue.get())) {
            return allValues(column.type(), hasNulls);
        }
        if (isNotFinite(maxValue, column.type())) {
            maxValue = Optional.empty();
        }
        if (minValue.isPresent() && maxValue.isPresent()) {
            return Domain.create(
                    ofRanges(range(column.type(), minValue.get(), true, maxValue.get(), true)),
                    hasNulls);
        }
        if (minValue.isPresent()) {
            return Domain.create(ofRanges(greaterThanOrEqual(column.type(), minValue.get())), hasNulls);
        }

        return maxValue
                .map(value -> Domain.create(ofRanges(lessThanOrEqual(column.type(), value)), hasNulls))
                .orElseGet(() -> Domain.all(column.type()));
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
        return Optional.ofNullable(database.getParameters().get(TRINO_QUERY_ID_NAME));
    }
}
