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
package io.trino.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveBasicStatistics;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.base.projection.ApplyProjectionUtil.ProjectedColumnRepresentation;
import io.trino.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior;
import io.trino.plugin.hive.HiveWritableTableHandle.BucketInfo;
import io.trino.plugin.hive.LocationService.WriteInfo;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.procedure.OptimizeTableProcedure;
import io.trino.plugin.hive.security.AccessControlMetadata;
import io.trino.plugin.hive.statistics.HiveStatisticsProvider;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.hive.util.SerdeConstants;
import io.trino.spi.ErrorType;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTablePartitioning;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatisticType;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.metastore.HiveBasicStatistics.createEmptyStatistics;
import static io.trino.metastore.HiveBasicStatistics.createZeroStatistics;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.Partition.toPartitionValues;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.metastore.PrincipalPrivileges.fromHivePrivilegeInfos;
import static io.trino.metastore.StatisticsUpdateMode.MERGE_INCREMENTAL;
import static io.trino.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.metastore.type.Category.PRIMITIVE;
import static io.trino.parquet.writer.ParquetWriter.SUPPORTED_BLOOM_FILTER_TYPES;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.hive.HiveAnalyzeProperties.getColumnNames;
import static io.trino.plugin.hive.HiveAnalyzeProperties.getPartitionList;
import static io.trino.plugin.hive.HiveApplyProjectionUtil.find;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveColumnHandle.mergeRowIdColumnHandle;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_COLUMN_ORDER_MISMATCH;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_VIEW_TRANSLATION_ERROR;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.HiveSessionProperties.NON_TRANSACTIONAL_OPTIMIZE_ENABLED;
import static io.trino.plugin.hive.HiveSessionProperties.getDeltaLakeCatalogName;
import static io.trino.plugin.hive.HiveSessionProperties.getHiveStorageFormat;
import static io.trino.plugin.hive.HiveSessionProperties.getHudiCatalogName;
import static io.trino.plugin.hive.HiveSessionProperties.getIcebergCatalogName;
import static io.trino.plugin.hive.HiveSessionProperties.getInsertExistingPartitionsBehavior;
import static io.trino.plugin.hive.HiveSessionProperties.getQueryPartitionFilterRequiredSchemas;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveSessionProperties.isBucketExecutionEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isCollectColumnStatisticsOnWrite;
import static io.trino.plugin.hive.HiveSessionProperties.isCreateEmptyBucketFiles;
import static io.trino.plugin.hive.HiveSessionProperties.isDelegateTransactionalManagedTableLocationToMetastore;
import static io.trino.plugin.hive.HiveSessionProperties.isNonTransactionalOptimizeEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isOptimizedMismatchedBucketCount;
import static io.trino.plugin.hive.HiveSessionProperties.isParallelPartitionedBucketedWrites;
import static io.trino.plugin.hive.HiveSessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isPropagateTableScanSortingProperties;
import static io.trino.plugin.hive.HiveSessionProperties.isQueryPartitionFilterRequired;
import static io.trino.plugin.hive.HiveSessionProperties.isRespectTableFormat;
import static io.trino.plugin.hive.HiveSessionProperties.isSortedWritingEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isStatisticsEnabled;
import static io.trino.plugin.hive.HiveTableProperties.AUTO_PURGE;
import static io.trino.plugin.hive.HiveTableProperties.AVRO_SCHEMA_LITERAL;
import static io.trino.plugin.hive.HiveTableProperties.AVRO_SCHEMA_URL;
import static io.trino.plugin.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.CSV_ESCAPE;
import static io.trino.plugin.hive.HiveTableProperties.CSV_QUOTE;
import static io.trino.plugin.hive.HiveTableProperties.CSV_SEPARATOR;
import static io.trino.plugin.hive.HiveTableProperties.EXTERNAL_LOCATION_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.NULL_FORMAT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.ORC_BLOOM_FILTER_COLUMNS;
import static io.trino.plugin.hive.HiveTableProperties.ORC_BLOOM_FILTER_FPP;
import static io.trino.plugin.hive.HiveTableProperties.PARQUET_BLOOM_FILTER_COLUMNS;
import static io.trino.plugin.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.REGEX_CASE_INSENSITIVE;
import static io.trino.plugin.hive.HiveTableProperties.REGEX_PATTERN;
import static io.trino.plugin.hive.HiveTableProperties.SKIP_FOOTER_LINE_COUNT;
import static io.trino.plugin.hive.HiveTableProperties.SKIP_HEADER_LINE_COUNT;
import static io.trino.plugin.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.TEXTFILE_FIELD_SEPARATOR;
import static io.trino.plugin.hive.HiveTableProperties.TEXTFILE_FIELD_SEPARATOR_ESCAPE;
import static io.trino.plugin.hive.HiveTableProperties.getAvroSchemaLiteral;
import static io.trino.plugin.hive.HiveTableProperties.getAvroSchemaUrl;
import static io.trino.plugin.hive.HiveTableProperties.getBucketInfo;
import static io.trino.plugin.hive.HiveTableProperties.getExternalLocation;
import static io.trino.plugin.hive.HiveTableProperties.getExtraProperties;
import static io.trino.plugin.hive.HiveTableProperties.getFooterSkipCount;
import static io.trino.plugin.hive.HiveTableProperties.getHeaderSkipCount;
import static io.trino.plugin.hive.HiveTableProperties.getHiveStorageFormat;
import static io.trino.plugin.hive.HiveTableProperties.getNullFormat;
import static io.trino.plugin.hive.HiveTableProperties.getOrcBloomFilterColumns;
import static io.trino.plugin.hive.HiveTableProperties.getOrcBloomFilterFpp;
import static io.trino.plugin.hive.HiveTableProperties.getParquetBloomFilterColumns;
import static io.trino.plugin.hive.HiveTableProperties.getPartitionedBy;
import static io.trino.plugin.hive.HiveTableProperties.getRegexPattern;
import static io.trino.plugin.hive.HiveTableProperties.getSingleCharacterProperty;
import static io.trino.plugin.hive.HiveTableProperties.isRegexCaseInsensitive;
import static io.trino.plugin.hive.HiveTableProperties.isTransactional;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.plugin.hive.HiveWritableTableHandle.BucketInfo.createBucketInfo;
import static io.trino.plugin.hive.HiveWriterFactory.computeNonTransactionalBucketedFilename;
import static io.trino.plugin.hive.HiveWriterFactory.computeTransactionalBucketedFilename;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static io.trino.plugin.hive.PartitionUpdate.UpdateMode.APPEND;
import static io.trino.plugin.hive.PartitionUpdate.UpdateMode.NEW;
import static io.trino.plugin.hive.PartitionUpdate.UpdateMode.OVERWRITE;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.ViewReaderUtil.createViewReader;
import static io.trino.plugin.hive.ViewReaderUtil.encodeViewData;
import static io.trino.plugin.hive.ViewReaderUtil.isHiveView;
import static io.trino.plugin.hive.ViewReaderUtil.isSomeKindOfAView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoMaterializedView;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoView;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.acid.AcidTransaction.forCreateTable;
import static io.trino.plugin.hive.metastore.MetastoreUtil.STATS_PROPERTIES;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getProtectMode;
import static io.trino.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.verifyOnline;
import static io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore.PartitionUpdateInfo;
import static io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore.cleanExtraOutputFiles;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getSupportedColumnStatistics;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.arePartitionProjectionPropertiesSet;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.getPartitionProjectionHiveTableProperties;
import static io.trino.plugin.hive.projection.PartitionProjectionProperties.getPartitionProjectionTrinoTableProperties;
import static io.trino.plugin.hive.util.AcidTables.deltaSubdir;
import static io.trino.plugin.hive.util.AcidTables.isFullAcidTable;
import static io.trino.plugin.hive.util.AcidTables.isTransactionalTable;
import static io.trino.plugin.hive.util.AcidTables.writeAcidVersionFile;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V2;
import static io.trino.plugin.hive.util.HiveBucketing.getBucketingVersion;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBucketHandle;
import static io.trino.plugin.hive.util.HiveBucketing.isSupportedBucketing;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.plugin.hive.util.HiveTypeUtil.getHiveDereferenceNames;
import static io.trino.plugin.hive.util.HiveTypeUtil.getHiveTypeForDereferences;
import static io.trino.plugin.hive.util.HiveTypeUtil.getType;
import static io.trino.plugin.hive.util.HiveTypeUtil.getTypeSignature;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.getRegularColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.getTableColumnMetadata;
import static io.trino.plugin.hive.util.HiveUtil.hiveColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.hive.util.HiveUtil.isSparkBucketedTable;
import static io.trino.plugin.hive.util.HiveUtil.parsePartitionValue;
import static io.trino.plugin.hive.util.HiveUtil.verifyPartitionTypeSupported;
import static io.trino.plugin.hive.util.HiveWriteUtils.checkTableIsWritable;
import static io.trino.plugin.hive.util.HiveWriteUtils.createPartitionValues;
import static io.trino.plugin.hive.util.HiveWriteUtils.isFileCreatedByQuery;
import static io.trino.plugin.hive.util.HiveWriteUtils.isWritableType;
import static io.trino.plugin.hive.util.RetryDriver.retry;
import static io.trino.plugin.hive.util.Statistics.createComputedStatisticsToPartitionMap;
import static io.trino.plugin.hive.util.Statistics.createEmptyPartitionStatistics;
import static io.trino.plugin.hive.util.Statistics.fromComputedStatistics;
import static io.trino.plugin.hive.util.SystemTables.getSourceTableNameFromSystemTable;
import static io.trino.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_VIEW_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.connector.SaveMode.REPLACE;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public class HiveMetadata
        implements TransactionalMetadata
{
    private static final Logger log = Logger.get(HiveMetadata.class);

    public static final String TRINO_VERSION_NAME = "trino_version";
    public static final String TRINO_CREATED_BY = "trino_created_by";
    public static final String TRINO_QUERY_ID_NAME = "trino_query_id";
    private static final String BUCKETING_VERSION = "bucketing_version";
    public static final String STORAGE_TABLE = "storage_table";
    public static final String TRANSACTIONAL = "transactional";
    public static final String PRESTO_VIEW_EXPANDED_TEXT_MARKER = "/* Presto View */";

    public static final String ORC_BLOOM_FILTER_COLUMNS_KEY = "orc.bloom.filter.columns";
    public static final String ORC_BLOOM_FILTER_FPP_KEY = "orc.bloom.filter.fpp";

    public static final String SKIP_HEADER_COUNT_KEY = SerdeConstants.HEADER_COUNT;
    public static final String SKIP_FOOTER_COUNT_KEY = SerdeConstants.FOOTER_COUNT;

    private static final String TEXT_FIELD_SEPARATOR_KEY = SerdeConstants.FIELD_DELIM;
    private static final String TEXT_FIELD_SEPARATOR_ESCAPE_KEY = SerdeConstants.ESCAPE_CHAR;
    private static final String NULL_FORMAT_KEY = SerdeConstants.SERIALIZATION_NULL_FORMAT;

    public static final String AVRO_SCHEMA_URL_KEY = "avro.schema.url";
    public static final String AVRO_SCHEMA_LITERAL_KEY = "avro.schema.literal";

    private static final String CSV_SEPARATOR_KEY = "separatorChar";
    private static final String CSV_QUOTE_KEY = "quoteChar";
    private static final String CSV_ESCAPE_KEY = "escapeChar";

    public static final String PARQUET_BLOOM_FILTER_COLUMNS_KEY = "parquet.bloom.filter.columns";

    private static final String REGEX_KEY = "input.regex";
    private static final String REGEX_CASE_SENSITIVE_KEY = "input.regex.case.insensitive";

    private static final String AUTO_PURGE_KEY = "auto.purge";

    public static final String MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE = "Modifying Hive table rows is only supported for transactional tables";

    private final CatalogName catalogName;
    private final SemiTransactionalHiveMetastore metastore;
    private final boolean autoCommit;
    private final Set<HiveFileWriterFactory> fileWriterFactories;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final HivePartitionManager partitionManager;
    private final TypeManager typeManager;
    private final MetadataProvider metadataProvider;
    private final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final boolean translateHiveViews;
    private final boolean hiveViewsRunAsInvoker;
    private final boolean hideDeltaLakeTables;
    private final String trinoVersion;
    private final HiveStatisticsProvider hiveStatisticsProvider;
    private final HiveRedirectionsProvider hiveRedirectionsProvider;
    private final Set<SystemTableProvider> systemTableProviders;
    private final AccessControlMetadata accessControlMetadata;
    private final DirectoryLister directoryLister;
    private final boolean partitionProjectionEnabled;
    private final boolean allowTableRename;
    private final long maxPartitionDropsPerQuery;
    private final HiveTimestampPrecision hiveViewsTimestampPrecision;

    public HiveMetadata(
            CatalogName catalogName,
            SemiTransactionalHiveMetastore metastore,
            boolean autoCommit,
            Set<HiveFileWriterFactory> fileWriterFactories,
            TrinoFileSystemFactory fileSystemFactory,
            HivePartitionManager partitionManager,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean translateHiveViews,
            boolean hiveViewsRunAsInvoker,
            boolean hideDeltaLakeTables,
            TypeManager typeManager,
            MetadataProvider metadataProvider,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            String trinoVersion,
            HiveStatisticsProvider hiveStatisticsProvider,
            HiveRedirectionsProvider hiveRedirectionsProvider,
            Set<SystemTableProvider> systemTableProviders,
            AccessControlMetadata accessControlMetadata,
            DirectoryLister directoryLister,
            boolean partitionProjectionEnabled,
            boolean allowTableRename,
            long maxPartitionDropsPerQuery,
            HiveTimestampPrecision hiveViewsTimestampPrecision)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.autoCommit = autoCommit;
        this.fileWriterFactories = ImmutableSet.copyOf(requireNonNull(fileWriterFactories, "fileWriterFactories is null"));
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metadataProvider = requireNonNull(metadataProvider, "metadataProvider is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.translateHiveViews = translateHiveViews;
        this.hiveViewsRunAsInvoker = hiveViewsRunAsInvoker;
        this.hideDeltaLakeTables = hideDeltaLakeTables;
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.hiveStatisticsProvider = requireNonNull(hiveStatisticsProvider, "hiveStatisticsProvider is null");
        this.hiveRedirectionsProvider = requireNonNull(hiveRedirectionsProvider, "hiveRedirectionsProvider is null");
        this.systemTableProviders = requireNonNull(systemTableProviders, "systemTableProviders is null");
        this.accessControlMetadata = requireNonNull(accessControlMetadata, "accessControlMetadata is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.partitionProjectionEnabled = partitionProjectionEnabled;
        this.allowTableRename = allowTableRename;
        this.maxPartitionDropsPerQuery = maxPartitionDropsPerQuery;
        this.hiveViewsTimestampPrecision = requireNonNull(hiveViewsTimestampPrecision, "hiveViewsTimestampPrecision is null");
    }

    @Override
    public SemiTransactionalHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public DirectoryLister getDirectoryLister()
    {
        return directoryLister;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        if (!schemaName.equals(schemaName.toLowerCase(ENGLISH))) {
            // Currently, Trino schemas are always lowercase, so this one cannot exist (https://github.com/trinodb/trino/issues/17)
            // In fact, some metastores (e.g., Glue) store database names lowercase only, but accepted mixed-case on lookup, so we need to filter out here.
            return false;
        }
        if (isHiveSystemSchema(schemaName)) {
            return false;
        }
        return metastore.getDatabase(schemaName).isPresent();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases().stream()
                .filter(schemaName -> !isHiveSystemSchema(schemaName))
                .collect(toImmutableList());
    }

    @Override
    public HiveTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        requireNonNull(tableName, "tableName is null");
        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return null;
        }
        Table table = metastore
                .getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElse(null);

        if (table == null) {
            return null;
        }

        if (isSomeKindOfAView(table)) {
            return null;
        }
        if (isDeltaLakeTable(table)) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, format("Cannot query Delta Lake table '%s'", tableName));
        }
        if (isIcebergTable(table)) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, format("Cannot query Iceberg table '%s'", tableName));
        }
        if (isHudiTable(table)) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, format("Cannot query Hudi table '%s'", tableName));
        }

        // we must not allow system tables due to how permissions are checked in SystemTableAwareAccessControl
        if (getSourceTableNameFromSystemTable(systemTableProviders, tableName).isPresent()) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Unexpected table present in Hive metastore: " + tableName);
        }

        verifyOnline(tableName, Optional.empty(), getProtectMode(table), table.getParameters());

        return new HiveTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getParameters(),
                getPartitionKeyColumnHandles(table, typeManager),
                getRegularColumnHandles(table, typeManager, getTimestampPrecision(session)),
                getHiveBucketHandle(session, table, typeManager));
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        Optional<List<List<String>>> partitionValuesList = getPartitionList(analyzeProperties);
        Optional<Set<String>> analyzeColumnNames = getColumnNames(analyzeProperties);

        List<String> partitionedBy = handle.getPartitionColumns().stream()
                .map(HiveColumnHandle::getName)
                .collect(toImmutableList());

        if (partitionValuesList.isPresent()) {
            List<List<String>> list = partitionValuesList.get();

            if (partitionedBy.isEmpty()) {
                throw new TrinoException(INVALID_ANALYZE_PROPERTY, "Partition list provided but table is not partitioned");
            }
            for (List<String> values : list) {
                if (values.size() != partitionedBy.size()) {
                    throw new TrinoException(INVALID_ANALYZE_PROPERTY, "Partition value count does not match partition column count");
                }
            }

            handle = handle.withAnalyzePartitionValues(list);
            HivePartitionResult partitions = partitionManager.getPartitions(handle, list);
            handle = partitionManager.applyPartitionResult(handle, partitions, alwaysTrue());
        }

        if (analyzeColumnNames.isPresent()) {
            Set<String> columnNames = analyzeColumnNames.get();
            Set<String> allColumnNames = ImmutableSet.<String>builder()
                    .addAll(handle.getDataColumns().stream()
                            .map(HiveColumnHandle::getName)
                            .collect(toImmutableSet()))
                    .addAll(partitionedBy)
                    .build();
            if (!allColumnNames.containsAll(columnNames)) {
                throw new TrinoException(
                        INVALID_ANALYZE_PROPERTY,
                        format("Invalid columns specified for analysis: %s", Sets.difference(columnNames, allColumnNames)));
            }
        }

        List<ColumnMetadata> columns = handle.getDataColumns().stream()
                .map(HiveColumnHandle::getColumnMetadata)
                .collect(toImmutableList());

        TableStatisticsMetadata metadata = getStatisticsCollectionMetadata(columns, partitionedBy, analyzeColumnNames, true);

        return new ConnectorAnalyzeMetadata(handle, metadata);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        for (SystemTableProvider systemTableProvider : systemTableProviders) {
            Optional<SystemTable> systemTable = systemTableProvider.getSystemTable(this, session, tableName);
            if (systemTable.isPresent()) {
                return systemTable;
            }
        }

        return Optional.empty();
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        return ((HiveTableHandle) table).getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        // This method does not calculate column metadata for the projected columns
        checkArgument(handle.getProjectedColumns().size() == handle.getPartitionColumns().size() + handle.getDataColumns().size(), "Unexpected projected columns");
        return getTableMetadata(session, handle.getSchemaTableName());
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            return doGetTableMetadata(session, tableName);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            // Errors related to invalid or unsupported information in the Metastore should be handled explicitly (e.g., as TrinoException(HIVE_INVALID_METADATA)).
            // This is just a catch-all solution so that we have any actionable information when e.g., SELECT * FROM information_schema.columns fails.
            throw new RuntimeException("Failed to construct table metadata for table " + tableName, e);
        }
    }

    private ConnectorTableMetadata doGetTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        if (isIcebergTable(table) || isDeltaLakeTable(table)) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, format("Not a Hive table '%s'", tableName));
        }

        boolean isTrinoView = isTrinoView(table);
        boolean isHiveView = isHiveView(table);
        boolean isTrinoMaterializedView = isTrinoMaterializedView(table);
        if (isHiveView && translateHiveViews) {
            // Produce metadata for a (translated) Hive view as if it was a table. This is incorrect from ConnectorMetadata.streamTableColumns
            // perspective, but is done on purpose to keep information_schema.columns working.
            // Because of fallback in ThriftHiveMetastoreClient.getAllViews, this method may return Trino/Presto views only,
            // so HiveMetadata.getViews may fail to return Hive views.
        }
        else if (isHiveView) {
            // When Hive view translation is not enabled, a Hive view is currently treated inconsistently
            //  - getView treats this as an unusable view (fails instead of returning Optional.empty)
            //  - getTableHandle treats this as a table (returns non-null)
            // In any case, returning metadata is not useful.
            throw new TableNotFoundException(tableName);
        }
        else if (isTrinoView || isTrinoMaterializedView) {
            // streamTableColumns should not include views and materialized views
            throw new TableNotFoundException(tableName);
        }

        List<ColumnMetadata> columns = getTableColumnMetadata(session, table, typeManager);

        // External location property
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (table.getTableType().equals(EXTERNAL_TABLE.name())) {
            properties.put(EXTERNAL_LOCATION_PROPERTY, table.getStorage().getLocation());
        }

        // Storage format property
        try {
            HiveStorageFormat format = extractHiveStorageFormat(table);
            properties.put(STORAGE_FORMAT_PROPERTY, format);
        }
        catch (TrinoException _) {
            // todo fail if format is not known
        }

        // Partitioning property
        List<String> partitionedBy = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        // Bucket properties
        table.getStorage().getBucketProperty().ifPresent(property -> {
            properties.put(BUCKETING_VERSION, getBucketingVersion(table.getParameters()).getVersion());
            properties.put(BUCKET_COUNT_PROPERTY, property.bucketCount());
            properties.put(BUCKETED_BY_PROPERTY, property.bucketedBy());
            properties.put(SORTED_BY_PROPERTY, property.sortedBy());
        });

        // Transactional properties
        String transactionalProperty = table.getParameters().get(TRANSACTIONAL);
        if (parseBoolean(transactionalProperty)) {
            properties.put(HiveTableProperties.TRANSACTIONAL, true);
        }

        // ORC format specific properties
        String orcBloomFilterColumns = table.getParameters().get(ORC_BLOOM_FILTER_COLUMNS_KEY);
        if (orcBloomFilterColumns != null) {
            properties.put(ORC_BLOOM_FILTER_COLUMNS, Splitter.on(',').trimResults().omitEmptyStrings().splitToList(orcBloomFilterColumns));
        }
        String orcBloomFilterFfp = table.getParameters().get(ORC_BLOOM_FILTER_FPP_KEY);
        if (orcBloomFilterFfp != null) {
            properties.put(ORC_BLOOM_FILTER_FPP, Double.parseDouble(orcBloomFilterFfp));
        }

        // Avro specific property
        String avroSchemaUrl = table.getParameters().get(AVRO_SCHEMA_URL_KEY);
        if (avroSchemaUrl != null) {
            properties.put(AVRO_SCHEMA_URL, avroSchemaUrl);
        }
        String avroSchemaLiteral = table.getParameters().get(AVRO_SCHEMA_LITERAL_KEY);
        if (avroSchemaLiteral != null) {
            properties.put(AVRO_SCHEMA_LITERAL, avroSchemaLiteral);
        }

        // Textfile and CSV specific properties
        getSerdeProperty(table, SKIP_HEADER_COUNT_KEY)
                .ifPresent(skipHeaderCount -> properties.put(SKIP_HEADER_LINE_COUNT, Integer.valueOf(skipHeaderCount)));
        getSerdeProperty(table, SKIP_FOOTER_COUNT_KEY)
                .ifPresent(skipFooterCount -> properties.put(SKIP_FOOTER_LINE_COUNT, Integer.valueOf(skipFooterCount)));

        // Multi-format property
        getSerdeProperty(table, NULL_FORMAT_KEY)
                .ifPresent(nullFormat -> properties.put(NULL_FORMAT_PROPERTY, nullFormat));

        // Textfile specific properties
        getSerdeProperty(table, TEXT_FIELD_SEPARATOR_KEY)
                .ifPresent(fieldSeparator -> properties.put(TEXTFILE_FIELD_SEPARATOR, fieldSeparator));
        getSerdeProperty(table, TEXT_FIELD_SEPARATOR_ESCAPE_KEY)
                .ifPresent(fieldEscape -> properties.put(TEXTFILE_FIELD_SEPARATOR_ESCAPE, fieldEscape));

        // CSV specific properties
        getCsvSerdeProperty(table, CSV_SEPARATOR_KEY)
                .ifPresent(csvSeparator -> properties.put(CSV_SEPARATOR, csvSeparator));
        getCsvSerdeProperty(table, CSV_QUOTE_KEY)
                .ifPresent(csvQuote -> properties.put(CSV_QUOTE, csvQuote));
        getCsvSerdeProperty(table, CSV_ESCAPE_KEY)
                .ifPresent(csvEscape -> properties.put(CSV_ESCAPE, csvEscape));

        // REGEX specific properties
        getSerdeProperty(table, REGEX_KEY)
                .ifPresent(regex -> properties.put(REGEX_PATTERN, regex));
        getSerdeProperty(table, REGEX_CASE_SENSITIVE_KEY)
                .ifPresent(regexCaseInsensitive -> properties.put(REGEX_CASE_INSENSITIVE, parseBoolean(regexCaseInsensitive)));

        Optional<String> comment = Optional.ofNullable(table.getParameters().get(Table.TABLE_COMMENT));

        String autoPurgeProperty = table.getParameters().get(AUTO_PURGE_KEY);
        if (parseBoolean(autoPurgeProperty)) {
            properties.put(AUTO_PURGE, true);
        }

        // Partition Projection specific properties
        properties.putAll(getPartitionProjectionTrinoTableProperties(table));

        return new ConnectorTableMetadata(tableName, columns, properties.buildOrThrow(), comment);
    }

    private static Optional<String> getCsvSerdeProperty(Table table, String key)
    {
        return getSerdeProperty(table, key).map(csvSerdeProperty -> csvSerdeProperty.substring(0, 1));
    }

    private static Optional<String> getSerdeProperty(Table table, String key)
    {
        String serdePropertyValue = table.getStorage().getSerdeParameters().get(key);
        String tablePropertyValue = table.getParameters().get(key);
        if (serdePropertyValue != null && tablePropertyValue != null && !tablePropertyValue.equals(serdePropertyValue)) {
            // in Hive one can set conflicting values for the same property, in such a case it looks like table properties are used
            throw new TrinoException(
                    HIVE_INVALID_METADATA,
                    format("Different values for '%s' set in serde properties and table properties: '%s' and '%s'", key, serdePropertyValue, tablePropertyValue));
        }
        return firstNonNullable(tablePropertyValue, serdePropertyValue);
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        List<String> partitionIds = hiveTableHandle.getPartitions()
                .map(partitions -> partitions.stream()
                        .map(HivePartition::getPartitionId)
                        .collect(toImmutableList()))
                .orElse(ImmutableList.of());

        Table table = metastore.getTable(hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(hiveTableHandle.getSchemaTableName()));
        Optional<String> tableDefaultFileFormat = HiveStorageFormat
                .getHiveStorageFormat(table.getStorage().getStorageFormat())
                .map(HiveStorageFormat::name);
        return Optional.of(new HiveInputInfo(
                partitionIds,
                !hiveTableHandle.getPartitionColumns().isEmpty(),
                tableDefaultFileFormat));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableSet.Builder<SchemaTableName> tableNames = ImmutableSet.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (TableInfo tableInfo : metastore.getTables(schemaName)) {
                tableNames.add(tableInfo.tableName());
            }
        }
        return tableNames.build().asList();
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableMap.Builder<SchemaTableName, RelationType> result = ImmutableMap.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (TableInfo tableInfo : metastore.getTables(schemaName)) {
                result.put(tableInfo.tableName(), tableInfo.extendedRelationType().toRelationType());
            }
        }
        return result.buildKeepingLast();
    }

    private List<String> listSchemas(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent()) {
            if (isHiveSystemSchema(schemaName.get())) {
                return ImmutableList.of();
            }
            return ImmutableList.of(schemaName.get());
        }
        return listSchemaNames(session);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = ((HiveTableHandle) tableHandle).getSchemaTableName();
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        return hiveColumnHandles(table, typeManager, getTimestampPrecision(session)).stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));
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
        return listTables(session, prefix).stream()
                .flatMap(tableName -> streamTableColumns(session, tableName))
                .iterator();
    }

    private Stream<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            if (redirectTable(session, tableName).isPresent()) {
                return Stream.of(TableColumnsMetadata.forRedirectedTable(tableName));
            }
            return Stream.of(TableColumnsMetadata.forTable(tableName, getTableMetadata(session, tableName).getColumns()));
        }
        catch (HiveViewNotSupportedException e) {
            // view is not supported
            return Stream.empty();
        }
        catch (TableNotFoundException e) {
            // it is not a table (e.g. it's a view) (TODO remove exception-driven logic for this case) OR
            // table disappeared during listing operation
            return Stream.empty();
        }
        catch (TrinoException e) {
            // Skip this table if there's a failure due to Hive, a bad Serde, or bad metadata
            if (e.getErrorCode().getType() == ErrorType.EXTERNAL) {
                return Stream.empty();
            }
            throw e;
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        Set<HiveColumnHandle> projectedColumns = hiveTableHandle.getProjectedColumns();
        // Return column statistics only for projectedColumns
        // plus, since column statistics are not supported for non-primitive types in hive, filter those out
        Map<String, ColumnHandle> columns = projectedColumns.stream()
                .filter(entry -> !entry.isHidden() && entry.getHiveType().getCategory() == PRIMITIVE)
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));

        Map<String, Type> columnTypes = columns.entrySet().stream()
                .collect(toImmutableMap(Entry::getKey, entry -> getColumnMetadata(session, tableHandle, entry.getValue()).getType()));
        HivePartitionResult partitionResult = partitionManager.getPartitions(metastore, tableHandle, new Constraint(hiveTableHandle.getEnforcedConstraint()));
        // If partitions are not loaded, then don't generate table statistics.
        // Note that the computation is not persisted in the table handle, so can be redone many times
        // TODO: https://github.com/trinodb/trino/issues/10980.
        Optional<List<HivePartition>> partitions = partitionManager.tryLoadPartitions(partitionResult);
        if (partitions.isPresent()) {
            return hiveStatisticsProvider.getTableStatistics(session, hiveTableHandle.getSchemaTableName(), columns, columnTypes, partitions.get());
        }
        return TableStatistics.empty();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchema().map(HiveUtil::isHiveSystemSchema).orElse(false)) {
            return ImmutableList.of();
        }
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        SchemaTableName tableName = prefix.toSchemaTableName();

        Optional<Table> optionalTable;
        try {
            optionalTable = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        }
        catch (HiveViewNotSupportedException e) {
            // exists, would be returned by listTables from schema
            return ImmutableList.of(tableName);
        }
        return optionalTable
                .filter(table -> !hideDeltaLakeTables || !isDeltaLakeTable(table))
                .map(table -> ImmutableList.of(tableName))
                .orElseGet(ImmutableList::of);
    }

    /**
     * NOTE: This method does not return column comment
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((HiveColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        Optional<String> location = HiveSchemaProperties.getLocation(properties).map(locationUri -> {
            try {
                fileSystemFactory.create(session).directoryExists(Location.of(locationUri));
            }
            catch (IOException | IllegalArgumentException e) {
                throw new TrinoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + locationUri, e);
            }
            return locationUri;
        });

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setLocation(location)
                .setOwnerType(accessControlMetadata.isUsingSystemSecurity() ? Optional.empty() : Optional.of(owner.getType()))
                .setOwnerName(accessControlMetadata.isUsingSystemSecurity() ? Optional.empty() : Optional.of(owner.getName()))
                .setParameters(ImmutableMap.of(TRINO_QUERY_ID_NAME, session.getQueryId()))
                .build();

        metastore.createDatabase(session, database);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (cascade) {
            // List all objects first because such operations after adding/dropping/altering tables/views in a transaction is disallowed
            List<SchemaTableName> views = listViews(session, Optional.of(schemaName));
            List<SchemaTableName> tables = listTables(session, Optional.of(schemaName)).stream()
                    .filter(table -> !views.contains(table))
                    .collect(toImmutableList());

            for (SchemaTableName viewName : views) {
                dropView(session, viewName);
            }

            for (SchemaTableName tableName : tables) {
                ConnectorTableHandle table = getTableHandle(session, tableName, Optional.empty(), Optional.empty());
                if (table == null) {
                    log.debug("Table disappeared during DROP SCHEMA CASCADE: %s", tableName);
                    continue;
                }
                dropTable(session, table);
            }

            // Commit and then drop the database with raw metastore because exclusive operation after dropping object is disallowed in SemiTransactionalHiveMetastore
            metastore.commit();
            boolean deleteData = metastore.shouldDeleteDatabaseData(session, schemaName);
            metastore.unsafeGetRawHiveMetastore().dropDatabase(schemaName, deleteData);
        }
        else {
            metastore.dropDatabase(session, schemaName);
        }
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        metastore.renameDatabase(source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String schemaName, TrinoPrincipal principal)
    {
        metastore.setDatabaseOwner(schemaName, HivePrincipal.from(principal));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        if (saveMode == REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<BucketInfo> bucketInfo = getBucketInfo(tableMetadata.getProperties());
        boolean isTransactional = isTransactional(tableMetadata.getProperties()).orElse(false);

        if (bucketInfo.isPresent() && getAvroSchemaUrl(tableMetadata.getProperties()) != null) {
            throw new TrinoException(NOT_SUPPORTED, "Bucketing columns not supported when Avro schema url is set");
        }

        if (bucketInfo.isPresent() && getAvroSchemaLiteral(tableMetadata.getProperties()) != null) {
            throw new TrinoException(NOT_SUPPORTED, "Bucketing/Partitioning columns not supported when Avro schema literal is set");
        }

        if (isTransactional) {
            metastore.checkSupportsHiveAcidTransactions();
        }

        validateTimestampColumns(tableMetadata.getColumns(), getTimestampPrecision(session));
        List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy));
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        Map<String, String> tableProperties = getEmptyTableProperties(tableMetadata, bucketInfo, session);

        hiveStorageFormat.validateColumns(columnHandles);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<HiveColumnHandle> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .collect(toImmutableList());
        checkPartitionTypesSupported(partitionColumns);

        Optional<Location> targetPath;
        boolean external;
        String externalLocation = getExternalLocation(tableMetadata.getProperties());
        if (externalLocation != null) {
            if (!createsOfNonManagedTablesEnabled) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot create non-managed Hive table");
            }

            external = true;
            targetPath = Optional.of(getValidatedExternalLocation(externalLocation));
            checkExternalPathAndCreateIfNotExists(session, targetPath.get());
        }
        else {
            external = false;
            if (isTransactional && isDelegateTransactionalManagedTableLocationToMetastore(session)) {
                targetPath = Optional.empty();
            }
            else {
                targetPath = Optional.of(locationService.forNewTable(metastore, session, schemaName, tableName));
            }
        }

        Table table = buildTableObject(
                session.getQueryId(),
                schemaName,
                tableName,
                session.getUser(),
                columnHandles,
                hiveStorageFormat,
                partitionedBy,
                bucketInfo,
                tableProperties,
                targetPath,
                external,
                trinoVersion,
                accessControlMetadata.isUsingSystemSecurity());
        PrincipalPrivileges principalPrivileges = accessControlMetadata.isUsingSystemSecurity() ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());
        HiveBasicStatistics basicStatistics = (!external && table.getPartitionColumns().isEmpty()) ? createZeroStatistics() : createEmptyStatistics();
        metastore.createTable(
                session,
                table,
                principalPrivileges,
                Optional.empty(),
                Optional.empty(),
                saveMode == SaveMode.IGNORE,
                new PartitionStatistics(basicStatistics, ImmutableMap.of()),
                false);
    }

    private Map<String, String> getEmptyTableProperties(ConnectorTableMetadata tableMetadata, Optional<BucketInfo> bucketInfo, ConnectorSession session)
    {
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();

        // When metastore is configured with metastore.create.as.acid=true, it will also change Trino-created tables
        // behind the scenes. In particular, this won't work with CTAS.
        boolean transactional = isTransactional(tableMetadata.getProperties()).orElse(false);
        tableProperties.put(TRANSACTIONAL, String.valueOf(transactional));

        boolean autoPurgeEnabled = HiveTableProperties.isAutoPurge(tableMetadata.getProperties()).orElse(false);
        tableProperties.put(AUTO_PURGE_KEY, String.valueOf(autoPurgeEnabled));

        bucketInfo.ifPresent(info -> tableProperties.put(BUCKETING_VERSION, String.valueOf(info.bucketingVersion().getVersion())));

        // ORC format specific properties
        List<String> orcBloomFilterColumns = getOrcBloomFilterColumns(tableMetadata.getProperties());
        if (orcBloomFilterColumns != null && !orcBloomFilterColumns.isEmpty()) {
            checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.ORC, ORC_BLOOM_FILTER_COLUMNS);
            validateOrcBloomFilterColumns(tableMetadata, orcBloomFilterColumns);
            tableProperties.put(ORC_BLOOM_FILTER_COLUMNS_KEY, Joiner.on(",").join(orcBloomFilterColumns));
            tableProperties.put(ORC_BLOOM_FILTER_FPP_KEY, String.valueOf(getOrcBloomFilterFpp(tableMetadata.getProperties())));
        }

        List<String> parquetBloomFilterColumns = getParquetBloomFilterColumns(tableMetadata.getProperties());
        if (parquetBloomFilterColumns != null && !parquetBloomFilterColumns.isEmpty()) {
            checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.PARQUET, PARQUET_BLOOM_FILTER_COLUMNS);
            validateParquetBloomFilterColumns(tableMetadata, parquetBloomFilterColumns);
            tableProperties.put(PARQUET_BLOOM_FILTER_COLUMNS_KEY, Joiner.on(",").join(parquetBloomFilterColumns));
            // TODO: Enable specifying FPP
        }

        // Avro specific properties
        String avroSchemaUrl = getAvroSchemaUrl(tableMetadata.getProperties());
        String avroSchemaLiteral = getAvroSchemaLiteral(tableMetadata.getProperties());
        checkAvroSchemaProperties(avroSchemaUrl, avroSchemaLiteral);
        if (avroSchemaUrl != null) {
            checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.AVRO, AVRO_SCHEMA_URL);
            tableProperties.put(AVRO_SCHEMA_URL_KEY, validateAvroSchemaUrl(session, avroSchemaUrl));
        }
        else if (avroSchemaLiteral != null) {
            checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.AVRO, AVRO_SCHEMA_LITERAL);
            tableProperties.put(AVRO_SCHEMA_LITERAL_KEY, validateAvroSchemaLiteral(avroSchemaLiteral));
        }

        // Textfile and CSV specific properties
        Set<HiveStorageFormat> csvAndTextFile = ImmutableSet.of(HiveStorageFormat.TEXTFILE, HiveStorageFormat.CSV);
        getHeaderSkipCount(tableMetadata.getProperties()).ifPresent(headerSkipCount -> {
            if (headerSkipCount > 0) {
                checkFormatForProperty(hiveStorageFormat, csvAndTextFile, SKIP_HEADER_LINE_COUNT);
                tableProperties.put(SKIP_HEADER_COUNT_KEY, String.valueOf(headerSkipCount));
            }
            if (headerSkipCount < 0) {
                throw new TrinoException(HIVE_INVALID_METADATA, format("Invalid value for %s property: %s", SKIP_HEADER_LINE_COUNT, headerSkipCount));
            }
        });

        getFooterSkipCount(tableMetadata.getProperties()).ifPresent(footerSkipCount -> {
            if (footerSkipCount > 0) {
                checkFormatForProperty(hiveStorageFormat, csvAndTextFile, SKIP_FOOTER_LINE_COUNT);
                tableProperties.put(SKIP_FOOTER_COUNT_KEY, String.valueOf(footerSkipCount));
            }
            if (footerSkipCount < 0) {
                throw new TrinoException(HIVE_INVALID_METADATA, format("Invalid value for %s property: %s", SKIP_FOOTER_LINE_COUNT, footerSkipCount));
            }
        });

        // null_format is allowed in textfile, rctext, and sequencefile
        Set<HiveStorageFormat> allowsNullFormat = ImmutableSet.of(
                HiveStorageFormat.TEXTFILE, HiveStorageFormat.RCTEXT, HiveStorageFormat.SEQUENCEFILE);
        getNullFormat(tableMetadata.getProperties())
                .ifPresent(format -> {
                    checkFormatForProperty(hiveStorageFormat, allowsNullFormat, NULL_FORMAT_PROPERTY);
                    tableProperties.put(NULL_FORMAT_KEY, format);
                });

        // Textfile-specific properties
        getSingleCharacterProperty(tableMetadata.getProperties(), TEXTFILE_FIELD_SEPARATOR)
                .ifPresent(separator -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.TEXTFILE, TEXT_FIELD_SEPARATOR_KEY);
                    tableProperties.put(TEXT_FIELD_SEPARATOR_KEY, separator.toString());
                });

        getSingleCharacterProperty(tableMetadata.getProperties(), TEXTFILE_FIELD_SEPARATOR_ESCAPE)
                .ifPresent(escape -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.TEXTFILE, TEXT_FIELD_SEPARATOR_ESCAPE_KEY);
                    tableProperties.put(TEXT_FIELD_SEPARATOR_ESCAPE_KEY, escape.toString());
                });

        // CSV specific properties
        getSingleCharacterProperty(tableMetadata.getProperties(), CSV_ESCAPE)
                .ifPresent(escape -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.CSV, CSV_ESCAPE);
                    tableProperties.put(CSV_ESCAPE_KEY, escape.toString());
                });
        getSingleCharacterProperty(tableMetadata.getProperties(), CSV_QUOTE)
                .ifPresent(quote -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.CSV, CSV_QUOTE);
                    tableProperties.put(CSV_QUOTE_KEY, quote.toString());
                });
        getSingleCharacterProperty(tableMetadata.getProperties(), CSV_SEPARATOR)
                .ifPresent(separator -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.CSV, CSV_SEPARATOR);
                    tableProperties.put(CSV_SEPARATOR_KEY, separator.toString());
                });

        // REGEX specific properties
        getRegexPattern(tableMetadata.getProperties())
                .ifPresentOrElse(
                        regexPattern -> {
                            checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.REGEX, REGEX_PATTERN);
                            try {
                                Pattern.compile(regexPattern);
                            }
                            catch (PatternSyntaxException e) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, "Invalid REGEX pattern value: " + regexPattern);
                            }
                            tableProperties.put(REGEX_KEY, regexPattern);
                        },
                        () -> {
                            if (hiveStorageFormat == HiveStorageFormat.REGEX) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, format("REGEX format requires the '%s' table property", REGEX_PATTERN));
                            }
                        });
        isRegexCaseInsensitive(tableMetadata.getProperties())
                .ifPresent(regexCaseInsensitive -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.REGEX, REGEX_CASE_INSENSITIVE);
                    tableProperties.put(REGEX_CASE_SENSITIVE_KEY, String.valueOf(regexCaseInsensitive));
                });

        // Set bogus table stats to prevent Hive 2.x from gathering these stats at table creation.
        // These stats are not useful by themselves and can take a very long time to collect when creating an
        // external table over a large data set.
        tableProperties.put("numFiles", "-1");
        tableProperties.put("totalSize", "-1");

        // Table comment property
        tableMetadata.getComment().ifPresent(value -> tableProperties.put(Table.TABLE_COMMENT, value));

        // Partition Projection specific properties
        if (partitionProjectionEnabled) {
            tableProperties.putAll(getPartitionProjectionHiveTableProperties(tableMetadata));
        }
        else if (arePartitionProjectionPropertiesSet(tableMetadata)) {
            throw new TrinoException(
                    INVALID_COLUMN_PROPERTY,
                    "Partition projection is disabled. Enable it in configuration by setting " + HiveConfig.CONFIGURATION_HIVE_PARTITION_PROJECTION_ENABLED + "=true");
        }

        Map<String, String> baseProperties = tableProperties.buildOrThrow();

        // Extra properties
        Map<String, String> extraProperties = getExtraProperties(tableMetadata.getProperties())
                .orElseGet(ImmutableMap::of);
        Set<String> illegalExtraProperties = Sets.intersection(
                ImmutableSet.<String>builder()
                        .addAll(baseProperties.keySet())
                        .addAll(STATS_PROPERTIES)
                        .build(),
                extraProperties.keySet());
        if (!illegalExtraProperties.isEmpty()) {
            throw new TrinoException(
                    INVALID_TABLE_PROPERTY,
                    "Illegal keys in extra_properties: " + illegalExtraProperties);
        }

        return ImmutableMap.<String, String>builder()
                .putAll(baseProperties)
                .putAll(extraProperties)
                .buildOrThrow();
    }

    private static void checkFormatForProperty(HiveStorageFormat actualStorageFormat, HiveStorageFormat expectedStorageFormat, String propertyName)
    {
        if (actualStorageFormat != expectedStorageFormat) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Cannot specify %s table property for storage format: %s", propertyName, actualStorageFormat));
        }
    }

    private static void checkFormatForProperty(HiveStorageFormat actualStorageFormat, Set<HiveStorageFormat> expectedStorageFormats, String propertyName)
    {
        if (!expectedStorageFormats.contains(actualStorageFormat)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Cannot specify %s table property for storage format: %s", propertyName, actualStorageFormat));
        }
    }

    private static void validateOrcBloomFilterColumns(ConnectorTableMetadata tableMetadata, List<String> orcBloomFilterColumns)
    {
        Set<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());
        if (!allColumns.containsAll(orcBloomFilterColumns)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Orc bloom filter columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(orcBloomFilterColumns), allColumns)));
        }
    }

    private static void validateParquetBloomFilterColumns(ConnectorTableMetadata tableMetadata, List<String> parquetBloomFilterColumns)
    {
        Map<String, Type> columnTypes = tableMetadata.getColumns().stream()
                .collect(toImmutableMap(ColumnMetadata::getName, ColumnMetadata::getType));
        for (String column : parquetBloomFilterColumns) {
            Type type = columnTypes.get(column);
            if (type == null) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, format("Parquet Bloom filter column %s not present in schema", column));
            }
            if (!SUPPORTED_BLOOM_FILTER_TYPES.contains(type)) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, format("Parquet Bloom filter column %s has unsupported type %s", column, type.getDisplayName()));
            }
        }
    }

    private String validateAvroSchemaUrl(ConnectorSession session, String url)
    {
        try {
            Location location = Location.of(url);
            if (!fileSystemFactory.create(session).newInputFile(location).exists()) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Cannot locate Avro schema file: " + url);
            }
            return location.toString();
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Avro schema file is not a valid file system URI: " + url, e);
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Cannot open Avro schema file: " + url, e);
        }
    }

    private static void checkAvroSchemaProperties(String avroSchemaUrl, String avroSchemaLiteral)
    {
        if (avroSchemaUrl != null && avroSchemaLiteral != null) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "avro_schema_url and avro_schema_literal cannot both be set");
        }
    }

    private static String validateAvroSchemaLiteral(String avroSchemaLiteral)
    {
        try {
            new Schema.Parser().parse(avroSchemaLiteral);
            return avroSchemaLiteral;
        }
        catch (SchemaParseException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Failed to parse Avro schema: " + avroSchemaLiteral, e);
        }
    }

    private static Location getValidatedExternalLocation(String location)
    {
        Location validated;
        try {
            validated = Location.of(location);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "External location is not a valid file system URI: " + location, e);
        }

        // TODO (https://github.com/trinodb/trino/issues/17803) We cannot accept locations with double slash until all relevant Hive connector components are migrated off Hadoop Path.
        // Hadoop Path "normalizes location", e.g.:
        //  - removes double slashes (such locations are rejected),
        //  - removes trailing slash (such locations are accepted; foo/bar and foo/bar/ are treated as equivalent, and rejecting locations with trailing slash could pose UX issues)
        //  - replaces file:///<local-path> with file:/<local-path> (such locations are accepted).
        if (validated.path().contains("//")) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Unsupported location that cannot be internally represented: " + location);
        }

        return validated;
    }

    private void checkExternalPathAndCreateIfNotExists(ConnectorSession session, Location location)
    {
        try {
            if (!fileSystemFactory.create(session).directoryExists(location).orElse(true)) {
                if (writesToNonManagedTablesEnabled) {
                    createDirectory(session, location);
                }
                else {
                    throw new TrinoException(INVALID_TABLE_PROPERTY, "External location must be a directory: " + location);
                }
            }
        }
        catch (IOException | IllegalArgumentException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "External location is not a valid file system URI: " + location, e);
        }
    }

    private void createDirectory(ConnectorSession session, Location location)
    {
        try {
            fileSystemFactory.create(session).createDirectory(location);
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, e.getMessage());
        }
    }

    private static void checkPartitionTypesSupported(List<HiveColumnHandle> partitionColumns)
    {
        for (HiveColumnHandle partitionColumn : partitionColumns) {
            verifyPartitionTypeSupported(partitionColumn.getName(), partitionColumn.getType());
        }
    }

    private static Table buildTableObject(
            String queryId,
            String schemaName,
            String tableName,
            String tableOwner,
            List<HiveColumnHandle> columnHandles,
            HiveStorageFormat hiveStorageFormat,
            List<String> partitionedBy,
            Optional<BucketInfo> bucketInfo,
            Map<String, String> additionalTableParameters,
            Optional<Location> targetPath,
            boolean external,
            String trinoVersion,
            boolean usingSystemSecurity)
    {
        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(Objects::requireNonNull)
                .map(HiveColumnHandle::toMetastoreColumn)
                .collect(toImmutableList());

        Set<String> partitionColumnNames = ImmutableSet.copyOf(partitionedBy);

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : columnHandles) {
            String name = columnHandle.getName();
            HiveType type = columnHandle.getHiveType();
            if (!partitionColumnNames.contains(name)) {
                verify(!columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
                columns.add(new Column(name, type, columnHandle.getComment(), ImmutableMap.of()));
            }
            else {
                verify(columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
            }
        }

        ImmutableMap.Builder<String, String> tableParameters = ImmutableMap.<String, String>builder()
                .put(TRINO_VERSION_NAME, trinoVersion)
                .put(TRINO_QUERY_ID_NAME, queryId)
                .putAll(additionalTableParameters);

        if (external) {
            tableParameters.put("EXTERNAL", "TRUE");
        }

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(usingSystemSecurity ? Optional.empty() : Optional.of(tableOwner))
                .setTableType((external ? EXTERNAL_TABLE : MANAGED_TABLE).name())
                .setDataColumns(columns.build())
                .setPartitionColumns(partitionColumns)
                .setParameters(tableParameters.buildOrThrow());

        tableBuilder.getStorageBuilder()
                .setStorageFormat(hiveStorageFormat.toStorageFormat())
                .setBucketProperty(bucketInfo.map(info -> new HiveBucketProperty(info.bucketedBy(), info.bucketCount(), info.sortedBy())))
                .setLocation(targetPath.map(Object::toString));

        return tableBuilder.build();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(handle);

        metastore.addColumn(handle.getSchemaName(), handle.getTableName(), column.getName(), toHiveType(column.getType()), column.getComment());
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(hiveTableHandle);
        HiveColumnHandle sourceHandle = (HiveColumnHandle) source;

        metastore.renameColumn(hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), sourceHandle.getName(), target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(hiveTableHandle);
        HiveColumnHandle columnHandle = (HiveColumnHandle) column;

        metastore.dropColumn(hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), columnHandle.getName());
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName table, TrinoPrincipal principal)
    {
        metastore.setTableOwner(table.getSchemaName(), table.getTableName(), HivePrincipal.from(principal));
    }

    private void failIfAvroSchemaIsSet(HiveTableHandle handle)
    {
        Table table = metastore.getTable(handle.getSchemaName(), handle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));
        if (table.getParameters().containsKey(AVRO_SCHEMA_URL_KEY) || table.getStorage().getSerdeParameters().containsKey(AVRO_SCHEMA_URL_KEY)) {
            throw new TrinoException(NOT_SUPPORTED, "ALTER TABLE not supported when Avro schema url is set");
        }
        if (table.getParameters().containsKey(AVRO_SCHEMA_LITERAL_KEY) || table.getStorage().getSerdeParameters().containsKey(AVRO_SCHEMA_LITERAL_KEY)) {
            throw new TrinoException(NOT_SUPPORTED, "ALTER TABLE not supported when Avro schema literal is set");
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        if (!allowTableRename) {
            throw new TrinoException(NOT_SUPPORTED, "Table rename is not supported with current metastore configuration");
        }
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        metastore.renameTable(handle.getSchemaName(), handle.getTableName(), newTableName.getSchemaName(), newTableName.getTableName());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        metastore.commentTable(handle.getSchemaName(), handle.getTableName(), comment);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        Table view = getTrinoView(viewName);

        ConnectorViewDefinition definition = toConnectorViewDefinition(session, viewName, Optional.of(view))
                .orElseThrow(() -> new ViewNotFoundException(viewName));
        ConnectorViewDefinition newDefinition = new ConnectorViewDefinition(
                definition.getOriginalSql(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns(),
                comment,
                definition.getOwner(),
                definition.isRunAsInvoker(),
                definition.getPath());

        replaceView(session, viewName, view, newDefinition);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        Table view = getTrinoView(viewName);

        ConnectorViewDefinition definition = toConnectorViewDefinition(session, viewName, Optional.of(view))
                .orElseThrow(() -> new ViewNotFoundException(viewName));
        ConnectorViewDefinition newDefinition = new ConnectorViewDefinition(
                definition.getOriginalSql(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns().stream()
                        .map(currentViewColumn -> columnName.equals(currentViewColumn.getName()) ? new ConnectorViewDefinition.ViewColumn(currentViewColumn.getName(), currentViewColumn.getType(), comment) : currentViewColumn)
                        .collect(toImmutableList()),
                definition.getComment(),
                definition.getOwner(),
                definition.isRunAsInvoker(),
                definition.getPath());

        replaceView(session, viewName, view, newDefinition);
    }

    private Table getTrinoView(SchemaTableName viewName)
    {
        Table view = metastore.getTable(viewName.getSchemaName(), viewName.getTableName())
                .filter(table -> isTrinoView(table) || isHiveView(table))
                .orElseThrow(() -> new ViewNotFoundException(viewName));
        if (!isTrinoView(view)) {
            throw new HiveViewNotSupportedException(viewName);
        }
        return view;
    }

    private void replaceView(ConnectorSession session, SchemaTableName viewName, Table view, ConnectorViewDefinition newViewDefinition)
    {
        Table.Builder viewBuilder = Table.builder(view)
                .setViewOriginalText(Optional.of(encodeViewData(newViewDefinition)));

        PrincipalPrivileges principalPrivileges = accessControlMetadata.isUsingSystemSecurity() ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());

        metastore.replaceTable(viewName.getSchemaName(), viewName.getTableName(), viewBuilder.build(), principalPrivileges);
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        HiveColumnHandle columnHandle = (HiveColumnHandle) column;
        metastore.commentColumn(handle.getSchemaName(), handle.getTableName(), columnHandle.getName(), comment);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        if (metastore.getTable(handle.getSchemaName(), handle.getTableName()).isEmpty()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }
        metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        if (metastore.getTable(handle.getSchemaName(), handle.getTableName()).isEmpty()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        verify(isStatisticsEnabled(session), "statistics not enabled");

        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = handle.getSchemaTableName();
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        List<Column> partitionColumns = table.getPartitionColumns();
        List<String> partitionColumnNames = partitionColumns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
        HiveTimestampPrecision timestampPrecision = getTimestampPrecision(session);
        List<HiveColumnHandle> hiveColumnHandles = hiveColumnHandles(table, typeManager, timestampPrecision);
        Map<String, Type> columnTypes = hiveColumnHandles.stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> getType(column.getHiveType(), typeManager, timestampPrecision)));

        Map<List<String>, ComputedStatistics> computedStatisticsMap = createComputedStatisticsToPartitionMap(computedStatistics, partitionColumnNames, columnTypes);

        if (partitionColumns.isEmpty()) {
            // commit the analysis result to an unpartitioned table
            metastore.setTableStatistics(table, createPartitionStatistics(columnTypes, computedStatisticsMap.get(ImmutableList.<String>of())));
        }
        else {
            List<String> partitionNames;
            List<List<String>> partitionValuesList;
            if (handle.getAnalyzePartitionValues().isPresent()) {
                partitionValuesList = handle.getAnalyzePartitionValues().get();
                partitionNames = partitionValuesList.stream()
                        // TODO (https://github.com/trinodb/trino/issues/15998) fix selective ANALYZE for table with non-canonical partition values
                        .map(partitionValues -> makePartitionName(partitionColumns, partitionValues))
                        .collect(toImmutableList());
            }
            else {
                partitionNames = metastore.getPartitionNames(handle.getSchemaName(), handle.getTableName())
                        .orElseThrow(() -> new TableNotFoundException(tableName));
                partitionValuesList = partitionNames
                        .stream()
                        .map(Partition::toPartitionValues)
                        .collect(toImmutableList());
            }

            ImmutableMap.Builder<List<String>, PartitionStatistics> partitionStatistics = ImmutableMap.builder();
            Map<String, Set<HiveColumnStatisticType>> columnStatisticTypes = hiveColumnHandles.stream()
                    .filter(columnHandle -> !partitionColumnNames.contains(columnHandle.getName()))
                    .filter(column -> !column.isHidden())
                    .collect(toImmutableMap(HiveColumnHandle::getName, column -> getSupportedColumnStatistics(column.getType())));
            Supplier<PartitionStatistics> emptyPartitionStatistics = Suppliers.memoize(() -> createEmptyPartitionStatistics(columnTypes, columnStatisticTypes));

            List<Type> partitionTypes = handle.getPartitionColumns().stream()
                    .map(HiveColumnHandle::getType)
                    .collect(toImmutableList());

            for (int i = 0; i < partitionNames.size(); i++) {
                String partitionName = partitionNames.get(i);
                List<String> partitionValues = partitionValuesList.get(i);
                ComputedStatistics collectedStatistics = computedStatisticsMap.containsKey(partitionValues)
                        ? computedStatisticsMap.get(partitionValues)
                        : computedStatisticsMap.get(canonicalizePartitionValues(partitionName, partitionValues, partitionTypes));
                if (collectedStatistics == null) {
                    partitionStatistics.put(partitionValues, emptyPartitionStatistics.get());
                }
                else {
                    partitionStatistics.put(partitionValues, createPartitionStatistics(columnTypes, collectedStatistics));
                }
            }
            metastore.setPartitionStatistics(table, partitionStatistics.buildOrThrow());
        }
    }

    private static List<String> canonicalizePartitionValues(String partitionName, List<String> partitionValues, List<Type> partitionTypes)
    {
        verify(partitionValues.size() == partitionTypes.size(), "Expected partitionTypes size to be %s but got %s", partitionValues.size(), partitionTypes.size());
        Block[] parsedPartitionValuesBlocks = new Block[partitionValues.size()];
        for (int i = 0; i < partitionValues.size(); i++) {
            String partitionValue = partitionValues.get(i);
            Type partitionType = partitionTypes.get(i);
            parsedPartitionValuesBlocks[i] = parsePartitionValue(partitionName, partitionValue, partitionType).asBlock();
        }

        return createPartitionValues(partitionTypes, new Page(parsedPartitionValuesBlocks), 0);
    }

    @Override
    public HiveOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout,
            RetryMode retryMode,
            boolean replace)
    {
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }

        Optional<Location> externalLocation = Optional.ofNullable(getExternalLocation(tableMetadata.getProperties()))
                .map(HiveMetadata::getValidatedExternalLocation);
        if (!createsOfNonManagedTablesEnabled && externalLocation.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Creating non-managed Hive tables is disabled");
        }

        if (!writesToNonManagedTablesEnabled && externalLocation.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Writes to non-managed Hive tables is disabled");
        }

        boolean isTransactional = isTransactional(tableMetadata.getProperties()).orElse(false);
        if (isTransactional) {
            metastore.checkSupportsHiveAcidTransactions();
        }

        if (isTransactional && externalLocation.isEmpty() && isDelegateTransactionalManagedTableLocationToMetastore(session)) {
            throw new TrinoException(NOT_SUPPORTED, "CREATE TABLE AS is not supported for transactional tables without explicit location if location determining is delegated to metastore");
        }

        if (isTransactional && retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "CREATE TABLE AS is not supported for transactional tables with query retries enabled");
        }

        if (getAvroSchemaUrl(tableMetadata.getProperties()) != null) {
            throw new TrinoException(NOT_SUPPORTED, "CREATE TABLE AS not supported when Avro schema url is set");
        }

        if (getAvroSchemaLiteral(tableMetadata.getProperties()) != null) {
            throw new TrinoException(NOT_SUPPORTED, "CREATE TABLE AS not supported when Avro schema literal is set");
        }

        getHeaderSkipCount(tableMetadata.getProperties()).ifPresent(headerSkipCount -> {
            if (headerSkipCount > 1) {
                throw new TrinoException(NOT_SUPPORTED, format("Creating Hive table with data with value of %s property greater than 1 is not supported", SKIP_HEADER_COUNT_KEY));
            }
        });

        getFooterSkipCount(tableMetadata.getProperties()).ifPresent(footerSkipCount -> {
            if (footerSkipCount > 0) {
                throw new TrinoException(NOT_SUPPORTED, format("Creating Hive table with data with value of %s property greater than 0 is not supported", SKIP_FOOTER_COUNT_KEY));
            }
        });

        HiveStorageFormat tableStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<BucketInfo> bucketProperty = getBucketInfo(tableMetadata.getProperties());

        // get the root directory for the database
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Map<String, String> tableProperties = getEmptyTableProperties(tableMetadata, bucketProperty, session);
        List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy));
        HiveStorageFormat partitionStorageFormat = isRespectTableFormat(session) ? tableStorageFormat : getHiveStorageFormat(session);

        // unpartitioned tables ignore the partition storage format
        HiveStorageFormat actualStorageFormat = partitionedBy.isEmpty() ? tableStorageFormat : partitionStorageFormat;
        actualStorageFormat.validateColumns(columnHandles);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<HiveColumnHandle> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .collect(toImmutableList());
        checkPartitionTypesSupported(partitionColumns);

        LocationHandle locationHandle = locationService.forNewTableAsSelect(metastore, session, schemaName, tableName, externalLocation);

        AcidTransaction transaction = isTransactional ? forCreateTable() : NO_ACID_TRANSACTION;

        HiveOutputTableHandle result = new HiveOutputTableHandle(
                schemaName,
                tableName,
                columnHandles,
                metastore.generatePageSinkMetadata(schemaTableName),
                locationHandle,
                tableStorageFormat,
                partitionStorageFormat,
                partitionedBy,
                bucketProperty,
                session.getUser(),
                tableProperties,
                transaction,
                externalLocation.isPresent(),
                retryMode != NO_RETRIES);

        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.declareIntentionToWrite(session, writeInfo.writeMode(), writeInfo.writePath(), schemaTableName);

        return result;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveOutputTableHandle handle = (HiveOutputTableHandle) tableHandle;

        List<PartitionUpdate> partitionUpdates = fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .collect(toImmutableList());

        WriteInfo writeInfo = locationService.getQueryWriteInfo(handle.getLocationHandle());
        Table table = buildTableObject(
                session.getQueryId(),
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTableOwner(),
                handle.getInputColumns(),
                handle.getTableStorageFormat(),
                handle.getPartitionedBy(),
                handle.getBucketInfo(),
                handle.getAdditionalTableParameters(),
                Optional.of(writeInfo.targetPath()),
                handle.isExternal(),
                trinoVersion,
                accessControlMetadata.isUsingSystemSecurity());
        PrincipalPrivileges principalPrivileges = accessControlMetadata.isUsingSystemSecurity() ? NO_PRIVILEGES : buildInitialPrivilegeSet(handle.getTableOwner());

        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        if (handle.getBucketInfo().isPresent() && isCreateEmptyBucketFiles(session)) {
            List<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(session, handle, true, partitionUpdates);
            // replace partitionUpdates before creating the empty files so that those files will be cleaned up if we end up rollback
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.getPartitionColumns().isEmpty() ? Optional.empty() : Optional.of(buildPartitionObject(session, table, partitionUpdate));
                Location writePath = Location.of(partitionUpdate.getWritePath().toString());
                createEmptyFiles(session, writePath, table, partition, partitionUpdate.getFileNames());
            }
            if (handle.isTransactional()) {
                AcidTransaction transaction = handle.getTransaction();
                List<String> partitionNames = partitionUpdates.stream().map(PartitionUpdate::getName).collect(toImmutableList());
                metastore.addDynamicPartitions(
                        handle.getSchemaName(),
                        handle.getTableName(),
                        partitionNames,
                        transaction.getAcidTransactionId(),
                        transaction.getWriteId(),
                        AcidOperation.CREATE_TABLE);
            }
        }

        Map<String, Type> columnTypes = handle.getInputColumns().stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> typeManager.getType(getTypeSignature(column.getHiveType()))));
        Map<List<String>, ComputedStatistics> partitionComputedStatistics = createComputedStatisticsToPartitionMap(computedStatistics, handle.getPartitionedBy(), columnTypes);

        PartitionStatistics tableStatistics;
        if (table.getPartitionColumns().isEmpty()) {
            HiveBasicStatistics basicStatistics = partitionUpdates.stream()
                    .map(PartitionUpdate::getStatistics)
                    .map(hiveBasicStatistics -> new PartitionStatistics(hiveBasicStatistics, ImmutableMap.of()))
                    .reduce(MERGE_INCREMENTAL::updatePartitionStatistics)
                    .map(PartitionStatistics::basicStatistics)
                    .orElse(createZeroStatistics());
            tableStatistics = createPartitionStatistics(basicStatistics, columnTypes, getColumnStatistics(partitionComputedStatistics, ImmutableList.of()));
        }
        else {
            tableStatistics = new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        }

        Optional<Location> writePath = Optional.of(writeInfo.writePath());
        if (handle.getPartitionedBy().isEmpty()) {
            List<String> fileNames;
            if (partitionUpdates.isEmpty()) {
                // creating empty table via CTAS ... WITH NO DATA
                fileNames = ImmutableList.of();
            }
            else {
                fileNames = getOnlyElement(partitionUpdates).getFileNames();
            }
            metastore.createTable(session, table, principalPrivileges, writePath, Optional.of(fileNames), false, tableStatistics, handle.isRetriesEnabled());
        }
        else {
            metastore.createTable(session, table, principalPrivileges, writePath, Optional.empty(), false, tableStatistics, false);
        }

        if (!handle.getPartitionedBy().isEmpty()) {
            if (isRespectTableFormat(session)) {
                verify(handle.getPartitionStorageFormat() == handle.getTableStorageFormat());
            }
            for (PartitionUpdate update : partitionUpdates) {
                Partition partition = buildPartitionObject(session, table, update);
                List<String> canonicalPartitionValues = partition.getValues();
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        update.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, canonicalPartitionValues));
                metastore.addPartition(
                        session,
                        handle.getSchemaName(),
                        handle.getTableName(),
                        buildPartitionObject(session, table, update),
                        update.getWritePath(),
                        Optional.of(update.getFileNames()),
                        partitionStatistics,
                        handle.isRetriesEnabled());
            }
        }

        return Optional.of(new HiveWrittenPartitions(
                partitionUpdates.stream()
                        .map(PartitionUpdate::getName)
                        .collect(toImmutableList())));
    }

    private static List<PartitionUpdate> computePartitionUpdatesForMissingBuckets(
            ConnectorSession session,
            HiveWritableTableHandle handle,
            boolean isCreateTable,
            List<PartitionUpdate> partitionUpdates)
    {
        ImmutableList.Builder<PartitionUpdate> partitionUpdatesForMissingBucketsBuilder = ImmutableList.builder();
        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            int bucketCount = handle.getBucketInfo().orElseThrow().bucketCount();

            List<String> fileNamesForMissingBuckets = computeFileNamesForMissingBuckets(
                    session,
                    bucketCount,
                    isCreateTable && handle.isTransactional(),
                    partitionUpdate);
            partitionUpdatesForMissingBucketsBuilder.add(new PartitionUpdate(
                    partitionUpdate.getName(),
                    partitionUpdate.getUpdateMode(),
                    partitionUpdate.getWritePath(),
                    partitionUpdate.getTargetPath(),
                    fileNamesForMissingBuckets,
                    0,
                    0,
                    0));
        }
        return partitionUpdatesForMissingBucketsBuilder.build();
    }

    private static List<String> computeFileNamesForMissingBuckets(
            ConnectorSession session,
            int bucketCount,
            boolean transactionalCreateTable,
            PartitionUpdate partitionUpdate)
    {
        if (partitionUpdate.getFileNames().size() == bucketCount) {
            // fast path for the common case
            return ImmutableList.of();
        }

        Set<String> fileNames = ImmutableSet.copyOf(partitionUpdate.getFileNames());
        Set<Integer> bucketsWithFiles = fileNames.stream()
                .map(HiveWriterFactory::getBucketFromFileName)
                .collect(toImmutableSet());

        ImmutableList.Builder<String> missingFileNamesBuilder = ImmutableList.builder();
        for (int i = 0; i < bucketCount; i++) {
            if (bucketsWithFiles.contains(i)) {
                continue;
            }
            missingFileNamesBuilder.add(transactionalCreateTable
                    ? computeTransactionalBucketedFilename(i)
                    : computeNonTransactionalBucketedFilename(session.getQueryId(), i));
        }
        List<String> missingFileNames = missingFileNamesBuilder.build();
        verify(fileNames.size() + missingFileNames.size() == bucketCount);
        return missingFileNames;
    }

    private void createEmptyFiles(ConnectorSession session, Location path, Table table, Optional<Partition> partition, List<String> fileNames)
    {
        Map<String, String> schema;
        StorageFormat format;
        if (partition.isPresent()) {
            schema = getHiveSchema(partition.get(), table);
            format = partition.get().getStorage().getStorageFormat();
        }
        else {
            schema = getHiveSchema(table);
            format = table.getStorage().getStorageFormat();
        }

        for (String fileName : fileNames) {
            Location location = path.appendPath(fileName);
            fileWriterFactories.stream()
                    .map(factory -> factory.createFileWriter(
                            location,
                            ImmutableList.of(),
                            format,
                            HiveCompressionCodec.NONE,
                            schema,
                            session,
                            OptionalInt.empty(),
                            NO_ACID_TRANSACTION,
                            false,
                            WriterKind.INSERT))
                    .flatMap(Optional::stream)
                    .findFirst()
                    .orElseThrow(() -> new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Writing not supported for " + format))
                    .commit();
        }
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        Optional<Map<String, String>> properties = handle.getTableParameters();
        if (isTransactionalTable(properties.orElseThrow())) {
            return DELETE_ROW_AND_INSERT_ROW;
        }
        // TODO: At some point we should add detection to see if the metastore supports
        //  transactional tables and just say merge isn't supported in the HMS
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        if (!isFullAcidTable(table.getParameters())) {
            throw new TrinoException(NOT_SUPPORTED, MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
        }

        HiveInsertTableHandle insertHandle = beginInsertOrMerge(session, tableHandle, retryMode, "Merging into", true);
        return new HiveMergeTableHandle(hiveTableHandle.withTransaction(insertHandle.getTransaction()), insertHandle);
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveMergeTableHandle mergeHandle = (HiveMergeTableHandle) mergeTableHandle;
        HiveInsertTableHandle insertHandle = mergeHandle.getInsertHandle();
        HiveTableHandle handle = mergeHandle.getTableHandle();
        checkArgument(handle.isAcidMerge(), "handle should be a merge handle, but is %s", handle);

        requireNonNull(fragments, "fragments is null");
        List<PartitionUpdateAndMergeResults> partitionMergeResults = fragments.stream()
                .map(Slice::getBytes)
                .map(PartitionUpdateAndMergeResults.CODEC::fromJson)
                .collect(toImmutableList());

        List<PartitionUpdate> partitionUpdates = partitionMergeResults.stream()
                .map(PartitionUpdateAndMergeResults::partitionUpdate)
                .collect(toImmutableList());

        Table table = finishChangingTable(AcidOperation.MERGE, "merge", session, insertHandle, partitionUpdates, computedStatistics);

        for (PartitionUpdateAndMergeResults results : partitionMergeResults) {
            results.deltaDirectory().ifPresent(directory -> createOrcAcidVersionFile(session.getIdentity(), directory));
            results.deleteDeltaDirectory().ifPresent(directory -> createOrcAcidVersionFile(session.getIdentity(), directory));
        }

        List<Partition> partitions = partitionUpdates.stream()
                .filter(update -> !update.getName().isEmpty())
                .map(update -> buildPartitionObject(session, table, update))
                .collect(toImmutableList());

        LocationHandle locationHandle = locationService.forExistingTable(metastore, session, table);
        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.finishMerge(session, table.getDatabaseName(), table.getTableName(), writeInfo.writePath(), partitionMergeResults, partitions);
    }

    @Override
    public HiveInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        return beginInsertOrMerge(session, tableHandle, retryMode, "Inserting into", false);
    }

    private HiveInsertTableHandle beginInsertOrMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode, String description, boolean isForMerge)
    {
        SchemaTableName tableName = ((HiveTableHandle) tableHandle).getSchemaTableName();
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        checkTableIsWritable(table, writesToNonManagedTablesEnabled);

        for (Column column : table.getDataColumns()) {
            if (!isWritableType(column.getType())) {
                throw new TrinoException(NOT_SUPPORTED, format("%s Hive table %s with column type %s not supported", description, tableName, column.getType()));
            }
        }

        boolean isTransactional = isTransactionalTable(table.getParameters());
        if (isTransactional && retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, description + " Hive transactional tables is not supported with query retries enabled");
        }
        if (isTransactional && !autoCommit) {
            throw new TrinoException(NOT_SUPPORTED, description + " Hive transactional tables is not supported in explicit transactions (use autocommit mode)");
        }
        if (isSparkBucketedTable(table)) {
            throw new TrinoException(NOT_SUPPORTED, description + " Spark bucketed tables is not supported");
        }

        List<HiveColumnHandle> handles = hiveColumnHandles(table, typeManager, getTimestampPrecision(session)).stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toImmutableList());

        HiveStorageFormat tableStorageFormat = extractHiveStorageFormat(table);
        Optional.ofNullable(table.getParameters().get(SKIP_HEADER_COUNT_KEY)).map(Integer::parseInt).ifPresent(headerSkipCount -> {
            if (headerSkipCount > 1) {
                throw new TrinoException(NOT_SUPPORTED, format("%s Hive table with value of %s property greater than 1 is not supported", description, SKIP_HEADER_COUNT_KEY));
            }
        });
        if (table.getParameters().containsKey(SKIP_FOOTER_COUNT_KEY)) {
            throw new TrinoException(NOT_SUPPORTED, format("%s Hive table with %s property not supported", description, SKIP_FOOTER_COUNT_KEY));
        }
        LocationHandle locationHandle = locationService.forExistingTable(metastore, session, table);

        AcidTransaction transaction = NO_ACID_TRANSACTION;
        if (isForMerge) {
            checkArgument(isTransactional, "The target table in Hive MERGE must be a transactional table");
            transaction = metastore.beginMerge(session, table);
        }
        else if (isTransactional) {
            transaction = metastore.beginInsert(session, table);
        }
        HiveInsertTableHandle result = new HiveInsertTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                handles,
                metastore.generatePageSinkMetadata(tableName),
                locationHandle,
                createBucketInfo(table),
                tableStorageFormat,
                isRespectTableFormat(session) ? tableStorageFormat : getHiveStorageFormat(session),
                transaction,
                retryMode != NO_RETRIES);

        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        if (getInsertExistingPartitionsBehavior(session) == InsertExistingPartitionsBehavior.OVERWRITE
                && writeInfo.writeMode() == DIRECT_TO_TARGET_EXISTING_DIRECTORY) {
            if (isTransactional) {
                throw new TrinoException(NOT_SUPPORTED, "Overwriting existing partition in transactional tables doesn't support DIRECT_TO_TARGET_EXISTING_DIRECTORY write mode");
            }
            // This check is required to prevent using partition overwrite operation during the user-managed transactions
            // Partition overwrite operation is nonatomic thus can't and shouldn't be used in non autocommit context.
            if (!autoCommit) {
                throw new TrinoException(NOT_SUPPORTED, "Overwriting existing partition in non auto commit context doesn't support DIRECT_TO_TARGET_EXISTING_DIRECTORY write mode");
            }
        }
        metastore.declareIntentionToWrite(session, writeInfo.writeMode(), writeInfo.writePath(), tableName);
        return result;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        HiveInsertTableHandle handle = (HiveInsertTableHandle) insertHandle;

        List<PartitionUpdate> partitionUpdates = fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .collect(toImmutableList());

        Table table = finishChangingTable(AcidOperation.INSERT, "insert", session, handle, partitionUpdates, computedStatistics);

        if (isFullAcidTable(table.getParameters())) {
            for (PartitionUpdate update : partitionUpdates) {
                String deltaSubdir = deltaSubdir(handle.getTransaction().getWriteId(), 0);
                String directory = table.getStorage().getLocation();
                if (!update.getName().isEmpty()) {
                    directory += "/" + update.getName();
                }
                directory += "/" + deltaSubdir;
                createOrcAcidVersionFile(session.getIdentity(), directory);
            }
        }

        return Optional.of(new HiveWrittenPartitions(
                partitionUpdates.stream()
                        .map(PartitionUpdate::getName)
                        .collect(toImmutableList())));
    }

    private Table finishChangingTable(AcidOperation acidOperation, String changeDescription, ConnectorSession session, HiveInsertTableHandle handle, List<PartitionUpdate> partitionUpdates, Collection<ComputedStatistics> computedStatistics)
    {
        HiveStorageFormat tableStorageFormat = handle.getTableStorageFormat();
        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        Table table = metastore.getTable(handle.getSchemaName(), handle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));
        if (!table.getStorage().getStorageFormat().getInputFormat().equals(tableStorageFormat.getInputFormat()) && isRespectTableFormat(session)) {
            throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during " + changeDescription);
        }

        if (handle.getBucketInfo().isPresent() && isCreateEmptyBucketFiles(session)) {
            List<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(session, handle, false, partitionUpdates);
            // replace partitionUpdates before creating the empty files so that those files will be cleaned up if we end up rollback
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.getPartitionColumns().isEmpty() ? Optional.empty() : Optional.of(buildPartitionObject(session, table, partitionUpdate));
                if (handle.isTransactional() && partition.isPresent()) {
                    PartitionStatistics statistics = PartitionStatistics.builder().setBasicStatistics(partitionUpdate.getStatistics()).build();
                    metastore.addPartition(
                            session,
                            handle.getSchemaName(),
                            handle.getTableName(),
                            partition.get(),
                            partitionUpdate.getWritePath(),
                            Optional.of(partitionUpdate.getFileNames()),
                            statistics,
                            handle.isRetriesEnabled());
                }
                Location writePath = Location.of(partitionUpdate.getWritePath().toString());
                createEmptyFiles(session, writePath, table, partition, partitionUpdate.getFileNames());
            }
        }

        List<String> partitionedBy = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        Map<String, Type> columnTypes = handle.getInputColumns().stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> typeManager.getType(getTypeSignature(column.getHiveType()))));
        Map<List<String>, ComputedStatistics> partitionComputedStatistics = createComputedStatisticsToPartitionMap(computedStatistics, partitionedBy, columnTypes);

        ImmutableList.Builder<PartitionUpdateInfo> partitionUpdateInfosBuilder = ImmutableList.builder();
        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            if (partitionUpdate.getName().isEmpty()) {
                // insert into unpartitioned table
                if (!table.getStorage().getStorageFormat().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && isRespectTableFormat(session)) {
                    throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during " + changeDescription);
                }

                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, ImmutableList.of()));

                if (partitionUpdate.getUpdateMode() == OVERWRITE) {
                    // get privileges from existing table
                    PrincipalPrivileges principalPrivileges = fromHivePrivilegeInfos(metastore.listTablePrivileges(handle.getSchemaName(), handle.getTableName(), Optional.empty()));

                    // first drop it
                    metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());

                    // create the table with the new location
                    metastore.createTable(
                            session,
                            table,
                            principalPrivileges,
                            Optional.of(partitionUpdate.getWritePath()),
                            Optional.of(partitionUpdate.getFileNames()),
                            false,
                            partitionStatistics,
                            handle.isRetriesEnabled());
                }
                else if (partitionUpdate.getUpdateMode() == NEW || partitionUpdate.getUpdateMode() == APPEND) {
                    // insert into unpartitioned table
                    metastore.finishChangingExistingTable(
                            acidOperation,
                            session,
                            handle.getSchemaName(),
                            handle.getTableName(),
                            partitionUpdate.getWritePath(),
                            partitionUpdate.getFileNames(),
                            partitionStatistics,
                            handle.isRetriesEnabled());
                }
                else {
                    throw new IllegalArgumentException("Unsupported update mode: " + partitionUpdate.getUpdateMode());
                }
            }
            else if (partitionUpdate.getUpdateMode() == APPEND) {
                // insert into existing partition
                String partitionName = partitionUpdate.getName();
                List<String> partitionValues = toPartitionValues(partitionName);
                List<Type> partitionTypes = partitionedBy.stream()
                        .map(columnTypes::get)
                        .collect(toImmutableList());
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, partitionName, partitionValues, partitionTypes));
                partitionUpdateInfosBuilder.add(
                        new PartitionUpdateInfo(
                                partitionValues,
                                partitionUpdate.getWritePath(),
                                partitionUpdate.getFileNames(),
                                partitionStatistics));
            }
            else if (partitionUpdate.getUpdateMode() == NEW || partitionUpdate.getUpdateMode() == OVERWRITE) {
                // insert into new partition or overwrite existing partition
                Partition partition = buildPartitionObject(session, table, partitionUpdate);
                if (!partition.getStorage().getStorageFormat().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && isRespectTableFormat(session)) {
                    throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Partition format changed during insert");
                }

                String partitionName = partitionUpdate.getName();
                List<String> partitionValues = partition.getValues();
                List<Type> partitionTypes = partitionedBy.stream()
                        .map(columnTypes::get)
                        .collect(toImmutableList());
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, partitionName, partitionValues, partitionTypes));
                if (partitionUpdate.getUpdateMode() == OVERWRITE) {
                    if (handle.getLocationHandle().getWriteMode() == DIRECT_TO_TARGET_EXISTING_DIRECTORY) {
                        removeNonCurrentQueryFiles(session, partitionUpdate.getTargetPath());
                        if (handle.isRetriesEnabled()) {
                            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
                            cleanExtraOutputFiles(fileSystem, session.getQueryId(), partitionUpdate.getTargetPath(), ImmutableSet.copyOf(partitionUpdate.getFileNames()));
                        }
                    }
                    else {
                        metastore.dropPartition(session, handle.getSchemaName(), handle.getTableName(), partition.getValues(), true);
                        metastore.addPartition(session, handle.getSchemaName(), handle.getTableName(), partition, partitionUpdate.getWritePath(), Optional.of(partitionUpdate.getFileNames()), partitionStatistics, handle.isRetriesEnabled());
                    }
                }
                else {
                    metastore.addPartition(session, handle.getSchemaName(), handle.getTableName(), partition, partitionUpdate.getWritePath(), Optional.of(partitionUpdate.getFileNames()), partitionStatistics, handle.isRetriesEnabled());
                }
            }
            else {
                throw new IllegalArgumentException(format("Unsupported update mode: %s", partitionUpdate.getUpdateMode()));
            }
        }

        List<PartitionUpdateInfo> partitionUpdateInfos = partitionUpdateInfosBuilder.build();
        if (!partitionUpdateInfos.isEmpty()) {
            metastore.finishInsertIntoExistingPartitions(
                    session,
                    handle.getSchemaName(),
                    handle.getTableName(),
                    partitionUpdateInfos,
                    handle.isRetriesEnabled());
        }
        return table;
    }

    private void removeNonCurrentQueryFiles(ConnectorSession session, Location partitionLocation)
    {
        String queryId = session.getQueryId();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        try {
            FileIterator iterator = fileSystem.listFiles(partitionLocation);
            while (iterator.hasNext()) {
                Location location = iterator.next().location();
                if (!isFileCreatedByQuery(location.fileName(), queryId)) {
                    fileSystem.deleteFile(location);
                }
            }
        }
        catch (Exception ex) {
            throw new TrinoException(
                    HIVE_FILESYSTEM_ERROR,
                    format("Failed to delete partition %s files during overwrite", partitionLocation),
                    ex);
        }
    }

    private void createOrcAcidVersionFile(ConnectorIdentity identity, String deltaDirectory)
    {
        try {
            writeAcidVersionFile(fileSystemFactory.create(identity), Location.of(deltaDirectory));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Exception writing _orc_acid_version file for delta directory: " + deltaDirectory, e);
        }
    }

    private Partition buildPartitionObject(ConnectorSession session, Table table, PartitionUpdate partitionUpdate)
    {
        return Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(extractPartitionValues(partitionUpdate.getName()))
                .setParameters(ImmutableMap.<String, String>builder()
                        .put(TRINO_VERSION_NAME, trinoVersion)
                        .put(TRINO_QUERY_ID_NAME, session.getQueryId())
                        .buildOrThrow())
                .withStorage(storage -> storage
                        .setStorageFormat(isRespectTableFormat(session) ?
                                table.getStorage().getStorageFormat() :
                                getHiveStorageFormat(session).toStorageFormat())
                        .setLocation(partitionUpdate.getTargetPath().toString())
                        .setBucketProperty(table.getStorage().getBucketProperty())
                        .setSerdeParameters(table.getStorage().getSerdeParameters()))
                .build();
    }

    private static PartitionStatistics createPartitionStatistics(
            Map<String, Type> columnTypes,
            ComputedStatistics computedStatistics)
    {
        Map<ColumnStatisticMetadata, Block> computedColumnStatistics = computedStatistics.getColumnStatistics();

        Block rowCountBlock = Optional.ofNullable(computedStatistics.getTableStatistics().get(ROW_COUNT))
                .orElseThrow(() -> new VerifyException("rowCount not present"));
        verify(!rowCountBlock.isNull(0), "rowCount must never be null");
        long rowCount = BIGINT.getLong(rowCountBlock, 0);
        HiveBasicStatistics rowCountOnlyBasicStatistics = new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(rowCount), OptionalLong.empty(), OptionalLong.empty());
        return createPartitionStatistics(rowCountOnlyBasicStatistics, columnTypes, computedColumnStatistics);
    }

    private static PartitionStatistics createPartitionStatistics(
            HiveBasicStatistics basicStatistics,
            Map<String, Type> columnTypes,
            Map<ColumnStatisticMetadata, Block> computedColumnStatistics)
    {
        long rowCount = basicStatistics.getRowCount().orElseThrow(() -> new IllegalArgumentException("rowCount not present"));
        Map<String, HiveColumnStatistics> columnStatistics = fromComputedStatistics(
                computedColumnStatistics,
                columnTypes,
                rowCount);
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    private static Map<ColumnStatisticMetadata, Block> getColumnStatistics(Map<List<String>, ComputedStatistics> statistics, List<String> partitionValues)
    {
        return Optional.ofNullable(statistics.get(partitionValues))
                .map(ComputedStatistics::getColumnStatistics)
                .orElse(ImmutableMap.of());
    }

    private static Map<ColumnStatisticMetadata, Block> getColumnStatistics(
            Map<List<String>, ComputedStatistics> statistics,
            String partitionName,
            List<String> partitionValues,
            List<Type> partitionTypes)
    {
        Optional<Map<ColumnStatisticMetadata, Block>> columnStatistics = Optional.ofNullable(statistics.get(partitionValues))
                .map(ComputedStatistics::getColumnStatistics);
        return columnStatistics
                .orElseGet(() -> getColumnStatistics(statistics, canonicalizePartitionValues(partitionName, partitionValues, partitionTypes)));
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            ConnectorTableHandle tableHandle,
            String procedureName,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        if (procedureName.equals(OptimizeTableProcedure.NAME)) {
            return getTableHandleForOptimize(session, tableHandle, executeProperties, retryMode);
        }
        throw new IllegalArgumentException("Unknown procedure '" + procedureName + "'");
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForOptimize(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> executeProperties, RetryMode retryMode)
    {
        // TODO lots of that is copied from beginInsert; refactoring opportunity
        if (!isNonTransactionalOptimizeEnabled(session)) {
            // OPTIMIZE procedure is disabled by default; even though code is written in a way to avoid data loss, calling procedure is inherently
            // unsafe due to the non-transactional nature of committing changes done to Hive table. If Trino loses connectivity to HDFS cluster, while deleting
            // post-optimize data files, duplicate rows will be left in table and manual cleanup from user will be required.
            throw new TrinoException(NOT_SUPPORTED, "OPTIMIZE procedure must be explicitly enabled via " + NON_TRANSACTIONAL_OPTIMIZE_ENABLED + " session property");
        }

        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();

        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        checkTableIsWritable(table, writesToNonManagedTablesEnabled);

        for (Column column : table.getDataColumns()) {
            if (!isWritableType(column.getType())) {
                throw new TrinoException(NOT_SUPPORTED, format("Optimizing Hive table %s with column type %s not supported", tableName, column.getType()));
            }
        }

        if (isTransactionalTable(table.getParameters())) {
            throw new TrinoException(NOT_SUPPORTED, format("Optimizing transactional Hive table %s is not supported", tableName));
        }

        if (table.getStorage().getBucketProperty().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, format("Optimizing bucketed Hive table %s is not supported", tableName));
        }

        // TODO forcing NANOSECONDS precision here so we do not loose data. In future we may be smarter; options:
        //    - respect timestamp_precision but recognize situation when rounding occurs, and fail query
        //    - detect data's precision and maintain it
        List<HiveColumnHandle> columns = hiveColumnHandles(table, typeManager, NANOSECONDS).stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toImmutableList());

        HiveStorageFormat tableStorageFormat = extractHiveStorageFormat(table);
        Optional.ofNullable(table.getParameters().get(SKIP_HEADER_COUNT_KEY)).map(Integer::parseInt).ifPresent(headerSkipCount -> {
            if (headerSkipCount > 1) {
                throw new TrinoException(NOT_SUPPORTED, format("Optimizing Hive table %s with value of %s property greater than 1 is not supported", tableName, SKIP_HEADER_COUNT_KEY));
            }
        });

        if (table.getParameters().containsKey(SKIP_FOOTER_COUNT_KEY)) {
            throw new TrinoException(NOT_SUPPORTED, format("Optimizing Hive table %s with %s property not supported", tableName, SKIP_FOOTER_COUNT_KEY));
        }
        LocationHandle locationHandle = locationService.forOptimize(metastore, session, table);

        DataSize fileSizeThreshold = (DataSize) executeProperties.get("file_size_threshold");

        return Optional.of(new HiveTableExecuteHandle(
                OptimizeTableProcedure.NAME,
                Optional.empty(),
                Optional.of(fileSizeThreshold.toBytes()),
                tableName.getSchemaName(),
                tableName.getTableName(),
                columns,
                metastore.generatePageSinkMetadata(tableName),
                locationHandle,
                createBucketInfo(table),
                tableStorageFormat,
                // TODO: test with multiple partitions using different storage format
                tableStorageFormat,
                NO_ACID_TRANSACTION,
                retryMode != NO_RETRIES));
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorTableHandle updatedSourceTableHandle)
    {
        String procedureName = ((HiveTableExecuteHandle) tableExecuteHandle).getProcedureName();

        if (procedureName.equals(OptimizeTableProcedure.NAME)) {
            return beginOptimize(session, tableExecuteHandle, updatedSourceTableHandle);
        }
        throw new IllegalArgumentException("Unknown procedure '" + procedureName + "'");
    }

    private BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginOptimize(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorTableHandle sourceTableHandle)
    {
        HiveTableExecuteHandle hiveExecuteHandle = (HiveTableExecuteHandle) tableExecuteHandle;
        HiveTableHandle hiveSourceTableHandle = (HiveTableHandle) sourceTableHandle;

        WriteInfo writeInfo = locationService.getQueryWriteInfo(hiveExecuteHandle.getLocationHandle());
        String writeDeclarationId = metastore.declareIntentionToWrite(session, writeInfo.writeMode(), writeInfo.writePath(), hiveExecuteHandle.getSchemaTableName());

        return new BeginTableExecuteResult<>(
                hiveExecuteHandle
                        .withWriteDeclarationId(writeDeclarationId),
                hiveSourceTableHandle
                        .withMaxScannedFileSize(hiveExecuteHandle.getMaxScannedFileSize())
                        .withRecordScannedFiles(true));
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        String procedureName = ((HiveTableExecuteHandle) tableExecuteHandle).getProcedureName();

        if (procedureName.equals(OptimizeTableProcedure.NAME)) {
            finishOptimize(session, tableExecuteHandle, fragments, splitSourceInfo);
            return;
        }
        throw new IllegalArgumentException("Unknown procedure '" + procedureName + "'");
    }

    private void finishOptimize(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        // TODO lots of that is copied from finishInsert; refactoring opportunity

        HiveTableExecuteHandle handle = (HiveTableExecuteHandle) tableExecuteHandle;
        checkArgument(handle.getWriteDeclarationId().isPresent(), "no write declaration id present in tableExecuteHandle");

        List<PartitionUpdate> partitionUpdates = fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .collect(toImmutableList());

        HiveStorageFormat tableStorageFormat = handle.getTableStorageFormat();
        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        Table table = metastore.getTable(handle.getSchemaName(), handle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));
        if (!table.getStorage().getStorageFormat().getInputFormat().equals(tableStorageFormat.getInputFormat()) && isRespectTableFormat(session)) {
            throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during optimize");
        }

        // Support for bucketed tables disabled mostly, so we do not need to think about grouped execution in an initial version. Possibly no change apart from testing required.
        verify(handle.getBucketInfo().isEmpty(), "bucketed table not supported");

        ImmutableList.Builder<PartitionUpdateInfo> partitionUpdateInfosBuilder = ImmutableList.builder();
        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            verify(partitionUpdate.getUpdateMode() == APPEND, "Expected partionUpdate mode to be APPEND but got %s", partitionUpdate.getUpdateMode()); // sanity check

            if (partitionUpdate.getName().isEmpty()) {
                // operating on an unpartitioned table
                if (!table.getStorage().getStorageFormat().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && isRespectTableFormat(session)) {
                    throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during optimize");
                }

                AcidOperation operation = handle.getTransaction().getOperation();
                if (operation == AcidOperation.NONE) {
                    operation = AcidOperation.INSERT;
                }
                metastore.finishChangingExistingTable(
                        operation,
                        session,
                        handle.getSchemaName(),
                        handle.getTableName(),
                        partitionUpdate.getWritePath(),
                        partitionUpdate.getFileNames(),
                        PartitionStatistics.empty(),
                        handle.isRetriesEnabled());
            }
            else {
                // operating on a partition
                List<String> partitionValues = toPartitionValues(partitionUpdate.getName());
                partitionUpdateInfosBuilder.add(
                        new PartitionUpdateInfo(
                                partitionValues,
                                partitionUpdate.getWritePath(),
                                partitionUpdate.getFileNames(),
                                PartitionStatistics.empty()));
            }
        }

        List<PartitionUpdateInfo> partitionUpdateInfos = partitionUpdateInfosBuilder.build();
        if (!partitionUpdateInfos.isEmpty()) {
            metastore.finishInsertIntoExistingPartitions(
                    session,
                    handle.getSchemaName(),
                    handle.getTableName(),
                    partitionUpdateInfos,
                    handle.isRetriesEnabled());
        }

        // path to be deleted
        Set<Location> scannedPaths = splitSourceInfo.stream()
                .map(file -> Location.of((String) file))
                .collect(toImmutableSet());
        // track remaining files to be delted for error reporting
        Set<Location> remainingFilesToDelete = new HashSet<>(scannedPaths);

        // delete loop
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        boolean someDeleted = false;
        Optional<Location> firstScannedPath = Optional.empty();
        try {
            for (Location scannedPath : scannedPaths) {
                if (firstScannedPath.isEmpty()) {
                    firstScannedPath = Optional.of(scannedPath);
                }
                retry().run("delete " + scannedPath, () -> {
                    try {
                        fileSystem.deleteFile(scannedPath);
                    }
                    catch (FileNotFoundException e) {
                        // ignore missing files
                    }
                    return null;
                });
                someDeleted = true;
                remainingFilesToDelete.remove(scannedPath);
            }
        }
        catch (Exception e) {
            if (!someDeleted && (firstScannedPath.isEmpty() || exists(fileSystem, firstScannedPath.get()))) {
                // we are good - we did not delete any source files, so we can just throw error and allow rollback to happend
                // if someDeleted flag is false we do an extra check if the first file we tried to delete is still there. There is a chance that
                // fs.delete above could throw an exception, but the file was actually deleted.
                throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Error while deleting original files", e);
            }

            // If we already deleted some original files, we disable the rollback routine so written files are not deleted.
            // The reported exception message and log entry lists files which need to be cleaned up by user manually.
            // Until the table is cleaned up, there will be duplicate rows present.
            metastore.dropDeclaredIntentionToWrite(handle.getWriteDeclarationId().get());
            String errorMessage = "Error while deleting data files in FINISH phase of OPTIMIZE for table " + table.getTableName() + "; remaining files need to be deleted manually:  " + remainingFilesToDelete;
            log.error(e, "%s", errorMessage);
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, errorMessage, e);
        }
    }

    private static boolean exists(TrinoFileSystem fs, Location location)
    {
        try {
            return fs.newInputFile(location).exists();
        }
        catch (IOException e) {
            // on failure pessimistically assume file does not exist
            return false;
        }
    }

    @Override
    public void createView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorViewDefinition definition,
            Map<String, Object> viewProperties,
            boolean replace)
    {
        if (accessControlMetadata.isUsingSystemSecurity()) {
            definition = definition.withoutOwner();
        }

        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        propertiesBuilder
                .put(Table.TABLE_COMMENT, TableInfo.PRESTO_VIEW_COMMENT)
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, "Trino Hive connector")
                .put(TRINO_VERSION_NAME, trinoVersion)
                .put(TRINO_QUERY_ID_NAME, session.getQueryId());

        Map<String, String> baseProperties = propertiesBuilder.buildOrThrow();

        // Extra properties
        Map<String, String> extraProperties = HiveViewProperties.getExtraProperties(viewProperties)
                .orElseGet(ImmutableMap::of);
        Set<String> illegalExtraProperties = Sets.intersection(
                ImmutableSet.<String>builder()
                        .addAll(baseProperties.keySet())
                        .build(),
                extraProperties.keySet());
        if (!illegalExtraProperties.isEmpty()) {
            throw new TrinoException(
                    INVALID_VIEW_PROPERTY,
                    "Illegal keys in extra_properties: " + illegalExtraProperties);
        }
        propertiesBuilder.putAll(extraProperties);

        Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty(), ImmutableMap.of());

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(viewName.getSchemaName())
                .setTableName(viewName.getTableName())
                .setOwner(accessControlMetadata.isUsingSystemSecurity() ? Optional.empty() : Optional.ofNullable(session.getUser()))
                .setTableType(VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(dummyColumn))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(propertiesBuilder.buildOrThrow())
                .setViewOriginalText(Optional.of(encodeViewData(definition)))
                .setViewExpandedText(Optional.of(PRESTO_VIEW_EXPANDED_TEXT_MARKER));

        tableBuilder.getStorageBuilder()
                .setStorageFormat(VIEW_STORAGE_FORMAT)
                .setLocation("");
        Table table = tableBuilder.build();
        PrincipalPrivileges principalPrivileges = accessControlMetadata.isUsingSystemSecurity() ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());

        Optional<Table> existing = metastore.getTable(viewName.getSchemaName(), viewName.getTableName());
        if (existing.isPresent()) {
            if (!replace || !isTrinoView(existing.get())) {
                throw new ViewAlreadyExistsException(viewName);
            }

            metastore.replaceTable(viewName.getSchemaName(), viewName.getTableName(), table, principalPrivileges);
            return;
        }

        try {
            metastore.createTable(session, table, principalPrivileges, Optional.empty(), Optional.empty(), false, new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of()), false);
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        // Not checking if source view exists as this is already done in RenameViewTask
        metastore.renameTable(source.getSchemaName(), source.getTableName(), target.getSchemaName(), target.getTableName());
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        // Not checking if view exists as this is already done in SetViewAuthorizationTask
        setTableAuthorization(session, viewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (getView(session, viewName).isEmpty()) {
            throw new ViewNotFoundException(viewName);
        }

        try {
            metastore.dropTable(session, viewName.getSchemaName(), viewName.getTableName());
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        return listSchemas(session, optionalSchemaName).stream()
                .map(metastore::getTables)
                .flatMap(List::stream)
                .filter(tableInfo -> tableInfo.extendedRelationType().toRelationType() == RelationType.VIEW)
                .map(TableInfo::tableName)
                .collect(toImmutableList());
    }

    @Override
    public Map<String, Object> getViewProperties(ConnectorSession session, SchemaTableName viewName)
    {
        // The only currently existing view property (extra_properties) is hidden,
        // no need to retrieve its value for SHOW CREATE VIEW.
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        if (isHiveSystemSchema(schemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "Schema properties are not supported for system schema: " + schemaName);
        }
        return metastore.getDatabase(schemaName)
                .map(HiveSchemaProperties::fromDatabase)
                .orElseThrow(() -> new SchemaNotFoundException(schemaName));
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        if (isHiveSystemSchema(schemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "Schema owner is not supported for system schema: " + schemaName);
        }
        return accessControlMetadata.getSchemaOwner(session, schemaName).map(HivePrincipal::toTrinoPrincipal);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (SchemaTableName name : listViews(session, schemaName)) {
            try {
                getView(session, name).ifPresent(view -> views.put(name, view));
            }
            catch (HiveViewNotSupportedException e) {
                // Ignore hive views when translation is disabled
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(HIVE_VIEW_TRANSLATION_ERROR.toErrorCode())) {
                    // Ignore hive views for which translation fails
                }
                else if (e.getErrorCode().equals(HIVE_INVALID_VIEW_DATA.toErrorCode())) {
                    // Ignore views that are not valid
                }
                else if (e.getErrorCode().equals(HIVE_UNSUPPORTED_FORMAT.toErrorCode())) {
                    // Ignore views that are not supported
                }
                else if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode()) || e instanceof TableNotFoundException || e instanceof ViewNotFoundException) {
                    // Ignore view that was dropped during query execution (race condition)
                }
            }
            catch (RuntimeException e) {
                log.warn(e, "Failed to get metadata for view: %s", name);
            }
        }
        return views.buildOrThrow();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        if (isHiveSystemSchema(viewName.getSchemaName())) {
            return Optional.empty();
        }
        return toConnectorViewDefinition(session, viewName, metastore.getTable(viewName.getSchemaName(), viewName.getTableName()));
    }

    private Optional<ConnectorViewDefinition> toConnectorViewDefinition(ConnectorSession session, SchemaTableName viewName, Optional<Table> table)
    {
        return table
                .flatMap(view -> {
                    if (isTrinoView(view)) {
                        // can handle
                    }
                    else if (isHiveView(view)) {
                        if (!translateHiveViews) {
                            throw new HiveViewNotSupportedException(viewName);
                        }
                        // can handle
                    }
                    else {
                        // actually not a view
                        return Optional.empty();
                    }

                    ConnectorViewDefinition definition = createViewReader(metastore, session, view, typeManager, this::redirectTable, metadataProvider, hiveViewsRunAsInvoker, hiveViewsTimestampPrecision)
                            .decodeViewData(view.getViewOriginalText().orElseThrow(), view, catalogName);
                    // use owner field from table metadata if it exists
                    if (view.getOwner().isPresent() && !definition.isRunAsInvoker()) {
                        definition = new ConnectorViewDefinition(
                                definition.getOriginalSql(),
                                definition.getCatalog(),
                                definition.getSchema(),
                                definition.getColumns(),
                                definition.getComment(),
                                view.getOwner(),
                                false,
                                definition.getPath());
                    }
                    return Optional.of(definition);
                });
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return mergeRowIdColumnHandle();
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getInsertLayout(session, tableHandle)
                .flatMap(ConnectorTableLayout::getPartitioning)
                .map(HivePartitioningHandle.class::cast)
                .map(handle -> new HiveUpdateHandle(
                        handle.getBucketingVersion(),
                        handle.getBucketCount(),
                        handle.getHiveTypes(),
                        handle.getMaxCompatibleBucketCount(),
                        handle.isUsePartitionedBucketing()));
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        Map<String, String> parameters = ((HiveTableHandle) handle).getTableParameters()
                .orElseThrow(() -> new IllegalStateException("tableParameters missing from handle"));

        return isFullAcidTable(parameters) ? Optional.empty() : Optional.of(handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle deleteHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) deleteHandle;

        Table table = metastore.getTable(handle.getSchemaName(), handle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));

        if (table.getPartitionColumns().isEmpty()) {
            metastore.truncateUnpartitionedTable(session, handle.getSchemaName(), handle.getTableName());
        }
        else {
            Iterator<HivePartition> partitions = partitionManager.getPartitions(metastore, handle);
            List<String> partitionIds = new ArrayList<>();
            while (partitions.hasNext()) {
                partitionIds.add(partitions.next().getPartitionId());
                if (partitionIds.size() > maxPartitionDropsPerQuery) {
                    throw new TrinoException(
                            NOT_SUPPORTED,
                            format(
                                    "Failed to drop partitions. The number of partitions to be dropped is greater than the maximum allowed partitions (%s).",
                                    maxPartitionDropsPerQuery));
                }
            }
            for (String partitionId : partitionIds) {
                metastore.dropPartition(session, handle.getSchemaName(), handle.getTableName(), toPartitionValues(partitionId), true);
            }
        }
        // it is too expensive to determine the exact number of deleted rows
        return OptionalLong.empty();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) table;

        List<ColumnHandle> partitionColumns = ImmutableList.copyOf(hiveTable.getPartitionColumns());
        TupleDomain<ColumnHandle> predicate = TupleDomain.all();
        Optional<DiscretePredicates> discretePredicates = Optional.empty();

        // If only partition names are loaded, then the predicates are partially enforced.
        // So computation of predicate and discretePredicates are not valid.
        if (hiveTable.getPartitionNames().isEmpty()) {
            Optional<List<HivePartition>> partitions = hiveTable.getPartitions()
                    // If the partitions are not loaded, try out if they can be loaded.
                    .or(() -> {
                        // We load the partitions to compute the predicates enforced by the table.
                        // Note that the computation is not persisted in the table handle, so can be redone many times
                        // TODO: https://github.com/trinodb/trino/issues/10980.
                        HivePartitionResult partitionResult = partitionManager.getPartitions(metastore, table, new Constraint(hiveTable.getEnforcedConstraint()));
                        return partitionManager.tryLoadPartitions(partitionResult);
                    });

            if (partitions.isPresent()) {
                List<HivePartition> hivePartitions = partitions.orElseThrow();
                // Since the partitions are fully loaded now, we need to compute
                predicate = createPredicate(partitionColumns, hivePartitions);

                // Un-partitioned tables can have a partition with ID - UNPARTITIONED,
                // this check allows us to ensure that table is partitioned
                if (!partitionColumns.isEmpty()) {
                    // Do not create tuple domains for every partition at the same time!
                    // There can be a huge number of partitions, so use an Iterable, so
                    // all domains do not need to be in memory at the same time.
                    Iterable<TupleDomain<ColumnHandle>> partitionDomains = Iterables.transform(hivePartitions, hivePartition -> TupleDomain.fromFixedValues(hivePartition.getKeys()));
                    discretePredicates = Optional.of(new DiscretePredicates(partitionColumns, partitionDomains));
                }
            }
        }

        Optional<ConnectorTablePartitioning> tablePartitioning = Optional.empty();
        List<LocalProperty<ColumnHandle>> sortingProperties = ImmutableList.of();
        if (hiveTable.getBucketHandle().isPresent()) {
            if (isPropagateTableScanSortingProperties(session) && !hiveTable.getBucketHandle().get().sortedBy().isEmpty()) {
                // Populating SortingProperty guarantees to the engine that it is reading pre-sorted input.
                // We detect compatibility between table and partition level sorted_by properties
                // and fail the query if there is a mismatch in HiveSplitManager#getPartitionMetadata.
                // This can lead to incorrect results if a sorted_by property is defined over unsorted files.
                Map<String, ColumnHandle> columnHandles = getColumnHandles(session, table);
                sortingProperties = hiveTable.getBucketHandle().get().sortedBy().stream()
                        .map(sortingColumn -> new SortingProperty<>(
                                columnHandles.get(sortingColumn.columnName()),
                                sortingColumn.order().getSortOrder()))
                        .collect(toImmutableList());
            }
            if (isBucketExecutionEnabled(session)) {
                tablePartitioning = hiveTable.getBucketHandle().map(bucketing -> new ConnectorTablePartitioning(
                        new HivePartitioningHandle(
                                bucketing.bucketingVersion(),
                                bucketing.readBucketCount(),
                                bucketing.columns().stream()
                                        .map(HiveColumnHandle::getHiveType)
                                        .collect(toImmutableList()),
                                OptionalInt.empty(),
                                false),
                        bucketing.columns().stream()
                                .map(ColumnHandle.class::cast)
                                .collect(toImmutableList())));
            }
        }

        return new ConnectorTableProperties(
                predicate,
                tablePartitioning,
                discretePredicates,
                sortingProperties);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        checkArgument(handle.getAnalyzePartitionValues().isEmpty() || constraint.getSummary().isAll(), "Analyze should not have a constraint");

        HivePartitionResult partitionResult = partitionManager.getPartitions(metastore, handle, constraint);
        HiveTableHandle newHandle = partitionManager.applyPartitionResult(handle, partitionResult, constraint);

        if (handle.getPartitions().equals(newHandle.getPartitions()) &&
                handle.getPartitionNames().equals(newHandle.getPartitionNames()) &&
                handle.getCompactEffectivePredicate().equals(newHandle.getCompactEffectivePredicate()) &&
                handle.getBucketFilter().equals(newHandle.getBucketFilter()) &&
                handle.getConstraintColumns().equals(newHandle.getConstraintColumns())) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> unenforcedConstraint = partitionResult.getEffectivePredicate();
        if (newHandle.getPartitions().isPresent()) {
            List<HiveColumnHandle> partitionColumns = partitionResult.getPartitionColumns();
            unenforcedConstraint = partitionResult.getEffectivePredicate().filter((column, domain) -> !partitionColumns.contains(column));
        }

        return Optional.of(new ConstraintApplicationResult<>(newHandle, unenforcedConstraint, constraint.getExpression(), false));
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        if (isQueryPartitionFilterRequiredForTable(session, handle.getSchemaTableName()) && handle.getAnalyzePartitionValues().isEmpty() && handle.getEnforcedConstraint().isAll()) {
            List<HiveColumnHandle> partitionColumns = handle.getPartitionColumns();
            if (!partitionColumns.isEmpty()) {
                Set<HiveColumnHandle> referencedColumns = handle.getConstraintColumns();
                if (Collections.disjoint(referencedColumns, partitionColumns)) {
                    String partitionColumnNames = partitionColumns.stream()
                            .map(HiveColumnHandle::getName)
                            .collect(joining(", "));
                    throw new TrinoException(
                            StandardErrorCode.QUERY_REJECTED,
                            format("Filter required on %s.%s for at least one partition column: %s", handle.getSchemaName(), handle.getTableName(), partitionColumnNames));
                }
            }
        }
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

        HiveTableHandle hiveTableHandle = (HiveTableHandle) handle;
        // all references are simple variables
        if (columnProjections.values().stream().allMatch(ProjectedColumnRepresentation::isVariable)) {
            Set<HiveColumnHandle> projectedColumns = assignments.values().stream()
                    .map(HiveColumnHandle.class::cast)
                    .collect(toImmutableSet());
            if (hiveTableHandle.getProjectedColumns().equals(projectedColumns)) {
                return Optional.empty();
            }
            List<Assignment> assignmentsList = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((HiveColumnHandle) assignment.getValue()).getType()))
                    .collect(toImmutableList());
            return Optional.of(new ProjectionApplicationResult<>(
                    hiveTableHandle.withProjectedColumns(projectedColumns),
                    projections,
                    assignmentsList,
                    false));
        }

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<HiveColumnHandle> projectedColumnsBuilder = ImmutableSet.builder();

        for (Entry<ConnectorExpression, ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ProjectedColumnRepresentation projectedColumn = entry.getValue();

            HiveColumnHandle projectedColumnHandle;
            String projectedColumnName;

            // See if input already contains a columnhandle for this projected column, avoid creating duplicates.
            Optional<String> existingColumn = find(assignments, projectedColumn);

            if (existingColumn.isPresent()) {
                projectedColumnName = existingColumn.get();
                projectedColumnHandle = (HiveColumnHandle) assignments.get(projectedColumnName);
            }
            else {
                // Create a new column handle
                HiveColumnHandle oldColumnHandle = (HiveColumnHandle) assignments.get(projectedColumn.getVariable().getName());
                projectedColumnHandle = createProjectedColumnHandle(oldColumnHandle, projectedColumn.getDereferenceIndices());
                projectedColumnName = projectedColumnHandle.getName();
            }

            Variable projectedColumnVariable = new Variable(projectedColumnName, expression.getType());
            Assignment newAssignment = new Assignment(projectedColumnName, projectedColumnHandle, expression.getType());
            newAssignments.put(projectedColumnName, newAssignment);

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
                hiveTableHandle.withProjectedColumns(projectedColumnsBuilder.build()),
                newProjections,
                outputAssignments,
                false));
    }

    private HiveColumnHandle createProjectedColumnHandle(HiveColumnHandle column, List<Integer> indices)
    {
        HiveType oldHiveType = column.getHiveType();
        HiveType newHiveType = getHiveTypeForDereferences(oldHiveType, indices).orElseThrow();

        HiveColumnProjectionInfo columnProjectionInfo = new HiveColumnProjectionInfo(
                // Merge indices
                ImmutableList.<Integer>builder()
                        .addAll(column.getHiveColumnProjectionInfo()
                                .map(HiveColumnProjectionInfo::getDereferenceIndices)
                                .orElse(ImmutableList.of()))
                        .addAll(indices)
                        .build(),
                // Merge names
                ImmutableList.<String>builder()
                        .addAll(column.getHiveColumnProjectionInfo()
                                .map(HiveColumnProjectionInfo::getDereferenceNames)
                                .orElse(ImmutableList.of()))
                        .addAll(getHiveDereferenceNames(oldHiveType, indices))
                        .build(),
                newHiveType,
                typeManager.getType(getTypeSignature(newHiveType)));

        return new HiveColumnHandle(
                column.getBaseColumnName(),
                column.getBaseHiveColumnIndex(),
                column.getBaseHiveType(),
                column.getBaseType(),
                Optional.of(columnProjectionInfo),
                column.getColumnType(),
                column.getComment());
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return hiveRedirectionsProvider.getTableScanRedirection(session, (HiveTableHandle) tableHandle);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        HivePartitioningHandle leftHandle = (HivePartitioningHandle) left;
        HivePartitioningHandle rightHandle = (HivePartitioningHandle) right;

        if (leftHandle.isUsePartitionedBucketing() != rightHandle.isUsePartitionedBucketing()) {
            return Optional.empty();
        }
        if (!leftHandle.getHiveTypes().equals(rightHandle.getHiveTypes())) {
            return Optional.empty();
        }
        if (leftHandle.getBucketingVersion() != rightHandle.getBucketingVersion()) {
            return Optional.empty();
        }
        if (leftHandle.getBucketCount() == rightHandle.getBucketCount()) {
            return Optional.of(leftHandle);
        }
        if (!isOptimizedMismatchedBucketCount(session)) {
            return Optional.empty();
        }

        int largerBucketCount = Math.max(leftHandle.getBucketCount(), rightHandle.getBucketCount());
        int smallerBucketCount = Math.min(leftHandle.getBucketCount(), rightHandle.getBucketCount());
        if (largerBucketCount % smallerBucketCount != 0) {
            // must be evenly divisible
            return Optional.empty();
        }
        if (Integer.bitCount(largerBucketCount / smallerBucketCount) != 1) {
            // ratio must be power of two
            return Optional.empty();
        }

        OptionalInt maxCompatibleBucketCount = min(leftHandle.getMaxCompatibleBucketCount(), rightHandle.getMaxCompatibleBucketCount());
        if (maxCompatibleBucketCount.isPresent() && maxCompatibleBucketCount.getAsInt() < smallerBucketCount) {
            // maxCompatibleBucketCount must be larger than or equal to smallerBucketCount
            // because the current code uses the smallerBucketCount as the common partitioning handle.
            return Optional.empty();
        }

        return Optional.of(new HivePartitioningHandle(
                leftHandle.getBucketingVersion(), // same as rightHandle.getBucketingVersion()
                smallerBucketCount,
                leftHandle.getHiveTypes(),
                maxCompatibleBucketCount,
                false));
    }

    private static OptionalInt min(OptionalInt left, OptionalInt right)
    {
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        return OptionalInt.of(Math.min(left.getAsInt(), right.getAsInt()));
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) tableHandle;
        HivePartitioningHandle hivePartitioningHandle = (HivePartitioningHandle) partitioningHandle;

        checkArgument(hiveTable.getBucketHandle().isPresent(), "Hive connector only provides alternative layout for bucketed table");
        HiveBucketHandle bucketHandle = hiveTable.getBucketHandle().get();
        ImmutableList<HiveType> bucketTypes = bucketHandle.columns().stream().map(HiveColumnHandle::getHiveType).collect(toImmutableList());
        checkArgument(
                hivePartitioningHandle.getHiveTypes().equals(bucketTypes),
                "Types from the new PartitioningHandle (%s) does not match the TableHandle (%s)",
                hivePartitioningHandle.getHiveTypes(),
                bucketTypes);
        int largerBucketCount = Math.max(bucketHandle.tableBucketCount(), hivePartitioningHandle.getBucketCount());
        int smallerBucketCount = Math.min(bucketHandle.tableBucketCount(), hivePartitioningHandle.getBucketCount());
        checkArgument(
                largerBucketCount % smallerBucketCount == 0 && Integer.bitCount(largerBucketCount / smallerBucketCount) == 1,
                "The requested partitioning is not a valid alternative for the table layout");

        return new HiveTableHandle(
                hiveTable.getSchemaName(),
                hiveTable.getTableName(),
                hiveTable.getTableParameters(),
                hiveTable.getPartitionColumns(),
                hiveTable.getDataColumns(),
                hiveTable.getPartitionNames(),
                hiveTable.getPartitions(),
                hiveTable.getCompactEffectivePredicate(),
                hiveTable.getEnforcedConstraint(),
                Optional.of(new HiveBucketHandle(
                        bucketHandle.columns(),
                        bucketHandle.bucketingVersion(),
                        bucketHandle.tableBucketCount(),
                        hivePartitioningHandle.getBucketCount(),
                        bucketHandle.sortedBy())),
                hiveTable.getBucketFilter(),
                hiveTable.getAnalyzePartitionValues(),
                ImmutableSet.of(),
                ImmutableSet.of(), // Projected columns logic is used only during the optimization phase of planning
                hiveTable.getTransaction(),
                hiveTable.isRecordScannedFiles(),
                hiveTable.getMaxScannedFileSize());
    }

    @VisibleForTesting
    static TupleDomain<ColumnHandle> createPredicate(List<ColumnHandle> partitionColumns, List<HivePartition> partitions)
    {
        if (partitions.isEmpty()) {
            return TupleDomain.none();
        }

        return withColumnDomains(
                partitionColumns.stream()
                        .collect(toMap(identity(), column -> buildColumnDomain(column, partitions))));
    }

    private static Domain buildColumnDomain(ColumnHandle column, List<HivePartition> partitions)
    {
        checkArgument(!partitions.isEmpty(), "partitions cannot be empty");

        boolean hasNull = false;
        boolean hasNaN = false;
        List<Object> nonNullValues = new ArrayList<>();
        Type type = ((HiveColumnHandle) column).getType();

        for (HivePartition partition : partitions) {
            NullableValue value = partition.getKeys().get(column);
            if (value == null) {
                throw new TrinoException(HIVE_UNKNOWN_ERROR, format("Partition %s does not have a value for partition column %s", partition, column));
            }

            if (value.isNull()) {
                hasNull = true;
            }
            else {
                if (isFloatingPointNaN(type, value.getValue())) {
                    hasNaN = true;
                }
                nonNullValues.add(value.getValue());
            }
        }

        Domain domain;
        if (nonNullValues.isEmpty()) {
            domain = Domain.none(type);
        }
        else if (hasNaN) {
            domain = Domain.notNull(type);
        }
        else {
            domain = Domain.multipleValues(type, nonNullValues);
        }

        if (hasNull) {
            domain = domain.union(Domain.onlyNull(type));
        }

        return domain;
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        if (table.getStorage().getBucketProperty().isPresent()) {
            if (!isSupportedBucketing(table)) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot write to a table bucketed on an unsupported type");
            }
        }
        // treat un-bucketed transactional table as having a single bucket on no columns
        // Note: we cannot use hiveTableHandle.isInAcidTransaction() here as transaction is not yet set in HiveTableHandle when getInsertLayout is called
        else if (isFullAcidTable(table.getParameters())) {
            table = Table.builder(table)
                    .setParameter(BUCKETING_VERSION, String.valueOf(BUCKETING_V2.getVersion()))
                    .withStorage(storage -> storage.setBucketProperty(Optional.of(new HiveBucketProperty(ImmutableList.of(), 1, ImmutableList.of()))))
                    .build();
        }

        Optional<HiveBucketHandle> hiveBucketHandle = getHiveBucketHandle(session, table, typeManager);
        List<Column> partitionColumns = table.getPartitionColumns();
        if (hiveBucketHandle.isEmpty()) {
            // return preferred layout which is partitioned by partition columns
            if (partitionColumns.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new ConnectorTableLayout(
                    partitionColumns.stream()
                            .map(Column::getName)
                            .collect(toImmutableList())));
        }
        HiveBucketProperty bucketProperty = table.getStorage().getBucketProperty()
                .orElseThrow(() -> new NoSuchElementException("Bucket property should be set"));
        if (!bucketProperty.sortedBy().isEmpty() && !isSortedWritingEnabled(session)) {
            throw new TrinoException(NOT_SUPPORTED, "Writing to bucketed sorted Hive tables is disabled");
        }

        ImmutableList.Builder<String> partitioningColumns = ImmutableList.builder();
        hiveBucketHandle.get().columns().stream()
                .map(HiveColumnHandle::getName)
                .forEach(partitioningColumns::add);
        partitionColumns.stream()
                .map(Column::getName)
                .forEach(partitioningColumns::add);

        // For transactional bucketed tables, we don't want to split output files, therefore, we need to have a single writer
        // per partition.
        boolean multipleWritersPerPartitionSupported = !isTransactionalTable(table.getParameters());

        HivePartitioningHandle partitioningHandle = new HivePartitioningHandle(
                hiveBucketHandle.get().bucketingVersion(),
                hiveBucketHandle.get().tableBucketCount(),
                hiveBucketHandle.get().columns().stream()
                        .map(HiveColumnHandle::getHiveType)
                        .collect(toImmutableList()),
                OptionalInt.of(hiveBucketHandle.get().tableBucketCount()),
                !partitionColumns.isEmpty() && isParallelPartitionedBucketedWrites(session));
        return Optional.of(new ConnectorTableLayout(partitioningHandle, partitioningColumns.build(), multipleWritersPerPartitionSupported));
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        validateTimestampColumns(tableMetadata.getColumns(), getTimestampPrecision(session));
        validatePartitionColumns(tableMetadata);
        validateBucketColumns(tableMetadata);
        validateColumns(tableMetadata);
        Optional<BucketInfo> bucketInfo = getBucketInfo(tableMetadata.getProperties());
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        if (bucketInfo.isEmpty()) {
            // return preferred layout which is partitioned by partition columns
            if (partitionedBy.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new ConnectorTableLayout(partitionedBy));
        }
        if (!bucketInfo.get().sortedBy().isEmpty() && !isSortedWritingEnabled(session)) {
            throw new TrinoException(NOT_SUPPORTED, "Writing to bucketed sorted Hive tables is disabled");
        }

        // For transactional bucketed tables, we don't want to split output files, therefore, we need to have a single writer
        // per partition.
        boolean multipleWritersPerPartitionSupported = !isTransactional(tableMetadata.getProperties()).orElse(false);

        List<String> bucketedBy = bucketInfo.get().bucketedBy();
        Map<String, HiveType> hiveTypeMap = tableMetadata.getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> toHiveType(column.getType())));
        return Optional.of(new ConnectorTableLayout(
                new HivePartitioningHandle(
                        bucketInfo.get().bucketingVersion(),
                        bucketInfo.get().bucketCount(),
                        bucketedBy.stream()
                                .map(hiveTypeMap::get)
                                .collect(toImmutableList()),
                        OptionalInt.of(bucketInfo.get().bucketCount()),
                        !partitionedBy.isEmpty() && isParallelPartitionedBucketedWrites(session)),
                ImmutableList.<String>builder()
                        .addAll(bucketedBy)
                        .addAll(partitionedBy)
                        .build(),
                multipleWritersPerPartitionSupported));
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, Type type)
    {
        if (type instanceof VarcharType varcharType && !varcharType.isUnbounded() && varcharType.getBoundedLength() == 0) {
            return Optional.of(VarcharType.createVarcharType(1));
        }
        return Optional.empty();
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle executeHandle)
    {
        HiveTableExecuteHandle hiveExecuteHandle = (HiveTableExecuteHandle) executeHandle;
        SchemaTableName tableName = hiveExecuteHandle.getSchemaTableName();
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        if (table.getStorage().getBucketProperty().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, format("Optimizing bucketed Hive table %s is not supported", tableName));
        }
        if (isTransactionalTable(table.getParameters())) {
            throw new TrinoException(NOT_SUPPORTED, format("Optimizing transactional Hive table %s is not supported", tableName));
        }

        List<Column> partitionColumns = table.getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new ConnectorTableLayout(
                partitionColumns.stream()
                        .map(Column::getName)
                        .collect(toImmutableList())));
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!isCollectColumnStatisticsOnWrite(session)) {
            return TableStatisticsMetadata.empty();
        }
        if (!isStatisticsEnabled(session)) {
            throw new TrinoException(NOT_SUPPORTED, "Table statistics must be enabled when column statistics collection on write is enabled");
        }
        if (isTransactional(tableMetadata.getProperties()).orElse(false)) {
            // TODO(https://github.com/trinodb/trino/issues/1956) updating table statistics for transactional not supported right now.
            return TableStatisticsMetadata.empty();
        }
        List<String> partitionedBy = firstNonNull(getPartitionedBy(tableMetadata.getProperties()), ImmutableList.of());
        return getStatisticsCollectionMetadata(tableMetadata.getColumns(), partitionedBy, Optional.empty(), false);
    }

    private static TableStatisticsMetadata getStatisticsCollectionMetadata(List<ColumnMetadata> columns, List<String> partitionedBy, Optional<Set<String>> analyzeColumns, boolean includeRowCount)
    {
        Set<ColumnStatisticMetadata> columnStatistics = columns.stream()
                .filter(column -> !partitionedBy.contains(column.getName()))
                .filter(column -> !column.isHidden())
                .filter(column -> analyzeColumns.isEmpty() || analyzeColumns.get().contains(column.getName()))
                .map(HiveMetadata::getColumnStatisticMetadata)
                .flatMap(List::stream)
                .collect(toImmutableSet());

        Set<TableStatisticType> tableStatistics = includeRowCount ? ImmutableSet.of(ROW_COUNT) : ImmutableSet.of();
        return new TableStatisticsMetadata(columnStatistics, tableStatistics, partitionedBy);
    }

    private static List<ColumnStatisticMetadata> getColumnStatisticMetadata(ColumnMetadata columnMetadata)
    {
        String columnName = columnMetadata.getName();
        return getSupportedColumnStatistics(columnMetadata.getType()).stream()
                .map(type -> type.createColumnStatisticMetadata(columnName))
                .collect(toImmutableList());
    }

    @Override
    public Collection<LanguageFunction> listLanguageFunctions(ConnectorSession session, String schemaName)
    {
        return metastore.getFunctions(schemaName);
    }

    @Override
    public Collection<LanguageFunction> getLanguageFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return metastore.getFunctions(name);
    }

    @Override
    public boolean languageFunctionExists(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        return metastore.functionExists(name, signatureToken);
    }

    @Override
    public void createLanguageFunction(ConnectorSession session, SchemaFunctionName name, LanguageFunction function, boolean replace)
    {
        if (replace) {
            metastore.replaceFunction(name, function);
        }
        else {
            metastore.createFunction(name, function);
        }
    }

    @Override
    public void dropLanguageFunction(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        metastore.dropFunction(name, signatureToken);
    }

    @Override
    public boolean roleExists(ConnectorSession session, String role)
    {
        return accessControlMetadata.roleExists(session, role);
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
    public void grantRoles(ConnectorSession session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        accessControlMetadata.grantRoles(session, roles, HivePrincipal.from(grantees), adminOption, grantor.map(HivePrincipal::from));
    }

    @Override
    public void revokeRoles(ConnectorSession session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        accessControlMetadata.revokeRoles(session, roles, HivePrincipal.from(grantees), adminOption, grantor.map(HivePrincipal::from));
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
    public void grantSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        accessControlMetadata.grantSchemaPrivileges(session, schemaName, privileges, HivePrincipal.from(grantee), grantOption);
    }

    @Override
    public void revokeSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        accessControlMetadata.revokeSchemaPrivileges(session, schemaName, privileges, HivePrincipal.from(grantee), grantOption);
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

    private static HiveStorageFormat extractHiveStorageFormat(Table table)
    {
        return extractHiveStorageFormat(table.getStorage().getStorageFormat());
    }

    public static HiveStorageFormat extractHiveStorageFormat(StorageFormat storageFormat)
    {
        String outputFormat = storageFormat.getOutputFormat();
        String serde = storageFormat.getSerde();

        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerde().equals(serde)) {
                return format;
            }
        }
        throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, format("Output format %s with SerDe %s is not supported", outputFormat, serde));
    }

    private static void validateBucketColumns(ConnectorTableMetadata tableMetadata)
    {
        Optional<BucketInfo> bucketInfo = getBucketInfo(tableMetadata.getProperties());
        if (bucketInfo.isEmpty()) {
            return;
        }
        Set<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());

        List<String> bucketedBy = bucketInfo.get().bucketedBy();
        if (!allColumns.containsAll(bucketedBy)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Bucketing columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(bucketedBy), allColumns)));
        }

        Set<String> partitionColumns = ImmutableSet.copyOf(getPartitionedBy(tableMetadata.getProperties()));
        if (bucketedBy.stream().anyMatch(partitionColumns::contains)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Bucketing columns %s are also used as partitioning columns", Sets.intersection(ImmutableSet.copyOf(bucketedBy), partitionColumns)));
        }

        List<String> sortedBy = bucketInfo.get().sortedBy().stream()
                .map(SortingColumn::columnName)
                .collect(toImmutableList());
        if (!allColumns.containsAll(sortedBy)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Sorting columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(sortedBy), allColumns)));
        }

        if (sortedBy.stream().anyMatch(partitionColumns::contains)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Sorting columns %s are also used as partitioning columns", Sets.intersection(ImmutableSet.copyOf(sortedBy), partitionColumns)));
        }

        List<Column> dataColumns = tableMetadata.getColumns().stream()
                .map(columnMetadata -> new Column(columnMetadata.getName(), toHiveType(columnMetadata.getType()), Optional.ofNullable(columnMetadata.getComment()), ImmutableMap.of()))
                .collect(toImmutableList());
        if (!isSupportedBucketing(bucketInfo.get().bucketedBy(), dataColumns, tableMetadata.getTable().getTableName())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot create a table bucketed on an unsupported type");
        }
    }

    private static void validatePartitionColumns(ConnectorTableMetadata tableMetadata)
    {
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

        List<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());

        if (!ImmutableSet.copyOf(allColumns).containsAll(partitionedBy)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Partition columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(partitionedBy), ImmutableSet.copyOf(allColumns))));
        }

        if (allColumns.size() == partitionedBy.size()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Table contains only partition columns");
        }

        if (!allColumns.subList(allColumns.size() - partitionedBy.size(), allColumns.size()).equals(partitionedBy)) {
            throw new TrinoException(HIVE_COLUMN_ORDER_MISMATCH, "Partition keys must be the last columns in the table and in the same order as the table properties: " + partitionedBy);
        }
    }

    private static List<HiveColumnHandle> getColumnHandles(ConnectorTableMetadata tableMetadata, Set<String> partitionColumnNames)
    {
        validatePartitionColumns(tableMetadata);
        validateBucketColumns(tableMetadata);
        validateColumns(tableMetadata);

        ImmutableList.Builder<HiveColumnHandle> columnHandles = ImmutableList.builder();
        int ordinal = 0;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            HiveColumnHandle.ColumnType columnType;
            if (partitionColumnNames.contains(column.getName())) {
                columnType = PARTITION_KEY;
            }
            else if (column.isHidden()) {
                columnType = SYNTHESIZED;
            }
            else {
                columnType = REGULAR;
            }
            columnHandles.add(createBaseColumn(
                    column.getName(),
                    ordinal,
                    toHiveType(column.getType()),
                    column.getType(),
                    columnType,
                    Optional.ofNullable(column.getComment())));
            ordinal++;
        }

        return columnHandles.build();
    }

    private static void validateColumns(ConnectorTableMetadata tableMetadata)
    {
        // Validate the name and the type of each column
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            String columnName = column.getName();
            verifyHiveColumnName(columnName);
            // validate type is supported
            toHiveType(column.getType());
        }

        if (getHiveStorageFormat(tableMetadata.getProperties()) != HiveStorageFormat.CSV) {
            return;
        }

        Set<String> partitionedBy = ImmutableSet.copyOf(getPartitionedBy(tableMetadata.getProperties()));
        List<ColumnMetadata> unsupportedColumns = tableMetadata.getColumns().stream()
                .filter(columnMetadata -> !partitionedBy.contains(columnMetadata.getName()))
                .filter(columnMetadata -> !columnMetadata.getType().equals(createUnboundedVarcharType()))
                .collect(toImmutableList());

        if (!unsupportedColumns.isEmpty()) {
            String joinedUnsupportedColumns = unsupportedColumns.stream()
                    .map(columnMetadata -> format("%s %s", columnMetadata.getName(), columnMetadata.getType()))
                    .collect(joining(", "));
            throw new TrinoException(NOT_SUPPORTED, "Hive CSV storage format only supports VARCHAR (unbounded). Unsupported columns: " + joinedUnsupportedColumns);
        }
    }

    public static void verifyHiveColumnName(String columnName)
    {
        if (columnName.startsWith(" ")) {
            throw new TrinoException(NOT_SUPPORTED, format("Hive column names must not start with a space: '%s'", columnName));
        }
        if (columnName.endsWith(" ")) {
            throw new TrinoException(NOT_SUPPORTED, format("Hive column names must not end with a space: '%s'", columnName));
        }
        if (columnName.contains(",")) {
            throw new TrinoException(NOT_SUPPORTED, format("Hive column names must not contain commas: '%s'", columnName));
        }
    }

    private static void validateTimestampColumns(List<ColumnMetadata> columns, HiveTimestampPrecision precision)
    {
        for (ColumnMetadata column : columns) {
            validateTimestampTypes(column.getType(), precision, column);
        }
    }

    private static void validateTimestampTypes(Type type, HiveTimestampPrecision precision, ColumnMetadata column)
    {
        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() != precision.getPrecision()) {
                throw new TrinoException(NOT_SUPPORTED, format(
                        "Incorrect timestamp precision for %s; the configured precision is %s; column name: %s",
                        type,
                        precision,
                        column.getName()));
            }
        }
        else if (type instanceof ArrayType arrayType) {
            validateTimestampTypes(arrayType.getElementType(), precision, column);
        }
        else if (type instanceof MapType mapType) {
            validateTimestampTypes(mapType.getKeyType(), precision, column);
            validateTimestampTypes(mapType.getValueType(), precision, column);
        }
        else if (type instanceof RowType) {
            for (Type fieldType : type.getTypeParameters()) {
                validateTimestampTypes(fieldType, precision, column);
            }
        }
    }

    @Override
    public void rollback()
    {
        metastore.rollback();
    }

    @Override
    public void commit()
    {
        if (!metastore.isFinished()) {
            metastore.commit();
        }
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        metastore.beginQuery(session);
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        metastore.cleanupQuery(session);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableName, "tableName is null");

        Optional<String> icebergCatalogName = getIcebergCatalogName(session);
        Optional<String> deltaLakeCatalogName = getDeltaLakeCatalogName(session);
        Optional<String> hudiCatalogName = getHudiCatalogName(session);

        if (icebergCatalogName.isEmpty() && deltaLakeCatalogName.isEmpty() && hudiCatalogName.isEmpty()) {
            return Optional.empty();
        }

        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return Optional.empty();
        }
        // we need to chop off any "$partitions" and similar suffixes from table name while querying the metastore for the Table object
        TableNameSplitResult tableNameSplit = splitTableName(tableName.getTableName());
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableNameSplit.getBaseTableName());
        if (table.isEmpty() || isSomeKindOfAView(table.get())) {
            return Optional.empty();
        }

        Optional<CatalogSchemaTableName> catalogSchemaTableName = Optional.<CatalogSchemaTableName>empty()
                .or(() -> redirectTableToIceberg(icebergCatalogName, table.get()))
                .or(() -> redirectTableToDeltaLake(deltaLakeCatalogName, table.get()))
                .or(() -> redirectTableToHudi(hudiCatalogName, table.get()));

        // stitch back the suffix we cut off.
        return catalogSchemaTableName.map(name -> new CatalogSchemaTableName(
                name.getCatalogName(),
                new SchemaTableName(
                        name.getSchemaTableName().getSchemaName(),
                        name.getSchemaTableName().getTableName() + tableNameSplit.getSuffix().orElse(""))));
    }

    private static Optional<CatalogSchemaTableName> redirectTableToIceberg(Optional<String> targetCatalogName, Table table)
    {
        if (targetCatalogName.isEmpty()) {
            return Optional.empty();
        }
        if (isIcebergTable(table)) {
            return targetCatalogName.map(catalog -> new CatalogSchemaTableName(catalog, table.getSchemaTableName()));
        }
        return Optional.empty();
    }

    private static Optional<CatalogSchemaTableName> redirectTableToDeltaLake(Optional<String> targetCatalogName, Table table)
    {
        if (targetCatalogName.isEmpty()) {
            return Optional.empty();
        }
        if (isDeltaLakeTable(table)) {
            return targetCatalogName.map(catalog -> new CatalogSchemaTableName(catalog, table.getSchemaTableName()));
        }
        return Optional.empty();
    }

    private static Optional<CatalogSchemaTableName> redirectTableToHudi(Optional<String> targetCatalogName, Table table)
    {
        if (targetCatalogName.isEmpty()) {
            return Optional.empty();
        }
        if (isHudiTable(table)) {
            return targetCatalogName.map(catalog -> new CatalogSchemaTableName(catalog, table.getSchemaTableName()));
        }
        return Optional.empty();
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = ((HiveTableHandle) tableHandle).getSchemaTableName();

        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        try {
            HiveStorageFormat hiveStorageFormat = extractHiveStorageFormat(table);
            return hiveStorageFormat == HiveStorageFormat.ORC || hiveStorageFormat == HiveStorageFormat.PARQUET;
        }
        catch (TrinoException ignored) {
            // unknown storage format
            return false;
        }
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

    private static TableNameSplitResult splitTableName(String tableName)
    {
        int metadataMarkerIndex = tableName.lastIndexOf('$');
        return metadataMarkerIndex <= 0 ? // marker not found or at the beginning of tableName
                new TableNameSplitResult(tableName, Optional.empty()) :
                new TableNameSplitResult(tableName.substring(0, metadataMarkerIndex), Optional.of(tableName.substring(metadataMarkerIndex)));
    }

    private static class TableNameSplitResult
    {
        private final String baseTableName;
        private final Optional<String> suffix;

        public TableNameSplitResult(String baseTableName, Optional<String> suffix)
        {
            this.baseTableName = requireNonNull(baseTableName, "baseTableName is null");
            this.suffix = requireNonNull(suffix, "suffix is null");
        }

        public String getBaseTableName()
        {
            return baseTableName;
        }

        public Optional<String> getSuffix()
        {
            return suffix;
        }
    }

    @SafeVarargs
    private static <T> Optional<T> firstNonNullable(T... values)
    {
        for (T value : values) {
            if (value != null) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }

    private static boolean isQueryPartitionFilterRequiredForTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Set<String> requiredSchemas = getQueryPartitionFilterRequiredSchemas(session);
        // If query_partition_filter_required_schemas is empty, then we would apply partition filter for all tables.
        return isQueryPartitionFilterRequired(session) &&
                (requiredSchemas.isEmpty() || requiredSchemas.contains(schemaTableName.getSchemaName()));
    }
}
