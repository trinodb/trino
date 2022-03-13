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
import io.airlift.slice.Slice;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveApplyProjectionUtil.ProjectedColumnRepresentation;
import io.trino.plugin.hive.LocationService.WriteInfo;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.acid.AcidSchema;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.security.AccessControlMetadata;
import io.trino.plugin.hive.statistics.HiveStatisticsProvider;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.hive.util.HiveWriteUtils;
import io.trino.spi.ErrorType;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorNewTableLayout;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTablePartitioning;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortingProperty;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
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
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Sets.intersection;
import static io.trino.plugin.hive.HiveAnalyzeProperties.getColumnNames;
import static io.trino.plugin.hive.HiveAnalyzeProperties.getPartitionList;
import static io.trino.plugin.hive.HiveApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.hive.HiveApplyProjectionUtil.find;
import static io.trino.plugin.hive.HiveApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.hive.HiveBasicStatistics.createEmptyStatistics;
import static io.trino.plugin.hive.HiveBasicStatistics.createZeroStatistics;
import static io.trino.plugin.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PARTITION_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveColumnHandle.updateRowIdColumnHandle;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_COLUMN_ORDER_MISMATCH;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_VIEW_TRANSLATION_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.HiveSessionProperties.getCompressionCodec;
import static io.trino.plugin.hive.HiveSessionProperties.getHiveStorageFormat;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveSessionProperties.isBucketExecutionEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isCollectColumnStatisticsOnWrite;
import static io.trino.plugin.hive.HiveSessionProperties.isCreateEmptyBucketFiles;
import static io.trino.plugin.hive.HiveSessionProperties.isOptimizedMismatchedBucketCount;
import static io.trino.plugin.hive.HiveSessionProperties.isParallelPartitionedBucketedWrites;
import static io.trino.plugin.hive.HiveSessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isPropagateTableScanSortingProperties;
import static io.trino.plugin.hive.HiveSessionProperties.isRespectTableFormat;
import static io.trino.plugin.hive.HiveSessionProperties.isSortedWritingEnabled;
import static io.trino.plugin.hive.HiveSessionProperties.isStatisticsEnabled;
import static io.trino.plugin.hive.HiveTableProperties.ANALYZE_COLUMNS_PROPERTY;
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
import static io.trino.plugin.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.SKIP_FOOTER_LINE_COUNT;
import static io.trino.plugin.hive.HiveTableProperties.SKIP_HEADER_LINE_COUNT;
import static io.trino.plugin.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.TEXTFILE_FIELD_SEPARATOR;
import static io.trino.plugin.hive.HiveTableProperties.TEXTFILE_FIELD_SEPARATOR_ESCAPE;
import static io.trino.plugin.hive.HiveTableProperties.getAnalyzeColumns;
import static io.trino.plugin.hive.HiveTableProperties.getAvroSchemaUrl;
import static io.trino.plugin.hive.HiveTableProperties.getBucketProperty;
import static io.trino.plugin.hive.HiveTableProperties.getExternalLocation;
import static io.trino.plugin.hive.HiveTableProperties.getFooterSkipCount;
import static io.trino.plugin.hive.HiveTableProperties.getHeaderSkipCount;
import static io.trino.plugin.hive.HiveTableProperties.getHiveStorageFormat;
import static io.trino.plugin.hive.HiveTableProperties.getNullFormat;
import static io.trino.plugin.hive.HiveTableProperties.getOrcBloomFilterColumns;
import static io.trino.plugin.hive.HiveTableProperties.getOrcBloomFilterFpp;
import static io.trino.plugin.hive.HiveTableProperties.getPartitionedBy;
import static io.trino.plugin.hive.HiveTableProperties.getSingleCharacterProperty;
import static io.trino.plugin.hive.HiveTableProperties.isTransactional;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.HiveWriterFactory.computeBucketedFileName;
import static io.trino.plugin.hive.PartitionUpdate.UpdateMode.APPEND;
import static io.trino.plugin.hive.PartitionUpdate.UpdateMode.NEW;
import static io.trino.plugin.hive.PartitionUpdate.UpdateMode.OVERWRITE;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.hive.ViewReaderUtil.createViewReader;
import static io.trino.plugin.hive.ViewReaderUtil.encodeViewData;
import static io.trino.plugin.hive.ViewReaderUtil.isHiveOrPrestoView;
import static io.trino.plugin.hive.ViewReaderUtil.isPrestoView;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.acid.AcidTransaction.forCreateTable;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getProtectMode;
import static io.trino.plugin.hive.metastore.MetastoreUtil.verifyOnline;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.fromHivePrivilegeInfos;
import static io.trino.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.CompressionConfigUtil.configureCompression;
import static io.trino.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.trino.plugin.hive.util.HiveBucketing.bucketedOnTimestamp;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBucketHandle;
import static io.trino.plugin.hive.util.HiveUtil.columnExtraInfo;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.getRegularColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.hiveColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.plugin.hive.util.HiveUtil.toPartitionValues;
import static io.trino.plugin.hive.util.HiveUtil.verifyPartitionTypeSupported;
import static io.trino.plugin.hive.util.HiveWriteUtils.checkTableIsWritable;
import static io.trino.plugin.hive.util.HiveWriteUtils.initializeSerializer;
import static io.trino.plugin.hive.util.HiveWriteUtils.isS3FileSystem;
import static io.trino.plugin.hive.util.HiveWriteUtils.isWritableType;
import static io.trino.plugin.hive.util.Statistics.ReduceOperator.ADD;
import static io.trino.plugin.hive.util.Statistics.createComputedStatisticsToPartitionMap;
import static io.trino.plugin.hive.util.Statistics.createEmptyPartitionStatistics;
import static io.trino.plugin.hive.util.Statistics.fromComputedStatistics;
import static io.trino.plugin.hive.util.Statistics.reduce;
import static io.trino.plugin.hive.util.SystemTables.getSourceTableNameFromSystemTable;
import static io.trino.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.ql.io.AcidUtils.OrcAcidVersion.writeVersionFile;
import static org.apache.hadoop.hive.ql.io.AcidUtils.deltaSubdir;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isFullAcidTable;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isTransactionalTable;

public class HiveMetadata
        implements TransactionalMetadata
{
    public static final String PRESTO_VERSION_NAME = "presto_version";
    public static final String TRINO_CREATED_BY = "trino_created_by";
    public static final String PRESTO_QUERY_ID_NAME = "presto_query_id";
    public static final String BUCKETING_VERSION = "bucketing_version";
    public static final String TABLE_COMMENT = "comment";
    public static final String STORAGE_TABLE = "storage_table";
    private static final String TRANSACTIONAL = "transactional";
    public static final String PRESTO_VIEW_COMMENT = "Presto View";
    public static final String PRESTO_VIEW_EXPANDED_TEXT_MARKER = "/* Presto View */";

    private static final String ORC_BLOOM_FILTER_COLUMNS_KEY = "orc.bloom.filter.columns";
    private static final String ORC_BLOOM_FILTER_FPP_KEY = "orc.bloom.filter.fpp";

    public static final String SKIP_HEADER_COUNT_KEY = serdeConstants.HEADER_COUNT;
    public static final String SKIP_FOOTER_COUNT_KEY = serdeConstants.FOOTER_COUNT;

    private static final String TEXT_FIELD_SEPARATOR_KEY = serdeConstants.FIELD_DELIM;
    private static final String TEXT_FIELD_SEPARATOR_ESCAPE_KEY = serdeConstants.ESCAPE_CHAR;
    private static final String NULL_FORMAT_KEY = serdeConstants.SERIALIZATION_NULL_FORMAT;

    public static final String AVRO_SCHEMA_URL_KEY = "avro.schema.url";

    private static final String CSV_SEPARATOR_KEY = OpenCSVSerde.SEPARATORCHAR;
    private static final String CSV_QUOTE_KEY = OpenCSVSerde.QUOTECHAR;
    private static final String CSV_ESCAPE_KEY = OpenCSVSerde.ESCAPECHAR;

    private final CatalogName catalogName;
    private final SemiTransactionalHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final boolean translateHiveViews;
    private final boolean hideDeltaLakeTables;
    private final String prestoVersion;
    private final HiveStatisticsProvider hiveStatisticsProvider;
    private final HiveRedirectionsProvider hiveRedirectionsProvider;
    private final Set<SystemTableProvider> systemTableProviders;
    private final HiveMaterializedViewMetadata hiveMaterializedViewMetadata;
    private final AccessControlMetadata accessControlMetadata;

    public HiveMetadata(
            CatalogName catalogName,
            SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean translateHiveViews,
            boolean hideDeltaLakeTables,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            String trinoVersion,
            HiveStatisticsProvider hiveStatisticsProvider,
            HiveRedirectionsProvider hiveRedirectionsProvider,
            Set<SystemTableProvider> systemTableProviders,
            HiveMaterializedViewMetadata hiveMaterializedViewMetadata,
            AccessControlMetadata accessControlMetadata)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.translateHiveViews = translateHiveViews;
        this.hideDeltaLakeTables = hideDeltaLakeTables;
        this.prestoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.hiveStatisticsProvider = requireNonNull(hiveStatisticsProvider, "hiveStatisticsProvider is null");
        this.hiveRedirectionsProvider = requireNonNull(hiveRedirectionsProvider, "hiveRedirectionsProvider is null");
        this.systemTableProviders = requireNonNull(systemTableProviders, "systemTableProviders is null");
        this.hiveMaterializedViewMetadata = requireNonNull(hiveMaterializedViewMetadata, "hiveMaterializedViewMetadata is null");
        this.accessControlMetadata = requireNonNull(accessControlMetadata, "accessControlMetadata is null");
    }

    @Override
    public SemiTransactionalHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases().stream()
                .filter(schemaName -> !HiveUtil.isHiveSystemSchema(schemaName))
                .collect(toImmutableList());
    }

    @Override
    public HiveTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return null;
        }
        Optional<Table> table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName());
        if (table.isEmpty()) {
            return null;
        }

        if (isDeltaLakeTable(table.get())) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Cannot query Delta Lake table");
        }

        // we must not allow system tables due to how permissions are checked in SystemTableAwareAccessControl
        if (getSourceTableNameFromSystemTable(systemTableProviders, tableName).isPresent()) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Unexpected table present in Hive metastore: " + tableName);
        }

        verifyOnline(tableName, Optional.empty(), getProtectMode(table.get()), table.get().getParameters());

        return new HiveTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().getParameters(),
                getPartitionKeyColumnHandles(table.get(), typeManager),
                getRegularColumnHandles(table.get(), typeManager, getTimestampPrecision(session)),
                getHiveBucketHandle(session, table.get(), typeManager));
    }

    @Override
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        HiveTableHandle handle = getTableHandle(session, tableName);
        if (handle == null) {
            return null;
        }
        Optional<List<List<String>>> partitionValuesList = getPartitionList(analyzeProperties);
        Optional<Set<String>> analyzeColumnNames = getColumnNames(analyzeProperties);
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, handle.getSchemaTableName());

        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

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
            handle = partitionManager.applyPartitionResult(handle, partitions, Optional.empty());
        }

        if (analyzeColumnNames.isPresent()) {
            Set<String> columnNames = analyzeColumnNames.get();
            Set<String> allColumnNames = tableMetadata.getColumns().stream()
                    .map(ColumnMetadata::getName)
                    .collect(toImmutableSet());
            if (!allColumnNames.containsAll(columnNames)) {
                throw new TrinoException(
                        INVALID_ANALYZE_PROPERTY,
                        format("Invalid columns specified for analysis: %s", Sets.difference(columnNames, allColumnNames)));
            }

            handle = handle.withAnalyzeColumnNames(columnNames);
        }

        return handle;
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
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, hiveTableHandle.getSchemaTableName());

        return hiveTableHandle.getAnalyzeColumnNames()
                .map(columnNames -> new ConnectorTableMetadata(
                        tableMetadata.getTable(),
                        tableMetadata.getColumns(),
                        ImmutableMap.<String, Object>builder()
                                .putAll(tableMetadata.getProperties())
                                // we use table properties as a vehicle to pass to the analyzer the subset of columns to be analyzed
                                .put(ANALYZE_COLUMNS_PROPERTY, columnNames)
                                .build(),
                        tableMetadata.getComment()))
                .orElse(tableMetadata);
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
            // Errors related to invalid or unsupported information in the Metastore should be handled explicitly (eg. as TrinoException(HIVE_INVALID_METADATA)).
            // This is just a catch-all solution so that we have any actionable information when eg. SELECT * FROM information_schema.columns fails.
            throw new RuntimeException("Failed to construct table metadata for table " + tableName, e);
        }
    }

    private ConnectorTableMetadata doGetTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        if (!translateHiveViews && isHiveOrPrestoView(table)) {
            throw new TableNotFoundException(tableName);
        }

        Function<HiveColumnHandle, ColumnMetadata> metadataGetter = columnMetadataGetter(table);
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(table, typeManager, getTimestampPrecision(session))) {
            columns.add(metadataGetter.apply(columnHandle));
        }

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
        catch (TrinoException ignored) {
            // todo fail if format is not known
        }

        // Partitioning property
        List<String> partitionedBy = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        // Bucket properties
        table.getStorage().getBucketProperty().ifPresent(property -> {
            properties.put(BUCKETING_VERSION, property.getBucketingVersion().getVersion());
            properties.put(BUCKET_COUNT_PROPERTY, property.getBucketCount());
            properties.put(BUCKETED_BY_PROPERTY, property.getBucketedBy());
            properties.put(SORTED_BY_PROPERTY, property.getSortedBy());
        });

        // Transactional properties
        String transactionalProperty = table.getParameters().get(HiveMetadata.TRANSACTIONAL);
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

        Optional<String> comment = Optional.ofNullable(table.getParameters().get(TABLE_COMMENT));

        return new ConnectorTableMetadata(tableName, columns.build(), properties.build(), comment);
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
            // in Hive one can set conflicting values for the same property, in such case it looks like table properties are used
            throw new TrinoException(
                    HIVE_INVALID_METADATA,
                    format("Different values for '%s' set in serde properties and table properties: '%s' and '%s'", key, serdePropertyValue, tablePropertyValue));
        }
        return firstNonNullable(tablePropertyValue, serdePropertyValue);
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return ((HiveTableHandle) table).getPartitions()
                .map(partitions -> new HiveInputInfo(
                        partitions.stream()
                                .map(HivePartition::getPartitionId)
                                .collect(toImmutableList()),
                        false));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (String tableName : metastore.getAllTables(schemaName)) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }

        tableNames.addAll(listMaterializedViews(session, optionalSchemaName));
        return tableNames.build();
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
        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        return hiveColumnHandles(table, typeManager, getTimestampPrecision(session)).stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));
    }

    @SuppressWarnings("TryWithIdenticalCatches")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            }
            catch (HiveViewNotSupportedException e) {
                // view is not supported
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
            catch (TrinoException e) {
                // Skip this table if there's a failure due to Hive, a bad Serde, or bad metadata
                if (!e.getErrorCode().getType().equals(ErrorType.EXTERNAL)) {
                    throw e;
                }
            }
        }
        return columns.build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }
        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle)
                .entrySet().stream()
                .filter(entry -> !((HiveColumnHandle) entry.getValue()).isHidden())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Type> columnTypes = columns.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> getColumnMetadata(session, tableHandle, entry.getValue()).getType()));
        HivePartitionResult partitionResult = partitionManager.getPartitions(metastore, new HiveIdentity(session), tableHandle, constraint);
        List<HivePartition> partitions = partitionManager.getPartitionsAsList(partitionResult);
        return hiveStatisticsProvider.getTableStatistics(session, ((HiveTableHandle) tableHandle).getSchemaTableName(), columns, columnTypes, partitions);
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        SchemaTableName tableName = prefix.toSchemaTableName();
        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return ImmutableList.of();
        }

        Optional<Table> optionalTable;
        try {
            optionalTable = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName());
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
                hdfsEnvironment.getFileSystem(new HdfsContext(session), new Path(locationUri));
            }
            catch (IOException e) {
                throw new TrinoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + locationUri, e);
            }
            return locationUri;
        });

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setLocation(location)
                .setOwnerType(owner.getType())
                .setOwnerName(owner.getName())
                .build();

        metastore.createDatabase(new HiveIdentity(session), database);
    }

    @Override
    public void initializeTesseractMetadataConfig(ConnectorSession session)
    {
        Optional<String> configLocation = HiveSessionProperties.getStylusMetadataConfigLocation(session);
        if (configLocation.isEmpty()){
            throw new TrinoException(INVALID_SESSION_PROPERTY,"Tesseract metadata config location not specified in hive catalog");
        }
        StylusMetadataConfig.initializeStylusMetadataConfig(hdfsEnvironment,session,configLocation.get());
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        metastore.dropDatabase(new HiveIdentity(session), schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        metastore.renameDatabase(new HiveIdentity(session), source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String source, TrinoPrincipal principal)
    {
        metastore.setDatabaseOwner(new HiveIdentity(session), source, HivePrincipal.from(principal));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());

        if ((bucketProperty.isPresent() || !partitionedBy.isEmpty()) && getAvroSchemaUrl(tableMetadata.getProperties()) != null) {
            throw new TrinoException(NOT_SUPPORTED, "Bucketing/Partitioning columns not supported when Avro schema url is set");
        }

        validateTimestampColumns(tableMetadata.getColumns(), getTimestampPrecision(session));
        List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy));
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        Map<String, String> tableProperties = getEmptyTableProperties(tableMetadata, bucketProperty, new HdfsContext(session));

        hiveStorageFormat.validateColumns(columnHandles);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(HiveColumnHandle::toMetastoreColumn)
                .collect(toList());
        checkPartitionTypesSupported(partitionColumns);

        Path targetPath;
        boolean external;
        String externalLocation = getExternalLocation(tableMetadata.getProperties());
        if (externalLocation != null) {
            if (!createsOfNonManagedTablesEnabled) {
                throw new TrinoException(NOT_SUPPORTED, "Cannot create non-managed Hive table");
            }

            external = true;
            targetPath = getExternalLocationAsPath(externalLocation);
            checkExternalPath(new HdfsContext(session), targetPath);
        }
        else {
            external = false;
            LocationHandle locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName, Optional.empty());
            targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();
        }

        Table table = buildTableObject(
                session.getQueryId(),
                schemaName,
                tableName,
                session.getUser(),
                columnHandles,
                hiveStorageFormat,
                partitionedBy,
                bucketProperty,
                tableProperties,
                targetPath,
                external,
                prestoVersion);
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner());
        HiveBasicStatistics basicStatistics = (!external && table.getPartitionColumns().isEmpty()) ? createZeroStatistics() : createEmptyStatistics();
        metastore.createTable(
                session,
                table,
                principalPrivileges,
                Optional.empty(),
                ignoreExisting,
                new PartitionStatistics(basicStatistics, ImmutableMap.of()));
    }

    private Map<String, String> getEmptyTableProperties(ConnectorTableMetadata tableMetadata, Optional<HiveBucketProperty> bucketProperty, HdfsContext hdfsContext)
    {
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();

        // When metastore is configured with metastore.create.as.acid=true, it will also change Presto-created tables
        // behind the scenes. In particular, this won't work with CTAS.
        // TODO (https://github.com/trinodb/trino/issues/1956) convert this into normal table property

        boolean transactional = HiveTableProperties.isTransactional(tableMetadata.getProperties()).orElse(false);
        tableProperties.put(TRANSACTIONAL, String.valueOf(transactional));

        bucketProperty.ifPresent(hiveBucketProperty ->
                tableProperties.put(BUCKETING_VERSION, Integer.toString(hiveBucketProperty.getBucketingVersion().getVersion())));

        // ORC format specific properties
        List<String> columns = getOrcBloomFilterColumns(tableMetadata.getProperties());
        if (columns != null && !columns.isEmpty()) {
            checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.ORC, ORC_BLOOM_FILTER_COLUMNS);
            tableProperties.put(ORC_BLOOM_FILTER_COLUMNS_KEY, Joiner.on(",").join(columns));
            tableProperties.put(ORC_BLOOM_FILTER_FPP_KEY, String.valueOf(getOrcBloomFilterFpp(tableMetadata.getProperties())));
        }

        // Avro specific properties
        String avroSchemaUrl = getAvroSchemaUrl(tableMetadata.getProperties());
        if (avroSchemaUrl != null) {
            checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.AVRO, AVRO_SCHEMA_URL);
            tableProperties.put(AVRO_SCHEMA_URL_KEY, validateAndNormalizeAvroSchemaUrl(avroSchemaUrl, hdfsContext));
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

        // Set bogus table stats to prevent Hive 2.x from gathering these stats at table creation.
        // These stats are not useful by themselves and can take very long time to collect when creating an
        // external table over large data set.
        tableProperties.put("numFiles", "-1");
        tableProperties.put("totalSize", "-1");

        // Table comment property
        tableMetadata.getComment().ifPresent(value -> tableProperties.put(TABLE_COMMENT, value));

        return tableProperties.build();
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

    private String validateAndNormalizeAvroSchemaUrl(String url, HdfsContext context)
    {
        try {
            new URL(url).openStream().close();
            return url;
        }
        catch (MalformedURLException e) {
            // try locally
            if (new File(url).exists()) {
                // hive needs url to have a protocol
                return new File(url).toURI().toString();
            }
            // try hdfs
            try {
                if (!hdfsEnvironment.getFileSystem(context, new Path(url)).exists(new Path(url))) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY, "Cannot locate Avro schema file: " + url);
                }
                return url;
            }
            catch (IOException ex) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Avro schema file is not a valid file system URI: " + url, ex);
            }
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Cannot open Avro schema file: " + url, e);
        }
    }

    private static Path getExternalLocationAsPath(String location)
    {
        try {
            return new Path(location);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "External location is not a valid file system URI: " + location, e);
        }
    }

    private void checkExternalPath(HdfsContext context, Path path)
    {
        try {
            if (!isS3FileSystem(context, hdfsEnvironment, path)) {
                if (!hdfsEnvironment.getFileSystem(context, path).isDirectory(path)) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY, "External location must be a directory: " + path);
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "External location is not a valid file system URI: " + path, e);
        }
    }

    private void checkPartitionTypesSupported(List<Column> partitionColumns)
    {
        for (Column partitionColumn : partitionColumns) {
            Type partitionType = typeManager.getType(partitionColumn.getType().getTypeSignature());
            verifyPartitionTypeSupported(partitionColumn.getName(), partitionType);
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
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> additionalTableParameters,
            Path targetPath,
            boolean external,
            String prestoVersion)
    {
        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(HiveColumnHandle::toMetastoreColumn)
                .collect(toList());

        Set<String> partitionColumnNames = ImmutableSet.copyOf(partitionedBy);

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : columnHandles) {
            String name = columnHandle.getName();
            HiveType type = columnHandle.getHiveType();
            if (!partitionColumnNames.contains(name)) {
                verify(!columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
                columns.add(new Column(name, type, columnHandle.getComment()));
            }
            else {
                verify(columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
            }
        }

        ImmutableMap.Builder<String, String> tableParameters = ImmutableMap.<String, String>builder()
                .put(PRESTO_VERSION_NAME, prestoVersion)
                .put(PRESTO_QUERY_ID_NAME, queryId)
                .putAll(additionalTableParameters);

        if (external) {
            tableParameters.put("EXTERNAL", "TRUE");
        }

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(tableOwner)
                .setTableType((external ? EXTERNAL_TABLE : MANAGED_TABLE).name())
                .setDataColumns(columns.build())
                .setPartitionColumns(partitionColumns)
                .setParameters(tableParameters.build());

        tableBuilder.getStorageBuilder()
                .setStorageFormat(fromHiveStorageFormat(hiveStorageFormat))
                .setBucketProperty(bucketProperty)
                .setLocation(targetPath.toString());

        return tableBuilder.build();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(session, handle);

        metastore.addColumn(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName(), column.getName(), toHiveType(column.getType()), column.getComment());
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(session, hiveTableHandle);
        HiveColumnHandle sourceHandle = (HiveColumnHandle) source;

        metastore.renameColumn(new HiveIdentity(session), hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), sourceHandle.getName(), target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(session, hiveTableHandle);
        HiveColumnHandle columnHandle = (HiveColumnHandle) column;

        metastore.dropColumn(new HiveIdentity(session), hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), columnHandle.getName());
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName table, TrinoPrincipal principal)
    {
        metastore.setTableOwner(new HiveIdentity(session), table.getSchemaName(), table.getTableName(), HivePrincipal.from(principal));
    }

    private void failIfAvroSchemaIsSet(ConnectorSession session, HiveTableHandle handle)
    {
        Table table = metastore.getTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));
        if (table.getParameters().containsKey(AVRO_SCHEMA_URL_KEY) || table.getStorage().getSerdeParameters().containsKey(AVRO_SCHEMA_URL_KEY)) {
            throw new TrinoException(NOT_SUPPORTED, "ALTER TABLE not supported when Avro schema url is set");
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        metastore.renameTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName(), newTableName.getSchemaName(), newTableName.getTableName());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        metastore.commentTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName(), comment);
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        HiveColumnHandle columnHandle = (HiveColumnHandle) column;
        metastore.commentColumn(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName(), columnHandle.getName(), comment);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        if (metastore.getTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName()).isEmpty()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }
        metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        if (metastore.getTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName()).isEmpty()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        HiveIdentity identity = new HiveIdentity(session);
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = handle.getSchemaTableName();
        Table table = metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));

        List<Column> partitionColumns = table.getPartitionColumns();
        List<String> partitionColumnNames = partitionColumns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
        HiveTimestampPrecision timestampPrecision = getTimestampPrecision(session);
        List<HiveColumnHandle> hiveColumnHandles = hiveColumnHandles(table, typeManager, timestampPrecision);
        Map<String, Type> columnTypes = hiveColumnHandles.stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager, timestampPrecision)));

        Map<List<String>, ComputedStatistics> computedStatisticsMap = createComputedStatisticsToPartitionMap(computedStatistics, partitionColumnNames, columnTypes);

        if (partitionColumns.isEmpty()) {
            // commit analyze to unpartitioned table
            metastore.setTableStatistics(identity, table, createPartitionStatistics(columnTypes, computedStatisticsMap.get(ImmutableList.<String>of())));
        }
        else {
            List<List<String>> partitionValuesList;
            if (handle.getAnalyzePartitionValues().isPresent()) {
                partitionValuesList = handle.getAnalyzePartitionValues().get();
            }
            else {
                partitionValuesList = metastore.getPartitionNames(identity, handle.getSchemaName(), handle.getTableName())
                        .orElseThrow(() -> new TableNotFoundException(((HiveTableHandle) tableHandle).getSchemaTableName()))
                        .stream()
                        .map(HiveUtil::toPartitionValues)
                        .collect(toImmutableList());
            }

            ImmutableMap.Builder<List<String>, PartitionStatistics> partitionStatistics = ImmutableMap.builder();
            Map<String, Set<ColumnStatisticType>> columnStatisticTypes = hiveColumnHandles.stream()
                    .filter(columnHandle -> !partitionColumnNames.contains(columnHandle.getName()))
                    .filter(column -> !column.isHidden())
                    .collect(toImmutableMap(HiveColumnHandle::getName, column -> ImmutableSet.copyOf(metastore.getSupportedColumnStatistics(column.getType()))));
            Supplier<PartitionStatistics> emptyPartitionStatistics = Suppliers.memoize(() -> createEmptyPartitionStatistics(columnTypes, columnStatisticTypes));

            int usedComputedStatistics = 0;
            for (List<String> partitionValues : partitionValuesList) {
                ComputedStatistics collectedStatistics = computedStatisticsMap.get(partitionValues);
                if (collectedStatistics == null) {
                    partitionStatistics.put(partitionValues, emptyPartitionStatistics.get());
                }
                else {
                    usedComputedStatistics++;
                    partitionStatistics.put(partitionValues, createPartitionStatistics(columnTypes, collectedStatistics));
                }
            }
            verify(usedComputedStatistics == computedStatistics.size(), "All computed statistics must be used");
            metastore.setPartitionStatistics(identity, table, partitionStatistics.build());
        }
    }

    @Override
    public HiveOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        Optional<Path> externalLocation = Optional.ofNullable(getExternalLocation(tableMetadata.getProperties()))
                .map(HiveMetadata::getExternalLocationAsPath);
        if (!createsOfNonManagedTablesEnabled && externalLocation.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Creating non-managed Hive tables is disabled");
        }

        if (!writesToNonManagedTablesEnabled && externalLocation.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Writes to non-managed Hive tables is disabled");
        }

        if (getAvroSchemaUrl(tableMetadata.getProperties()) != null) {
            throw new TrinoException(NOT_SUPPORTED, "CREATE TABLE AS not supported when Avro schema url is set");
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
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());

        // get the root directory for the database
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Map<String, String> tableProperties = getEmptyTableProperties(tableMetadata, bucketProperty, new HdfsContext(session));
        List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy));
        HiveStorageFormat partitionStorageFormat = isRespectTableFormat(session) ? tableStorageFormat : getHiveStorageFormat(session);

        // unpartitioned tables ignore the partition storage format
        HiveStorageFormat actualStorageFormat = partitionedBy.isEmpty() ? tableStorageFormat : partitionStorageFormat;
        actualStorageFormat.validateColumns(columnHandles);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(HiveColumnHandle::toMetastoreColumn)
                .collect(toList());
        checkPartitionTypesSupported(partitionColumns);

        LocationHandle locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName, externalLocation);

        boolean transactional = isTransactional(tableMetadata.getProperties()).orElse(false);
        AcidTransaction transaction = transactional ? forCreateTable() : NO_ACID_TRANSACTION;

        HiveOutputTableHandle result = new HiveOutputTableHandle(
                schemaName,
                tableName,
                columnHandles,
                metastore.generatePageSinkMetadata(new HiveIdentity(session), schemaTableName),
                locationHandle,
                tableStorageFormat,
                partitionStorageFormat,
                partitionedBy,
                bucketProperty,
                session.getUser(),
                tableProperties,
                transaction,
                externalLocation.isPresent());

        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.declareIntentionToWrite(session, writeInfo.getWriteMode(), writeInfo.getWritePath(), schemaTableName);

        return result;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveOutputTableHandle handle = (HiveOutputTableHandle) tableHandle;

        List<PartitionUpdate> partitionUpdates = fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .collect(toList());

        WriteInfo writeInfo = locationService.getQueryWriteInfo(handle.getLocationHandle());
        Table table = buildTableObject(
                session.getQueryId(),
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTableOwner(),
                handle.getInputColumns(),
                handle.getTableStorageFormat(),
                handle.getPartitionedBy(),
                handle.getBucketProperty(),
                handle.getAdditionalTableParameters(),
                writeInfo.getTargetPath(),
                handle.isExternal(),
                prestoVersion);
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(handle.getTableOwner());

        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        if (handle.getBucketProperty().isPresent() && isCreateEmptyBucketFiles(session)) {
            List<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(session, handle, table, true, partitionUpdates);
            // replace partitionUpdates before creating the empty files so that those files will be cleaned up if we end up rollback
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.getPartitionColumns().isEmpty() ? Optional.empty() : Optional.of(buildPartitionObject(session, table, partitionUpdate));
                createEmptyFiles(session, partitionUpdate.getWritePath(), table, partition, partitionUpdate.getFileNames());
            }
            if (handle.isTransactional()) {
                AcidTransaction transaction = handle.getTransaction();
                List<String> partitionNames = partitionUpdates.stream().map(PartitionUpdate::getName).collect(toImmutableList());
                metastore.addDynamicPartitions(
                        new HiveIdentity(session),
                        handle.getSchemaName(),
                        handle.getTableName(),
                        partitionNames,
                        transaction.getAcidTransactionId(),
                        transaction.getWriteId(),
                        AcidOperation.CREATE_TABLE);
            }
        }

        Map<String, Type> columnTypes = handle.getInputColumns().stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager)));
        Map<List<String>, ComputedStatistics> partitionComputedStatistics = createComputedStatisticsToPartitionMap(computedStatistics, handle.getPartitionedBy(), columnTypes);

        PartitionStatistics tableStatistics;
        if (table.getPartitionColumns().isEmpty()) {
            HiveBasicStatistics basicStatistics = partitionUpdates.stream()
                    .map(PartitionUpdate::getStatistics)
                    .reduce((first, second) -> reduce(first, second, ADD))
                    .orElse(createZeroStatistics());
            tableStatistics = createPartitionStatistics(basicStatistics, columnTypes, getColumnStatistics(partitionComputedStatistics, ImmutableList.of()));
        }
        else {
            tableStatistics = new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        }

        metastore.createTable(session, table, principalPrivileges, Optional.of(writeInfo.getWritePath()), false, tableStatistics);

        if (!handle.getPartitionedBy().isEmpty()) {
            if (isRespectTableFormat(session)) {
                verify(handle.getPartitionStorageFormat() == handle.getTableStorageFormat());
            }
            for (PartitionUpdate update : partitionUpdates) {
                Partition partition = buildPartitionObject(session, table, update);
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        update.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, partition.getValues()));
                metastore.addPartition(
                        session,
                        handle.getSchemaName(),
                        handle.getTableName(),
                        buildPartitionObject(session, table, update),
                        update.getWritePath(),
                        partitionStatistics);
            }
        }

        return Optional.of(new HiveWrittenPartitions(
                partitionUpdates.stream()
                        .map(PartitionUpdate::getName)
                        .collect(toImmutableList())));
    }

    private List<PartitionUpdate> computePartitionUpdatesForMissingBuckets(
            ConnectorSession session,
            HiveWritableTableHandle handle,
            Table table,
            boolean isCreateTable,
            List<PartitionUpdate> partitionUpdates)
    {
        ImmutableList.Builder<PartitionUpdate> partitionUpdatesForMissingBucketsBuilder = ImmutableList.builder();
        HiveStorageFormat storageFormat = table.getPartitionColumns().isEmpty() ? handle.getTableStorageFormat() : handle.getPartitionStorageFormat();
        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            int bucketCount = handle.getBucketProperty().get().getBucketCount();

            List<String> fileNamesForMissingBuckets = computeFileNamesForMissingBuckets(
                    session,
                    storageFormat,
                    partitionUpdate.getTargetPath(),
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

    private List<String> computeFileNamesForMissingBuckets(
            ConnectorSession session,
            HiveStorageFormat storageFormat,
            Path targetPath,
            int bucketCount,
            boolean transactionalCreateTable,
            PartitionUpdate partitionUpdate)
    {
        if (partitionUpdate.getFileNames().size() == bucketCount) {
            // fast path for common case
            return ImmutableList.of();
        }
        HdfsContext hdfsContext = new HdfsContext(session);
        JobConf conf = toJobConf(hdfsEnvironment.getConfiguration(hdfsContext, targetPath));
        configureCompression(conf, getCompressionCodec(session));
        String fileExtension = HiveWriterFactory.getFileExtension(conf, fromHiveStorageFormat(storageFormat));
        Set<String> fileNames = ImmutableSet.copyOf(partitionUpdate.getFileNames());
        ImmutableList.Builder<String> missingFileNamesBuilder = ImmutableList.builder();
        for (int i = 0; i < bucketCount; i++) {
            String fileName;
            if (transactionalCreateTable) {
                fileName = computeBucketedFileName(Optional.empty(), i) + fileExtension;
            }
            else {
                fileName = computeBucketedFileName(Optional.of(session.getQueryId()), i) + fileExtension;
            }
            if (!fileNames.contains(fileName)) {
                missingFileNamesBuilder.add(fileName);
            }
        }
        List<String> missingFileNames = missingFileNamesBuilder.build();
        verify(fileNames.size() + missingFileNames.size() == bucketCount);
        return missingFileNames;
    }

    private void createEmptyFiles(ConnectorSession session, Path path, Table table, Optional<Partition> partition, List<String> fileNames)
    {
        JobConf conf = toJobConf(hdfsEnvironment.getConfiguration(new HdfsContext(session), path));
        configureCompression(conf, getCompressionCodec(session));

        Properties schema;
        StorageFormat format;
        if (partition.isPresent()) {
            schema = getHiveSchema(partition.get(), table);
            format = partition.get().getStorage().getStorageFormat();
        }
        else {
            schema = getHiveSchema(table);
            format = table.getStorage().getStorageFormat();
        }
        hdfsEnvironment.doAs(session.getIdentity(), () -> {
            for (String fileName : fileNames) {
                writeEmptyFile(session, new Path(path, fileName), conf, schema, format.getSerDe(), format.getOutputFormat());
            }
        });
    }

    private static void writeEmptyFile(ConnectorSession session, Path target, JobConf conf, Properties properties, String serDe, String outputFormatName)
    {
        // Some serializers such as Avro set a property in the schema.
        initializeSerializer(conf, properties, serDe);

        // The code below is not a try with resources because RecordWriter is not Closeable.
        FileSinkOperator.RecordWriter recordWriter = HiveWriteUtils.createRecordWriter(target, conf, properties, outputFormatName, session);
        try {
            recordWriter.close(false);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error write empty file to Hive", e);
        }
    }

    @Override
    public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        HiveIdentity identity = new HiveIdentity(session);
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Table table = metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        if (!isFullAcidTable(table.getParameters())) {
            throw new TrinoException(NOT_SUPPORTED, "Hive update is only supported for ACID transactional tables");
        }

        // Verify that none of the updated columns are partition columns or bucket columns

        Set<String> updatedColumnNames = updatedColumns.stream().map(handle -> ((HiveColumnHandle) handle).getName()).collect(toImmutableSet());
        Set<String> partitionColumnNames = table.getPartitionColumns().stream().map(Column::getName).collect(toImmutableSet());
        if (!intersection(updatedColumnNames, partitionColumnNames).isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Updating Hive table partition columns is not supported");
        }

        hiveTableHandle.getBucketHandle().ifPresent(handle -> {
            Set<String> bucketColumnNames = handle.getColumns().stream().map(HiveColumnHandle::getName).collect(toImmutableSet());
            if (!intersection(updatedColumnNames, bucketColumnNames).isEmpty()) {
                throw new TrinoException(NOT_SUPPORTED, "Updating Hive table bucket columns is not supported");
            }
        });

        checkTableIsWritable(table, writesToNonManagedTablesEnabled);

        for (Column column : table.getDataColumns()) {
            if (!isWritableType(column.getType())) {
                throw new TrinoException(NOT_SUPPORTED, format("Updating a Hive table with column type %s not supported", column.getType()));
            }
        }

        List<HiveColumnHandle> allDataColumns = getRegularColumnHandles(table, typeManager, getTimestampPrecision(session)).stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toList());
        List<HiveColumnHandle> hiveUpdatedColumns = updatedColumns.stream().map(HiveColumnHandle.class::cast).collect(toImmutableList());

        if (table.getParameters().containsKey(SKIP_HEADER_COUNT_KEY)) {
            throw new TrinoException(NOT_SUPPORTED, format("Updating a Hive table with %s property not supported", SKIP_HEADER_COUNT_KEY));
        }
        if (table.getParameters().containsKey(SKIP_FOOTER_COUNT_KEY)) {
            throw new TrinoException(NOT_SUPPORTED, format("Updating a Hive table with %s property not supported", SKIP_FOOTER_COUNT_KEY));
        }
        LocationHandle locationHandle = locationService.forExistingTable(metastore, session, table);

        HiveUpdateProcessor updateProcessor = new HiveUpdateProcessor(allDataColumns, hiveUpdatedColumns);
        AcidTransaction transaction = metastore.beginUpdate(session, table, updateProcessor);
        HiveTableHandle updateHandle = hiveTableHandle.withTransaction(transaction);

        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.declareIntentionToWrite(session, writeInfo.getWriteMode(), writeInfo.getWritePath(), tableName);
        return updateHandle;
    }

    @Override
    public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        checkArgument(handle.isAcidUpdate(), "handle should be a update handle, but is %s", handle);

        requireNonNull(fragments, "fragments is null");

        SchemaTableName tableName = handle.getSchemaTableName();
        HiveIdentity identity = new HiveIdentity(session);
        Table table = metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        List<PartitionAndStatementId> partitionAndStatementIds = fragments.stream()
                .map(Slice::getBytes)
                .map(PartitionAndStatementId.CODEC::fromJson)
                .collect(toList());

        HdfsContext context = new HdfsContext(session);
        for (PartitionAndStatementId ps : partitionAndStatementIds) {
            createOrcAcidVersionFile(context, new Path(ps.getDeleteDeltaDirectory()));
        }

        LocationHandle locationHandle = locationService.forExistingTable(metastore, session, table);
        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.finishUpdate(session, table.getDatabaseName(), table.getTableName(), writeInfo.getWritePath(), partitionAndStatementIds);
    }

    @Override
    public HiveInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveIdentity identity = new HiveIdentity(session);
        SchemaTableName tableName = ((HiveTableHandle) tableHandle).getSchemaTableName();
        Table table = metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        checkTableIsWritable(table, writesToNonManagedTablesEnabled);

        for (Column column : table.getDataColumns()) {
            if (!isWritableType(column.getType())) {
                throw new TrinoException(NOT_SUPPORTED, format("Inserting into Hive table %s with column type %s not supported", tableName, column.getType()));
            }
        }

        List<HiveColumnHandle> handles = hiveColumnHandles(table, typeManager, getTimestampPrecision(session)).stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toList());

        HiveStorageFormat tableStorageFormat = extractHiveStorageFormat(table);
        Optional.ofNullable(table.getParameters().get(SKIP_HEADER_COUNT_KEY)).map(Integer::parseInt).ifPresent(headerSkipCount -> {
            if (headerSkipCount > 1) {
                throw new TrinoException(NOT_SUPPORTED, format("Inserting into Hive table with value of %s property greater than 1 is not supported", SKIP_HEADER_COUNT_KEY));
            }
        });
        if (table.getParameters().containsKey(SKIP_FOOTER_COUNT_KEY)) {
            throw new TrinoException(NOT_SUPPORTED, format("Inserting into Hive table with %s property not supported", SKIP_FOOTER_COUNT_KEY));
        }
        LocationHandle locationHandle = locationService.forExistingTable(metastore, session, table);

        AcidTransaction transaction = isTransactionalTable(table.getParameters()) ? metastore.beginInsert(session, table) : NO_ACID_TRANSACTION;

        HiveInsertTableHandle result = new HiveInsertTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                handles,
                metastore.generatePageSinkMetadata(identity, tableName),
                locationHandle,
                table.getStorage().getBucketProperty(),
                tableStorageFormat,
                isRespectTableFormat(session) ? tableStorageFormat : getHiveStorageFormat(session),
                transaction);

        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.declareIntentionToWrite(session, writeInfo.getWriteMode(), writeInfo.getWritePath(), tableName);
        return result;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveInsertTableHandle handle = (HiveInsertTableHandle) insertHandle;

        List<PartitionUpdate> partitionUpdates = fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .collect(toList());

        HiveStorageFormat tableStorageFormat = handle.getTableStorageFormat();
        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        Table table = metastore.getTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));
        if (!table.getStorage().getStorageFormat().getInputFormat().equals(tableStorageFormat.getInputFormat()) && isRespectTableFormat(session)) {
            throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during insert");
        }

        if (handle.getBucketProperty().isPresent() && isCreateEmptyBucketFiles(session)) {
            List<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(session, handle, table, false, partitionUpdates);
            // replace partitionUpdates before creating the empty files so that those files will be cleaned up if we end up rollback
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.getPartitionColumns().isEmpty() ? Optional.empty() : Optional.of(buildPartitionObject(session, table, partitionUpdate));
                if (handle.isTransactional() && partition.isPresent()) {
                    PartitionStatistics statistics = PartitionStatistics.builder().setBasicStatistics(partitionUpdate.getStatistics()).build();
                    metastore.addPartition(session, handle.getSchemaName(), handle.getTableName(), partition.get(), partitionUpdate.getWritePath(), statistics);
                }
                createEmptyFiles(session, partitionUpdate.getWritePath(), table, partition, partitionUpdate.getFileNames());
            }
        }

        List<String> partitionedBy = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        Map<String, Type> columnTypes = handle.getInputColumns().stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager)));
        Map<List<String>, ComputedStatistics> partitionComputedStatistics = createComputedStatisticsToPartitionMap(computedStatistics, partitionedBy, columnTypes);

        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            if (partitionUpdate.getName().isEmpty()) {
                // insert into unpartitioned table
                if (!table.getStorage().getStorageFormat().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && isRespectTableFormat(session)) {
                    throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during insert");
                }

                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, ImmutableList.of()));

                if (partitionUpdate.getUpdateMode() == OVERWRITE) {
                    // get privileges from existing table
                    PrincipalPrivileges principalPrivileges = fromHivePrivilegeInfos(metastore.listTablePrivileges(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName(), Optional.empty()));

                    // first drop it
                    metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());

                    // create the table with the new location
                    metastore.createTable(session, table, principalPrivileges, Optional.of(partitionUpdate.getWritePath()), false, partitionStatistics);
                }
                else if (partitionUpdate.getUpdateMode() == NEW || partitionUpdate.getUpdateMode() == APPEND) {
                    // insert into unpartitioned table
                    metastore.finishInsertIntoExistingTable(
                            session,
                            handle.getSchemaName(),
                            handle.getTableName(),
                            partitionUpdate.getWritePath(),
                            partitionUpdate.getFileNames(),
                            partitionStatistics);
                }
                else {
                    throw new IllegalArgumentException("Unsupported update mode: " + partitionUpdate.getUpdateMode());
                }
            }
            else if (partitionUpdate.getUpdateMode() == APPEND) {
                // insert into existing partition
                List<String> partitionValues = toPartitionValues(partitionUpdate.getName());
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, partitionValues));
                metastore.finishInsertIntoExistingPartition(
                        session,
                        handle.getSchemaName(),
                        handle.getTableName(),
                        partitionValues,
                        partitionUpdate.getWritePath(),
                        partitionUpdate.getFileNames(),
                        partitionStatistics);
            }
            else if (partitionUpdate.getUpdateMode() == NEW || partitionUpdate.getUpdateMode() == OVERWRITE) {
                // insert into new partition or overwrite existing partition
                Partition partition = buildPartitionObject(session, table, partitionUpdate);
                if (!partition.getStorage().getStorageFormat().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && isRespectTableFormat(session)) {
                    throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Partition format changed during insert");
                }
                if (partitionUpdate.getUpdateMode() == OVERWRITE) {
                    metastore.dropPartition(session, handle.getSchemaName(), handle.getTableName(), partition.getValues(), true);
                }
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, partition.getValues()));
                metastore.addPartition(session, handle.getSchemaName(), handle.getTableName(), partition, partitionUpdate.getWritePath(), partitionStatistics);
            }
            else {
                throw new IllegalArgumentException(format("Unsupported update mode: %s", partitionUpdate.getUpdateMode()));
            }
        }

        if (isFullAcidTable(table.getParameters())) {
            HdfsContext context = new HdfsContext(session);
            for (PartitionUpdate update : partitionUpdates) {
                long writeId = handle.getTransaction().getWriteId();
                Path deltaDirectory = new Path(format("%s/%s/%s", table.getStorage().getLocation(), update.getName(), deltaSubdir(writeId, writeId, 0)));
                createOrcAcidVersionFile(context, deltaDirectory);
            }
        }

        return Optional.of(new HiveWrittenPartitions(
                partitionUpdates.stream()
                        .map(PartitionUpdate::getName)
                        .collect(toImmutableList())));
    }

    private void createOrcAcidVersionFile(HdfsContext context, Path deltaDirectory)
    {
        try {
            FileSystem fs = hdfsEnvironment.getFileSystem(context, deltaDirectory);
            writeVersionFile(deltaDirectory, fs);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Exception writing _orc_acid_version file for deltaDirectory " + deltaDirectory, e);
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
                        .put(PRESTO_VERSION_NAME, prestoVersion)
                        .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                        .build())
                .withStorage(storage -> storage
                        .setStorageFormat(isRespectTableFormat(session) ?
                                table.getStorage().getStorageFormat() :
                                fromHiveStorageFormat(getHiveStorageFormat(session)))
                        .setLocation(partitionUpdate.getTargetPath().toString())
                        .setBucketProperty(table.getStorage().getBucketProperty())
                        .setSerdeParameters(table.getStorage().getSerdeParameters()))
                .build();
    }

    private PartitionStatistics createPartitionStatistics(
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

    private PartitionStatistics createPartitionStatistics(
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

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        HiveIdentity identity = new HiveIdentity(session);
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(TABLE_COMMENT, PRESTO_VIEW_COMMENT)
                .put(PRESTO_VIEW_FLAG, "true")
                .put(TRINO_CREATED_BY, "Trino Hive connector")
                .put(PRESTO_VERSION_NAME, prestoVersion)
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .build();

        Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty());

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(viewName.getSchemaName())
                .setTableName(viewName.getTableName())
                .setOwner(session.getUser())
                .setTableType(TableType.VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(dummyColumn))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(properties)
                .setViewOriginalText(Optional.of(encodeViewData(definition)))
                .setViewExpandedText(Optional.of(PRESTO_VIEW_EXPANDED_TEXT_MARKER));

        tableBuilder.getStorageBuilder()
                .setStorageFormat(VIEW_STORAGE_FORMAT)
                .setLocation("");
        Table table = tableBuilder.build();
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(session.getUser());

        Optional<Table> existing = metastore.getTable(identity, viewName.getSchemaName(), viewName.getTableName());
        if (existing.isPresent()) {
            if (!replace || !isPrestoView(existing.get())) {
                throw new ViewAlreadyExistsException(viewName);
            }

            metastore.replaceTable(identity, viewName.getSchemaName(), viewName.getTableName(), table, principalPrivileges);
            return;
        }

        try {
            metastore.createTable(session, table, principalPrivileges, Optional.empty(), false, new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of()));
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        // Not checking if source view exists as this is already done in RenameViewTask
        metastore.renameTable(new HiveIdentity(session), source.getSchemaName(), source.getTableName(), target.getSchemaName(), target.getTableName());
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
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (String tableName : metastore.getAllViews(schemaName)) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, CatalogSchemaName schemaName)
    {
        checkState(!isHiveSystemSchema(schemaName.getSchemaName()), "Schema is not accessible: %s", schemaName);

        Optional<Database> db = metastore.getDatabase(schemaName.getSchemaName());
        if (db.isPresent()) {
            return HiveSchemaProperties.fromDatabase(db.get());
        }

        throw new SchemaNotFoundException(schemaName.getSchemaName());
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, CatalogSchemaName schemaName)
    {
        checkState(!isHiveSystemSchema(schemaName.getSchemaName()), "Schema is not accessible: %s", schemaName);

        Optional<Database> database = metastore.getDatabase(schemaName.getSchemaName());
        if (database.isPresent()) {
            return database.flatMap(db -> Optional.of(new TrinoPrincipal(db.getOwnerType(), db.getOwnerName())));
        }

        throw new SchemaNotFoundException(schemaName.getSchemaName());
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        for (SchemaTableName name : listViews(session, schemaName)) {
            try {
                getView(session, name).ifPresent(view -> views.put(name, view));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(HIVE_VIEW_TRANSLATION_ERROR.toErrorCode())) {
                    // Ignore hive views for which translation fails
                }
                else if (e.getErrorCode().equals(TABLE_NOT_FOUND.toErrorCode())) {
                    // Ignore view that was dropped during query execution (race condition)
                }
                else {
                    throw e;
                }
            }
        }
        return views.build();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        if (isHiveSystemSchema(viewName.getSchemaName())) {
            return Optional.empty();
        }
        return metastore.getTable(new HiveIdentity(session), viewName.getSchemaName(), viewName.getTableName())
                .filter(ViewReaderUtil::canDecodeView)
                .map(view -> {
                    if (!translateHiveViews && !isPrestoView(view)) {
                        throw new HiveViewNotSupportedException(viewName);
                    }

                    ConnectorViewDefinition definition = createViewReader(metastore, session, view, typeManager)
                            .decodeViewData(view.getViewOriginalText().get(), view, catalogName);
                    // use owner from table metadata if it exists
                    if (view.getOwner() != null && !definition.isRunAsInvoker()) {
                        definition = new ConnectorViewDefinition(
                                definition.getOriginalSql(),
                                definition.getCatalog(),
                                definition.getSchema(),
                                definition.getColumns(),
                                definition.getComment(),
                                Optional.of(view.getOwner()),
                                false);
                    }
                    return definition;
                });
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = handle.getSchemaTableName();

        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        ensureTableSupportsDelete(table);

        LocationHandle locationHandle = locationService.forExistingTable(metastore, session, table);

        AcidTransaction transaction = metastore.beginDelete(session, table);

        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.declareIntentionToWrite(session, writeInfo.getWriteMode(), writeInfo.getWritePath(), handle.getSchemaTableName());

        return handle.withTransaction(transaction);
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        checkArgument(handle.isAcidDelete(), "handle should be a delete handle, but is %s", handle);

        requireNonNull(fragments, "fragments is null");

        SchemaTableName tableName = handle.getSchemaTableName();
        HiveIdentity identity = new HiveIdentity(session);
        Table table = metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        ensureTableSupportsDelete(table);

        List<PartitionAndStatementId> partitionAndStatementIds = fragments.stream()
                .map(Slice::getBytes)
                .map(PartitionAndStatementId.CODEC::fromJson)
                .collect(toList());

        HdfsContext context = new HdfsContext(session);
        for (PartitionAndStatementId ps : partitionAndStatementIds) {
            createOrcAcidVersionFile(context, new Path(ps.getDeleteDeltaDirectory()));
        }

        LocationHandle locationHandle = locationService.forExistingTable(metastore, session, table);
        WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.finishRowLevelDelete(session, table.getDatabaseName(), table.getTableName(), writeInfo.getWritePath(), partitionAndStatementIds);
    }

    private void ensureTableSupportsDelete(Table table)
    {
        if (table.getParameters().isEmpty() || !isFullAcidTable(table.getParameters())) {
            throw new TrinoException(NOT_SUPPORTED, "Deletes must match whole partitions for non-transactional tables");
        }
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return HiveColumnHandle.getDeleteRowIdColumnHandle();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        HiveTableHandle table = (HiveTableHandle) tableHandle;
        return updateRowIdColumnHandle(table.getDataColumns(), updatedColumns);
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

        Optional<Table> table = metastore.getTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName());
        if (table.isEmpty()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }

        if (table.get().getPartitionColumns().isEmpty()) {
            metastore.truncateUnpartitionedTable(session, handle.getSchemaName(), handle.getTableName());
        }
        else {
            for (HivePartition hivePartition : partitionManager.getOrLoadPartitions(metastore, new HiveIdentity(session), handle)) {
                metastore.dropPartition(session, handle.getSchemaName(), handle.getTableName(), toPartitionValues(hivePartition.getPartitionId()), true);
            }
        }
        // it is too expensive to determine the exact number of deleted rows
        return OptionalLong.empty();
    }

    @VisibleForTesting
    static Predicate<Map<ColumnHandle, NullableValue>> convertToPredicate(TupleDomain<ColumnHandle> tupleDomain)
    {
        return bindings -> tupleDomain.contains(TupleDomain.fromFixedValues(bindings));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) table;

        List<ColumnHandle> partitionColumns = ImmutableList.copyOf(hiveTable.getPartitionColumns());
        List<HivePartition> partitions = partitionManager.getOrLoadPartitions(metastore, new HiveIdentity(session), hiveTable);

        TupleDomain<ColumnHandle> predicate = createPredicate(partitionColumns, partitions);

        Optional<DiscretePredicates> discretePredicates = Optional.empty();
        if (!partitionColumns.isEmpty()) {
            // Do not create tuple domains for every partition at the same time!
            // There can be a huge number of partitions so use an iterable so
            // all domains do not need to be in memory at the same time.
            Iterable<TupleDomain<ColumnHandle>> partitionDomains = Iterables.transform(partitions, (hivePartition) -> TupleDomain.fromFixedValues(hivePartition.getKeys()));
            discretePredicates = Optional.of(new DiscretePredicates(partitionColumns, partitionDomains));
        }

        Optional<ConnectorTablePartitioning> tablePartitioning = Optional.empty();
        List<LocalProperty<ColumnHandle>> sortingProperties = ImmutableList.of();
        if (hiveTable.getBucketHandle().isPresent()) {
            if (isPropagateTableScanSortingProperties(session) && !hiveTable.getBucketHandle().get().getSortedBy().isEmpty()) {
                // Populating SortingProperty guarantees to the engine that it is reading pre-sorted input.
                // We detect compatibility between table and partition level sorted_by properties
                // and fail the query if there is a mismatch in HiveSplitManager#getPartitionMetadata.
                // This can lead to incorrect results if a sorted_by property is defined over unsorted files.
                Map<String, ColumnHandle> columnHandles = getColumnHandles(session, table);
                sortingProperties = hiveTable.getBucketHandle().get().getSortedBy().stream()
                        .map(sortingColumn -> new SortingProperty<>(
                                columnHandles.get(sortingColumn.getColumnName()),
                                sortingColumn.getOrder().getSortOrder()))
                        .collect(toImmutableList());
            }
            if (isBucketExecutionEnabled(session)) {
                tablePartitioning = hiveTable.getBucketHandle().map(bucketing -> new ConnectorTablePartitioning(
                        new HivePartitioningHandle(
                                bucketing.getBucketingVersion(),
                                bucketing.getReadBucketCount(),
                                bucketing.getColumns().stream()
                                        .map(HiveColumnHandle::getHiveType)
                                        .collect(toImmutableList()),
                                OptionalInt.empty(),
                                false),
                        bucketing.getColumns().stream()
                                .map(ColumnHandle.class::cast)
                                .collect(toImmutableList())));
            }
        }

        return new ConnectorTableProperties(
                predicate,
                tablePartitioning,
                Optional.empty(),
                discretePredicates,
                sortingProperties);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        checkArgument(handle.getAnalyzePartitionValues().isEmpty() || constraint.getSummary().isAll(), "Analyze should not have a constraint");

        HivePartitionResult partitionResult = partitionManager.getPartitions(metastore, new HiveIdentity(session), handle, constraint);
        HiveTableHandle newHandle = partitionManager.applyPartitionResult(handle, partitionResult, constraint.getPredicateColumns());

        if (handle.getPartitions().equals(newHandle.getPartitions()) &&
                handle.getCompactEffectivePredicate().equals(newHandle.getCompactEffectivePredicate()) &&
                handle.getBucketFilter().equals(newHandle.getBucketFilter()) &&
                handle.getConstraintColumns().equals(newHandle.getConstraintColumns())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(newHandle, partitionResult.getUnenforcedConstraint(), false));
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        if (HiveSessionProperties.isQueryPartitionFilterRequired(session) && handle.getAnalyzePartitionValues().isEmpty() && handle.getEnforcedConstraint().isAll()) {
            List<HiveColumnHandle> partitionColumns = handle.getPartitionColumns();
            if (!partitionColumns.isEmpty()) {
                Optional<Set<ColumnHandle>> referencedColumns = handle.getConstraintColumns();
                if (referencedColumns.isEmpty() || Collections.disjoint(referencedColumns.get(), partitionColumns)) {
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
                .collect(toImmutableMap(Function.identity(), HiveApplyProjectionUtil::createProjectedColumnRepresentation));

        HiveTableHandle hiveTableHandle = (HiveTableHandle) handle;
        // all references are simple variables
        if (columnProjections.values().stream().allMatch(ProjectedColumnRepresentation::isVariable)) {
            Set<ColumnHandle> projectedColumns = ImmutableSet.copyOf(assignments.values());
            if (hiveTableHandle.getProjectedColumns().isPresent()
                    && hiveTableHandle.getProjectedColumns().get().equals(projectedColumns)) {
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
        ImmutableSet.Builder<ColumnHandle> projectedColumnsBuilder = ImmutableSet.builder();

        for (Map.Entry<ConnectorExpression, ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ProjectedColumnRepresentation projectedColumn = entry.getValue();

            ColumnHandle projectedColumnHandle;
            String projectedColumnName;

            // See if input already contains a columnhandle for this projected column, avoid creating duplicates.
            Optional<String> existingColumn = find(assignments, projectedColumn);

            if (existingColumn.isPresent()) {
                projectedColumnName = existingColumn.get();
                projectedColumnHandle = assignments.get(projectedColumnName);
            }
            else {
                // Create a new column handle
                HiveColumnHandle oldColumnHandle = (HiveColumnHandle) assignments.get(projectedColumn.getVariable().getName());
                projectedColumnHandle = createProjectedColumnHandle(oldColumnHandle, projectedColumn.getDereferenceIndices());
                projectedColumnName = ((HiveColumnHandle) projectedColumnHandle).getName();
            }

            Variable projectedColumnVariable = new Variable(projectedColumnName, expression.getType());
            Assignment newAssignment = new Assignment(projectedColumnName, projectedColumnHandle, expression.getType());
            newAssignments.put(projectedColumnName, newAssignment);

            newVariablesBuilder.put(expression, projectedColumnVariable);
            projectedColumnsBuilder.add(projectedColumnHandle);
        }

        // Modify projections to refer to new variables
        Map<ConnectorExpression, Variable> newVariables = newVariablesBuilder.build();
        List<ConnectorExpression> newProjections = projections.stream()
                .map(expression -> replaceWithNewVariables(expression, newVariables))
                .collect(toImmutableList());

        List<Assignment> outputAssignments = newAssignments.values().stream().collect(toImmutableList());
        return Optional.of(new ProjectionApplicationResult<>(
                hiveTableHandle.withProjectedColumns(projectedColumnsBuilder.build()),
                newProjections,
                outputAssignments,
                false));
    }

    private HiveColumnHandle createProjectedColumnHandle(HiveColumnHandle column, List<Integer> indices)
    {
        HiveType oldHiveType = column.getHiveType();
        HiveType newHiveType = oldHiveType.getHiveTypeForDereferences(indices).get();

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
                        .addAll(oldHiveType.getHiveDereferenceNames(indices))
                        .build(),
                newHiveType,
                newHiveType.getType(typeManager));

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
        ImmutableList<HiveType> bucketTypes = bucketHandle.getColumns().stream().map(HiveColumnHandle::getHiveType).collect(toImmutableList());
        checkArgument(
                hivePartitioningHandle.getHiveTypes().equals(bucketTypes),
                "Types from the new PartitioningHandle (%s) does not match the TableHandle (%s)",
                hivePartitioningHandle.getHiveTypes(),
                bucketTypes);
        int largerBucketCount = Math.max(bucketHandle.getTableBucketCount(), hivePartitioningHandle.getBucketCount());
        int smallerBucketCount = Math.min(bucketHandle.getTableBucketCount(), hivePartitioningHandle.getBucketCount());
        checkArgument(
                largerBucketCount % smallerBucketCount == 0 && Integer.bitCount(largerBucketCount / smallerBucketCount) == 1,
                "The requested partitioning is not a valid alternative for the table layout");

        return new HiveTableHandle(
                hiveTable.getSchemaName(),
                hiveTable.getTableName(),
                hiveTable.getTableParameters(),
                hiveTable.getPartitionColumns(),
                hiveTable.getDataColumns(),
                hiveTable.getPartitions(),
                hiveTable.getCompactEffectivePredicate(),
                hiveTable.getEnforcedConstraint(),
                Optional.of(new HiveBucketHandle(
                        bucketHandle.getColumns(),
                        bucketHandle.getBucketingVersion(),
                        bucketHandle.getTableBucketCount(),
                        hivePartitioningHandle.getBucketCount(),
                        bucketHandle.getSortedBy())),
                hiveTable.getBucketFilter(),
                hiveTable.getAnalyzePartitionValues(),
                hiveTable.getAnalyzeColumnNames(),
                Optional.empty(),
                Optional.empty(), // Projected columns is used only during optimization phase of planning
                hiveTable.getTransaction());
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
    public Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        if (table.getStorage().getBucketProperty().isPresent()) {
            if (bucketedOnTimestamp(table.getStorage().getBucketProperty().get(), table)) {
                throw new TrinoException(NOT_SUPPORTED, "Writing to tables bucketed on timestamp not supported");
            }
        }
        // treat un-bucketed transactional table as having a single bucket on no columns
        else if (hiveTableHandle.isInAcidTransaction()) {
            table = Table.builder(table)
                    .withStorage(storage -> storage.setBucketProperty(Optional.of(
                            new HiveBucketProperty(ImmutableList.of(), HiveBucketing.BucketingVersion.BUCKETING_V2, 1, ImmutableList.of()))))
                    .build();
        }

        Optional<HiveBucketHandle> hiveBucketHandle = getHiveBucketHandle(session, table, typeManager);
        List<Column> partitionColumns = table.getPartitionColumns();
        if (hiveBucketHandle.isEmpty()) {
            // return preferred layout which is partitioned by partition columns
            if (partitionColumns.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new ConnectorNewTableLayout(
                    partitionColumns.stream()
                            .map(Column::getName)
                            .collect(toImmutableList())));
        }
        HiveBucketProperty bucketProperty = table.getStorage().getBucketProperty()
                .orElseThrow(() -> new NoSuchElementException("Bucket property should be set"));
        if (!bucketProperty.getSortedBy().isEmpty() && !isSortedWritingEnabled(session)) {
            throw new TrinoException(NOT_SUPPORTED, "Writing to bucketed sorted Hive tables is disabled");
        }

        ImmutableList.Builder<String> partitioningColumns = ImmutableList.builder();
        hiveBucketHandle.get().getColumns().stream()
                .map(HiveColumnHandle::getName)
                .forEach(partitioningColumns::add);
        partitionColumns.stream()
                .map(Column::getName)
                .forEach(partitioningColumns::add);

        HivePartitioningHandle partitioningHandle = new HivePartitioningHandle(
                hiveBucketHandle.get().getBucketingVersion(),
                hiveBucketHandle.get().getTableBucketCount(),
                hiveBucketHandle.get().getColumns().stream()
                        .map(HiveColumnHandle::getHiveType)
                        .collect(toList()),
                OptionalInt.of(hiveBucketHandle.get().getTableBucketCount()),
                !partitionColumns.isEmpty() && isParallelPartitionedBucketedWrites(session));
        return Optional.of(new ConnectorNewTableLayout(partitioningHandle, partitioningColumns.build()));
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        validateTimestampColumns(tableMetadata.getColumns(), getTimestampPrecision(session));
        validatePartitionColumns(tableMetadata);
        validateBucketColumns(tableMetadata);
        validateColumns(tableMetadata);
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        if (bucketProperty.isEmpty()) {
            // return preferred layout which is partitioned by partition columns
            if (partitionedBy.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new ConnectorNewTableLayout(partitionedBy));
        }
        if (!bucketProperty.get().getSortedBy().isEmpty() && !isSortedWritingEnabled(session)) {
            throw new TrinoException(NOT_SUPPORTED, "Writing to bucketed sorted Hive tables is disabled");
        }

        List<String> bucketedBy = bucketProperty.get().getBucketedBy();
        Map<String, HiveType> hiveTypeMap = tableMetadata.getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> toHiveType(column.getType())));
        return Optional.of(new ConnectorNewTableLayout(
                new HivePartitioningHandle(
                        bucketProperty.get().getBucketingVersion(),
                        bucketProperty.get().getBucketCount(),
                        bucketedBy.stream()
                                .map(hiveTypeMap::get)
                                .collect(toImmutableList()),
                        OptionalInt.of(bucketProperty.get().getBucketCount()),
                        !partitionedBy.isEmpty() && isParallelPartitionedBucketedWrites(session)),
                ImmutableList.<String>builder()
                        .addAll(bucketedBy)
                        .addAll(partitionedBy)
                        .build()));
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!isCollectColumnStatisticsOnWrite(session)) {
            return TableStatisticsMetadata.empty();
        }
        if (isTransactional(tableMetadata.getProperties()).orElse(false)) {
            // TODO(https://github.com/trinodb/trino/issues/1956) updating table statistics for trasactional not supported right now.
            return TableStatisticsMetadata.empty();
        }
        List<String> partitionedBy = firstNonNull(getPartitionedBy(tableMetadata.getProperties()), ImmutableList.of());
        return getStatisticsCollectionMetadata(tableMetadata.getColumns(), partitionedBy, Optional.empty(), false);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        List<String> partitionedBy = firstNonNull(getPartitionedBy(tableMetadata.getProperties()), ImmutableList.of());
        return getStatisticsCollectionMetadata(tableMetadata.getColumns(), partitionedBy, getAnalyzeColumns(tableMetadata.getProperties()), true);
    }

    private TableStatisticsMetadata getStatisticsCollectionMetadata(List<ColumnMetadata> columns, List<String> partitionedBy, Optional<Set<String>> analyzeColumns, boolean includeRowCount)
    {
        Set<ColumnStatisticMetadata> columnStatistics = columns.stream()
                .filter(column -> !partitionedBy.contains(column.getName()))
                .filter(column -> !column.isHidden())
                .filter(column -> analyzeColumns.isEmpty() || analyzeColumns.get().contains(column.getName()))
                .map(this::getColumnStatisticMetadata)
                .flatMap(List::stream)
                .collect(toImmutableSet());

        Set<TableStatisticType> tableStatistics = includeRowCount ? ImmutableSet.of(ROW_COUNT) : ImmutableSet.of();
        return new TableStatisticsMetadata(columnStatistics, tableStatistics, partitionedBy);
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(ColumnMetadata columnMetadata)
    {
        return getColumnStatisticMetadata(columnMetadata.getName(), metastore.getSupportedColumnStatistics(columnMetadata.getType()));
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(String columnName, Set<ColumnStatisticType> statisticTypes)
    {
        return statisticTypes.stream()
                .map(type -> new ColumnStatisticMetadata(columnName, type))
                .collect(toImmutableList());
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
    public Set<RoleGrant> listAllRoleGrants(ConnectorSession session, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit)
    {
        return ImmutableSet.copyOf(accessControlMetadata.listAllRoleGrants(session, roles, grantees, limit));
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
        StorageFormat storageFormat = table.getStorage().getStorageFormat();
        String outputFormat = storageFormat.getOutputFormat();
        String serde = storageFormat.getSerDe();

        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerDe().equals(serde)) {
                return format;
            }
        }
        throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, format("Output format %s with SerDe %s is not supported", outputFormat, serde));
    }

    private static void validateBucketColumns(ConnectorTableMetadata tableMetadata)
    {
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        if (bucketProperty.isEmpty()) {
            return;
        }
        Set<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());

        List<String> bucketedBy = bucketProperty.get().getBucketedBy();
        if (!allColumns.containsAll(bucketedBy)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Bucketing columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(bucketedBy), allColumns)));
        }

        Set<String> partitionColumns = ImmutableSet.copyOf(getPartitionedBy(tableMetadata.getProperties()));
        if (bucketedBy.stream().anyMatch(partitionColumns::contains)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Bucketing columns %s are also used as partitioning columns", Sets.intersection(ImmutableSet.copyOf(bucketedBy), partitionColumns)));
        }

        List<String> sortedBy = bucketProperty.get().getSortedBy().stream()
                .map(SortingColumn::getColumnName)
                .collect(toImmutableList());
        if (!allColumns.containsAll(sortedBy)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Sorting columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(sortedBy), allColumns)));
        }

        if (sortedBy.stream().anyMatch(partitionColumns::contains)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Sorting columns %s are also used as partitioning columns", Sets.intersection(ImmutableSet.copyOf(sortedBy), partitionColumns)));
        }
    }

    private static void validatePartitionColumns(ConnectorTableMetadata tableMetadata)
    {
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

        List<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toList());

        if (!allColumns.containsAll(partitionedBy)) {
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
        // Validate types are supported
        for (ColumnMetadata column : tableMetadata.getColumns()) {
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

    private static void validateTimestampColumns(List<ColumnMetadata> columns, HiveTimestampPrecision precision)
    {
        for (ColumnMetadata column : columns) {
            validateTimestampTypes(column.getType(), precision);
        }
    }

    private static void validateTimestampTypes(Type type, HiveTimestampPrecision precision)
    {
        if (type instanceof TimestampType) {
            if (((TimestampType) type).getPrecision() != precision.getPrecision()) {
                throw new TrinoException(NOT_SUPPORTED, format("Incorrect timestamp precision for %s; the configured precision is %s", type, precision));
            }
        }
        else if (type instanceof ArrayType) {
            validateTimestampTypes(((ArrayType) type).getElementType(), precision);
        }
        else if (type instanceof MapType) {
            validateTimestampTypes(((MapType) type).getKeyType(), precision);
            validateTimestampTypes(((MapType) type).getValueType(), precision);
        }
        else if (type instanceof RowType) {
            for (Type fieldType : ((RowType) type).getTypeParameters()) {
                validateTimestampTypes(fieldType, precision);
            }
        }
    }

    private static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter(Table table)
    {
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        table.getPartitionColumns().stream().map(Column::getName).forEach(columnNames::add);
        table.getDataColumns().stream().map(Column::getName).forEach(columnNames::add);
        List<String> allColumnNames = columnNames.build();
        if (allColumnNames.size() > Sets.newHashSet(allColumnNames).size()) {
            throw new TrinoException(HIVE_INVALID_METADATA,
                    format("Hive metadata for table %s is invalid: Table descriptor contains duplicate columns", table.getTableName()));
        }

        List<Column> tableColumns = table.getDataColumns();
        ImmutableMap.Builder<String, Optional<String>> builder = ImmutableMap.builder();
        for (Column field : concat(tableColumns, table.getPartitionColumns())) {
            if (field.getComment().isPresent() && !field.getComment().get().equals("from deserializer")) {
                builder.put(field.getName(), field.getComment());
            }
            else {
                builder.put(field.getName(), Optional.empty());
            }
        }

        // add hidden columns
        builder.put(PATH_COLUMN_NAME, Optional.empty());
        if (table.getStorage().getBucketProperty().isPresent()) {
            builder.put(BUCKET_COLUMN_NAME, Optional.empty());
        }
        builder.put(FILE_SIZE_COLUMN_NAME, Optional.empty());
        builder.put(FILE_MODIFIED_TIME_COLUMN_NAME, Optional.empty());
        if (!table.getPartitionColumns().isEmpty()) {
            builder.put(PARTITION_COLUMN_NAME, Optional.empty());
        }

        if (isFullAcidTable(table.getParameters())) {
            for (String name : AcidSchema.ACID_COLUMN_NAMES) {
                builder.put(name, Optional.empty());
            }
        }

        Map<String, Optional<String>> columnComment = builder.build();

        return handle -> ColumnMetadata.builder()
                .setName(handle.getName())
                .setType(handle.getType())
                .setComment(columnComment.get(handle.getName()))
                .setExtraInfo(Optional.ofNullable(columnExtraInfo(handle.isPartitionKey())))
                .setHidden(handle.isHidden())
                .build();
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
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        hiveMaterializedViewMetadata.createMaterializedView(session, viewName, definition, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        hiveMaterializedViewMetadata.dropMaterializedView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMaterializedViewMetadata.listMaterializedViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMaterializedViewMetadata.getMaterializedViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return hiveMaterializedViewMetadata.getMaterializedView(session, viewName);
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name)
    {
        return hiveMaterializedViewMetadata.getMaterializedViewFreshness(session, name);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return hiveMaterializedViewMetadata.delegateMaterializedViewRefreshToConnector(session, viewName);
    }

    @Override
    public CompletableFuture<?> refreshMaterializedView(ConnectorSession session, SchemaTableName name)
    {
        return hiveMaterializedViewMetadata.refreshMaterializedView(session, name);
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
}
