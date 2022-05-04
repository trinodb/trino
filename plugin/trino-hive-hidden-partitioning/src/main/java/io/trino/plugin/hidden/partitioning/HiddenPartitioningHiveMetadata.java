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
package io.trino.plugin.hidden.partitioning;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveMaterializedViewMetadata;
import io.trino.plugin.hive.HiveMetadata;
import io.trino.plugin.hive.HiveOutputTableHandle;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.HivePartitionResult;
import io.trino.plugin.hive.HiveRedirectionsProvider;
import io.trino.plugin.hive.HiveSessionProperties;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveTableRedirectionsProvider;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.LocationHandle;
import io.trino.plugin.hive.LocationService;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.PartitionUpdate;
import io.trino.plugin.hive.SystemTableProvider;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.security.AccessControlMetadata;
import io.trino.plugin.hive.statistics.HiveStatisticsProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.iceberg.PartitionSpec;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static io.trino.plugin.hidden.partitioning.HiddenPartitioningSessionProperties.isPartitionSpecEnabled;
import static io.trino.plugin.hidden.partitioning.HiddenPartitioningSpecParser.validateSpecJson;
import static io.trino.plugin.hidden.partitioning.HiddenPartitioningTableProperties.PARTITION_SPEC_PROPERTY;
import static io.trino.plugin.hidden.partitioning.HiddenPartitioningTableProperties.getPartitionSpecJson;
import static io.trino.plugin.hidden.partitioning.HiddenPartitioningUtil.toIcebergPartitionSpec;
import static io.trino.plugin.hidden.partitioning.HiddenPartitioningUtil.transformFilteredPartitionResult;
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
import static io.trino.plugin.hive.HiveErrorCode.HIVE_COLUMN_ORDER_MISMATCH;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_TIMEZONE_MISMATCH;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveSessionProperties.isRespectTableFormat;
import static io.trino.plugin.hive.HiveTableProperties.ANALYZE_COLUMNS_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.AVRO_SCHEMA_URL;
import static io.trino.plugin.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.CSV_ESCAPE;
import static io.trino.plugin.hive.HiveTableProperties.CSV_QUOTE;
import static io.trino.plugin.hive.HiveTableProperties.CSV_SEPARATOR;
import static io.trino.plugin.hive.HiveTableProperties.EXTERNAL_LOCATION_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.ORC_BLOOM_FILTER_COLUMNS;
import static io.trino.plugin.hive.HiveTableProperties.ORC_BLOOM_FILTER_FPP;
import static io.trino.plugin.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.SKIP_FOOTER_LINE_COUNT;
import static io.trino.plugin.hive.HiveTableProperties.SKIP_HEADER_LINE_COUNT;
import static io.trino.plugin.hive.HiveTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.TEXTFILE_FIELD_SEPARATOR;
import static io.trino.plugin.hive.HiveTableProperties.TEXTFILE_FIELD_SEPARATOR_ESCAPE;
import static io.trino.plugin.hive.HiveTableProperties.getAvroSchemaUrl;
import static io.trino.plugin.hive.HiveTableProperties.getBucketProperty;
import static io.trino.plugin.hive.HiveTableProperties.getExternalLocation;
import static io.trino.plugin.hive.HiveTableProperties.getFooterSkipCount;
import static io.trino.plugin.hive.HiveTableProperties.getHeaderSkipCount;
import static io.trino.plugin.hive.HiveTableProperties.getHiveStorageFormat;
import static io.trino.plugin.hive.HiveTableProperties.getOrcBloomFilterColumns;
import static io.trino.plugin.hive.HiveTableProperties.getOrcBloomFilterFpp;
import static io.trino.plugin.hive.HiveTableProperties.getPartitionedBy;
import static io.trino.plugin.hive.HiveTableProperties.getSingleCharacterProperty;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.HiveUtil.columnExtraInfo;
import static io.trino.plugin.hive.util.HiveUtil.hiveColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.verifyPartitionTypeSupported;
import static io.trino.plugin.hive.util.HiveWriteUtils.isS3FileSystem;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;

public class HiddenPartitioningHiveMetadata
        extends HiveMetadata
{
    public static final String PARTITION_SPEC_KEY = "trino_partition_spec";

    private static final String TRANSACTIONAL = "transactional";

    private static final String ORC_BLOOM_FILTER_COLUMNS_KEY = "orc.bloom.filter.columns";
    private static final String ORC_BLOOM_FILTER_FPP_KEY = "orc.bloom.filter.fpp";

    private static final String TEXT_FIELD_SEPARATOR_KEY = serdeConstants.FIELD_DELIM;
    private static final String TEXT_FIELD_SEPARATOR_ESCAPE_KEY = serdeConstants.ESCAPE_CHAR;

    private static final String CSV_SEPARATOR_KEY = OpenCSVSerde.SEPARATORCHAR;
    private static final String CSV_QUOTE_KEY = OpenCSVSerde.QUOTECHAR;
    private static final String CSV_ESCAPE_KEY = OpenCSVSerde.ESCAPECHAR;

    private final SemiTransactionalHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final boolean translateHiveViews;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final String prestoVersion;

    public HiddenPartitioningHiveMetadata(
            CatalogName catalogName,
            SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            DateTimeZone timeZone,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean hideDeltaLakeTables,
            boolean translateHiveViews,
            TypeManager typeManager,
            MetadataProvider metadataProvider,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            String trinoVersion,
            HiveStatisticsProvider hiveStatisticsProvider,
            HiveRedirectionsProvider hiveRedirectionsProvider,
            Set<SystemTableProvider> systemTableProviders,
            HiveMaterializedViewMetadata hiveMaterializedViewMetadata,
            AccessControlMetadata accessControlMetadata,
            HiveTableRedirectionsProvider hiveTableRedirectionsProvider,
            boolean autocommit,
            DirectoryLister directoryLister)
    {
        super(
                catalogName,
                metastore,
                autocommit,
                hdfsEnvironment,
                partitionManager,
                writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled,
                translateHiveViews,
                hideDeltaLakeTables,
                typeManager,
                metadataProvider,
                locationService,
                partitionUpdateCodec,
                trinoVersion,
                hiveStatisticsProvider,
                hiveRedirectionsProvider,
                systemTableProviders,
                hiveMaterializedViewMetadata,
                accessControlMetadata,
                hiveTableRedirectionsProvider,
                directoryLister);
        this.metastore = metastore;
        this.hdfsEnvironment = hdfsEnvironment;
        this.partitionManager = partitionManager;
        this.timeZone = timeZone;
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.translateHiveViews = translateHiveViews;
        this.typeManager = typeManager;
        this.locationService = locationService;
        this.prestoVersion = trinoVersion;
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
                                .buildOrThrow(),
                        tableMetadata.getComment()))
                .orElse(tableMetadata);
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

        List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy));
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        Map<String, String> tableProperties = getEmptyTableProperties(tableMetadata, bucketProperty, new HdfsEnvironment.HdfsContext(session));

        hiveStorageFormat.validateColumns(columnHandles);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new Column(column.getName(), column.getHiveType(), column.getComment()))
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
            targetPath = getExternalPath(new HdfsEnvironment.HdfsContext(session), externalLocation);
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
        PrincipalPrivileges principalPrivileges = buildInitialPrivilegeSet(table.getOwner().get());
        HiveBasicStatistics basicStatistics = table.getPartitionColumns().isEmpty() ? createZeroStatistics() : createEmptyStatistics();
        metastore.createTable(
                session,
                table,
                principalPrivileges,
                Optional.empty(),
                Optional.empty(),
                ignoreExisting,
                new PartitionStatistics(basicStatistics, ImmutableMap.of()),
                false);
    }

    private String validateAndNormalizeAvroSchemaUrl(String url, HdfsEnvironment.HdfsContext context)
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

    private Path getExternalPath(HdfsEnvironment.HdfsContext context, String location)
    {
        try {
            Path path = new Path(location);
            if (!isS3FileSystem(context, hdfsEnvironment, path)) {
                if (!hdfsEnvironment.getFileSystem(context, path).isDirectory(path)) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY, "External location must be a directory: " + location);
                }
            }
            return path;
        }
        catch (IllegalArgumentException | IOException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "External location is not a valid file system URI: " + location, e);
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
                .map(column -> new Column(column.getName(), column.getHiveType(), column.getComment()))
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
                .setOwner(Optional.of(tableOwner))
                .setTableType((external ? EXTERNAL_TABLE : MANAGED_TABLE).name())
                .setDataColumns(columns.build())
                .setPartitionColumns(partitionColumns)
                .setParameters(tableParameters.buildOrThrow());

        tableBuilder.getStorageBuilder()
                .setStorageFormat(fromHiveStorageFormat(hiveStorageFormat))
                .setBucketProperty(bucketProperty)
                .setLocation(targetPath.toString());

        return tableBuilder.build();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout)
    {
        verifyJvmTimeZone();

        if ((!writesToNonManagedTablesEnabled || !createsOfNonManagedTablesEnabled) && getExternalLocation(tableMetadata.getProperties()) != null) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot create a non-managed Hive table using CREATE TABLE AS");
        }

        if (getAvroSchemaUrl(tableMetadata.getProperties()) != null) {
            throw new TrinoException(NOT_SUPPORTED, "CREATE TABLE AS not supported when Avro schema url is set");
        }

        HiveStorageFormat tableStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());

        // get the root directory for the database
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Map<String, String> tableProperties = getEmptyTableProperties(tableMetadata, bucketProperty, new HdfsEnvironment.HdfsContext(session));
        List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy));
        HiveStorageFormat partitionStorageFormat = isRespectTableFormat(session) ? tableStorageFormat : HiveSessionProperties.getHiveStorageFormat(session);

        // unpartitioned tables ignore the partition storage format
        HiveStorageFormat actualStorageFormat = partitionedBy.isEmpty() ? tableStorageFormat : partitionStorageFormat;
        actualStorageFormat.validateColumns(columnHandles);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new Column(column.getName(), column.getHiveType(), column.getComment()))
                .collect(toList());
        checkPartitionTypesSupported(partitionColumns);

        LocationHandle locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName, Optional.empty());
        Optional<String> externalLocation = Optional.ofNullable(getExternalLocation(tableMetadata.getProperties()));
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
                NO_ACID_TRANSACTION,
                externalLocation.isPresent(),
                true);

        LocationService.WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.declareIntentionToWrite(session, writeInfo.getWriteMode(), writeInfo.getWritePath(), schemaTableName);

        return result;
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
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent() || (!translateHiveViews && isHiveOrPrestoView(table.get()))) {
            throw new TableNotFoundException(tableName);
        }

        Function<HiveColumnHandle, ColumnMetadata> metadataGetter = columnMetadataGetter(table.get());
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(table.get(), typeManager, getTimestampPrecision(session))) {
            columns.add(metadataGetter.apply(columnHandle));
        }

        // External location property
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (table.get().getTableType().equals(EXTERNAL_TABLE.name())) {
            properties.put(EXTERNAL_LOCATION_PROPERTY, table.get().getStorage().getLocation());
        }

        // Storage format property
        try {
            HiveStorageFormat format = extractHiveStorageFormat(table.get());
            properties.put(STORAGE_FORMAT_PROPERTY, format);
        }
        catch (TrinoException ignored) {
            // todo fail if format is not known
        }

        // Partitioning property
        List<String> partitionedBy = table.get().getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        // Bucket properties
        table.get().getStorage().getBucketProperty().ifPresent(property -> {
            properties.put(BUCKETING_VERSION, property.getBucketingVersion().getVersion());
            properties.put(BUCKET_COUNT_PROPERTY, property.getBucketCount());
            properties.put(BUCKETED_BY_PROPERTY, property.getBucketedBy());
            properties.put(SORTED_BY_PROPERTY, property.getSortedBy());
        });

        // ORC format specific properties
        String orcBloomFilterColumns = table.get().getParameters().get(ORC_BLOOM_FILTER_COLUMNS_KEY);
        if (orcBloomFilterColumns != null) {
            properties.put(ORC_BLOOM_FILTER_COLUMNS, Splitter.on(',').trimResults().omitEmptyStrings().splitToList(orcBloomFilterColumns));
        }
        String orcBloomFilterFfp = table.get().getParameters().get(ORC_BLOOM_FILTER_FPP_KEY);
        if (orcBloomFilterFfp != null) {
            properties.put(ORC_BLOOM_FILTER_FPP, Double.parseDouble(orcBloomFilterFfp));
        }

        // Avro specific property
        String avroSchemaUrl = table.get().getParameters().get(AVRO_SCHEMA_URL_KEY);
        if (avroSchemaUrl != null) {
            properties.put(AVRO_SCHEMA_URL, avroSchemaUrl);
        }

        // Textfile and CSV specific property
        getSerdeProperty(table.get(), SKIP_HEADER_COUNT_KEY)
                .ifPresent(skipHeaderCount -> properties.put(SKIP_HEADER_LINE_COUNT, Integer.valueOf(skipHeaderCount)));
        getSerdeProperty(table.get(), SKIP_FOOTER_COUNT_KEY)
                .ifPresent(skipFooterCount -> properties.put(SKIP_FOOTER_LINE_COUNT, Integer.valueOf(skipFooterCount)));

        // Textfile specific property
        getSerdeProperty(table.get(), TEXT_FIELD_SEPARATOR_KEY)
                .ifPresent(fieldSeparator -> properties.put(TEXTFILE_FIELD_SEPARATOR, fieldSeparator));
        getSerdeProperty(table.get(), TEXT_FIELD_SEPARATOR_ESCAPE_KEY)
                .ifPresent(fieldEscape -> properties.put(TEXTFILE_FIELD_SEPARATOR_ESCAPE, fieldEscape));

        // CSV specific property
        getCsvSerdeProperty(table.get(), CSV_SEPARATOR_KEY)
                .ifPresent(csvSeparator -> properties.put(CSV_SEPARATOR, csvSeparator));
        getCsvSerdeProperty(table.get(), CSV_QUOTE_KEY)
                .ifPresent(csvQuote -> properties.put(CSV_QUOTE, csvQuote));
        getCsvSerdeProperty(table.get(), CSV_ESCAPE_KEY)
                .ifPresent(csvEscape -> properties.put(CSV_ESCAPE, csvEscape));

        String trinoPartitionSpec = table.get().getParameters().get(PARTITION_SPEC_KEY);
        if (trinoPartitionSpec != null) {
            properties.put(PARTITION_SPEC_PROPERTY, trinoPartitionSpec);
        }

        Optional<String> comment = Optional.ofNullable(table.get().getParameters().get(TABLE_COMMENT));

        return new ConnectorTableMetadata(tableName, columns.build(), properties.buildOrThrow(), comment);
    }

    private static Optional<String> getCsvSerdeProperty(Table table, String key)
    {
        return getSerdeProperty(table, key).map(csvSerdeProperty -> {
            if (csvSerdeProperty.length() > 1) {
                throw new TrinoException(HIVE_INVALID_METADATA, "Only single character can be set for property: " + key);
            }
            return csvSerdeProperty;
        });
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

    private void verifyJvmTimeZone()
    {
        if (!timeZone.equals(DateTimeZone.getDefault())) {
            throw new TrinoException(HIVE_TIMEZONE_MISMATCH, format(
                    "To write Hive data, your JVM timezone must match the Hive storage timezone. Add -Duser.timezone=%s to your JVM arguments.",
                    timeZone.getID()));
        }
    }

    private Map<String, String> getEmptyTableProperties(ConnectorTableMetadata tableMetadata, Optional<HiveBucketProperty> bucketProperty, HdfsEnvironment.HdfsContext hdfsContext)
    {
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();

        // When metastore is configured with metastore.create.as.acid=true, it will also change Presto-created tables
        // behind the scenes. In particular, this won't work with CTAS.
        // TODO (https://github.com/trino/presto/issues/1956) convert this into normal table property
        tableProperties.put(TRANSACTIONAL, "false");

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

        String partitionSpecJson = getPartitionSpecJson(tableMetadata.getProperties());
        if (partitionSpecJson != null) {
            validateSpecJson(partitionSpecJson);
            tableProperties.put(PARTITION_SPEC_KEY, partitionSpecJson);
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

        // Table comment property
        tableMetadata.getComment().ifPresent(value -> tableProperties.put(TABLE_COMMENT, value));

        return tableProperties.buildOrThrow();
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

    private void checkPartitionTypesSupported(List<Column> partitionColumns)
    {
        for (Column partitionColumn : partitionColumns) {
            Type partitionType = typeManager.getType(partitionColumn.getType().getTypeSignature());
            verifyPartitionTypeSupported(partitionColumn.getName(), partitionType);
        }
    }

    private boolean isHiveOrPrestoView(Table table)
    {
        return table.getTableType().equals(TableType.VIRTUAL_VIEW.name());
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        checkArgument(!handle.getAnalyzePartitionValues().isPresent() || constraint.getSummary().isAll(), "Analyze should not have a constraint");

        HivePartitionResult partitionResult = partitionManager.getPartitions(metastore, handle, constraint);

        if (isPartitionSpecEnabled(session)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
            String partitionSpecJson = getPartitionSpecJson(tableMetadata.getProperties());
            if (partitionSpecJson != null) {
                PartitionSpec partitionSpec = toIcebergPartitionSpec(tableMetadata, HiddenPartitioningSpecParser.fromJson(partitionSpecJson));
                partitionResult = transformFilteredPartitionResult(partitionResult, partitionSpec);
            }
        }

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

        return Optional.of(new ConstraintApplicationResult<>(newHandle, unenforcedConstraint, false));
    }

    private static HiveStorageFormat extractHiveStorageFormat(Table table)
    {
        StorageFormat storageFormat = table.getStorage().getStorageFormat();
        String outputFormat = storageFormat.getOutputFormat();
        String serde = storageFormat.getSerde();

        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerde().equals(serde)) {
                return format;
            }
        }
        throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, format("Output format %s with SerDe %s is not supported", outputFormat, serde));
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
        Map<String, Optional<String>> columnComment = builder.buildOrThrow();

        return handle -> ColumnMetadata.builder()
                .setName(handle.getName())
                .setType(handle.getType())
                .setComment(columnComment.get(handle.getName()))
                .setExtraInfo(Optional.ofNullable(columnExtraInfo(handle.isPartitionKey())))
                .setHidden(handle.isHidden())
                .build();
    }

    private static void validateBucketColumns(ConnectorTableMetadata tableMetadata)
    {
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        if (!bucketProperty.isPresent()) {
            return;
        }
        Set<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toSet());

        List<String> bucketedBy = bucketProperty.get().getBucketedBy();
        if (!allColumns.containsAll(bucketedBy)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Bucketing columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(bucketedBy), ImmutableSet.copyOf(allColumns))));
        }

        List<String> sortedBy = bucketProperty.get().getSortedBy().stream()
                .map(SortingColumn::getColumnName)
                .collect(toImmutableList());
        if (!allColumns.containsAll(sortedBy)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format("Sorting columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(sortedBy), ImmutableSet.copyOf(allColumns))));
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
        validateCsvColumns(tableMetadata);

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
            columnHandles.add(new HiveColumnHandle(
                    column.getName(),
                    ordinal,
                    toHiveType(column.getType()),
                    column.getType(),
                    Optional.empty(),
                    columnType,
                    Optional.ofNullable(column.getComment())));
            ordinal++;
        }

        return columnHandles.build();
    }

    private static void validateCsvColumns(ConnectorTableMetadata tableMetadata)
    {
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
