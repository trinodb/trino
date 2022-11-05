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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.PartitionAndStatementId;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.security.AccessControlMetadata;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.StorageFormat.create;
import static io.trino.plugin.hudi.HudiUtil.getCurrentInstantTime;
import static io.trino.plugin.hudi.HudiUtil.getHoodieKeyGenerator;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class HudiWriterOperations
{
    private final Configuration configuration;
    private final HiveMetastore metastore;
    private final AccessControlMetadata accessControlMetadata;

    private static final String HOODIE_NAMESPACE_PREFIX = "hoodie.";
    private static final String HOODIE_PRIMARY_KEY = "primaryKey";
    private static final String HOODIE_LAST_COMMIT_TIME = "last_commit_time_sync";
    private static final String TYPE = "type";
    private static final String PATH = "path";
    private static final String FILES = "files";
    private static final String ARCHIVED = "archived";
    private static final String SPARK_SCHEMA_FIELDS = "fields";
    private static final String SPARK_SCHEMA_NAME = "name";
    private static final String SPARK_SCHEMA_NULLABLE = "nullable";
    private static final String SPARK_SCHEMA_METADATA = "metadata";
    private static final String SPARK_SCHEMA_STRUCT = "struct";
    private static final String SPARK_SOURCES_SCHEMA = "spark.sql.sources.schema.part.0";
    private static final String SPARK_PARTITION_NUM = "spark.sql.sources.schema.numPartCols";
    private static final String SPARK_PARTITION_FORMAT = "spark.sql.sources.schema.partCol.%d";

    private static final List<String> HOODIE_META_COLUMNS = ImmutableList.of(
            "_hoodie_commit_time",
            "_hoodie_commit_seqno",
            "_hoodie_record_key",
            "_hoodie_partition_path",
            "_hoodie_file_name");

    private static final Map<String, String> HOODIE_DEFAULT_PARAMETERS = ImmutableMap.of(
            "serialization.format", "1",
            "EXTERNAL", "TRUE",
            "spark.sql.sources.provider", "hudi",
            "spark.sql.sources.schema.numParts", "1");

    public HudiWriterOperations(
            HiveMetastore metastore,
            Configuration configuration,
            AccessControlMetadata accessControlMetadata)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.accessControlMetadata = requireNonNull(accessControlMetadata, "accessControlMetadata is null");
    }

    public HudiOutputTableHandle createTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            String targetPath,
            HudiTableType tableType,
            Optional<List<String>> recordKeys,
            Optional<String> preCombineField,
            Optional<List<String>> partitions,
            HudiStorageFormat storageFormat)
    {
        HudiOutputTableHandle tableHandle = createHudiOutputTableHandle(tableMetadata, tableType, targetPath, partitions, storageFormat, recordKeys);
        HoodieJavaWriteClient<HoodieAvroPayload> writeClient = createWriteClient(
                tableHandle,
                preCombineField.orElse(null),
                partitions.orElse(ImmutableList.of()));
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        writeClient.startCommitWithTime(String.valueOf(timestamp.getTime()));

        Table hiveTable = createMetastoreTable(session, tableHandle);
        PrincipalPrivileges principalPrivileges = accessControlMetadata.isUsingSystemSecurity() ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());
        metastore.createTable(hiveTable, principalPrivileges);
        return tableHandle;
    }

    public void deleteTable(HudiTableHandle tableHandle, Collection<Slice> fragments)
    {
        List<String> partitions = fragments.stream()
                .map(Slice::getBytes)
                .map(PartitionAndStatementId.CODEC::fromJson)
                .map(PartitionAndStatementId::getPartitionName)
                .collect(toImmutableList());

        metastore.dropPartition(tableHandle.getSchemaName(), tableHandle.getTableName(), partitions, false);
        metastore.dropTable(tableHandle.getSchemaName(), tableHandle.getTableName(), false);
    }

    private HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient(
            HudiOutputTableHandle tableHandle,
            String preCombineField,
            List<String> partitions)
    {
        String tableCreateSchema = generateTableCreateSchema(tableHandle.getTableName(), tableHandle.getDataColumns(), tableHandle.getPartitionColumns());
        try {
            HoodieTableMetaClient.PropertyBuilder hoodiePropertyBuilder = HoodieTableMetaClient.withPropertyBuilder()
                    .setTableType(tableHandle.getTableType().getHoodieTableType())
                    .setTableName(tableHandle.getTableName())
                    .setTableCreateSchema(tableCreateSchema)
                    .setHiveStylePartitioningEnable(true)
                    .setKeyGeneratorClassProp(getHoodieKeyGenerator(tableHandle.getPartitionColumns().size()))
                    .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .setRecordKeyFields(tableHandle.getPrimaryKey())
                    .setArchiveLogFolder(ARCHIVED)
                    .setPreCombineField(preCombineField);
            if (partitions.isEmpty()) {
                hoodiePropertyBuilder.setMetadataPartitions(FILES);
            }
            else {
                hoodiePropertyBuilder.setShouldDropPartitionColumns(false);
                hoodiePropertyBuilder.setPartitionFields(String.join(",", partitions));
            }
            hoodiePropertyBuilder.initTable(configuration, tableHandle.getTablePath());
        }
        catch (IOException e) {
            throw new RuntimeException("Could not init table " + tableHandle.getTableName());
        }

        HoodieIndexConfig indexConfig = HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build();
        HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder().build();
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
                .withPath(tableHandle.getTablePath())
                .withSchema(tableCreateSchema)
                .withParallelism(2, 2)
                .withDeleteParallelism(2)
                .forTable(tableHandle.getTableName())
                .withIndexConfig(indexConfig)
                .withCompactionConfig(compactionConfig)
                .withEmbeddedTimelineServerEnabled(false)
                .withMarkersType(MarkerType.DIRECT.name())
                .build();
        return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(configuration), cfg);
    }

    private HudiOutputTableHandle createHudiOutputTableHandle(
            ConnectorTableMetadata tableMetadata,
            HudiTableType tableType,
            String tablePath,
            Optional<List<String>> partitions,
            HudiStorageFormat storageFormat,
            Optional<List<String>> recordKeys)
    {
        ImmutableList.Builder<HudiColumnHandle> dataColumnsBuilder = ImmutableList.builder();
        ImmutableList.Builder<HudiColumnHandle> partitionColumnsBuilder = ImmutableList.builder();
        for (String metaColumn : HOODIE_META_COLUMNS) {
            dataColumnsBuilder.add(HudiColumnHandle.createHudiColumn(metaColumn,
                    REGULAR,
                    HIVE_STRING,
                    VarcharType.VARCHAR,
                    Schema.Type.STRING));
        }
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (partitions.isPresent() && partitions.get().contains(column.getName())) {
                partitionColumnsBuilder.add(HudiColumnHandle.createHudiColumn(column.getName(),
                        PARTITION_KEY,
                        toHiveType(column.getType()),
                        column.getType(),
                        TypeConverter.toSchemaType(column.getType())));
            }
            else {
                dataColumnsBuilder.add(HudiColumnHandle.createHudiColumn(column.getName(),
                        REGULAR,
                        toHiveType(column.getType()),
                        column.getType(),
                        TypeConverter.toSchemaType(column.getType())));
            }
        }
        String primaryKey = String.join(",", recordKeys.orElse(ImmutableList.of()));
        return new HudiOutputTableHandle(
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                tableType,
                tablePath,
                dataColumnsBuilder.build(),
                partitionColumnsBuilder.build(),
                storageFormat,
                primaryKey);
    }

    private String generateTableCreateSchema(
            String tableName,
            List<HudiColumnHandle> dataColumns,
            List<HudiColumnHandle> partitionColumns)
    {
        List<Schema.Field> allColumns = Stream.of(dataColumns, partitionColumns).flatMap(Collection::stream)
                .map(column -> new Schema.Field(column.getColumnName(), Schema.create(TypeConverter.toSchemaType(column.getBaseType()))))
                .collect(toUnmodifiableList());
        return Schema.createRecord(tableName, null, HOODIE_NAMESPACE_PREFIX + tableName, false, allColumns).toString();
    }

    private String generateSparkSchema(
            List<HudiColumnHandle> dataColumns,
            List<HudiColumnHandle> partitionColumns)
    {
        JsonArray sparkFields = new JsonArray();
        Stream.of(dataColumns, partitionColumns).flatMap(Collection::stream)
                .map(column -> createSparkField(column.getColumnName(), TypeConverter.toSparkType(column.getBaseType()), column.isNullable()))
                .forEach(field -> sparkFields.add(field));
        JsonObject sparkSchema = new JsonObject();
        sparkSchema.addProperty(TYPE, SPARK_SCHEMA_STRUCT);
        sparkSchema.add(SPARK_SCHEMA_FIELDS, sparkFields);
        return sparkSchema.toString();
    }

    private JsonObject createSparkField(String name, String type, boolean nullable)
    {
        JsonObject field = new JsonObject();
        field.addProperty(SPARK_SCHEMA_NAME, name);
        field.addProperty(TYPE, type.toLowerCase(Locale.getDefault()));
        field.addProperty(SPARK_SCHEMA_NULLABLE, nullable);
        field.add(SPARK_SCHEMA_METADATA, new JsonObject());
        return field;
    }

    private Table createMetastoreTable(ConnectorSession session, HudiOutputTableHandle tableHandle)
    {
        List<Column> dataColumnNames = tableHandle.getDataColumns().stream().map(HudiColumnHandle::toColumn).collect(toUnmodifiableList());
        List<Column> partitionColumns = tableHandle.getPartitionColumns().stream().map(HudiColumnHandle::toColumn).collect(toUnmodifiableList());
        ImmutableMap.Builder<String, String> parameterBuilder = ImmutableMap.builder();
        parameterBuilder.putAll(HOODIE_DEFAULT_PARAMETERS)
                .put(HOODIE_PRIMARY_KEY, tableHandle.getPrimaryKey())
                .put(SPARK_SOURCES_SCHEMA, generateSparkSchema(tableHandle.getDataColumns(), tableHandle.getPartitionColumns()))
                .put(TYPE, tableHandle.getTableType().getName())
                .put(HOODIE_LAST_COMMIT_TIME, getCurrentInstantTime());
        if (!partitionColumns.isEmpty()) {
            parameterBuilder.put(SPARK_PARTITION_NUM, String.valueOf(partitionColumns.size()));
            for (int i = 0; i < partitionColumns.size(); i++) {
                parameterBuilder.put(String.format(SPARK_PARTITION_FORMAT, i), partitionColumns.get(i).getName());
            }
        }
        HudiStorageFormat hudiStorageFormat = tableHandle.getStorageFormat();

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(tableHandle.getSchemaName())
                .setTableName(tableHandle.getTableName())
                .setTableType(TableType.EXTERNAL_TABLE.name())
                .setPartitionColumns(partitionColumns)
                .setDataColumns(dataColumnNames)
                .setOwner(Optional.of(session.getUser()))
                .setParameters(parameterBuilder.buildOrThrow())
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(create(hudiStorageFormat.getSerde(), hudiStorageFormat.getInputFormat(), hudiStorageFormat.getOutputFormat()))
                        .setLocation(tableHandle.getTablePath())
                        .setSerdeParameters(ImmutableMap.of(PATH, tableHandle.getTablePath())).build());
        return tableBuilder.build();
    }
}
