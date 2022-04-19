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
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.acid.AcidSchema;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieTableType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.concat;
import static io.trino.plugin.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PARTITION_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveTableProperties.EXTERNAL_LOCATION_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.plugin.hive.util.HiveUtil.columnExtraInfo;
import static io.trino.plugin.hive.util.HiveUtil.hiveColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNKNOWN_TABLE_TYPE;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isFullAcidTable;
import static org.apache.hudi.common.fs.FSUtils.getFs;
import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.exception.TableNotFoundException.checkTableValidity;

public class HudiMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(HudiMetadata.class);
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;

    public HudiMetadata(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment, TypeManager typeManager)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public HudiTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return null;
        }
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table.isEmpty()) {
            return null;
        }
        if (!isHudiTable(session, table.get())) {
            throw new TrinoException(HUDI_UNKNOWN_TABLE_TYPE, format("Not a Hudi table: %s", tableName));
        }
        return new HudiTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().getStorage().getLocation(),
                HoodieTableType.COPY_ON_WRITE,
                TupleDomain.all(),
                TupleDomain.all());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) table;
        return getTableMetadata(hudiTableHandle.getSchemaTableName());
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        HudiTableHandle handle = (HudiTableHandle) tableHandle;
        HudiPredicates predicates = HudiPredicates.from(constraint.getSummary());
        HudiTableHandle newHudiTableHandle = handle.withPredicates(
                predicates.getPartitionColumnPredicates(),
                predicates.getRegularColumnPredicates());

        if (handle.getPartitionPredicates().equals(newHudiTableHandle.getPartitionPredicates())
                && handle.getRegularPredicates().equals(newHudiTableHandle.getRegularPredicates())) {
            log.debug("No new predicates to apply");
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                newHudiTableHandle,
                newHudiTableHandle.getRegularPredicates().transformKeys(ColumnHandle.class::cast),
                false));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) tableHandle;
        Table table = metastore.getTable(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())));
        return hiveColumnHandles(table, typeManager, NANOSECONDS).stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((HiveColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        HudiTableHandle hudiTable = (HudiTableHandle) table;
        return Optional.of(HudiTableInfo.from(hudiTable));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (String tableName : metastore.getAllTables(schemaName)) {
                tableNames.add(schemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    @Override
    public Stream<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTable()
                .map(ignored -> singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));
        return tables.stream().map(table -> {
            List<ColumnMetadata> columns = getTableMetadata(table).getColumns();
            return TableColumnsMetadata.forTable(table, columns);
        });
    }

    HiveMetastore getMetastore()
    {
        return metastore;
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

        Map<String, Optional<String>> columnComment = builder.buildOrThrow();

        return handle -> ColumnMetadata.builder()
                .setName(handle.getName())
                .setType(handle.getType())
                .setComment(columnComment.get(handle.getName()))
                .setExtraInfo(Optional.ofNullable(columnExtraInfo(handle.isPartitionKey())))
                .setHidden(handle.isHidden())
                .build();
    }

    private boolean isHudiTable(ConnectorSession session, Table table)
    {
        String basePath = table.getStorage().getLocation();
        Configuration conf = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session), new Path(basePath));
        try {
            checkTableValidity(getFs(basePath, conf), new Path(basePath), new Path(basePath, METAFOLDER_NAME));
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName(tableName.getSchemaName(), tableName.getTableName())));
        Function<HiveColumnHandle, ColumnMetadata> metadataGetter = columnMetadataGetter(table);
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(table, typeManager, NANOSECONDS)) {
            columns.add(metadataGetter.apply(columnHandle));
        }

        // External location property
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (table.getTableType().equals(EXTERNAL_TABLE.name())) {
            properties.put(EXTERNAL_LOCATION_PROPERTY, table.getStorage().getLocation());
        }

        // Partitioning property
        List<String> partitionedBy = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        Optional<String> comment = Optional.ofNullable(table.getParameters().get(TABLE_COMMENT));
        return new ConnectorTableMetadata(tableName, columns.build(), properties.buildOrThrow(), comment);
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
}
