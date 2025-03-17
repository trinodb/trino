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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.plugin.hive.util.HiveUtil.columnMetadataGetter;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.hiveColumnHandles;
import static io.trino.plugin.hive.util.HiveUtil.isHiveSystemSchema;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiSessionProperties.getColumnsToHide;
import static io.trino.plugin.hudi.HudiSessionProperties.isQueryPartitionFilterRequired;
import static io.trino.plugin.hudi.HudiTableProperties.LOCATION_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hudi.HudiUtil.hudiMetadataExists;
import static io.trino.plugin.hudi.model.HudiTableType.COPY_ON_WRITE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class HudiMetadata
        implements ConnectorMetadata
{
    private final HiveMetastore metastore;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;

    public HudiMetadata(HiveMetastore metastore, TrinoFileSystemFactory fileSystemFactory, TypeManager typeManager)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases().stream()
                .filter(schemaName -> !isHiveSystemSchema(schemaName))
                .collect(toImmutableList());
    }

    @Override
    public HudiTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        if (isHiveSystemSchema(tableName.getSchemaName())) {
            return null;
        }
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table.isEmpty()) {
            return null;
        }
        if (!isHudiTable(table.get())) {
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, format("Not a Hudi table: %s", tableName));
        }
        Location location = Location.of(table.get().getStorage().getLocation());
        if (!hudiMetadataExists(fileSystemFactory.create(session), location)) {
            throw new TrinoException(HUDI_BAD_DATA, "Location of table %s does not contain Hudi table metadata: %s".formatted(tableName, location));
        }

        return new HudiTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().getStorage().getLocation(),
                COPY_ON_WRITE,
                getPartitionKeyColumnHandles(table.get(), typeManager),
                TupleDomain.all(),
                TupleDomain.all());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(tableName, session)
                .map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    private Optional<SystemTable> getRawSystemTable(SchemaTableName tableName, ConnectorSession session)
    {
        Optional<HudiTableName> nameOptional = HudiTableName.from(tableName.getTableName());
        if (nameOptional.isEmpty()) {
            return Optional.empty();
        }
        HudiTableName name = nameOptional.get();
        if (name.tableType() == TableType.DATA) {
            return Optional.empty();
        }

        Optional<Table> tableOptional = metastore.getTable(tableName.getSchemaName(), name.tableName());
        if (tableOptional.isEmpty()) {
            return Optional.empty();
        }
        if (!isHudiTable(tableOptional.get())) {
            return Optional.empty();
        }
        return switch (name.tableType()) {
            case DATA -> throw new AssertionError();
            case TIMELINE -> {
                SchemaTableName systemTableName = new SchemaTableName(tableName.getSchemaName(), name.tableNameWithType());
                yield Optional.of(new TimelineTable(fileSystemFactory.create(session), systemTableName, tableOptional.get()));
            }
        };
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) table;
        return getTableMetadata(hudiTableHandle.getSchemaTableName(), getColumnsToHide(session));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        HudiTableHandle handle = (HudiTableHandle) tableHandle;
        HudiPredicates predicates = HudiPredicates.from(constraint.getSummary());
        TupleDomain<HiveColumnHandle> regularColumnPredicates = predicates.getRegularColumnPredicates();
        TupleDomain<HiveColumnHandle> partitionColumnPredicates = predicates.getPartitionColumnPredicates();

        // TODO Since the constraint#predicate isn't utilized during split generation. So,
        //  Let's not add constraint#predicateColumns to newConstraintColumns.
        Set<HiveColumnHandle> newConstraintColumns = Stream.concat(
                        Stream.concat(
                                regularColumnPredicates.getDomains().stream()
                                        .map(Map::keySet)
                                        .flatMap(Collection::stream),
                                partitionColumnPredicates.getDomains().stream()
                                        .map(Map::keySet)
                                        .flatMap(Collection::stream)),
                        handle.getConstraintColumns().stream())
                .collect(toImmutableSet());

        HudiTableHandle newHudiTableHandle = handle.applyPredicates(
                newConstraintColumns,
                partitionColumnPredicates,
                regularColumnPredicates);

        if (handle.getPartitionPredicates().equals(newHudiTableHandle.getPartitionPredicates())
                && handle.getRegularPredicates().equals(newHudiTableHandle.getRegularPredicates())
                && handle.getConstraintColumns().equals(newHudiTableHandle.getConstraintColumns())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                newHudiTableHandle,
                newHudiTableHandle.getRegularPredicates().transformKeys(ColumnHandle.class::cast),
                constraint.getExpression(),
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
    public Optional<Object> getInfo(ConnectorTableHandle tableHandle)
    {
        HudiTableHandle table = (HudiTableHandle) tableHandle;
        return Optional.of(new HudiTableInfo(table.getSchemaTableName(), table.getTableType().name(), table.getBasePath()));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (TableInfo tableInfo : metastore.getTables(schemaName)) {
                tableNames.add(tableInfo.tableName());
            }
        }
        return tableNames.build();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new)
                .orElseGet(SchemaTablePrefix::new);
        List<SchemaTableName> tables = prefix.getTable()
                .map(_ -> singletonList(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = tables.stream()
                .map(table -> getTableColumnMetadata(session, table))
                .flatMap(Optional::stream)
                .collect(toImmutableMap(RelationColumnsMetadata::name, Function.identity()));
        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) handle;
        if (isQueryPartitionFilterRequired(session)) {
            if (!hudiTableHandle.getPartitionColumns().isEmpty()) {
                Set<String> partitionColumns = hudiTableHandle.getPartitionColumns().stream()
                        .map(HiveColumnHandle::getName)
                        .collect(toImmutableSet());
                Set<String> constraintColumns = hudiTableHandle.getConstraintColumns().stream()
                        .map(HiveColumnHandle::getBaseColumnName)
                        .collect(toImmutableSet());
                if (Collections.disjoint(constraintColumns, partitionColumns)) {
                    throw new TrinoException(
                            QUERY_REJECTED,
                            format("Filter required on %s for at least one of the partition columns: %s", hudiTableHandle.getSchemaTableName(), String.join(", ", partitionColumns)));
                }
            }
        }
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // hudi supports only a columnar (parquet) storage format
        return true;
    }

    HiveMetastore getMetastore()
    {
        return metastore;
    }

    private Optional<RelationColumnsMetadata> getTableColumnMetadata(ConnectorSession session, SchemaTableName table)
    {
        try {
            List<ColumnMetadata> columns = getTableMetadata(table, getColumnsToHide(session)).getColumns();
            return Optional.of(RelationColumnsMetadata.forTable(table, columns));
        }
        catch (TableNotFoundException _) {
            return Optional.empty();
        }
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName, Collection<String> columnsToHide)
    {
        Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        Function<HiveColumnHandle, ColumnMetadata> metadataGetter = columnMetadataGetter(table);
        List<ColumnMetadata> columns = hiveColumnHandles(table, typeManager, NANOSECONDS).stream()
                .filter(column -> !columnsToHide.contains(column.getName()))
                .map(metadataGetter)
                .collect(toImmutableList());

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        // Location property
        String location = table.getStorage().getLocation();
        if (!isNullOrEmpty(location)) {
            properties.put(LOCATION_PROPERTY, location);
        }

        // Partitioning property
        List<String> partitionedBy = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        Optional<String> comment = Optional.ofNullable(table.getParameters().get(TABLE_COMMENT));
        return new ConnectorTableMetadata(tableName, columns, properties.buildOrThrow(), comment);
    }

    private List<String> listSchemas(ConnectorSession session, Optional<String> schemaName)
    {
        return schemaName
                .filter(name -> !isHiveSystemSchema(name))
                .map(Collections::singletonList)
                .orElseGet(() -> listSchemaNames(session));
    }
}
