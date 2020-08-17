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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFilesTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HistoryTable;
import org.apache.iceberg.ManifestEntriesTable;
import org.apache.iceberg.ManifestsTable;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotsTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expression;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.prestosql.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.prestosql.plugin.iceberg.TypeConverter.toPrestoType;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

final class IcebergUtil
{
    private IcebergUtil() {}

    public static boolean isIcebergTable(io.prestosql.plugin.hive.metastore.Table table)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_TYPE_PROP));
    }

    public static Table getIcebergTable(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment, ConnectorSession session, IcebergTableHandle tableHandle)
    {
        SchemaTableName table = tableHandle.getSchemaTableName();
        TableIdentifier tableIdentifier = tableHandle.toTableIdentifier();
        if (MetadataTableType.from(tableIdentifier.name()) != null && tableIdentifier.namespace().levels().length == 2) {
            HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
            HiveIdentity identity = new HiveIdentity(session);
            return loadMetadataTable(MetadataTableType.from(tableIdentifier.name()), table, metastore, hdfsEnvironment, hdfsContext, identity);
        }
        else {
            return getIcebergTable(metastore, hdfsEnvironment, session, table);
        }
    }

    public static Table getIcebergTable(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment, ConnectorSession session, SchemaTableName table)
    {
        HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
        HiveIdentity identity = new HiveIdentity(session);
        TableOperations operations = new HiveTableOperations(metastore, hdfsEnvironment, hdfsContext, identity, table.getSchemaName(), table.getTableName());
        return new BaseTable(operations, table.getSchemaName() + "." + table.getTableName());
    }

    public static List<IcebergColumnHandle> getColumns(Schema schema, TypeManager typeManager)
    {
        return schema.columns().stream()
                .map(column -> new IcebergColumnHandle(
                        column.fieldId(),
                        column.name(),
                        toPrestoType(column.type(), typeManager),
                        Optional.ofNullable(column.doc())))
                .collect(toImmutableList());
    }

    public static Map<PartitionField, Integer> getIdentityPartitions(PartitionSpec partitionSpec)
    {
        ImmutableMap.Builder<PartitionField, Integer> columns = ImmutableMap.builder();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (field.transform().isIdentity()) {
                columns.put(field, i);
            }
        }
        return columns.build();
    }

    public static String getDataPath(String location)
    {
        if (!location.endsWith("/")) {
            location += "/";
        }
        return location + "data";
    }

    public static FileFormat getFileFormat(Table table)
    {
        return FileFormat.valueOf(table.properties()
                .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toUpperCase(Locale.ENGLISH));
    }

    public static Optional<String> getTableComment(Table table)
    {
        return Optional.ofNullable(table.properties().get(TABLE_COMMENT));
    }

    public static TableScan getTableScan(ConnectorSession session, TupleDomain<IcebergColumnHandle> predicates, Optional<Long> snapshotId, Table icebergTable)
    {
        Expression expression = ExpressionConverter.toIcebergExpression(predicates);
        TableScan tableScan = icebergTable.newScan().filter(expression);
        return snapshotId
                .map(id -> isSnapshot(icebergTable, id) ? tableScan.useSnapshot(id) : tableScan.asOfTime(id))
                .orElse(tableScan);
    }

    private static boolean isSnapshot(Table icebergTable, Long id)
    {
        return stream(icebergTable.snapshots())
                .anyMatch(snapshot -> snapshot.snapshotId() == id);
    }

    private static Table loadMetadataTable(MetadataTableType type, SchemaTableName table, HiveMetastore metastore, HdfsEnvironment hdfsEnvironment, HdfsContext hdfsContext, HiveIdentity identity)
    {
        if (type != null) {
            TableOperations ops = new HiveTableOperations(metastore, hdfsEnvironment, hdfsContext, identity, table.getSchemaName(), table.getTableName());
            if (ops.current() == null) {
                throw new NoSuchTableException("Table does not exist: " + table);
            }

            BaseTable baseTable = new BaseTable(ops, table.getSchemaName() + "." + table.getTableName());

            switch (type) {
                case ENTRIES:
                    return new ManifestEntriesTable(ops, baseTable);
                case FILES:
                    return new DataFilesTable(ops, baseTable);
                case HISTORY:
                    return new HistoryTable(ops, baseTable);
                case SNAPSHOTS:
                    return new SnapshotsTable(ops, baseTable);
                case MANIFESTS:
                    return new ManifestsTable(ops, baseTable);
                case PARTITIONS:
                    return new PartitionsTable(ops, baseTable);
                default:
                    throw new NoSuchTableException("Unknown metadata table type: %s for %s", type, table);
            }
        }
        else {
            throw new NoSuchTableException("Table does not exist: " + table);
        }
    }
}
