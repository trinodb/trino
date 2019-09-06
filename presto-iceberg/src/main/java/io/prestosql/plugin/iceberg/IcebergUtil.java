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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveColumnHandle.ColumnType;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.Streams.stream;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveType.toHiveType;
import static io.prestosql.plugin.iceberg.TypeConveter.toPrestoType;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

final class IcebergUtil
{
    private static final TypeTranslator TYPE_TRANSLATOR = new HiveTypeTranslator();

    private IcebergUtil() {}

    public static boolean isIcebergTable(io.prestosql.plugin.hive.metastore.Table table)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_TYPE_PROP));
    }

    public static Table getIcebergTable(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment, ConnectorSession session, SchemaTableName table)
    {
        HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
        HiveIdentity identity = new HiveIdentity(session);
        TableOperations operations = new HiveTableOperations(metastore, hdfsEnvironment, hdfsContext, identity, table.getSchemaName(), table.getTableName());
        return new BaseTable(operations, table.getSchemaName() + "." + table.getTableName());
    }

    public static List<HiveColumnHandle> getColumns(Schema schema, PartitionSpec spec, TypeManager typeManager)
    {
        // Iceberg may or may not store identity columns in data file and the identity transformations have the same name as data column.
        // So we remove the identity columns from the set of regular columns which does not work with some of Presto's validation.

        List<PartitionField> partitionFields = ImmutableList.copyOf(getIdentityPartitions(spec).keySet());
        Map<String, PartitionField> partitionColumnNames = uniqueIndex(partitionFields, PartitionField::name);

        int columnIndex = 0;
        ImmutableList.Builder<HiveColumnHandle> builder = ImmutableList.builder();

        for (Types.NestedField column : schema.columns()) {
            Type type = column.type();
            ColumnType columnType = REGULAR;
            if (partitionColumnNames.containsKey(column.name())) {
                PartitionField partitionField = partitionColumnNames.get(column.name());
                Type sourceType = schema.findType(partitionField.sourceId());
                type = partitionField.transform().getResultType(sourceType);
                columnType = PARTITION_KEY;
            }
            io.prestosql.spi.type.Type prestoType = toPrestoType(type, typeManager);
            HiveType hiveType = toHiveType(TYPE_TRANSLATOR, coerceForHive(prestoType));
            HiveColumnHandle columnHandle = new HiveColumnHandle(column.name(), hiveType, prestoType.getTypeSignature(), columnIndex, columnType, Optional.empty());
            columnIndex++;
            builder.add(columnHandle);
        }

        return builder.build();
    }

    public static List<HiveColumnHandle> getPartitionColumns(Schema schema, PartitionSpec spec, TypeManager typeManager)
    {
        List<PartitionField> partitionFields = ImmutableList.copyOf(getIdentityPartitions(spec).keySet());

        int columnIndex = 0;
        ImmutableList.Builder<HiveColumnHandle> builder = ImmutableList.builder();

        for (PartitionField partitionField : partitionFields) {
            Type sourceType = schema.findType(partitionField.sourceId());
            Type type = partitionField.transform().getResultType(sourceType);
            io.prestosql.spi.type.Type prestoType = toPrestoType(type, typeManager);
            HiveType hiveType = toHiveType(TYPE_TRANSLATOR, coerceForHive(prestoType));
            HiveColumnHandle columnHandle = new HiveColumnHandle(partitionField.name(), hiveType, prestoType.getTypeSignature(), columnIndex, PARTITION_KEY, Optional.empty());
            columnIndex++;
            builder.add(columnHandle);
        }
        return builder.build();
    }

    public static io.prestosql.spi.type.Type coerceForHive(io.prestosql.spi.type.Type prestoType)
    {
        if (prestoType.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return TIMESTAMP;
        }
        return prestoType;
    }

    public static Map<PartitionField, Integer> getIdentityPartitions(PartitionSpec partitionSpec)
    {
        // TODO: expose transform information in Iceberg library
        ImmutableMap.Builder<PartitionField, Integer> columns = ImmutableMap.builder();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (field.transform().toString().equals("identity")) {
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

    public static TableScan getTableScan(ConnectorSession session, TupleDomain<HiveColumnHandle> predicates, Optional<Long> snapshotId, Table icebergTable)
    {
        Expression expression = ExpressionConverter.toIcebergExpression(predicates, session);
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
}
