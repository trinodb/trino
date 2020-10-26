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
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.reverse;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.prestosql.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.prestosql.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_SNAPSHOT_ID;
import static io.prestosql.plugin.iceberg.TypeConverter.toPrestoType;
import static java.lang.String.format;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

final class IcebergUtil
{
    private static final Pattern SIMPLE_NAME = Pattern.compile("[a-z][a-z0-9]*");

    private IcebergUtil() {}

    public static boolean isIcebergTable(io.prestosql.plugin.hive.metastore.Table table)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_TYPE_PROP));
    }

    public static HadoopCatalog getHadoopCatalog(HdfsEnvironment hdfsEnvironment, ConnectorSession session)
    {
        HdfsContext hdfsContext = new HdfsContext(session, "not used");
        Path path = new Path("/");
        Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);
        String warehouse = configuration.get("hive.metastore.warehouse.dir");
        if (warehouse == null || warehouse.isEmpty()) {
            throw new PrestoException(HIVE_METASTORE_ERROR, format("hive.metastore.warehouse.dir not set"));
        }
        return getHadoopCatalog(hdfsEnvironment, session, warehouse);
    }

    public static HadoopCatalog getHadoopCatalog(HdfsEnvironment hdfsEnvironment, ConnectorSession session, String location)
    {
        HdfsContext hdfsContext = new HdfsContext(session, "not used");
        Path path = new Path(location);
        Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);
        return new HadoopCatalog(configuration, location);
    }

    public static boolean icebergSchemaExists(HdfsEnvironment hdfsEnvironment, ConnectorSession session, String schemaName)
    {
        try {
            HadoopCatalog hadoopCatalog = getHadoopCatalog(hdfsEnvironment, session);
            Namespace namespace = Namespace.of(schemaName);
            hadoopCatalog.loadNamespaceMetadata(namespace);
            return true;
        }
        catch (NoSuchNamespaceException e) {
            return false;
        }
    }

    public static boolean icebergTableExists(HdfsEnvironment hdfsEnvironment, ConnectorSession session, SchemaTableName table)
    {
        try {
            TableIdentifier tableIdentifier = TableIdentifier.of(table.getSchemaName(), table.getTableName());
            HadoopCatalog hadoopCatalog = getHadoopCatalog(hdfsEnvironment, session);
            hadoopCatalog.loadTable(tableIdentifier);
            return true;
        }
        catch (NoSuchTableException e) {
            return false;
        }
    }

    public static boolean useMetastore(ConnectorSession session)
    {
        return !IcebergSessionProperties.isHadoopMode(session);
    }

    public static Table getIcebergTable(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment, ConnectorSession session, SchemaTableName table)
    {
        if (useMetastore(session)) {
            HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
            HiveIdentity identity = new HiveIdentity(session);
            TableOperations operations = new HiveTableOperations(metastore, hdfsEnvironment, hdfsContext, identity, table.getSchemaName(), table.getTableName());
            return new BaseTable(operations, quotedTableName(table));
        }
        else {
            TableIdentifier tableIdentifier = TableIdentifier.of(table.getSchemaName(), table.getTableName());
            HadoopCatalog hadoopCatalog = getHadoopCatalog(hdfsEnvironment, session);
            return hadoopCatalog.loadTable(tableIdentifier);
        }
    }

    public static long resolveSnapshotId(Table table, long snapshotId)
    {
        if (table.snapshot(snapshotId) != null) {
            return snapshotId;
        }

        return reverse(table.history()).stream()
                .filter(entry -> entry.timestampMillis() <= snapshotId)
                .map(HistoryEntry::snapshotId)
                .findFirst()
                .orElseThrow(() -> new PrestoException(ICEBERG_INVALID_SNAPSHOT_ID, format("Invalid snapshot [%s] for table: %s", snapshotId, table)));
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

    public static Optional<String> getTableComment(Table table)
    {
        return Optional.ofNullable(table.properties().get(TABLE_COMMENT));
    }

    private static String quotedTableName(SchemaTableName name)
    {
        return quotedName(name.getSchemaName()) + "." + quotedName(name.getTableName());
    }

    private static String quotedName(String name)
    {
        if (SIMPLE_NAME.matcher(name).matches()) {
            return name;
        }
        return '"' + name.replace("\"", "\"\"") + '"';
    }
}
