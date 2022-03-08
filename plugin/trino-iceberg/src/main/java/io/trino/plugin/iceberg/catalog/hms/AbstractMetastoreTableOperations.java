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
package io.trino.plugin.iceberg.catalog.hms;

import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.iceberg.UnknownTableTypeException;
import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Optional;

import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.ViewReaderUtil.isHiveOrPrestoView;
import static io.trino.plugin.hive.ViewReaderUtil.isPrestoView;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergUtil.isIcebergTable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

@NotThreadSafe
public abstract class AbstractMetastoreTableOperations
        extends AbstractIcebergTableOperations
{
    protected final HiveMetastore metastore;

    protected AbstractMetastoreTableOperations(
            FileIO fileIo,
            HiveMetastore metastore,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, session, database, table, owner, location);
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    protected final String getRefreshedLocation()
    {
        Table table = getTable();

        if (isPrestoView(table) && isHiveOrPrestoView(table)) {
            // this is a Hive view, hence not a table
            throw new TableNotFoundException(getSchemaTableName());
        }
        if (!isIcebergTable(table)) {
            throw new UnknownTableTypeException(getSchemaTableName());
        }

        String metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
        if (metadataLocation == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Table is missing [%s] property: %s", METADATA_LOCATION_PROP, getSchemaTableName()));
        }

        return metadataLocation;
    }

    @Override
    protected final void commitNewTable(TableMetadata metadata)
    {
        String newMetadataLocation = writeNewMetadata(metadata, version + 1);

        Table.Builder builder = Table.builder()
                .setDatabaseName(database)
                .setTableName(tableName)
                .setOwner(owner)
                // Table needs to be EXTERNAL, otherwise table rename in HMS would rename table directory and break table contents.
                .setTableType(TableType.EXTERNAL_TABLE.name())
                .setDataColumns(toHiveColumns(metadata.schema().columns()))
                .withStorage(storage -> storage.setLocation(metadata.location()))
                .withStorage(storage -> storage.setStorageFormat(STORAGE_FORMAT))
                // This is a must-have property for the EXTERNAL_TABLE table type
                .setParameter("EXTERNAL", "TRUE")
                .setParameter(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE)
                .setParameter(METADATA_LOCATION_PROP, newMetadataLocation);
        String tableComment = metadata.properties().get(TABLE_COMMENT);
        if (tableComment != null) {
            builder.setParameter(TABLE_COMMENT, tableComment);
        }
        Table table = builder.build();

        PrincipalPrivileges privileges = owner.map(MetastoreUtil::buildInitialPrivilegeSet).orElse(NO_PRIVILEGES);
        metastore.createTable(table, privileges);
    }

    protected Table getTable()
    {
        return metastore.getTable(database, tableName)
                .orElseThrow(() -> new TableNotFoundException(getSchemaTableName()));
    }
}
