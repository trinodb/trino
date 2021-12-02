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

import io.trino.plugin.hive.authentication.HiveIdentity;
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
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

@NotThreadSafe
public abstract class AbstractMetastoreTableOperations
        extends AbstractIcebergTableOperations
{
    protected HiveMetastore metastore;

    public AbstractMetastoreTableOperations(
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
    protected String getRefreshedLocation()
    {
        Table table = getTable();

        if (isPrestoView(table) && isHiveOrPrestoView(table)) {
            // this is a Hive view, hence not a table
            throw new TableNotFoundException(getSchemaTableName());
        }
        if (!isIcebergTable(table)) {
            throw new UnknownTableTypeException(getSchemaTableName());
        }

        String metadataLocation = table.getParameters().get(METADATA_LOCATION);
        if (metadataLocation == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Table is missing [%s] property: %s", METADATA_LOCATION, getSchemaTableName()));
        }

        return metadataLocation;
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        String newMetadataLocation = writeNewMetadata(metadata, version + 1);

        Table table;
        try {
            Table.Builder builder = Table.builder()
                    .setDatabaseName(database)
                    .setTableName(tableName)
                    .setOwner(owner)
                    .setTableType(TableType.EXTERNAL_TABLE.name())
                    .setDataColumns(toHiveColumns(metadata.schema().columns()))
                    .withStorage(storage -> storage.setLocation(metadata.location()))
                    .withStorage(storage -> storage.setStorageFormat(STORAGE_FORMAT))
                    .setParameter("EXTERNAL", "TRUE")
                    .setParameter(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE)
                    .setParameter(METADATA_LOCATION, newMetadataLocation);
            String tableComment = metadata.properties().get(TABLE_COMMENT);
            if (tableComment != null) {
                builder.setParameter(TABLE_COMMENT, tableComment);
            }
            table = builder.build();
        }
        catch (RuntimeException e) {
            try {
                io().deleteFile(newMetadataLocation);
            }
            catch (RuntimeException ex) {
                e.addSuppressed(ex);
            }
            throw e;
        }

        PrincipalPrivileges privileges = owner.map(MetastoreUtil::buildInitialPrivilegeSet).orElse(NO_PRIVILEGES);
        HiveIdentity identity = new HiveIdentity(session);
        metastore.createTable(identity, table, privileges);
    }

    protected Table getTable()
    {
        return metastore.getTable(new HiveIdentity(session), database, tableName)
                .orElseThrow(() -> new TableNotFoundException(getSchemaTableName()));
    }
}
