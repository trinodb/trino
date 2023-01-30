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
package io.trino.plugin.iceberg.catalog.hadoop;

import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static java.lang.String.format;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

@NotThreadSafe
public class HadoopIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private final TrinoHadoopCatalog catalog;

    protected HadoopIcebergTableOperations(FileIO fileIo, ConnectorSession session, String database, String table, Optional<String> owner, Optional<String> location, TrinoHadoopCatalog catalog)
    {
        super(fileIo, session, database, table, owner, location);
        this.catalog = catalog;
    }

    @Override
    protected String getRefreshedLocation(boolean invalidateCaches)
    {
        Table table = getTable();

        if (table == null) {
            throw new TableNotFoundException(getSchemaTableName());
        }

        String metadataLocation = table.properties().get(METADATA_LOCATION_PROP);
        if (metadataLocation == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Table is missing [%s] property: %s", METADATA_LOCATION_PROP, getSchemaTableName()));
        }

        return metadataLocation;
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        verify(version.isEmpty(), "commitNewTable called on a table which already exists");
        String newMetadataLocation = writeNewMetadata(metadata, 0);

        Map<String, String> properties = Map.of(
                TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE,
                METADATA_LOCATION_PROP, newMetadataLocation);
        String tableComment = metadata.properties().get(TABLE_COMMENT);
        if (tableComment != null) {
            properties.put(TABLE_COMMENT, tableComment);
        }

        this.catalog.getCatalog(this.session).createTable(TableIdentifier.of(database, tableName), metadata.schema(), metadata.spec(), metadata.location(), properties);
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        Table table = getTable();
        checkState(currentMetadataLocation != null, "No current metadata location for existing table");
        String metadataLocation = table.properties().get(METADATA_LOCATION_PROP);
        if (!currentMetadataLocation.equals(metadataLocation)) {
            throw new CommitFailedException("Metadata location [%s] is not same as table metadata location [%s] for %s",
                    currentMetadataLocation, metadataLocation, getSchemaTableName());
        }
        Map<String, String> properties = table.properties();
        properties.put(METADATA_LOCATION_PROP, metadataLocation);
        properties.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation);

        this.catalog.getCatalog(session).createTable(TableIdentifier.of(database, tableName), metadata.schema(), metadata.spec(), metadata.location(), properties);
    }

    private Table getTable()
    {
        return catalog.loadTable(session, getSchemaTableName());
    }
}
