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
import org.apache.iceberg.io.FileIO;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static java.lang.String.format;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;

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
        // TODO Auto-generated method stub
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        // TODO Auto-generated method stub
    }

    private Table getTable()
    {
        return catalog.loadTable(session, getSchemaTableName());
    }
}
