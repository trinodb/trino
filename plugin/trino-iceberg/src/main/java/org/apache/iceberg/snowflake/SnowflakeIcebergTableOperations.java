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
package org.apache.iceberg.snowflake;

import io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations;
import io.trino.plugin.iceberg.catalog.snowflake.TrinoSnowflakeCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class SnowflakeIcebergTableOperations
        extends AbstractIcebergTableOperations
{
    private final SnowflakeTableOperations icebergSnowflakeTableOperations;

    public SnowflakeIcebergTableOperations(
            TrinoSnowflakeCatalog trinoSnowflakeCatalog,
            FileIO fileIo,
            ConnectorSession session,
            String snowflakeDatabase,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        super(fileIo, session, database, table, owner, location);
        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(snowflakeDatabase, database), table);
        this.icebergSnowflakeTableOperations = requireNonNull((SnowflakeTableOperations) trinoSnowflakeCatalog.getSnowflakeCatalog().newTableOps(tableIdentifier), "snowflakeTableOperations is null");
    }

    // Overridden to skip setting version field as Snowflake's Iceberg table's metadata file version is not following Trino's metadata file naming convention(IcebergUtil#parseVersion).
    // Additionally, version is used only for writes which Snowflake catalog does not support at this time and hence it's safe to ignore setting version.
    @Override
    public void initializeFromMetadata(TableMetadata tableMetadata)
    {
        checkState(currentMetadata == null, "already initialized");
        currentMetadata = tableMetadata;
        currentMetadataLocation = tableMetadata.metadataFileLocation();
        shouldRefresh = false;
    }

    @Override
    protected String getRefreshedLocation(boolean invalidateCaches)
    {
        if (invalidateCaches) {
            icebergSnowflakeTableOperations.refresh();
            return icebergSnowflakeTableOperations.currentMetadataLocation();
        }

        String location = icebergSnowflakeTableOperations.currentMetadataLocation();
        if (isNull(location)) {
            icebergSnowflakeTableOperations.refresh();
            return icebergSnowflakeTableOperations.currentMetadataLocation();
        }
        return location;
    }

    @Override
    protected void commitNewTable(TableMetadata metadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "Snowflake managed Iceberg tables do not support modifications");
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "Snowflake managed Iceberg tables do not support modifications");
    }

    @Override
    protected void commitMaterializedViewRefresh(TableMetadata base, TableMetadata metadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "Snowflake managed Iceberg tables do not support modifications");
    }
}
