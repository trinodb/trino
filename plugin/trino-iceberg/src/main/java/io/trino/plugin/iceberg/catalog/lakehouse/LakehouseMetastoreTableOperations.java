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
package io.trino.plugin.iceberg.catalog.lakehouse;

import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.hms.AbstractMetastoreTableOperations;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.lakehouse.LakehouseErrorTranslator.notSupported;

public class LakehouseMetastoreTableOperations
        extends AbstractMetastoreTableOperations
{
    public LakehouseMetastoreTableOperations(
            FileIO fileIo,
            CachingHiveMetastore metastore,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location,
            EncryptionManagerFactory encryptionManagerFactory)
    {
        super(fileIo, metastore, session, database, table, owner, location, encryptionManagerFactory);
    }

    @Override
    protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
    {
        throw notSupported("updating table metadata");
    }

    @Override
    protected void commitMaterializedView(TableMetadata base, TableMetadata metadata)
    {
        throw notSupported("updating materialized views");
    }
}
