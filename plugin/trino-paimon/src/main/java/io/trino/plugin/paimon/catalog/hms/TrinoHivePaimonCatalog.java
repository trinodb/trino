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
package io.trino.plugin.paimon.catalog.hms;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.plugin.paimon.catalog.AbstractPaimonTrinoCatalog;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TrinoHivePaimonCatalog
        extends AbstractPaimonTrinoCatalog
{
    private final CachingHiveMetastore metastore;

    public TrinoHivePaimonCatalog(
            PaimonConfig config,
            CachingHiveMetastore metastore,
            TrinoFileSystemFactory fileSystemFactory)
    {
        super(
                config,
                fileSystemFactory);

        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String database)
    {
        return metastore.getDatabase(database).isPresent();
    }

    @Override
    public List<String> listDatabases(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public List<String> listTables(ConnectorSession session, Optional<String> namespace)
    {
        if (namespace.isPresent()) {
            String databaseName = namespace.get();
            if (!databaseExists(session, databaseName)) {
                throw new SchemaNotFoundException("Database does not exist: " + databaseName);
            }
            return metastore.getTables(databaseName).stream().map(t -> t.tableName().getTableName()).collect(Collectors.toList());
        }

        return metastore.getAllDatabases().stream().flatMap(name -> metastore.getTables(name).stream())
                .map(t -> t.tableName().getTableName())
                .collect(Collectors.toList());
    }
}
