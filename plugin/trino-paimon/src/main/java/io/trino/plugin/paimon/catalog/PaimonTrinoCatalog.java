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
package io.trino.plugin.paimon.catalog;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.plugin.paimon.catalog.inner.TrinoHiveCatalog;
import io.trino.plugin.paimon.fileio.PaimonFileIO;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Catalog.TableAlreadyExistException;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PaimonTrinoCatalog
{
    private final PaimonFileIO paimonFileIO;

    private Catalog catalog;

    public PaimonTrinoCatalog(
            PaimonConfig config,
            TrinoFileSystemFactory fileSystemFactory,
            Optional<HiveMetastoreFactory> hiveMetastoreFactory,
            ConnectorIdentity identity)
    {
        requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        requireNonNull(hiveMetastoreFactory, "hiveMetastoreFactory is null");
        requireNonNull(identity, "identity is null");

        paimonFileIO = new PaimonFileIO(fileSystemFactory, identity, null);
        switch (config.getCatalogType()) {
            case FILESYSTEM -> {
                checkArgument(config.getWarehouse() != null, "Warehouse is required for filesystem catalog");
                catalog = new FileSystemCatalog(paimonFileIO, new Path(config.getWarehouse()), config.toOptions());
            }
            case HIVE -> {
                checkArgument(hiveMetastoreFactory.isPresent(), "Hive metastore factory is required for hive catalog");
                checkArgument(config.getWarehouse() != null, "Warehouse is required for hive catalog");
                HiveMetastore metastore = hiveMetastoreFactory.orElseThrow().createMetastore(Optional.of(identity));
                FileSystemCatalog fileSystemCatalog = new FileSystemCatalog(paimonFileIO, new Path(config.getWarehouse()), config.toOptions())
                {
                    @Override
                    public Database getDatabaseImpl(String name)
                    {
                        // Paimon database is dynamic, we judge the existence of database by its directory existence.
                        // But filesystem like S3 does not have the concept of empty directory, so just leave it to metastore.
                        return Database.of(name);
                    }
                };
                catalog = new TrinoHiveCatalog(metastore, fileSystemCatalog);
            }
        }
    }

    public boolean exists(ConnectorSession session, Path path)
            throws IOException
    {
        paimonFileIO.setConnectorSession(session);
        return paimonFileIO.exists(path);
    }

    public void createDatabase(ConnectorSession session, String name, boolean ignoreIfExists)
            throws Catalog.DatabaseAlreadyExistException
    {
        paimonFileIO.setConnectorSession(session);
        catalog.createDatabase(name, ignoreIfExists);
    }

    public List<String> listDatabases(ConnectorSession session)
    {
        paimonFileIO.setConnectorSession(session);
        return catalog.listDatabases();
    }

    public Database getDatabase(ConnectorSession session, String name)
            throws DatabaseNotExistException
    {
        paimonFileIO.setConnectorSession(session);
        return catalog.getDatabase(name);
    }

    public Table getTable(ConnectorSession session, Identifier identifier)
            throws TableNotExistException
    {
        paimonFileIO.setConnectorSession(session);
        return catalog.getTable(identifier);
    }

    public List<String> listTables(ConnectorSession session, String s)
            throws DatabaseNotExistException
    {
        paimonFileIO.setConnectorSession(session);
        return catalog.listTables(s);
    }

    public void createTable(ConnectorSession session, Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException
    {
        paimonFileIO.setConnectorSession(session);
        catalog.createTable(identifier, schema, ignoreIfExists);
    }

    public void dropTable(ConnectorSession session, Identifier identifier, boolean b)
            throws TableNotExistException
    {
        paimonFileIO.setConnectorSession(session);
        catalog.dropTable(identifier, b);
    }

    public void close()
            throws Exception
    {
        if (catalog != null) {
            catalog.close();
        }
    }

    @VisibleForTesting
    public Catalog getCurrentCatalog()
    {
        return catalog;
    }
}
