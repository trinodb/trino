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
import io.trino.plugin.paimon.ClassLoaderUtils;
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.plugin.paimon.fileio.PaimonFileIO;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Trino catalog, use it after set session.
 */
public class PaimonTrinoCatalog
{
    private final PaimonConfig config;

    private final TrinoFileSystemFactory trinoFileSystemFactory;

    private Catalog current;

    private PaimonFileIO paimonFileIO;

    public PaimonTrinoCatalog(
            PaimonConfig config,
            TrinoFileSystemFactory trinoFileSystemFactory,
            ConnectorIdentity identity)
    {
        this.config = config;
        this.trinoFileSystemFactory = trinoFileSystemFactory;
        init(identity);
    }

    public void init(ConnectorIdentity identity)
    {
        current =
                ClassLoaderUtils.runWithContextClassLoader(
                        () -> {
                            paimonFileIO = new PaimonFileIO(trinoFileSystemFactory, identity, null);

                            switch (config.getCatalogType()) {
                                case FILESYSTEM:
                                    checkArgument(config.getWarehouse() != null, "Warehouse is required for filesystem catalog");
                                    return new FileSystemCatalog(paimonFileIO, new Path(config.getWarehouse()), config.toOptions());
                                default:
                                    throw new IllegalArgumentException("Unsupported catalog type: " + config.getCatalogType());
                            }
                        },
                        this.getClass().getClassLoader());
    }

    public boolean exists(ConnectorSession session, Path path)
            throws IOException
    {
        paimonFileIO.setConnectorSession(session);
        return paimonFileIO.exists(path);
    }

    public List<String> listDatabases(ConnectorSession session)
    {
        paimonFileIO.setConnectorSession(session);
        return current.listDatabases();
    }

    public Database getDatabase(ConnectorSession session, String name)
            throws Catalog.DatabaseNotExistException
    {
        paimonFileIO.setConnectorSession(session);
        return current.getDatabase(name);
    }

    public void dropDatabase(ConnectorSession session, String s, boolean b, boolean b1)
            throws Catalog.DatabaseNotExistException, Catalog.DatabaseNotEmptyException
    {
        paimonFileIO.setConnectorSession(session);
        current.dropDatabase(s, b, b1);
    }

    public Table getTable(ConnectorSession session, Identifier identifier)
            throws Catalog.TableNotExistException
    {
        paimonFileIO.setConnectorSession(session);
        return current.getTable(identifier);
    }

    public List<String> listTables(ConnectorSession session, String s)
            throws Catalog.DatabaseNotExistException
    {
        paimonFileIO.setConnectorSession(session);
        return current.listTables(s);
    }

    public void dropTable(ConnectorSession session, Identifier identifier, boolean b)
            throws Catalog.TableNotExistException
    {
        paimonFileIO.setConnectorSession(session);
        current.dropTable(identifier, b);
    }

    public void createTable(
            ConnectorSession session, Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws Catalog.TableAlreadyExistException, Catalog.DatabaseNotExistException
    {
        paimonFileIO.setConnectorSession(session);
        current.createTable(identifier, schema, ignoreIfExists);
    }

    public void renameTable(
            ConnectorSession session,
            Identifier fromTable,
            Identifier toTable,
            boolean ignoreIfExistsb)
            throws Catalog.TableNotExistException, Catalog.TableAlreadyExistException
    {
        paimonFileIO.setConnectorSession(session);
        current.renameTable(fromTable, toTable, ignoreIfExistsb);
    }

    public void alterTable(
            ConnectorSession session,
            Identifier identifier,
            List<SchemaChange> list,
            boolean ignoreIfExists)
            throws Catalog.TableNotExistException,
            Catalog.ColumnAlreadyExistException,
            Catalog.ColumnNotExistException
    {
        paimonFileIO.setConnectorSession(session);
        current.alterTable(identifier, list, ignoreIfExists);
    }

    public void close()
            throws Exception
    {
        if (current != null) {
            current.close();
        }
    }

    public void createDatabase(ConnectorSession session, String name, boolean ignoreIfExists)
            throws Catalog.DatabaseAlreadyExistException
    {
        paimonFileIO.setConnectorSession(session);
        current.createDatabase(name, ignoreIfExists);
    }
}
