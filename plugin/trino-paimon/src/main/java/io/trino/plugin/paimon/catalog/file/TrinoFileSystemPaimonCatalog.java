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

package io.trino.plugin.paimon.catalog.file;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.plugin.paimon.catalog.AbstractPaimonTrinoCatalog;
import io.trino.plugin.paimon.fileio.PaimonFileIO;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.PATH;

public class TrinoFileSystemPaimonCatalog
        extends AbstractPaimonTrinoCatalog
{
    public TrinoFileSystemPaimonCatalog(
            PaimonConfig config,
            TrinoFileSystemFactory fileSystemFactory)
    {
        super(config, fileSystemFactory);
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String database)
    {
        FileIO fileIO = getFileIO(session);
        return uncheck(() -> fileIO.exists(newDatabasePathInFileSystem(database)));
    }

    @Override
    public List<String> listDatabases(ConnectorSession session)
    {
        return listDatabasesInFileSystem(session);
    }

    @Override
    public List<String> listTables(ConnectorSession session, Optional<String> namespace)
    {
        if (namespace.isPresent()) {
            return listTablesInFileSystem(session, newDatabasePathInFileSystem(namespace.get()));
        }
        else {
            return listDatabases(session).stream()
                    .flatMap(db -> listTablesInFileSystem(session, newDatabasePathInFileSystem(db)).stream())
                    .toList();
        }
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Identifier identifier = Identifier.create(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        Path tablePath = new Path(newDatabasePathInFileSystem(identifier.getDatabaseName()), identifier.getTableName());
        PaimonFileIO fileIO = new PaimonFileIO(fileSystemFactory.create(session), tablePath);
        TableSchema schema = loadTableSchema(fileIO, identifier).orElseThrow(() -> new TableNotFoundException(schemaTableName));
        Path path = new Path(schema.options().get(PATH.key()));

        return FileStoreTableFactory.create(fileIO, path, schema);
    }

    public Optional<TableSchema> loadTableSchema(FileIO fileIO, Identifier identifier)
    {
        Path tablePath = new Path(newDatabasePathInFileSystem(identifier.getDatabaseName()), identifier.getObjectName());
        Optional<TableSchema> schema = new SchemaManager(fileIO, tablePath).latest();
        schema.ifPresent(s -> s.options().put(PATH.key(), tablePath.toString()));
        return schema;
    }
}
