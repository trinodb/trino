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
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.plugin.paimon.fileio.PaimonFileIO;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.CoreOptions.PATH;

public abstract class AbstractPaimonTrinoCatalog
        implements TrinoCatalog
{
    protected final TrinoFileSystemFactory fileSystemFactory;
    private final PaimonConfig config;
    private final Path warehouse;

    protected AbstractPaimonTrinoCatalog(
            PaimonConfig config,
            TrinoFileSystemFactory fileSystemFactory)
    {
        requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileSystemFactory = fileSystemFactory;
        this.config = config;
        this.warehouse = new Path(config.getWarehouse());
    }

    protected static <T> T uncheck(Callable<T> callable)
    {
        try {
            return callable.call();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected FileIO getFileIO(ConnectorSession session)
    {
        return new PaimonFileIO(fileSystemFactory.create(session), warehouse);
    }

    @Override
    public String warehouse()
    {
        return warehouse.toString();
    }

    protected List<String> listDatabasesInFileSystem(ConnectorSession session)
    {
        FileIO fileIO = getFileIO(session);
        return uncheck(() -> {
            List<String> databases = new ArrayList<>();
            for (FileStatus status : fileIO.listDirectories(warehouse)) {
                Path path = status.getPath();
                if (status.isDir() && path.getName().endsWith(DB_SUFFIX)) {
                    String fileName = path.getName();
                    databases.add(fileName.substring(0, fileName.length() - DB_SUFFIX.length()));
                }
            }
            return databases;
        });
    }

    protected List<String> listTablesInFileSystem(ConnectorSession session, Path databasePath)
    {
        FileIO fileIO = getFileIO(session);
        return uncheck(() -> {
            List<String> tables = new ArrayList<>();
            for (FileStatus status : fileIO.listDirectories(databasePath)) {
                if (status.isDir() && tableExistsInFileSystem(fileIO, status.getPath())) {
                    tables.add(status.getPath().getName());
                }
            }
            return tables;
        });
    }

    protected boolean tableExistsInFileSystem(FileIO fileIO, Path tablePath)
    {
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);

        // in order to improve the performance, check the schema-0 firstly.
        boolean schemaZeroExists = schemaManager.schemaExists(0);
        if (schemaZeroExists) {
            return true;
        }
        else {
            // if schema-0 not exists, fallback to check other schemas
            return !schemaManager.listAllIds().isEmpty();
        }
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Identifier identifier = Identifier.create(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        Path tablePath = new Path(newDatabasePath(identifier.getDatabaseName()), identifier.getTableName());
        PaimonFileIO fileIO = new PaimonFileIO(fileSystemFactory.create(session), tablePath);
        TableSchema schema = loadTableSchema(fileIO, identifier).orElseThrow(() -> new TableNotFoundException(schemaTableName));
        Path path = new Path(schema.options().get(PATH.key()));

        return FileStoreTableFactory.create(fileIO, path, schema);
    }

    @Override
    public PaimonConfig config()
    {
        return config;
    }

    public Optional<TableSchema> loadTableSchema(FileIO fileIO, Identifier identifier)
    {
        Path tablePath = new Path(newDatabasePath(identifier.getDatabaseName()), identifier.getObjectName());
        Optional<TableSchema> schema = new SchemaManager(fileIO, tablePath).latest();
        schema.ifPresent(s -> s.options().put(PATH.key(), tablePath.toString()));
        return schema;
    }

    protected Path newDatabasePath(String database)
    {
        return newDatabasePath(warehouse(), database);
    }

    protected Path newDatabasePath(String warehouse, String database)
    {
        return new Path(warehouse, database + DB_SUFFIX);
    }
}
