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
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;

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

    protected Optional<TableSchema> tableSchemaInFileSystem(FileIO fileIO, Path tablePath, String branchName)
    {
        Optional<TableSchema> schema =
                new SchemaManager(fileIO, tablePath, branchName).latest();
        if (!DEFAULT_MAIN_BRANCH.equals(branchName)) {
            schema =
                    schema.map(
                            s -> {
                                Options branchOptions = new Options(s.options());
                                branchOptions.set(CoreOptions.BRANCH, branchName);
                                return s.copy(branchOptions.toMap());
                            });
        }
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

    protected Path getTableLocation(Identifier identifier)
    {
        return new Path(newDatabasePath(identifier.getDatabaseName()), identifier.getTableName());
    }

    @Override
    public PaimonConfig config()
    {
        return config;
    }
}
