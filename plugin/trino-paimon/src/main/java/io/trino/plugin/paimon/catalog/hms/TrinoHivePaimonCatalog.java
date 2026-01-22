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
import io.trino.metastore.Database;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.TableType;
import io.trino.plugin.paimon.PaimonConfig;
import io.trino.plugin.paimon.catalog.AbstractPaimonTrinoCatalog;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.CoreOptions.PATH;

public class TrinoHivePaimonCatalog
        extends AbstractPaimonTrinoCatalog
{
    private static final String INPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonInputFormat";
    private static final String OUTPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonOutputFormat";

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

    private static boolean isPaimonTable(io.trino.metastore.Table table)
    {
        return INPUT_FORMAT_CLASS_NAME.equals(table.getStorage().getStorageFormat().getInputFormat())
                && OUTPUT_FORMAT_CLASS_NAME.equals(table.getStorage().getStorageFormat().getOutputFormat());
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
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Identifier identifier = Identifier.create(schemaTableName.getSchemaName(), schemaTableName.getTableName());
        FileIO fileIO = getFileIO(session);
        TableMetadata tableMetadata = loadTableMetadata(fileIO, identifier);
        Path path = new Path(tableMetadata.schema().options().get(PATH.key()));
        CatalogEnvironment catalogEnv =
                new CatalogEnvironment(
                        identifier,
                        tableMetadata.uuid(),
                        null,
                        null,
                        null,
                        false);
        return FileStoreTableFactory.create(fileIO, path, tableMetadata.schema(), catalogEnv);
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

    protected TableMetadata loadTableMetadata(FileIO fileIO, Identifier identifier)
    {
        return loadTableMetadata(fileIO, identifier, getHmsTable(identifier));
    }

    public io.trino.metastore.Table getHmsTable(Identifier identifier)
    {
        return metastore.getTable(identifier.getDatabaseName(), identifier.getTableName()).orElseThrow(() -> new TableNotFoundException(SchemaTableName.schemaTableName(identifier.getDatabaseName(), identifier.getTableName())));
    }

    private TableMetadata loadTableMetadata(FileIO fileIO, Identifier identifier, io.trino.metastore.Table table)
    {
        return new TableMetadata(
                loadTableSchema(fileIO, identifier, table),
                isExternalTable(table),
                identifier.getFullName() + "." + table.getParameters().get("transient_lastDdlTime"));
    }

    private TableSchema loadTableSchema(FileIO fileIO, Identifier identifier, io.trino.metastore.Table table)
    {
        if (isPaimonTable(table)) {
            return tableSchemaInFileSystem(
                    fileIO,
                    getTableLocation(identifier, table),
                    identifier.getBranchNameOrDefault())
                    .orElseThrow(() -> new TableNotFoundException(SchemaTableName.schemaTableName(identifier.getDatabaseName(), identifier.getTableName())));
        }

        throw new TableNotFoundException(SchemaTableName.schemaTableName(identifier.getDatabaseName(), identifier.getTableName()));
    }

    private boolean isExternalTable(io.trino.metastore.Table table)
    {
        return table != null && TableType.EXTERNAL_TABLE.name().equals(table.getTableType());
    }

    private Path getTableLocation(Identifier identifier, @Nullable io.trino.metastore.Table table)
    {
        Optional<String> tableLocationString = table.getStorage().getOptionalLocation();
        if (tableLocationString.isPresent()) {
            return new Path(tableLocationString.get());
        }
        String databaseName = identifier.getDatabaseName();
        String tableName = identifier.getTableName();
        Database database = metastore.getDatabase(databaseName).orElseThrow(() -> new SchemaNotFoundException("Database does not exist: " + databaseName));
        Optional<Path> tableLocationByDatabase = database.getLocation().map(databaseLocation -> new Path(databaseLocation, tableName));
        return tableLocationByDatabase.orElse(super.getTableLocation(identifier));
    }
}
