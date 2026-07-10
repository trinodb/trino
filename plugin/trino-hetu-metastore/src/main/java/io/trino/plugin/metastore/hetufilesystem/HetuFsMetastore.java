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
package io.trino.plugin.metastore.hetufilesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.metastore.jdbc.JdbcMetadataUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogAlreadyExistsException;
import io.trino.spi.connector.CatalogNotEmptyException;
import io.trino.spi.connector.CatalogNotFoundException;
import io.trino.spi.connector.SchemaAlreadyExistsException;
import io.trino.spi.connector.SchemaNotEmptyException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableAlreadyExistsException;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.filesystem.FileBasedLock;
import io.trino.spi.filesystem.HetuFileSystemClient;
import io.trino.spi.filesystem.util.SecurePathWhiteList;
import io.trino.spi.metastore.HetuMetastore;
import io.trino.spi.metastore.model.CatalogEntity;
import io.trino.spi.metastore.model.DatabaseEntity;
import io.trino.spi.metastore.model.TableEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.metastore.HetuErrorCode.HETU_METASTORE_CODE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class HetuFsMetastore
        implements HetuMetastore
{
    private static final Logger LOG = Logger.get(JdbcMetadataUtil.class);
    private static final JsonCodec<CatalogEntity> CATALOG_CODEC = JsonCodec.jsonCodec(CatalogEntity.class);
    private static final JsonCodec<DatabaseEntity> DATABASE_CODEC = JsonCodec.jsonCodec(DatabaseEntity.class);
    private static final JsonCodec<TableEntity> TABLE_CODEC = JsonCodec.jsonCodec(TableEntity.class);
    private static final String METADATA_SUFFIX = ".metadata";
    private final HetuFileSystemClient client;
    private final String metadataPath;

    /*
     * catalog1.metadata
     * catalog1/
     *       database1.metadata
     *       database1/
     *               table1.metadata
     *               table2.metadata
     *       database2.metadata
     *       database2/
     *               table1.metadata
     *               table2.metadata
     *  catalog2.metadata
     *  catalog2/
     *       database1.metadata
     *       database1/
     *               table1.metedata
     * */
    @Inject
    public HetuFsMetastore(HetuFsMetastoreConfig metadataConfig, @HetuFsMetadataClient HetuFileSystemClient client)
    {
        this.metadataPath = metadataConfig.getHetuFileSystemMetastorePath();
        this.client = client;

        try {
            checkArgument(!metadataPath.contains("../"),
                    "Metadata directory path must be absolute and at user workspace: " + SecurePathWhiteList.getSecurePathWhiteList().toString());
            checkArgument(SecurePathWhiteList.isSecurePath(metadataPath),
                    "Metadata directory path must be at user workspace " + SecurePathWhiteList.getSecurePathWhiteList().toString());
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to get secure path list.", e);
        }

        if (!client.exists(Paths.get(metadataPath))) {
            try {
                client.createDirectories(Paths.get(metadataPath));
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Failed to create the directories.", e);
            }
        }
    }

    public synchronized void runTransaction(HetuFsMetadataHandler<TrinoException> handler)
    {
        try {
            Lock transactionLock;
            transactionLock = new FileBasedLock(client, Paths.get(metadataPath));
            transactionLock.lock();
            try {
                handler.handle();
            }
            finally {
                transactionLock.unlock();
            }
        }
        catch (IOException e) {
            throw new TrinoException(HETU_METASTORE_CODE, "Create transaction lock failed", e);
        }
    }

    private Path getCatalogMetadataPath(String catalogName)
    {
        return Paths.get(metadataPath, catalogName + METADATA_SUFFIX);
    }

    private Path getCatalogMetadataDir(String catalogName)
    {
        return Paths.get(metadataPath, catalogName);
    }

    private void assertCatalogExist(String catalogName)
    {
        if (!client.exists(getCatalogMetadataPath(catalogName))) {
            throw new CatalogNotFoundException(catalogName);
        }
    }

    private void assertCatalogNotExist(String catalogName)
    {
        if (client.exists(getCatalogMetadataPath(catalogName))) {
            throw new CatalogAlreadyExistsException(catalogName);
        }
    }

    @Override
    public void createCatalog(CatalogEntity catalog)
    {
        checkArgument(catalog.getName().matches("[\\p{Alnum}_]+"), "Invalid catalog name");

        runTransaction(() -> {
            assertCatalogNotExist(catalog.getName());
            try (OutputStream outputStream = client.newOutputStream(getCatalogMetadataPath(catalog.getName()))) {
                outputStream.write(CATALOG_CODEC.toJsonBytes(catalog));
            }
            catch (IOException e) {
                throw new TrinoException(HETU_METASTORE_CODE, e);
            }
        });
    }

    @Override
    public void createCatalogIfNotExist(CatalogEntity catalog)
    {
        try {
            createCatalog(catalog);
        }
        catch (TrinoException e) {
            Optional<CatalogEntity> existedCatalog = getCatalog(catalog.getName());
            if (!(existedCatalog.isPresent() && existedCatalog.get().equals(catalog))) {
                throw new TrinoException(HETU_METASTORE_CODE, e.getMessage(), e.getCause());
            }
        }
    }

    @Override
    public void alterCatalog(String catalogName, CatalogEntity newCatalog)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
        checkArgument(newCatalog.getName().matches("[\\p{Alnum}_]+"), "Invalid new catalog name");

        runTransaction(() -> {
            if (!catalogName.equals(newCatalog.getName())) {
                throw new TrinoException(HETU_METASTORE_CODE, "Cannot alter a catalog's name");
            }
            assertCatalogExist(catalogName);
            try {
                CatalogEntity orgCatalog;
                try (InputStream inputStream = client.newInputStream(getCatalogMetadataPath(catalogName))) {
                    String catalogJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
                    orgCatalog = CATALOG_CODEC.fromJson(catalogJson);
                }
                try (OutputStream outputStream = client.newOutputStream(getCatalogMetadataPath(catalogName))) {
                    // can not update the create time.
                    newCatalog.setCreateTime(orgCatalog.getCreateTime());
                    outputStream.write(CATALOG_CODEC.toJsonBytes(newCatalog));
                }
            }
            catch (IOException e) {
                throw new TrinoException(HETU_METASTORE_CODE, e);
            }
        });
    }

    @Override
    public void dropCatalog(String catalogName)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");

        runTransaction(() -> {
            assertCatalogExist(catalogName);
            Path catalogMetadataDir = getCatalogMetadataDir(catalogName);
            if (client.exists(catalogMetadataDir)) {
                try (Stream<Path> databasePaths = client.list(catalogMetadataDir)) {
                    // there are no database in this catalog.
                    if (databasePaths.count() == 0) {
                        try {
                            client.deleteIfExists(getCatalogMetadataPath(catalogName));
                        }
                        catch (IOException e) {
                            LOG.error(e, "Delete catalog metadata file failed");
                        }
                        client.deleteRecursively(catalogMetadataDir);
                    }
                    else {
                        throw new CatalogNotEmptyException(catalogName);
                    }
                }
                catch (IOException e) {
                    throw new TrinoException(HETU_METASTORE_CODE, e);
                }
            }
            else {
                try {
                    client.deleteIfExists(getCatalogMetadataPath(catalogName));
                }
                catch (IOException e) {
                    throw new TrinoException(HETU_METASTORE_CODE, e);
                }
            }
        });
    }

    @Override
    public Optional<CatalogEntity> getCatalog(String catalogName)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");

        try {
            assertCatalogExist(catalogName);
        }
        catch (CatalogNotFoundException e) {
            return Optional.empty();
        }
        try (InputStream inputStream = client.newInputStream(getCatalogMetadataPath(catalogName))) {
            String catalogJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
            return Optional.of(CATALOG_CODEC.fromJson(catalogJson));
        }
        catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<CatalogEntity> getCatalogs()
    {
        List<CatalogEntity> catalogs = new ArrayList<>();
        try (Stream<Path> paths = client.list(Paths.get(metadataPath))) {
            List<Path> catalogPaths = paths.filter(path -> path.toString().endsWith(METADATA_SUFFIX))
                    .collect(Collectors.toList());

            for (Path catalogPath : catalogPaths) {
                try (InputStream inputStream = client.newInputStream(catalogPath)) {
                    String catalogJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
                    CatalogEntity catalog = CATALOG_CODEC.fromJson(catalogJson);
                    catalogs.add(catalog);
                }
                catch (IOException e) {
                    LOG.error(e, "Get catalog metadata failed");
                }
            }
            return catalogs;
        }
        catch (IOException e) {
            throw new TrinoException(HETU_METASTORE_CODE, e);
        }
    }

    private Path getDatabaseMetadataPath(String catalogName, String databaseName)
    {
        return Paths.get(metadataPath, catalogName, databaseName + METADATA_SUFFIX);
    }

    private Path getDatabaseMetadataDir(String catalogName, String databaseName)
    {
        return Paths.get(metadataPath, catalogName, databaseName);
    }

    private void assertDatabaseExist(String catalogName, String databaseName)
    {
        if (!client.exists(getDatabaseMetadataPath(catalogName, databaseName))) {
            throw new SchemaNotFoundException(databaseName);
        }
    }

    private void assertDatabaseNotExist(String catalogName, String databaseName)
    {
        if (client.exists(getDatabaseMetadataPath(catalogName, databaseName))) {
            throw new SchemaAlreadyExistsException(databaseName);
        }
    }

    @Override
    public void createDatabase(DatabaseEntity database)
    {
        checkArgument(database.getName().matches("[\\p{Alnum}_]+"), "Invalid database name");
        checkArgument(database.getCatalogName().matches("[\\p{Alnum}_]+"), "Invalid catalog name");

        runTransaction(() -> {
            try {
                assertCatalogExist(database.getCatalogName());
                assertDatabaseNotExist(database.getCatalogName(), database.getName());
                Path databaseMetadataDir = getDatabaseMetadataDir(database.getCatalogName(), database.getName());
                if (!client.exists(databaseMetadataDir)) {
                    client.createDirectories(databaseMetadataDir);
                }

                try (OutputStream outputStream = client.newOutputStream(getDatabaseMetadataPath(database.getCatalogName(), database.getName()))) {
                    outputStream.write(DATABASE_CODEC.toJsonBytes(database));
                }
            }
            catch (IOException e) {
                throw new TrinoException(HETU_METASTORE_CODE, e);
            }
        });
    }

    @Override
    public void createDatabaseIfNotExist(DatabaseEntity database)
    {
        try {
            createDatabase(database);
        }
        catch (TrinoException e) {
            Optional<DatabaseEntity> existedDatabase = getDatabase(database.getCatalogName(), database.getName());
            if (!(existedDatabase.isPresent() && existedDatabase.get().equals(database))) {
                throw new TrinoException(HETU_METASTORE_CODE, e.getMessage(), e.getCause());
            }
        }
    }

    @Override
    public void alterDatabase(String catalogName, String databaseName, DatabaseEntity newDatabase)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
        checkArgument(databaseName.matches("[\\p{Alnum}_]+"), "Invalid database name");
        checkArgument(newDatabase.getName().matches("[\\p{Alnum}_]+"), "Invalid new database name");

        runTransaction(() -> {
            if (!catalogName.equals(newDatabase.getCatalogName())) {
                throw new TrinoException(HETU_METASTORE_CODE, "The catalog name is not correct");
            }
            assertCatalogExist(catalogName);
            assertDatabaseExist(catalogName, databaseName);

            try {
                // modify the name
                Path orgDbDirectory = getDatabaseMetadataDir(catalogName, databaseName);
                Path newDbDirectory = getDatabaseMetadataDir(catalogName, newDatabase.getName());
                Path orgDbMetadata = getDatabaseMetadataPath(catalogName, databaseName);
                Path newDbMetadata = getDatabaseMetadataPath(catalogName, newDatabase.getName());
                if (!databaseName.equals(newDatabase.getName())) {
                    client.move(orgDbDirectory, newDbDirectory);
                    client.move(orgDbMetadata, newDbMetadata);
                    try (Stream<Path> tablePaths = client.list(newDbDirectory)) {
                        tablePaths.filter(tablePath -> tablePath.getFileName().toString().endsWith(METADATA_SUFFIX)).forEach(tablePath -> {
                            try {
                                // alter table's database name.
                                TableEntity tableEntity;
                                try (InputStream inputStream = client.newInputStream(tablePath)) {
                                    String tableJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
                                    tableEntity = TABLE_CODEC.fromJson(tableJson);
                                }
                                tableEntity.setDatabaseName(newDatabase.getName());
                                try (OutputStream outputStream = client.newOutputStream(tablePath)) {
                                    outputStream.write(TABLE_CODEC.toJsonBytes(tableEntity));
                                }
                            }
                            catch (IOException e) {
                                throw new TrinoException(HETU_METASTORE_CODE, e);
                            }
                        });
                    }
                }

                DatabaseEntity databaseEntity;
                try (InputStream inputStream = client.newInputStream(newDbMetadata)) {
                    String tableJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
                    databaseEntity = DATABASE_CODEC.fromJson(tableJson);
                }
                try (OutputStream outputStream = client.newOutputStream(newDbMetadata)) {
                    newDatabase.setCreateTime(databaseEntity.getCreateTime());
                    outputStream.write(DATABASE_CODEC.toJsonBytes(newDatabase));
                }
            }
            catch (IOException e) {
                throw new TrinoException(HETU_METASTORE_CODE, e);
            }
        });
    }

    @Override
    public void dropDatabase(String catalogName, String databaseName)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
        checkArgument(databaseName.matches("[\\p{Alnum}_]+"), "Invalid database name");

        runTransaction(() -> {
            assertCatalogExist(catalogName);
            assertDatabaseExist(catalogName, databaseName);
            Path databaseMetadataDir = getDatabaseMetadataDir(catalogName, databaseName);
            Path databaseMetadataPath = getDatabaseMetadataPath(catalogName, databaseName);
            if (client.exists(databaseMetadataDir)) {
                try (Stream<Path> tablePaths = client.list(databaseMetadataDir)) {
                    if (tablePaths.count() == 0) {
                        try {
                            client.deleteIfExists(databaseMetadataPath);
                        }
                        catch (IOException e) {
                            LOG.error(e, "Delete database metadata failed");
                        }
                        client.deleteRecursively(databaseMetadataDir);
                    }
                    else {
                        throw new SchemaNotEmptyException(databaseName);
                    }
                }
                catch (IOException e) {
                    throw new TrinoException(HETU_METASTORE_CODE, e);
                }
            }
            else {
                try {
                    client.deleteIfExists(databaseMetadataPath);
                }
                catch (IOException e) {
                    LOG.error(e, "Delete database metadata failed");
                }
            }
        });
    }

    @Override
    public Optional<DatabaseEntity> getDatabase(String catalogName, String databaseName)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
        checkArgument(databaseName.matches("[\\p{Alnum}_]+"), "Invalid database name");

        try {
            assertCatalogExist(catalogName);
            assertDatabaseExist(catalogName, databaseName);
        }
        catch (CatalogNotFoundException | SchemaNotFoundException e) {
            return Optional.empty();
        }

        try (InputStream inputStream = client.newInputStream(getDatabaseMetadataPath(catalogName, databaseName))) {
            String databaseJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
            return Optional.of(DATABASE_CODEC.fromJson(databaseJson));
        }
        catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<DatabaseEntity> getAllDatabases(String catalogName)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");

        List<DatabaseEntity> databases = new ArrayList<>();
        try {
            assertCatalogExist(catalogName);
        }
        catch (CatalogNotFoundException e) {
            return ImmutableList.of();
        }
        try (Stream<Path> paths = client.list(getCatalogMetadataDir(catalogName))) {
            List<Path> databasePaths = paths.filter(path -> path.toString().endsWith(METADATA_SUFFIX))
                    .collect(Collectors.toList());

            for (Path databasePath : databasePaths) {
                try (InputStream inputStream = client.newInputStream(databasePath)) {
                    String catalogJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
                    DatabaseEntity database = DATABASE_CODEC.fromJson(catalogJson);
                    databases.add(database);
                }
                catch (IOException e) {
                    LOG.error(e, "Get database metadata failed");
                }
            }
            return databases;
        }
        catch (IOException e) {
            throw new TrinoException(HETU_METASTORE_CODE, e);
        }
    }

    private Path getTableMetadataPath(String catalogName, String databaseName, String tableName)
    {
        return Paths.get(metadataPath, catalogName, databaseName, tableName + METADATA_SUFFIX);
    }

    private void assertTableExist(String catalogName, String databaseName, String tableName)
    {
        if (!client.exists(getTableMetadataPath(catalogName, databaseName, tableName))) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
    }

    private void assertTableNotExist(String catalogName, String databaseName, String tableName)
    {
        if (client.exists(getTableMetadataPath(catalogName, databaseName, tableName))) {
            throw new TableAlreadyExistsException(new SchemaTableName(databaseName, tableName));
        }
    }

    @Override
    public void createTable(TableEntity table)
    {
        runTransaction(() -> {
            String catalogName = table.getCatalogName();
            String databaseName = table.getDatabaseName();
            String tableName = table.getName();

            checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
            checkArgument(databaseName.matches("[\\p{Alnum}_]+"), "Invalid database name");
            checkArgument(tableName.matches("[\\p{Alnum}_]+"), "Invalid table name");

            assertCatalogExist(catalogName);
            assertDatabaseExist(catalogName, databaseName);
            assertTableNotExist(catalogName, databaseName, tableName);

            try (OutputStream outputStream = client.newOutputStream(getTableMetadataPath(catalogName, databaseName, tableName))) {
                outputStream.write(TABLE_CODEC.toJsonBytes(table));
            }
            catch (IOException e) {
                throw new TrinoException(HETU_METASTORE_CODE, e);
            }
        });
    }

    @Override
    public void createTableIfNotExist(TableEntity table)
    {
        try {
            createTable(table);
        }
        catch (TrinoException e) {
            Optional<TableEntity> existedTable = getTable(table.getCatalogName(), table.getDatabaseName(), table.getName());
            if (!(existedTable.isPresent() && existedTable.get().equals(table))) {
                throw new TrinoException(HETU_METASTORE_CODE, e.getMessage(), e.getCause());
            }
        }
    }

    @Override
    public void dropTable(String catalogName, String databaseName, String tableName)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
        checkArgument(databaseName.matches("[\\p{Alnum}_]+"), "Invalid database name");
        checkArgument(tableName.matches("[\\p{Alnum}_]+"), "Invalid table name");

        runTransaction(() -> {
            assertCatalogExist(catalogName);
            assertDatabaseExist(catalogName, databaseName);
            try {
                if (!client.deleteIfExists(getTableMetadataPath(catalogName, databaseName, tableName))) {
                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                }
            }
            catch (IOException e) {
                throw new TrinoException(HETU_METASTORE_CODE, e);
            }
        });
    }

    @Override
    public void alterTable(String catalogName, String databaseName, String oldTableName, TableEntity newTable)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
        checkArgument(databaseName.matches("[\\p{Alnum}_]+"), "Invalid database name");
        checkArgument(oldTableName.matches("[\\p{Alnum}_]+"), "Invalid table name");
        checkArgument(newTable.getName().matches("[\\p{Alnum}_]+"), "Invalid new table name");

        runTransaction(() -> {
            if (!catalogName.equals(newTable.getCatalogName()) || !databaseName.equals(newTable.getDatabaseName())) {
                throw new TrinoException(HETU_METASTORE_CODE, "The catalog name or schema name is not correct");
            }
            assertCatalogExist(catalogName);
            assertDatabaseExist(catalogName, databaseName);
            assertTableExist(catalogName, databaseName, oldTableName);

            try {
                Path orgTablePath = getTableMetadataPath(catalogName, databaseName, oldTableName);
                Path newTablePath = getTableMetadataPath(catalogName, databaseName, newTable.getName());
                // alter table name.
                if (!oldTableName.equals(newTable.getName())) {
                    client.move(orgTablePath, newTablePath);
                }

                TableEntity orgTable;
                try (InputStream inputStream = client.newInputStream(newTablePath)) {
                    String tableJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
                    orgTable = TABLE_CODEC.fromJson(tableJson);
                }
                try (OutputStream outputStream = client.newOutputStream(newTablePath)) {
                    newTable.setCreateTime(orgTable.getCreateTime());
                    outputStream.write(TABLE_CODEC.toJsonBytes(newTable));
                }
            }
            catch (IOException e) {
                throw new TrinoException(HETU_METASTORE_CODE, e);
            }
        });
    }

    @Override
    public Optional<TableEntity> getTable(String catalogName, String databaseName, String table)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
        checkArgument(databaseName.matches("[\\p{Alnum}_]+"), "Invalid database name");
        checkArgument(table.matches("[\\p{Alnum}_]+"), "Invalid table name");

        try {
            assertCatalogExist(catalogName);
            assertDatabaseExist(catalogName, databaseName);
            assertTableExist(catalogName, databaseName, table);
        }
        catch (CatalogNotFoundException | SchemaNotFoundException | TableNotFoundException e) {
            return Optional.empty();
        }

        try (InputStream inputStream = client.newInputStream(getTableMetadataPath(catalogName, databaseName, table))) {
            String tableJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
            return Optional.of(TABLE_CODEC.fromJson(tableJson));
        }
        catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<TableEntity> getAllTables(String catalogName, String databaseName)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
        checkArgument(databaseName.matches("[\\p{Alnum}_]+"), "Invalid database name");

        List<TableEntity> tables = new ArrayList<>();

        try {
            assertCatalogExist(catalogName);
            assertDatabaseExist(catalogName, databaseName);
        }
        catch (CatalogNotFoundException | SchemaNotFoundException e) {
            return ImmutableList.of();
        }

        try (Stream<Path> paths = client.list(getDatabaseMetadataDir(catalogName, databaseName))) {
            List<Path> tablePaths = paths.filter(path -> path.toString().endsWith(METADATA_SUFFIX))
                    .collect(Collectors.toList());

            for (Path tablePath : tablePaths) {
                try (InputStream inputStream = client.newInputStream(tablePath)) {
                    String catalogJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
                    TableEntity table = TABLE_CODEC.fromJson(catalogJson);
                    tables.add(table);
                }
                catch (IOException e) {
                    LOG.error(e, "Get table metadata failed");
                }
            }
            return tables;
        }
        catch (IOException e) {
            throw new TrinoException(HETU_METASTORE_CODE, e);
        }
    }

    @Override
    public void alterCatalogParameter(String catalogName, String key, String value)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
        runTransaction(() -> {
            assertCatalogExist(catalogName);

            try {
                Path catalogMetadataPath = getCatalogMetadataPath(catalogName);
                CatalogEntity catalogEntity;
                try (InputStream inputStream = client.newInputStream(catalogMetadataPath)) {
                    String catalogJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
                    catalogEntity = CATALOG_CODEC.fromJson(catalogJson);
                }

                if (value == null) {
                    catalogEntity.getParameters().remove(key);
                }
                else {
                    catalogEntity.getParameters().put(key, value);
                }
                try (OutputStream outputStream = client.newOutputStream(catalogMetadataPath)) {
                    outputStream.write(CATALOG_CODEC.toJsonBytes(catalogEntity));
                }
            }
            catch (IOException e) {
                throw new TrinoException(HETU_METASTORE_CODE, e);
            }
        });
    }

    @Override
    public void alterDatabaseParameter(String catalogName, String databaseName, String key, String value)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
        checkArgument(databaseName.matches("[\\p{Alnum}_]+"), "Invalid database name");

        runTransaction(() -> {
            assertCatalogExist(catalogName);
            assertDatabaseExist(catalogName, databaseName);

            try {
                Path databaseMetadataPath = getDatabaseMetadataPath(catalogName, databaseName);
                DatabaseEntity databaseEntity;
                try (InputStream inputStream = client.newInputStream(databaseMetadataPath)) {
                    String tableJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
                    databaseEntity = DATABASE_CODEC.fromJson(tableJson);
                }

                if (value == null) {
                    databaseEntity.getParameters().remove(key);
                }
                else {
                    databaseEntity.getParameters().put(key, value);
                }
                try (OutputStream outputStream = client.newOutputStream(databaseMetadataPath)) {
                    outputStream.write(DATABASE_CODEC.toJsonBytes(databaseEntity));
                }
            }
            catch (IOException e) {
                throw new TrinoException(HETU_METASTORE_CODE, e);
            }
        });
    }

    @Override
    public void alterTableParameter(String catalogName, String databaseName, String tableName, String key, String value)
    {
        checkArgument(catalogName.matches("[\\p{Alnum}_]+"), "Invalid catalog name");
        checkArgument(databaseName.matches("[\\p{Alnum}_]+"), "Invalid database name");
        checkArgument(tableName.matches("[\\p{Alnum}_]+"), "Invalid table name");

        runTransaction(() -> {
            assertCatalogExist(catalogName);
            assertDatabaseExist(catalogName, databaseName);
            assertTableExist(catalogName, databaseName, tableName);

            try {
                Path tableMetadataPath = getTableMetadataPath(catalogName, databaseName, tableName);
                TableEntity tableEntity;
                try (InputStream inputStream = client.newInputStream(tableMetadataPath)) {
                    String tableJson = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
                    tableEntity = TABLE_CODEC.fromJson(tableJson);
                }

                if (value == null) {
                    tableEntity.getParameters().remove(key);
                }
                else {
                    tableEntity.getParameters().put(key, value);
                }
                try (OutputStream outputStream = client.newOutputStream(tableMetadataPath)) {
                    outputStream.write(TABLE_CODEC.toJsonBytes(tableEntity));
                }
            }
            catch (IOException e) {
                throw new TrinoException(HETU_METASTORE_CODE, e);
            }
        });
    }
}
