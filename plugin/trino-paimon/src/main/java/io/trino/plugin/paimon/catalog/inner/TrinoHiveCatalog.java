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
package io.trino.plugin.paimon.catalog.inner;

import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveType;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StorageFormat;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.type.TypeInfoUtils.getTypeInfoFromTypeString;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static java.util.Objects.requireNonNull;

public class TrinoHiveCatalog
        implements Catalog
{
    private static final String INPUT_FORMAT_CLASS_NAME = "org.apache.paimon.hive.mapred.PaimonInputFormat";
    private static final String OUTPUT_FORMAT_CLASS_NAME = "org.apache.paimon.hive.mapred.PaimonOutputFormat";
    private static final String SERDE_CLASS_NAME = "org.apache.paimon.hive.PaimonSerDe";

    public static final StorageFormat PAIMON_METASTORE_STORAGE_FORMAT = StorageFormat.create(
            SERDE_CLASS_NAME,
            INPUT_FORMAT_CLASS_NAME,
            OUTPUT_FORMAT_CLASS_NAME);

    private final HiveMetastore hiveMetastore;
    private final Catalog baseCatalog;

    public TrinoHiveCatalog(HiveMetastore hiveMetastore, Catalog baseCatalog)
    {
        this.hiveMetastore = requireNonNull(hiveMetastore, "hiveMetastore is null");
        this.baseCatalog = requireNonNull(baseCatalog, "baseCatalog is null");
    }

    public static List<Column> toHiveColumns(List<DataField> columns)
    {
        return columns.stream()
                .map(column -> new Column(
                        column.name(),
                        HiveType.fromTypeInfo(getTypeInfoFromTypeString(column.type().asSQLString().toLowerCase(Locale.ROOT))),
                        Optional.ofNullable(column.description()),
                        Map.of()))
                .collect(toImmutableList());
    }

    @Override
    public String warehouse()
    {
        return baseCatalog.warehouse();
    }

    @Override
    public FileIO fileIO()
    {
        return baseCatalog.fileIO();
    }

    @Override
    public Map<String, String> options()
    {
        return baseCatalog.options();
    }

    @Override
    public boolean caseSensitive()
    {
        return baseCatalog.caseSensitive();
    }

    @Override
    public List<String> listDatabases()
    {
        return hiveMetastore.getAllDatabases();
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> options)
            throws DatabaseAlreadyExistException
    {
        if (databaseExists(name)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(name);
        }

        hiveMetastore.createDatabase(io.trino.metastore.Database.builder().setDatabaseName(name).setOwnerName(Optional.empty()).setOwnerType(Optional.empty()).setParameters(options).build());
        baseCatalog.createDatabase(name, ignoreIfExists);
    }

    private boolean databaseExists(String databaseName)
    {
        return hiveMetastore.getDatabase(databaseName).isPresent();
    }

    @Override
    public Database getDatabase(String name)
            throws DatabaseNotExistException
    {
        Optional<io.trino.metastore.Database> database = hiveMetastore.getDatabase(name);
        if (database.isEmpty()) {
            throw new DatabaseNotExistException(name);
        }

        return new Database.DatabaseImpl(name, database.get().getParameters(), database.get().getComment().orElse(null));
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException
    {
        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(databaseName);
        }

        hiveMetastore.dropDatabase(databaseName, false);
        baseCatalog.dropDatabase(databaseName, ignoreIfNotExists, cascade);
    }

    @Override
    public void alterDatabase(String databaseName, List<PropertyChange> propertyChanges, boolean ignoreIfNotExists)
            throws DatabaseNotExistException
    {
        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(databaseName);
        }

        baseCatalog.alterDatabase(databaseName, propertyChanges, ignoreIfNotExists);
    }

    @Override
    public Table getTable(Identifier identifier)
            throws TableNotExistException
    {
        return baseCatalog.getTable(identifier);
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException
    {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(databaseName);
        }
        return hiveMetastore.getTables(databaseName).stream().map(t -> t.tableName().getTableName()).collect(Collectors.toList());
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException
    {
        hiveMetastore.dropTable(identifier.getDatabaseName(), identifier.getTableName(), false);
        baseCatalog.dropTable(identifier, ignoreIfNotExists);
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException
    {
        if (!databaseExists(identifier.getDatabaseName())) {
            throw new DatabaseNotExistException(identifier.getDatabaseName());
        }

        if (hiveMetastore.getTable(identifier.getDatabaseName(), identifier.getTableName()).isPresent()) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(identifier);
            }
        }

        hiveMetastore.createTable(io.trino.metastore.Table.builder()
                .setDatabaseName(identifier.getDatabaseName())
                .setTableName(identifier.getTableName())
                .setDataColumns(toHiveColumns(schema.fields()))
                .setOwner(Optional.of("paimon"))
                .setTableType(EXTERNAL_TABLE.name())
                .withStorage(storage -> storage.setLocation(baseCatalog.warehouse() + "/" + identifier.getDatabaseName() + ".db/" + identifier.getTableName()))
                .withStorage(storage -> storage.setStorageFormat(PAIMON_METASTORE_STORAGE_FORMAT))
                .setParameter("EXTERNAL", "TRUE")
                .setParameters(schema.options())
                .build(), PrincipalPrivileges.NO_PRIVILEGES);

        baseCatalog.createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void renameTable(Identifier identifier, Identifier newIdentifier, boolean b)
            throws TableNotExistException, TableAlreadyExistException
    {
        hiveMetastore.renameTable(identifier.getDatabaseName(), identifier.getTableName(), newIdentifier.getDatabaseName(), newIdentifier.getTableName());
        baseCatalog.renameTable(identifier, newIdentifier, b);
    }

    @Override
    public void alterTable(Identifier identifier, List<SchemaChange> list, boolean b)
    {
        // TODO: enable atler table in hive catalog
        throw new UnsupportedOperationException("Alter table is not supported yet");
    }

    @Override
    public void createPartition(Identifier identifier, Map<String, String> map)
    {
        // TODO: enable create partition in hive catalog
        throw new UnsupportedOperationException("Create partition is not supported yet");
    }

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> map)
    {
        // TODO: enable drop partition in hive catalog
        throw new UnsupportedOperationException("Drop partition is not supported yet");
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier)
            throws TableNotExistException
    {
        return baseCatalog.listPartitions(identifier);
    }

    @Override
    public void close()
            throws Exception
    {
        baseCatalog.close();
    }
}
