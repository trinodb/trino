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
package io.trino.plugin.deltalake.metastore;

import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.PATH_PROPERTY;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.ViewReaderUtil.isSomeKindOfAView;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveMetastoreBackedDeltaLakeMetastore
        implements DeltaLakeMetastore
{
    public static final String TABLE_PROVIDER_PROPERTY = "spark.sql.sources.provider";
    public static final String TABLE_PROVIDER_VALUE = "DELTA";

    private final HiveMetastore delegate;

    public HiveMetastoreBackedDeltaLakeMetastore(HiveMetastore delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public List<String> getAllDatabases()
    {
        return delegate.getAllDatabases();
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public List<TableInfo> getAllTables(String databaseName)
    {
        return delegate.getTables(databaseName);
    }

    @Override
    public Optional<Table> getRawMetastoreTable(String databaseName, String tableName)
    {
        return delegate.getTable(databaseName, tableName);
    }

    @Override
    public Optional<DeltaMetastoreTable> getTable(String databaseName, String tableName)
    {
        return getRawMetastoreTable(databaseName, tableName)
                .map(HiveMetastoreBackedDeltaLakeMetastore::convertToDeltaMetastoreTable);
    }

    public static void verifyDeltaLakeTable(Table table)
    {
        if (isSomeKindOfAView(table)) {
            // Looks like a view, so not a table
            throw new NotADeltaLakeTableException(table.getDatabaseName(), table.getTableName());
        }
        if (!TABLE_PROVIDER_VALUE.equalsIgnoreCase(table.getParameters().get(TABLE_PROVIDER_PROPERTY))) {
            throw new NotADeltaLakeTableException(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        delegate.createDatabase(database);
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        delegate.dropDatabase(databaseName, deleteData);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        delegate.createTable(table, principalPrivileges);
    }

    @Override
    public void replaceTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        delegate.replaceTable(table.getDatabaseName(), table.getTableName(), table, principalPrivileges);
    }

    @Override
    public void dropTable(SchemaTableName schemaTableName, String tableLocation, boolean deleteData)
    {
        delegate.dropTable(schemaTableName.getSchemaName(), schemaTableName.getTableName(), deleteData);
    }

    @Override
    public void renameTable(SchemaTableName from, SchemaTableName to)
    {
        delegate.renameTable(from.getSchemaName(), from.getTableName(), to.getSchemaName(), to.getTableName());
    }

    public static DeltaMetastoreTable convertToDeltaMetastoreTable(Table table)
    {
        verifyDeltaLakeTable(table);
        return new DeltaMetastoreTable(
                new SchemaTableName(table.getDatabaseName(), table.getTableName()),
                table.getTableType().equals(MANAGED_TABLE.name()),
                getTableLocation(table));
    }

    public static String getTableLocation(Table table)
    {
        Map<String, String> serdeParameters = table.getStorage().getSerdeParameters();
        String location = serdeParameters.get(PATH_PROPERTY);
        if (location == null) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("No %s property defined for table: %s", PATH_PROPERTY, table));
        }
        return location;
    }
}
