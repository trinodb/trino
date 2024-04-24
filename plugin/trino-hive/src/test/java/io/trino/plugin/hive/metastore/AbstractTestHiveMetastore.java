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
package io.trino.plugin.hive.metastore;

import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.spi.connector.TableNotFoundException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
abstract class AbstractTestHiveMetastore
{
    protected abstract HiveMetastore getMetastore();

    @Test
    void testCreateDatabase()
    {
        String databaseName = "test_database_" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        Database.Builder database = Database.builder()
                .setDatabaseName(databaseName)
                .setParameters(Map.of(TRINO_QUERY_ID_NAME, "query_id"))
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty());
        getMetastore().createDatabase(database.build());
        // second call with the same query ID succeeds
        getMetastore().createDatabase(database.build());

        database.setParameters(Map.of(TRINO_QUERY_ID_NAME, "another_query_id"));
        assertThatThrownBy(() -> getMetastore().createDatabase(database.build()))
                .isInstanceOf(SchemaAlreadyExistsException.class);

        getMetastore().dropDatabase(databaseName, false);
    }

    @Test
    void testCreateTable()
    {
        String databaseName = "test_database_" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        Database.Builder database = Database.builder()
                .setDatabaseName(databaseName)
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty());
        getMetastore().createDatabase(database.build());

        String tableName = "test_table";
        Table.Builder table = Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setParameters(Map.of(TRINO_QUERY_ID_NAME, "query_id"))
                .setTableType(EXTERNAL_TABLE.name())
                .setDataColumns(List.of(new Column("test_column", HIVE_STRING, Optional.empty(), Map.of())))
                .setOwner(Optional.empty());
        table.getStorageBuilder()
                .setLocation(Optional.of("/tmp/location"))
                .setStorageFormat(fromHiveStorageFormat(PARQUET));
        getMetastore().createTable(table.build(), NO_PRIVILEGES);
        // second call with the same query ID succeeds
        getMetastore().createTable(table.build(), NO_PRIVILEGES);

        table.setParameters(Map.of(TRINO_QUERY_ID_NAME, "another_query_id"));
        assertThatThrownBy(() -> getMetastore().createTable(table.build(), NO_PRIVILEGES))
                .isInstanceOf(TableAlreadyExistsException.class);

        getMetastore().dropTable(databaseName, tableName, false);
        getMetastore().dropDatabase(databaseName, false);
    }

    @Test
    public void testDropNotExistingTable()
    {
        String databaseName = "test_database_" + randomNameSuffix();
        Database.Builder database = Database.builder()
                .setDatabaseName(databaseName)
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty());
        getMetastore().createDatabase(database.build());

        assertThatThrownBy(() -> getMetastore().dropTable(databaseName, "not_existing", false))
                .isInstanceOf(TableNotFoundException.class);

        getMetastore().dropDatabase(databaseName, false);
    }
}
