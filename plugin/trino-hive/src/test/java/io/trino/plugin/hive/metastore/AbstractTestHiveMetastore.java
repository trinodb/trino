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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
abstract class AbstractTestHiveMetastore
{
    private HiveMetastore metastore;

    public void setMetastore(HiveMetastore metastore)
    {
        this.metastore = metastore;
    }

    @Test
    void testCreateDatabase()
    {
        String databaseName = "test_database_" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        Database.Builder database = Database.builder()
                .setDatabaseName(databaseName)
                .setParameters(Map.of(TRINO_QUERY_ID_NAME, "query_id"))
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty());
        metastore.createDatabase(database.build());
        // second call with the same query ID succeeds
        metastore.createDatabase(database.build());

        database.setParameters(Map.of(TRINO_QUERY_ID_NAME, "another_query_id"));
        assertThatThrownBy(() -> metastore.createDatabase(database.build()))
                .isInstanceOf(SchemaAlreadyExistsException.class);

        metastore.dropDatabase(databaseName, false);
    }

    @Test
    void testCreateTable()
    {
        String databaseName = "test_database_" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        Database.Builder database = Database.builder()
                .setDatabaseName(databaseName)
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty());
        metastore.createDatabase(database.build());

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
        metastore.createTable(table.build(), NO_PRIVILEGES);
        // second call with the same query ID succeeds
        metastore.createTable(table.build(), NO_PRIVILEGES);

        table.setParameters(Map.of(TRINO_QUERY_ID_NAME, "another_query_id"));
        assertThatThrownBy(() -> metastore.createTable(table.build(), NO_PRIVILEGES))
                .isInstanceOf(TableAlreadyExistsException.class);

        metastore.dropTable(databaseName, tableName, false);
        metastore.dropDatabase(databaseName, false);
    }
}
