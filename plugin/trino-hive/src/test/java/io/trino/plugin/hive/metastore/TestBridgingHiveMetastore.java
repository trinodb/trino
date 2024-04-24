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
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.MetastoreClientAdapterProvider;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.reflect.Reflection.newProxy;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestBridgingHiveMetastore
        extends AbstractTestHiveMetastore
{
    private final HiveHadoop hiveHadoop;
    private final HiveMetastore metastore;

    TestBridgingHiveMetastore()
    {
        hiveHadoop = HiveHadoop.builder().build();
        hiveHadoop.start();

        MetastoreClientAdapterProvider metastoreClientAdapterProvider = delegate -> newProxy(ThriftMetastoreClient.class, (proxy, method, methodArgs) -> {
            Object result;
            try {
                result = method.invoke(delegate, methodArgs);
            }
            catch (InvocationTargetException e) {
                throw e.getCause();
            }
            if (method.getName().equals("createDatabase") || method.getName().equals("createTable") || method.getName().equals("dropTable")) {
                throw new RuntimeException("Test-simulated Hive Metastore timeout exception");
            }
            return result;
        });

        metastore = new BridgingHiveMetastore(testingThriftHiveMetastoreBuilder()
                .metastoreClient(hiveHadoop.getHiveMetastoreEndpoint(), metastoreClientAdapterProvider)
                .thriftMetastoreConfig(new ThriftMetastoreConfig().setDeleteFilesOnDrop(true))
                .build());
    }

    @AfterAll
    void afterAll()
    {
        hiveHadoop.stop();
    }

    @Override
    protected HiveMetastore getMetastore()
    {
        return metastore;
    }

    @Test
    public void testCreateDatabaseWithRetries()
    {
        // This test is similar to AbstractTestHiveMetastore#testCreateDatabase but with simulating timeout in ThriftMetastoreClient
        String databaseName = "test_database_" + randomNameSuffix();
        Database.Builder database = Database.builder()
                .setDatabaseName(databaseName)
                .setParameters(Map.of(TRINO_QUERY_ID_NAME, "query_id"))
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty());
        getMetastore().createDatabase(database.build());

        database.setParameters(Map.of(TRINO_QUERY_ID_NAME, "another_query_id"));
        assertThatThrownBy(() -> getMetastore().createDatabase(database.build()))
                .isInstanceOf(SchemaAlreadyExistsException.class);

        getMetastore().dropDatabase(databaseName, false);
    }

    @Test
    public void testCreateTableWithRetries()
    {
        // This test is similar to AbstractTestHiveMetastore#testCreateTable but with simulating timeout in ThriftMetastoreClient
        String databaseName = "test_database_" + randomNameSuffix();
        Database.Builder database = Database.builder()
                .setDatabaseName(databaseName)
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty());
        getMetastore().createDatabase(database.build());

        String tableName = "test_table" + randomNameSuffix();
        Table.Builder table = Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setParameters(Map.of(TRINO_QUERY_ID_NAME, "query_id"))
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.empty());
        table.getStorageBuilder()
                .setStorageFormat(fromHiveStorageFormat(PARQUET));
        getMetastore().createTable(table.build(), NO_PRIVILEGES);

        table.setParameters(Map.of(TRINO_QUERY_ID_NAME, "another_query_id"));
        assertThatThrownBy(() -> getMetastore().createTable(table.build(), NO_PRIVILEGES))
                .isInstanceOf(TableAlreadyExistsException.class);

        getMetastore().dropTable(databaseName, tableName, false);
        getMetastore().dropDatabase(databaseName, false);
    }

    @Test
    public void testDropTableWithRetries()
    {
        String databaseName = "test_database_" + randomNameSuffix();
        Database.Builder database = Database.builder()
                .setDatabaseName(databaseName)
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty());
        getMetastore().createDatabase(database.build());

        String tableName = "test_table" + randomNameSuffix();
        Table.Builder table = Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.empty());
        table.getStorageBuilder()
                .setStorageFormat(fromHiveStorageFormat(PARQUET));
        getMetastore().createTable(table.build(), NO_PRIVILEGES);

        assertThat(getMetastore().getTable(databaseName, tableName)).isPresent();
        getMetastore().dropTable(databaseName, tableName, false);
        assertThat(getMetastore().getTable(databaseName, tableName)).isEmpty();

        getMetastore().dropDatabase(databaseName, false);
    }
}
