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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.hive.thrift.metastore.Table;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.plugin.hive.metastore.AcidTransactionOwner;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.InMemoryThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.TestingIcebergConnectorFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Optional;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestIcebergHiveMetastoreTableOperationsReleaseLockFailure
        extends AbstractTestQueryFramework
{
    private static final String ICEBERG_CATALOG = "iceberg";
    private static final String SCHEMA_NAME = "test_schema";
    private File baseDir;

    @Override
    protected LocalQueryRunner createQueryRunner() throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA_NAME)
                .build();

        baseDir = Files.createTempDirectory(null).toFile();
        baseDir.deleteOnExit();

        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);

        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        queryRunner.addFunctions(functions.build());

        ThriftMetastore thriftMetastore = createMetastoreWithReleaseLockFailure();
        HiveMetastore hiveMetastore = new BridgingHiveMetastore(thriftMetastore);
        TestingIcebergHiveMetastoreCatalogModule testModule = new TestingIcebergHiveMetastoreCatalogModule(hiveMetastore, buildThriftMetastoreFactory(thriftMetastore));

        queryRunner.createCatalog(
                ICEBERG_CATALOG,
                new TestingIcebergConnectorFactory(Optional.of(testModule), Optional.empty(), EMPTY_MODULE),
                ImmutableMap.of());

        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        hiveMetastore.createDatabase(database);

        return queryRunner;
    }

    @Test
    public void testReleaseLockFailureDoesNotCorruptTheTable()
    {
        String tableName = "test_release_lock_failure";
        query(format("CREATE TABLE %s (a_varchar) AS VALUES ('Trino')", tableName));
        query(format("INSERT INTO %s VALUES 'rocks'", tableName));
        assertQuery("SELECT * FROM " + tableName, "VALUES 'Trino', 'rocks'");
    }

    private InMemoryThriftMetastore createMetastoreWithReleaseLockFailure()
    {
        return new InMemoryThriftMetastore(new File(baseDir + "/metastore"), new ThriftMetastoreConfig()) {
            @Override
            public long acquireTableExclusiveLock(AcidTransactionOwner transactionOwner, String queryId, String dbName, String tableName)
            {
                // returning dummy lock
                return 100;
            }

            @Override
            public void releaseTableLock(long lockId)
            {
                throw new RuntimeException("Release table lock has failed!");
            }

            @Override
            public synchronized void createTable(Table table)
            {
                // InMemoryThriftMetastore throws an exception if the table has any privileges set
                table.setPrivileges(null);
                super.createTable(table);
            }
        };
    }

    private static ThriftMetastoreFactory buildThriftMetastoreFactory(ThriftMetastore thriftMetastore)
    {
        return new ThriftMetastoreFactory()
        {
            @Override
            public boolean isImpersonationEnabled()
            {
                return false;
            }

            @Override
            public ThriftMetastore createMetastore(Optional<ConnectorIdentity> identity)
            {
                return thriftMetastore;
            }
        };
    }
}
