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
package io.trino.plugin.iceberg.catalog.file;

import io.trino.Session;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.Table;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.spi.NodeVersion;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestIcebergFileMetastoreInsertRollback
        extends AbstractTestQueryFramework
{
    private static final String ICEBERG_CATALOG = "iceberg";
    private static final String SCHEMA_NAME = "test_schema";

    private Path dataDirectory;
    private HiveMetastore metastore;
    private final AtomicReference<RuntimeException> replaceTableException = new AtomicReference<>();

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        this.dataDirectory = Files.createTempDirectory("test_iceberg_insert_rollback");
        this.metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                new LocalFileSystemFactory(Path.of(dataDirectory.toString())),
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory("local://"))
        {
            @Override
            public synchronized void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
            {
                RuntimeException exception = replaceTableException.get();
                if (exception != null) {
                    throw exception;
                }
                super.replaceTable(databaseName, tableName, newTable, principalPrivileges, environmentContext);
            }
        };

        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA_NAME)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new TestingIcebergPlugin(Path.of(dataDirectory.toString()), () -> Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore))));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg");
        queryRunner.execute("CREATE SCHEMA " + SCHEMA_NAME);

        return queryRunner;
    }

    @AfterAll
    public void cleanup()
            throws Exception
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA_NAME, true);
        }
        if (dataDirectory != null) {
            deleteRecursively(dataDirectory, ALLOW_INSECURE);
        }
    }

    @Test
    public void testInsertRollbackCleansUpTransaction()
    {
        String tableName = "test_insert_rollback_" + randomNameSuffix();
        String tableLocation = "local:///" + tableName;

        // Create the table successfully
        getQueryRunner().execute("CREATE TABLE " + tableName + " (a varchar) WITH (location = '" + tableLocation + "')");
        getQueryRunner().execute("INSERT INTO " + tableName + " VALUES 'initial'");
        assertQuery("SELECT * FROM " + tableName, "VALUES 'initial'");

        // Inject a failure during the next INSERT's commit phase.
        // This causes commitTransaction to fail after the Iceberg Transaction was opened,
        // triggering the rollback path in IcebergMetadata.
        replaceTableException.set(new RuntimeException("Simulated metastore failure during commit"));
        assertThatThrownBy(() -> getQueryRunner().execute("INSERT INTO " + tableName + " VALUES 'should_fail'"))
                .hasMessageContaining("Simulated metastore failure during commit");

        // Remove the failure injection
        replaceTableException.set(null);

        // Verify the table is still accessible and the failed insert did not corrupt it
        assertQuery("SELECT * FROM " + tableName, "VALUES 'initial'");

        // Verify a subsequent insert works correctly
        getQueryRunner().execute("INSERT INTO " + tableName + " VALUES 'after_rollback'");
        assertQuery("SELECT * FROM " + tableName, "VALUES 'initial', 'after_rollback'");

        getQueryRunner().execute("DROP TABLE " + tableName);
    }
}
