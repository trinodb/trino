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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.TestingIcebergConnectorFactory;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestIcebergFileMetastoreTableOperationsInsertFailure
        extends AbstractTestQueryFramework
{
    private static final String ICEBERG_CATALOG = "iceberg";
    private static final String SCHEMA_NAME = "test_schema";
    private File baseDir;

    @Override
    protected LocalQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(SCHEMA_NAME)
                .build();

        baseDir = Files.createTempDirectory(null).toFile();

        HiveMetastore metastore = new FileHiveMetastore(
                new NodeVersion("testversion"),
                HDFS_ENVIRONMENT,
                new HiveMetastoreConfig().isHideDeltaLakeTables(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(baseDir.toURI().toString())
                        .setMetastoreUser("test"))
        {
            @Override
            public synchronized void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
            {
                super.replaceTable(databaseName, tableName, newTable, principalPrivileges);
                throw new RuntimeException("Test-simulated metastore timeout exception");
            }
        };
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);

        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        queryRunner.addFunctions(functions.build());

        queryRunner.createCatalog(
                ICEBERG_CATALOG,
                new TestingIcebergConnectorFactory(Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore)), Optional.empty(), EMPTY_MODULE),
                ImmutableMap.of());

        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        metastore.createDatabase(database);

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws Exception
    {
        if (baseDir != null) {
            deleteRecursively(baseDir.toPath(), ALLOW_INSECURE);
        }
    }

    @Test
    public void testInsertFailureDoesNotCorruptTheTableMetadata()
    {
        String tableName = "test_insert_failure";

        getQueryRunner().execute(format("CREATE TABLE %s (a_varchar) AS VALUES ('Trino')", tableName));
        assertThatThrownBy(() -> getQueryRunner().execute("INSERT INTO " + tableName + " VALUES 'rocks'"))
                .isInstanceOf(CommitStateUnknownException.class)
                .hasMessageContaining("Test-simulated metastore timeout exception");
        assertQuery("SELECT * FROM " + tableName, "VALUES 'Trino', 'rocks'");
    }
}
