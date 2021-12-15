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
package io.trino.plugin.hive.metastore.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Lists.cartesianProduct;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.authentication.HiveIdentity.none;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestCachingHiveMetastoreWithQueryRunner
{
    private static final String CATALOG = "test";
    private static final String SCHEMA = "test";
    private static final Session ADMIN = getTestSession(Identity.forUser("admin")
            .withConnectorRole(CATALOG, new SelectedRole(ROLE, Optional.of("admin")))
            .build());
    private static final String ALICE_NAME = "alice";
    private static final Session ALICE = getTestSession(new Identity.Builder(ALICE_NAME).build());

    private DistributedQueryRunner queryRunner;
    private Path temporaryMetastoreDirectory;

    @BeforeMethod
    public void createQueryRunner()
            throws Exception
    {
        queryRunner = DistributedQueryRunner
                .builder(ADMIN)
                .setNodeCount(1)
                .build();
        queryRunner.installPlugin(new HivePlugin());
        temporaryMetastoreDirectory = createTempDirectory(null);
        queryRunner.createCatalog(CATALOG, "hive", ImmutableMap.of(
                "hive.metastore", "file",
                "hive.metastore.catalog.dir", temporaryMetastoreDirectory.toUri().toString(),
                "hive.security", "sql-standard",
                "hive.metastore-cache-ttl", "60m",
                "hive.metastore-refresh-interval", "10m"));
        queryRunner.execute(ADMIN, "CREATE SCHEMA " + SCHEMA);
        queryRunner.execute("CREATE TABLE test (test INT)");
    }

    @AfterMethod(alwaysRun = true)
    public void cleanUp()
            throws IOException
    {
        queryRunner.close();
        deleteRecursively(temporaryMetastoreDirectory, ALLOW_INSECURE);
    }

    private static Session getTestSession(Identity identity)
    {
        return testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .setIdentity(identity)
                .build();
    }

    @Test
    public void testCacheRefreshOnGrantAndRevoke()
    {
        assertThatThrownBy(() -> queryRunner.execute(ALICE, "SELECT * FROM test"))
                .hasMessageContaining("Access Denied");
        queryRunner.execute("GRANT SELECT ON test TO " + ALICE_NAME);
        queryRunner.execute(ALICE, "SELECT * FROM test");
        queryRunner.execute("REVOKE SELECT ON test FROM " + ALICE_NAME);
        assertThatThrownBy(() -> queryRunner.execute(ALICE, "SELECT * FROM test"))
                .hasMessageContaining("Access Denied");
    }

    @Test(dataProvider = "testCacheRefreshOnRoleGrantAndRevokeParams")
    public void testCacheRefreshOnRoleGrantAndRevoke(List<String> grantRoleStatements, String revokeRoleStatement)
    {
        assertThatThrownBy(() -> queryRunner.execute(ALICE, "SELECT * FROM test"))
                .hasMessageContaining("Access Denied");
        queryRunner.execute("CREATE ROLE test_role IN " + CATALOG);
        grantRoleStatements.forEach(queryRunner::execute);
        queryRunner.execute(ALICE, "SELECT * FROM test");
        queryRunner.execute(revokeRoleStatement);
        assertThatThrownBy(() -> queryRunner.execute(ALICE, "SELECT * FROM test"))
                .hasMessageContaining("Access Denied");
    }

    @Test
    public void testFlushHiveMetastoreCacheProcedureCallable()
    {
        queryRunner.execute("CREATE TABLE cached (initial varchar)");
        queryRunner.execute("SELECT initial FROM cached");

        // Rename column name in Metastore outside Trino
        FileHiveMetastore fileHiveMetastore = FileHiveMetastore.createTestingFileHiveMetastore(temporaryMetastoreDirectory.toFile());
        fileHiveMetastore.renameColumn(none(), "test", "cached", "initial", "renamed");

        String renamedColumnQuery = "SELECT renamed FROM cached";
        // Should fail as Trino has old metadata cached
        assertThatThrownBy(() -> queryRunner.execute(renamedColumnQuery))
                .hasMessageMatching(".*Column 'renamed' cannot be resolved");

        // Should success after flushing Trino JDBC metadata cache
        queryRunner.execute("CALL system.flush_metadata_cache()");
        queryRunner.execute(renamedColumnQuery);
    }

    @Test
    public void testIllegalFlushHiveMetastoreCacheProcedureCalls()
    {
        var illegalParameterMessage = "Illegal parameter set passed. ";
        var validUsageExample = "Valid usages:\n - 'flush_metadata_cache()'\n - flush_metadata_cache(schema_name => ..., table_name => ..., partition_column => ARRAY['...'], partition_value => ARRAY['...'])";

        assertThatThrownBy(() -> queryRunner.execute("CALL system.flush_metadata_cache('dummy_schema')"))
                .hasMessage("Procedure should only be invoked with named parameters. " + validUsageExample);

        assertThatThrownBy(() -> queryRunner.execute("CALL system.flush_metadata_cache(schema_name => 'dummy_schema')"))
                .hasMessage(illegalParameterMessage + validUsageExample);
        assertThatThrownBy(() -> queryRunner.execute("CALL system.flush_metadata_cache(schema_name => 'dummy_schema', table_name => 'dummy_table')"))
                .hasMessage(illegalParameterMessage + validUsageExample);

        assertThatThrownBy(() -> queryRunner.execute("CALL system.flush_metadata_cache(schema_name => 'dummy_schema', table_name => 'dummy_table', partition_column => ARRAY['dummy_partition'])"))
                .hasMessage("Parameters partition_column and partition_value should have same length");
    }

    @DataProvider
    public Object[][] testCacheRefreshOnRoleGrantAndRevokeParams()
    {
        String grantSelectStatement = "GRANT SELECT ON test TO ROLE test_role";
        String grantRoleStatement = "GRANT test_role TO " + ALICE_NAME + " IN " + CATALOG;
        List<List<String>> grantRoleStatements = ImmutableList.of(
                ImmutableList.of(grantSelectStatement, grantRoleStatement),
                ImmutableList.of(grantRoleStatement, grantSelectStatement));
        List<String> revokeRoleStatements = ImmutableList.of(
                "DROP ROLE test_role IN " + CATALOG,
                "REVOKE SELECT ON test FROM ROLE test_role",
                "REVOKE test_role FROM " + ALICE_NAME + " IN " + CATALOG);
        return cartesianProduct(grantRoleStatements, revokeRoleStatements).stream()
                .map(a -> a.toArray(Object[]::new)).toArray(Object[][]::new);
    }
}
