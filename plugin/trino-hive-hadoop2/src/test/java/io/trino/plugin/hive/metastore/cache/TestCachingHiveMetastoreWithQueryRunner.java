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
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Lists.cartesianProduct;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("UnstableApiUsage")
@Test(singleThreaded = true)
public class TestCachingHiveMetastoreWithQueryRunner
{
    private static final String CATALOG = "test";
    private static final String SCHEMA = "test";
    private static final Session ADMIN = getTestSession(Identity.forUser("admin")
            .withRole(CATALOG, new SelectedRole(ROLE, Optional.of("admin")))
            .build());
    private static final String ALICE_NAME = "alice";
    private static final Session ALICE = getTestSession(new Identity.Builder(ALICE_NAME).build());

    private DistributedQueryRunner queryRunner;
    private File temporaryDirectory;

    @BeforeMethod
    public void createQueryRunner()
            throws Exception
    {
        queryRunner = DistributedQueryRunner
                .builder(ADMIN)
                .setNodeCount(1)
                .build();
        queryRunner.installPlugin(new HivePlugin());
        temporaryDirectory = createTempDir();
        queryRunner.createCatalog(CATALOG, "hive", ImmutableMap.of(
                "hive.metastore", "file",
                "hive.metastore.catalog.dir", temporaryDirectory.toURI().toString(),
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
        deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
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
        queryRunner.execute("CREATE ROLE test_role");
        grantRoleStatements.forEach(queryRunner::execute);
        queryRunner.execute(ALICE, "SELECT * FROM test");
        queryRunner.execute(revokeRoleStatement);
        assertThatThrownBy(() -> queryRunner.execute(ALICE, "SELECT * FROM test"))
                .hasMessageContaining("Access Denied");
    }

    @DataProvider
    private Object[][] testCacheRefreshOnRoleGrantAndRevokeParams()
    {
        String grantSelectStatement = "GRANT SELECT ON test TO ROLE test_role";
        String grantRoleStatement = "GRANT test_role TO " + ALICE_NAME;
        List<List<String>> grantRoleStatements = ImmutableList.of(
                ImmutableList.of(grantSelectStatement, grantRoleStatement),
                ImmutableList.of(grantRoleStatement, grantSelectStatement));
        List<String> revokeRoleStatements = ImmutableList.of(
                "DROP ROLE test_role",
                "REVOKE SELECT ON test FROM ROLE test_role",
                "REVOKE test_role FROM " + ALICE_NAME);
        return cartesianProduct(grantRoleStatements, revokeRoleStatements).stream()
                .map(a -> a.toArray(Object[]::new)).toArray(Object[][]::new);
    }
}
