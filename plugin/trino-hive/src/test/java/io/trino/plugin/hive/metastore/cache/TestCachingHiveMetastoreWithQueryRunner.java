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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Lists.cartesianProduct;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestCachingHiveMetastoreWithQueryRunner
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = HiveQueryRunner.HIVE_CATALOG;
    private static final String SCHEMA = "test";
    private static final Session ADMIN = getTestSession(Identity.forUser("admin")
            .withConnectorRole(CATALOG, new SelectedRole(ROLE, Optional.of("admin")))
            .build());
    private static final String ALICE_NAME = "alice";
    private static final Session ALICE = getTestSession(new Identity.Builder(ALICE_NAME).build());

    private FileHiveMetastore fileHiveMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path temporaryMetastoreDirectory = createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(temporaryMetastoreDirectory, ALLOW_INSECURE));

        DistributedQueryRunner queryRunner = HiveQueryRunner.builder(ADMIN)
                .setNodeCount(3)
                // Required by testPartitionAppend test.
                // Coordinator needs to be excluded from workers to deterministically reproduce the original problem
                // https://github.com/trinodb/trino/pull/6853
                .setCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                .setMetastore(distributedQueryRunner -> fileHiveMetastore = createTestingFileHiveMetastore(temporaryMetastoreDirectory.toFile()))
                .setHiveProperties(ImmutableMap.of(
                        "hive.security", "sql-standard",
                        "hive.metastore-cache-ttl", "60m",
                        "hive.metastore-refresh-interval", "10m"))
                .build();

        queryRunner.execute(ADMIN, "CREATE SCHEMA " + SCHEMA);
        queryRunner.execute("CREATE TABLE test (test INT)");

        return queryRunner;
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
        assertThatThrownBy(() -> getQueryRunner().execute(ALICE, "SELECT * FROM test"))
                .hasMessageContaining("Access Denied");
        getQueryRunner().execute("GRANT SELECT ON test TO " + ALICE_NAME);
        getQueryRunner().execute(ALICE, "SELECT * FROM test");
        getQueryRunner().execute("REVOKE SELECT ON test FROM " + ALICE_NAME);
        assertThatThrownBy(() -> getQueryRunner().execute(ALICE, "SELECT * FROM test"))
                .hasMessageContaining("Access Denied");
    }

    @Test(dataProvider = "testCacheRefreshOnRoleGrantAndRevokeParams")
    public void testCacheRefreshOnRoleGrantAndRevoke(List<String> grantRoleStatements, String revokeRoleStatement)
    {
        assertThatThrownBy(() -> getQueryRunner().execute(ALICE, "SELECT * FROM test"))
                .hasMessageContaining("Access Denied");
        getQueryRunner().execute("CREATE ROLE test_role IN " + CATALOG);
        grantRoleStatements.forEach(getQueryRunner()::execute);
        getQueryRunner().execute(ALICE, "SELECT * FROM test");
        getQueryRunner().execute(revokeRoleStatement);
        assertThatThrownBy(() -> getQueryRunner().execute(ALICE, "SELECT * FROM test"))
                .hasMessageContaining("Access Denied");
        // Cleanup
        String removeByDropStatement = "DROP ROLE test_role IN " + CATALOG;
        if (!revokeRoleStatement.equals(removeByDropStatement)) {
            getQueryRunner().execute(removeByDropStatement);
        }
    }

    @Test
    public void testFlushHiveMetastoreCacheProcedureCallable()
    {
        getQueryRunner().execute("CREATE TABLE cached (initial varchar)");
        getQueryRunner().execute("SELECT initial FROM cached");

        // Rename column name in Metastore outside Trino
        fileHiveMetastore.renameColumn("test", "cached", "initial", "renamed");

        String renamedColumnQuery = "SELECT renamed FROM cached";
        // Should fail as Trino has old metadata cached
        assertThatThrownBy(() -> getQueryRunner().execute(renamedColumnQuery))
                .hasMessageMatching(".*Column 'renamed' cannot be resolved");

        // Should success after flushing Trino JDBC metadata cache
        getQueryRunner().execute("CALL system.flush_metadata_cache()");
        getQueryRunner().execute(renamedColumnQuery);
    }

    @Test
    public void testIllegalFlushHiveMetastoreCacheProcedureCalls()
    {
        String illegalParameterMessage = "Illegal parameter set passed. Valid usages:\n" +
                " - 'flush_metadata_cache()'\n" +
                " - flush_metadata_cache(schema_name => ..., table_name => ...)" +
                " - flush_metadata_cache(schema_name => ..., table_name => ..., partition_columns => ARRAY['...'], partition_values => ARRAY['...'])";

        assertThatThrownBy(() -> getQueryRunner().execute("CALL system.flush_metadata_cache('dummy_schema')"))
                .hasMessageContaining("Only named arguments are allowed for this procedure");

        assertThatThrownBy(() -> getQueryRunner().execute("CALL system.flush_metadata_cache(schema_name => 'dummy_schema')"))
                .hasMessage(illegalParameterMessage);

        assertThatThrownBy(() -> getQueryRunner().execute("CALL system.flush_metadata_cache(schema_name => 'dummy_schema', table_name => 'dummy_table', partition_column => ARRAY['dummy_partition'])"))
                .hasMessage("Parameters partition_column and partition_value should have same length");

        assertThatThrownBy(
                () -> getQueryRunner().execute("CALL system.flush_metadata_cache(" +
                        "partition_columns => ARRAY['example'], " +
                        "partition_values => ARRAY['0'], " +
                        "partition_column => ARRAY['example'], " +
                        "partition_value => ARRAY['0']" +
                        ")"))
                .hasMessage(
                        "Procedure should only be invoked with single pair of partition definition named params: " +
                                "partition_columns and partition_values or partition_column and partition_value");
    }

    @Test
    public void testPartitionAppend()
    {
        int nodeCount = getQueryRunner().getNodeCount();
        verify(nodeCount > 1, "this test requires a multinode query runner");

        getQueryRunner().execute("CREATE TABLE test_part_append " +
                "(name varchar, partkey varchar) " +
                "WITH (partitioned_by = ARRAY['partkey'])");

        String row = "('some name', 'part1')";

        // if metastore caching was enabled on workers than any worker which tries to INSERT into same partition twice
        // will fail because it would've cached the absence of the partition
        for (int i = 0; i < nodeCount + 1; i++) {
            getQueryRunner().execute("INSERT INTO test_part_append VALUES " + row);
        }

        String expected = Joiner.on(",").join(nCopies(nodeCount + 1, row));
        assertQuery("SELECT * FROM test_part_append", "VALUES " + expected);
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
