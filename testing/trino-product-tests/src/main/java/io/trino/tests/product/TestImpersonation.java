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
package io.trino.tests.product;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import io.trino.tempto.query.QueryExecutor;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.net.URI;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HDFS_IMPERSONATION;
import static io.trino.tests.product.TestGroups.HDFS_NO_IMPERSONATION;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static io.trino.tests.product.utils.QueryExecutors.connectToTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestImpersonation
        extends ProductTest
{
    private QueryExecutor aliceExecutor;
    private QueryExecutor bobExecutor;

    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.alice@presto.jdbc_user")
    private String aliceJdbcUser;

    @Inject
    @Named("databases.bob@presto.jdbc_user")
    private String bobJdbcUser;

    // The value for configuredHdfsUser is profile dependent
    // For non-Kerberos environments this variable will be equal to -DHADOOP_USER_NAME as set in jvm.config
    // For Kerberized environments this variable will be equal to the hive.hdfs.trino.principal property as set in hive.properties
    @Inject
    @Named("databases.presto.configured_hdfs_user")
    private String configuredHdfsUser;

    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseLocation;

    @BeforeMethodWithContext
    public void setup()
    {
        aliceExecutor = connectToTrino("alice@presto");
        bobExecutor = connectToTrino("bob@presto");
    }

    @AfterMethodWithContext
    public void cleanup()
    {
        // should not be closed, this would close a shared, global QueryExecutor
        aliceExecutor = null;
        bobExecutor = null;
    }

    @Test(groups = {HDFS_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testExternalLocationTableCreationFailure()
    {
        String commonExternalLocationPath = warehouseLocation + "/nested_" + randomNameSuffix();

        String tableNameBob = "bob_external_table" + randomNameSuffix();
        String tableLocationBob = commonExternalLocationPath + "/bob_table";
        bobExecutor.executeQuery(format("CREATE TABLE %s (a bigint) WITH (external_location = '%s')", tableNameBob, tableLocationBob));
        String owner = hdfsClient.getOwner(commonExternalLocationPath);
        assertEquals(owner, bobJdbcUser);

        String tableNameAlice = "alice_external_table" + randomNameSuffix();
        String tableLocationAlice = commonExternalLocationPath + "/alice_table";
        assertQueryFailure(() -> aliceExecutor.executeQuery(format("CREATE TABLE %s (a bigint) WITH (external_location = '%s')", tableNameAlice, tableLocationAlice)))
                .hasStackTraceContaining("Permission denied");

        bobExecutor.executeQuery(format("DROP TABLE IF EXISTS %s", tableNameBob));
        aliceExecutor.executeQuery(format("DROP TABLE IF EXISTS %s", tableNameAlice));
    }

    @Test(groups = {HDFS_NO_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testExternalLocationTableCreationSuccess()
    {
        String commonExternalLocationPath = warehouseLocation + "/nested_" + randomNameSuffix();

        String tableNameBob = "bob_external_table" + randomNameSuffix();
        String tableLocationBob = commonExternalLocationPath + "/bob_table";
        bobExecutor.executeQuery(format("CREATE TABLE %s (a bigint) WITH (external_location = '%s')", tableNameBob, tableLocationBob));
        String owner = hdfsClient.getOwner(tableLocationBob);
        assertEquals(owner, configuredHdfsUser);

        String tableNameAlice = "alice_external_table" + randomNameSuffix();
        String tableLocationAlice = commonExternalLocationPath + "/alice_table";
        aliceExecutor.executeQuery(format("CREATE TABLE %s (a bigint) WITH (external_location = '%s')", tableNameAlice, tableLocationAlice));
        assertThat(aliceExecutor.executeQuery(format("SELECT * FROM %s", tableNameAlice))).hasRowsCount(0);
        owner = hdfsClient.getOwner(tableLocationAlice);
        assertEquals(owner, configuredHdfsUser);

        bobExecutor.executeQuery(format("DROP TABLE IF EXISTS %s", tableNameBob));
        aliceExecutor.executeQuery(format("DROP TABLE IF EXISTS %s", tableNameAlice));
    }

    @Test(groups = {HDFS_NO_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testHdfsImpersonationDisabled()
    {
        String tableName = "check_hdfs_impersonation_disabled";
        checkTableOwner(tableName, configuredHdfsUser, aliceExecutor);
    }

    @Test(groups = {HDFS_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    public void testHdfsImpersonationEnabled()
    {
        String tableName = "check_hdfs_impersonation_enabled";
        checkTableOwner(tableName, aliceJdbcUser, aliceExecutor);
        checkTableGroup(tableName, aliceExecutor);
    }

    private static String getTableLocation(QueryExecutor executor, String tableName)
    {
        String location = getOnlyElement(executor.executeQuery(format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM %s", tableName)).column(1));
        if (location.startsWith("hdfs://")) {
            return URI.create(location).getPath();
        }
        return location;
    }

    private void checkTableOwner(String tableName, String expectedOwner, QueryExecutor executor)
    {
        executor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        executor.executeQuery(format("CREATE TABLE %s AS SELECT 'abc' c", tableName));
        String tableLocation = getTableLocation(executor, tableName);
        String owner = hdfsClient.getOwner(tableLocation);
        assertEquals(owner, expectedOwner);
        executor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
    }

    private void checkTableGroup(String tableName, QueryExecutor executor)
    {
        executor.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        executor.executeQuery(format("CREATE TABLE %s AS SELECT 'abc' c", tableName));
        String tableLocation = getTableLocation(executor, tableName);
        String warehouseLocation = tableLocation.substring(0, tableLocation.lastIndexOf("/"));

        // user group info of warehouseLocation(/user/hive/warehouse) is alice:supergroup
        // tableLocation is /user/hive/warehouse/check_hdfs_impersonation_enabled. When create table,
        // user alice doesn't have permission to setOwner, so the user group info should be alice:supergroup still
        String warehouseLocationGroup = hdfsClient.getGroup(warehouseLocation);
        String tableLocationGroup = hdfsClient.getGroup(warehouseLocation);
        assertEquals(tableLocationGroup, warehouseLocationGroup);
    }
}
