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
package io.trino.tests.product.hive;

import io.trino.testing.TestingNames;
import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@ProductTest
@RequiresEnvironment(ProductTestEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.HdfsImpersonation
@TestGroup.ProfileSpecificTests
class TestImpersonationJunit
{
    @Test
    void testExternalLocationTableCreationFailure(ProductTestEnvironment env)
    {
        assumeFalse(env instanceof HiveKerberosEnvironment, "Skipped for Kerberos impersonation environments");

        String commonExternalLocationPath = warehouseDirectory(env) + "/nested_" + TestingNames.randomNameSuffix();

        String tableNameBob = "bob_external_table_" + TestingNames.randomNameSuffix();
        String tableLocationBob = commonExternalLocationPath + "/bob_table";
        String tableNameAlice = "alice_external_table_" + TestingNames.randomNameSuffix();
        String tableLocationAlice = commonExternalLocationPath + "/alice_table";

        HdfsClient hdfsClient = hdfsClient(env, "bob");
        try {
            hdfsClient.createDirectory(tableLocationBob);
            executeTrinoUpdateAs(env, "bob", format("CREATE TABLE %s (a bigint) WITH (external_location = '%s')", tableNameBob, tableLocationBob));
            assertThat(hdfsClient.getFileOwner(commonExternalLocationPath)).isEqualTo("bob");

            assertThatThrownBy(() -> executeTrinoUpdateAs(env, "alice", format("CREATE TABLE %s (a bigint) WITH (external_location = '%s')", tableNameAlice, tableLocationAlice)))
                    .hasMessageMatching("(?s).*(Permission denied|External location must be a directory).*");
        }
        finally {
            executeTrinoUpdateAs(env, "bob", format("DROP TABLE IF EXISTS %s", tableNameBob));
            executeTrinoUpdateAs(env, "alice", format("DROP TABLE IF EXISTS %s", tableNameAlice));
        }
    }

    @Test
    void testHdfsImpersonationEnabled(ProductTestEnvironment env)
    {
        assumeFalse(env instanceof HiveKerberosEnvironment, "Skipped for Kerberos impersonation environments");

        String tableName = "check_hdfs_impersonation_enabled_" + TestingNames.randomNameSuffix();
        checkTableOwner(tableName, "alice", env);
        checkTableGroup(tableName, env);
    }

    private static void checkTableOwner(String tableName, String expectedOwner, ProductTestEnvironment env)
    {
        HdfsClient hdfsClient = hdfsClient(env, "alice");
        try {
            executeTrinoUpdateAs(env, "alice", format("DROP TABLE IF EXISTS %s", tableName));
            executeTrinoUpdateAs(env, "alice", format("CREATE TABLE %s AS SELECT 'abc' c", tableName));
            String tableLocation = getTableLocation(env, tableName);
            assertThat(hdfsClient.getFileOwner(tableLocation)).isEqualTo(expectedOwner);
        }
        finally {
            executeTrinoUpdateAs(env, "alice", format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    private static void checkTableGroup(String tableName, ProductTestEnvironment env)
    {
        HdfsClient hdfsClient = hdfsClient(env, "alice");
        try {
            executeTrinoUpdateAs(env, "alice", format("DROP TABLE IF EXISTS %s", tableName));
            executeTrinoUpdateAs(env, "alice", format("CREATE TABLE %s AS SELECT 'abc' c", tableName));
            String tableLocation = getTableLocation(env, tableName);
            String warehouseLocation = tableLocation.substring(0, tableLocation.lastIndexOf('/'));
            assertThat(hdfsClient.getFileGroup(tableLocation)).isEqualTo(hdfsClient.getFileGroup(warehouseLocation));
        }
        finally {
            executeTrinoUpdateAs(env, "alice", format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    private static String getTableLocation(ProductTestEnvironment env, String tableName)
    {
        String location = executeTrinoAs(env, "alice", format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM %s", tableName))
                .column(1)
                .getFirst()
                .toString();
        if (location.startsWith("hdfs://")) {
            return URI.create(location).getPath();
        }
        return location;
    }

    private static QueryResult executeTrinoAs(ProductTestEnvironment env, String user, String sql)
    {
        if (env instanceof HiveImpersonationEnvironment hiveImpersonationEnvironment) {
            return hiveImpersonationEnvironment.executeTrinoAs(user, sql);
        }
        return env.executeTrino(sql, user);
    }

    private static int executeTrinoUpdateAs(ProductTestEnvironment env, String user, String sql)
    {
        return env.executeTrinoUpdate(sql, user);
    }

    private static HdfsClient hdfsClient(ProductTestEnvironment env, String user)
    {
        if (env instanceof HiveImpersonationEnvironment hiveImpersonationEnvironment) {
            return hiveImpersonationEnvironment.createHdfsClient(user);
        }
        throw new IllegalStateException("Unsupported environment for impersonation test: " + env.getClass().getSimpleName());
    }

    private static String warehouseDirectory(ProductTestEnvironment env)
    {
        if (env instanceof HiveImpersonationEnvironment hiveImpersonationEnvironment) {
            return hiveImpersonationEnvironment.getWarehouseDirectory();
        }
        throw new IllegalStateException("Unsupported environment for impersonation test: " + env.getClass().getSimpleName());
    }
}
