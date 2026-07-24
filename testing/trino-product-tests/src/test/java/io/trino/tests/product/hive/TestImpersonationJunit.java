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
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.net.URI;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        String commonExternalLocationPath = warehouseDirectory(env) + "/nested_" + TestingNames.randomNameSuffix();

        String tableNameBob = "bob_external_table_" + TestingNames.randomNameSuffix();
        String tableLocationBob = commonExternalLocationPath + "/bob_table";
        String tableNameAlice = "alice_external_table_" + TestingNames.randomNameSuffix();
        String tableLocationAlice = commonExternalLocationPath + "/alice_table";

        try {
            executeTrinoUpdateAs(env, "bob", format("CREATE TABLE %s (a bigint) WITH (external_location = '%s')", tableNameBob, tableLocationBob));
            assertThat(getFileOwner(env, commonExternalLocationPath, "bob")).isEqualTo("bob");
            // Some environments also run authorization tests, so restrictive permissions cannot be configured globally.
            setFilePermission(env, commonExternalLocationPath, "0700", "bob");

            assertThatThrownBy(() -> executeTrinoUpdateAs(env, "alice", format("CREATE TABLE %s (a bigint) WITH (external_location = '%s')", tableNameAlice, tableLocationAlice)))
                    .hasMessageContaining("Permission denied");
        }
        finally {
            executeTrinoUpdateAs(env, "bob", format("DROP TABLE IF EXISTS %s", tableNameBob));
            executeTrinoUpdateAs(env, "alice", format("DROP TABLE IF EXISTS %s", tableNameAlice));
        }
    }

    @Test
    void testHdfsImpersonationEnabled(ProductTestEnvironment env)
    {
        String tableName = "check_hdfs_impersonation_enabled_" + TestingNames.randomNameSuffix();
        checkTableOwner(tableName, "alice", env);
        checkTableGroup(tableName, env);
    }

    private static void checkTableOwner(String tableName, String expectedOwner, ProductTestEnvironment env)
    {
        try {
            executeTrinoUpdateAs(env, "alice", format("DROP TABLE IF EXISTS %s", tableName));
            executeTrinoUpdateAs(env, "alice", format("CREATE TABLE %s AS SELECT 'abc' c", tableName));
            String tableLocation = getTableLocation(env, tableName);
            assertThat(getFileOwner(env, tableLocation, "alice")).isEqualTo(expectedOwner);
        }
        finally {
            executeTrinoUpdateAs(env, "alice", format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    private static void checkTableGroup(String tableName, ProductTestEnvironment env)
    {
        try {
            executeTrinoUpdateAs(env, "alice", format("DROP TABLE IF EXISTS %s", tableName));
            executeTrinoUpdateAs(env, "alice", format("CREATE TABLE %s AS SELECT 'abc' c", tableName));
            String tableLocation = getTableLocation(env, tableName);
            String warehouseLocation = tableLocation.substring(0, tableLocation.lastIndexOf('/'));
            assertThat(getFileGroup(env, tableLocation, "alice")).isEqualTo(getFileGroup(env, warehouseLocation, "alice"));
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

    private static String getFileOwner(ProductTestEnvironment env, String path, String user)
    {
        if (env instanceof HiveKerberosEnvironment kerberosEnvironment) {
            return getKerberosFileStatus(kerberosEnvironment, path, "u");
        }
        return hdfsClient(env, user).getFileOwner(path);
    }

    private static String getFileGroup(ProductTestEnvironment env, String path, String user)
    {
        if (env instanceof HiveKerberosEnvironment kerberosEnvironment) {
            return getKerberosFileStatus(kerberosEnvironment, path, "g");
        }
        return hdfsClient(env, user).getFileGroup(path);
    }

    private static String getKerberosFileStatus(HiveKerberosEnvironment env, String path, String field)
    {
        String command = format(
                "kinit -kt %s %s@%s && hdfs dfs -stat '%%%s' \"$1\"",
                HiveKerberosEnvironment.HADOOP_HDFS_KEYTAB,
                HiveKerberosEnvironment.HDFS_PRINCIPAL,
                env.getKerberosRealm(),
                field);
        try {
            Container.ExecResult result = env.getHadoop().execInContainer("bash", "-lc", command, "hdfs-stat", path);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("Failed to read HDFS status for " + path + ": " + result.getStderr());
            }
            return result.getStdout().trim();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read HDFS status for " + path, e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while reading HDFS status for " + path, e);
        }
    }

    private static void setKerberosFilePermission(HiveKerberosEnvironment env, String path, String permission)
    {
        String command = format(
                "kinit -kt %s %s@%s && hdfs dfs -chmod \"$1\" \"$2\"",
                HiveKerberosEnvironment.HADOOP_HDFS_KEYTAB,
                HiveKerberosEnvironment.HDFS_PRINCIPAL,
                env.getKerberosRealm());
        try {
            Container.ExecResult result = env.getHadoop().execInContainer("bash", "-lc", command, "hdfs-chmod", permission, path);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("Failed to set HDFS permissions for " + path + ": " + result.getStderr());
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to set HDFS permissions for " + path, e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while setting HDFS permissions for " + path, e);
        }
    }

    private static void setFilePermission(ProductTestEnvironment env, String path, String permission, String user)
    {
        if (env instanceof HiveKerberosEnvironment kerberosEnvironment) {
            setKerberosFilePermission(kerberosEnvironment, path, permission);
            return;
        }
        hdfsClient(env, user).setPermission(path, permission);
    }

    private static String warehouseDirectory(ProductTestEnvironment env)
    {
        if (env instanceof HiveImpersonationEnvironment hiveImpersonationEnvironment) {
            return hiveImpersonationEnvironment.getWarehouseDirectory();
        }
        if (env instanceof HiveKerberosEnvironment kerberosEnvironment) {
            return kerberosEnvironment.getHadoop().getWarehouseDirectory();
        }
        throw new IllegalStateException("Unsupported environment for impersonation test: " + env.getClass().getSimpleName());
    }
}
