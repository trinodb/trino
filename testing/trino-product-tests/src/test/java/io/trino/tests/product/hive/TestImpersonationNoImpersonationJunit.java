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
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.HdfsNoImpersonation
@TestGroup.ProfileSpecificTests
class TestImpersonationNoImpersonationJunit
{
    private static final String CONFIGURED_HDFS_USER = "trino";

    @Test
    void testExternalLocationTableCreationSuccess(HiveBasicEnvironment env)
    {
        String commonExternalLocationPath = env.getWarehouseDirectory() + "/nested_" + TestingNames.randomNameSuffix();

        String tableNameBob = "bob_external_table_" + TestingNames.randomNameSuffix();
        String tableLocationBob = commonExternalLocationPath + "/bob_table";
        String tableNameAlice = "alice_external_table_" + TestingNames.randomNameSuffix();
        String tableLocationAlice = commonExternalLocationPath + "/alice_table";

        HdfsClient hdfsClient = env.createHdfsClient();
        try {
            hdfsClient.createDirectory(tableLocationBob);
            env.executeTrinoUpdate(format("CREATE TABLE %s (a bigint) WITH (external_location = '%s')", tableNameBob, tableLocationBob), "bob");
            String expectedOwner = hdfsClient.getFileOwner(tableLocationBob);

            hdfsClient.createDirectory(tableLocationAlice);
            env.executeTrinoUpdate(format("CREATE TABLE %s (a bigint) WITH (external_location = '%s')", tableNameAlice, tableLocationAlice), "alice");
            assertThat(env.executeTrino("SELECT * FROM " + tableNameAlice, "alice").getRowsCount()).isEqualTo(0);

            assertThat(hdfsClient.getFileOwner(tableLocationAlice)).isEqualTo(expectedOwner);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableNameBob, "bob");
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableNameAlice, "alice");
        }
    }

    @Test
    void testHdfsImpersonationDisabled(HiveBasicEnvironment env)
    {
        checkTableOwner("check_hdfs_impersonation_disabled_" + TestingNames.randomNameSuffix(), CONFIGURED_HDFS_USER, env);
    }

    private static void checkTableOwner(String tableName, String expectedOwner, HiveBasicEnvironment env)
    {
        HdfsClient hdfsClient = env.createHdfsClient();
        try {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", tableName), "alice");
            env.executeTrinoUpdate(format("CREATE TABLE %s AS SELECT 'abc' c", tableName), "alice");
            String tableLocation = getTableLocation(env, tableName);
            assertThat(hdfsClient.getFileOwner(tableLocation)).isEqualTo(expectedOwner);
        }
        finally {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s", tableName), "alice");
        }
    }

    private static String getTableLocation(HiveBasicEnvironment env, String tableName)
    {
        String location = env.executeTrino(
                format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM %s", tableName),
                "alice")
                .column(1)
                .getFirst()
                .toString();
        if (location.startsWith("hdfs://")) {
            return URI.create(location).getPath();
        }
        return location;
    }
}
