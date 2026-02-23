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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verification tests for the MultinodeHive4Environment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The MinIO, Hive Metastore, HiveServer2, and Trino containers start correctly</li>
 *   <li>Trino can connect to the Hive 4 Metastore</li>
 *   <li>Trino can read/write to S3-compatible storage (MinIO)</li>
 *   <li>The multinode cluster has both coordinator and worker nodes</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(MultinodeHive4Environment.class)
class TestMultinodeHive4Environment
{
    @Test
    void verifyTrinoConnectivity(MultinodeHive4Environment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(MultinodeHive4Environment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyTpchCatalogExists(MultinodeHive4Environment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'tpch'")).containsOnly(row("tpch"));
    }

    @Test
    void verifyTpchQueryable(MultinodeHive4Environment env)
    {
        // Verify we can query the TPCH catalog (useful for distributed query testing)
        assertThat(env.executeTrino("SELECT count(*) FROM tpch.tiny.nation"))
                .containsOnly(row(25L));
    }

    @Test
    void verifyHiveDefaultSchema(MultinodeHive4Environment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadTable(MultinodeHive4Environment env)
    {
        String tableName = "test_multinode_hive4_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table (requires S3 write access via MinIO)
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");

            // Insert data (requires S3 write access via MinIO)
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (42)");

            // Read data (requires S3 read access via MinIO)
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyMultinodeCluster(MultinodeHive4Environment env)
    {
        // Verify the cluster has 2 nodes (1 coordinator + 1 worker)
        assertThat(env.executeTrino("SELECT count(*) FROM system.runtime.nodes"))
                .containsOnly(row(2L));
    }

    @Test
    void verifyDistributedQueryExecution(MultinodeHive4Environment env)
    {
        String tableName = "test_distributed_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table with enough data to ensure distribution
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (id int, value varchar)");
            env.executeTrinoUpdate("INSERT INTO " + fullTableName +
                    " SELECT nationkey, name FROM tpch.tiny.nation");

            // Run a query that requires distribution (aggregation)
            assertThat(env.executeTrino("SELECT count(DISTINCT value) FROM " + fullTableName))
                    .containsOnly(row(25L));

            // Verify cluster is fully active
            assertThat(env.isClusterFullyActive()).isTrue();
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyClusterUtilityMethods(MultinodeHive4Environment env)
    {
        assertThat(env.getExpectedNodeCount()).isEqualTo(2);
        assertThat(env.getWorkerCount()).isEqualTo(1);
        assertThat(env.isClusterFullyActive()).isTrue();
    }
}
