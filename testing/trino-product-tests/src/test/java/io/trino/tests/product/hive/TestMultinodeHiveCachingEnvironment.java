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
 * Verification tests for the MultinodeHiveCachingEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The Hadoop and multinode Trino cluster containers start correctly</li>
 *   <li>Both hive (cached) and hivenoncached catalogs are available</li>
 *   <li>Trino can connect to the Hive Metastore and read/write data</li>
 *   <li>The cluster has both coordinator and worker nodes</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(MultinodeHiveCachingEnvironment.class)
class TestMultinodeHiveCachingEnvironment
{
    @Test
    void verifyTrinoConnectivity(MultinodeHiveCachingEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(MultinodeHiveCachingEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyHiveNonCachedCatalogExists(MultinodeHiveCachingEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hivenoncached'")).containsOnly(row("hivenoncached"));
    }

    @Test
    void verifyHiveDefaultSchema(MultinodeHiveCachingEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadTable(MultinodeHiveCachingEnvironment env)
    {
        String tableName = "test_caching_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table using cached Hive catalog
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");

            // Insert data
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (42)");

            // Read data
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyMultinodeCluster(MultinodeHiveCachingEnvironment env)
    {
        // Verify the cluster has 2 nodes (1 coordinator + 1 worker)
        assertThat(env.executeTrino("SELECT count(*) FROM system.runtime.nodes"))
                .containsOnly(row(2L));
    }

    @Test
    void verifyTpchCatalogExists(MultinodeHiveCachingEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'tpch'")).containsOnly(row("tpch"));
    }

    @Test
    void verifyTpchQueryable(MultinodeHiveCachingEnvironment env)
    {
        assertThat(env.executeTrino("SELECT count(*) FROM tpch.tiny.nation")).containsOnly(row(25L));
    }

    @Test
    void verifyJmxCatalogExists(MultinodeHiveCachingEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'jmx'")).containsOnly(row("jmx"));
    }

    @Test
    void verifyCacheStatisticsAccessible(MultinodeHiveCachingEnvironment env)
    {
        String tableName = "test_cache_stats_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create and populate a table
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (1), (2), (3)");

            // Read the table twice to populate cache
            env.executeTrino("SELECT * FROM " + fullTableName);
            env.executeTrino("SELECT * FROM " + fullTableName);

            // Verify cache statistics are accessible (doesn't throw)
            var stats = env.getCacheStatistics();
            assertThat(stats).isNotNull();
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }
}
