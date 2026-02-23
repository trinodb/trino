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

/**
 * Verification tests for the TwoMixedHivesEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The KDC, Hadoop clusters, and Trino containers start correctly</li>
 *   <li>Both hive1 (Kerberized) and hive2 (Standard) catalogs are accessible</li>
 *   <li>Trino can read/write to the Kerberos-enabled cluster via hive1 catalog</li>
 *   <li>Trino can read/write to the standard cluster via hive2 catalog</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(TwoMixedHivesEnvironment.class)
class TestTwoMixedHivesEnvironment
{
    @Test
    void verifyTrinoConnectivity(TwoMixedHivesEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHive1CatalogExists(TwoMixedHivesEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive1'")).containsOnly(row("hive1"));
    }

    @Test
    void verifyHive2CatalogExists(TwoMixedHivesEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive2'")).containsOnly(row("hive2"));
    }

    @Test
    void verifyHive1DefaultSchema(TwoMixedHivesEnvironment env)
    {
        // hive1 connects to Kerberized cluster
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive1")).contains(row("default"));
    }

    @Test
    void verifyHive2DefaultSchema(TwoMixedHivesEnvironment env)
    {
        // hive2 connects to Standard cluster (no auth)
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive2")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadTableInKerberizedCluster(TwoMixedHivesEnvironment env)
    {
        String tableName = "test_kerberos_" + randomNameSuffix();
        String fullTableName = "hive1.default." + tableName;

        try {
            // Create table in Kerberized cluster (requires HDFS write access with Kerberos)
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");

            // Insert data (requires HDFS write access with Kerberos)
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (42)");

            // Read data (requires HDFS read access with Kerberos)
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyCreateAndReadTableInStandardCluster(TwoMixedHivesEnvironment env)
    {
        String tableName = "test_standard_" + randomNameSuffix();
        String fullTableName = "hive2.default." + tableName;

        try {
            // Create table in Standard cluster (no Kerberos)
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");

            // Insert data
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (99)");

            // Read data
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(99));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyTpchCatalogExists(TwoMixedHivesEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'tpch'")).containsOnly(row("tpch"));
    }

    @Test
    void verifyTpchQueryable(TwoMixedHivesEnvironment env)
    {
        assertThat(env.executeTrino("SELECT count(*) FROM tpch.tiny.nation")).containsOnly(row(25L));
    }

    @Test
    void verifyCrossClusterJoin(TwoMixedHivesEnvironment env)
    {
        String tableName1 = "test_cross_join_1_" + randomNameSuffix();
        String tableName2 = "test_cross_join_2_" + randomNameSuffix();

        try {
            // Create table in Kerberized cluster
            env.executeTrinoUpdate("CREATE TABLE hive1.default." + tableName1 + " (id int)");
            env.executeTrinoUpdate("INSERT INTO hive1.default." + tableName1 + " VALUES (1), (2)");

            // Create table in Standard cluster
            env.executeTrinoUpdate("CREATE TABLE hive2.default." + tableName2 + " (id int)");
            env.executeTrinoUpdate("INSERT INTO hive2.default." + tableName2 + " VALUES (10), (20)");

            // Cross-authentication-domain join
            assertThat(env.executeTrino(
                    "SELECT count(*) FROM hive1.default." + tableName1 + " a, hive2.default." + tableName2 + " b"))
                    .containsOnly(row(4L));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive1.default." + tableName1);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive2.default." + tableName2);
        }
    }
}
