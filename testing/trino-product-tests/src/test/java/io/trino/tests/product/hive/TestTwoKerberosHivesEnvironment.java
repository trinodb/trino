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
 * Verification tests for the TwoKerberosHivesEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The shared KDC, both Hadoop clusters, and Trino containers start correctly</li>
 *   <li>Kerberos authentication is properly configured for both clusters</li>
 *   <li>Trino can connect to both Kerberos-enabled Hive Metastores</li>
 *   <li>Trino can read/write to both Kerberos-enabled HDFS clusters</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(TwoKerberosHivesEnvironment.class)
class TestTwoKerberosHivesEnvironment
{
    @Test
    void verifyTrinoConnectivity(TwoKerberosHivesEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHive1CatalogExists(TwoKerberosHivesEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive1'")).containsOnly(row("hive1"));
    }

    @Test
    void verifyHive2CatalogExists(TwoKerberosHivesEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive2'")).containsOnly(row("hive2"));
    }

    @Test
    void verifyHive1DefaultSchema(TwoKerberosHivesEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive1")).contains(row("default"));
    }

    @Test
    void verifyHive2DefaultSchema(TwoKerberosHivesEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive2")).contains(row("default"));
    }

    @Test
    void verifyCreateAndReadTableInHive1(TwoKerberosHivesEnvironment env)
    {
        String tableName = "test_two_kerberos_hive1_" + randomNameSuffix();
        String fullTableName = "hive1.default." + tableName;

        try {
            // Create table (requires HDFS write access with Kerberos to first cluster)
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
    void verifyCreateAndReadTableInHive2(TwoKerberosHivesEnvironment env)
    {
        String tableName = "test_two_kerberos_hive2_" + randomNameSuffix();
        String fullTableName = "hive2.default." + tableName;

        try {
            // Create table (requires HDFS write access with Kerberos to second cluster)
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");

            // Insert data (requires HDFS write access with Kerberos)
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (99)");

            // Read data (requires HDFS read access with Kerberos)
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(99));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    @Test
    void verifyTpchCatalogExists(TwoKerberosHivesEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'tpch'")).containsOnly(row("tpch"));
    }

    @Test
    void verifyTpchQueryable(TwoKerberosHivesEnvironment env)
    {
        assertThat(env.executeTrino("SELECT count(*) FROM tpch.tiny.nation")).containsOnly(row(25L));
    }

    @Test
    void verifyCrossClusterJoin(TwoKerberosHivesEnvironment env)
    {
        String tableName1 = "test_cross_join_1_" + randomNameSuffix();
        String tableName2 = "test_cross_join_2_" + randomNameSuffix();

        try {
            // Create table in first cluster
            env.executeTrinoUpdate("CREATE TABLE hive1.default." + tableName1 + " (id int)");
            env.executeTrinoUpdate("INSERT INTO hive1.default." + tableName1 + " VALUES (1), (2)");

            // Create table in second cluster
            env.executeTrinoUpdate("CREATE TABLE hive2.default." + tableName2 + " (id int)");
            env.executeTrinoUpdate("INSERT INTO hive2.default." + tableName2 + " VALUES (10), (20)");

            // Cross-cluster join (2 * 2 = 4 rows)
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
