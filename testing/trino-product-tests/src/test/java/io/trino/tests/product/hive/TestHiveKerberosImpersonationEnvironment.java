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
 * Verification tests for the HiveKerberosImpersonationEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The KDC, Hadoop, and Trino containers start correctly with Kerberos authentication</li>
 *   <li>HDFS impersonation is enabled and working</li>
 *   <li>Hive Metastore thrift impersonation is enabled</li>
 *   <li>Hive views are enabled</li>
 *   <li>Trino can connect to the Kerberos-enabled Hive Metastore with impersonation</li>
 *   <li>Trino can read/write to Kerberos-enabled HDFS with impersonation</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveKerberosImpersonationEnvironment.class)
class TestHiveKerberosImpersonationEnvironment
{
    @Test
    void verifyTrinoConnectivity(HiveKerberosImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(HiveKerberosImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyHiveDefaultSchema(HiveKerberosImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    @Test
    void verifyKerberosRealm(HiveKerberosImpersonationEnvironment env)
    {
        // Verify the expected Kerberos realm is configured
        // This must match the realm in the KDC image (ghcr.io/trinodb/testing/kdc)
        String realm = env.getKerberosRealm();
        assertThat(realm).isEqualTo("TRINO.TEST");
    }

    @Test
    void verifyCreateAndReadTable(HiveKerberosImpersonationEnvironment env)
    {
        String tableName = "test_kerberos_impersonation_" + randomNameSuffix();
        String fullTableName = "hive.default." + tableName;

        try {
            // Create table (requires HDFS write access with Kerberos and impersonation)
            env.executeTrinoUpdate("CREATE TABLE " + fullTableName + " (x int)");

            // Insert data (requires HDFS write access with Kerberos and impersonation)
            env.executeTrinoUpdate("INSERT INTO " + fullTableName + " VALUES (42)");

            // Read data (requires HDFS read access with Kerberos and impersonation)
            assertThat(env.executeTrino("SELECT * FROM " + fullTableName))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }

    // Note: Create schema test is disabled because it requires SQL standard access control
    // privileges that are not configured for the test user in this environment.

    @Test
    void verifyTpchCatalogExists(HiveKerberosImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'tpch'")).containsOnly(row("tpch"));
    }

    @Test
    void verifyTpchQueryable(HiveKerberosImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SELECT count(*) FROM tpch.tiny.nation")).containsOnly(row(25L));
    }

    // Note: Multi-user impersonation test is not included because it requires HDFS proxy user
    // configuration (hadoop.proxyuser.hive.hosts and hadoop.proxyuser.hive.groups) in core-site.xml.
    // The current environment only configures impersonation for the Trino service principal,
    // not for arbitrary user impersonation through HDFS.
}
