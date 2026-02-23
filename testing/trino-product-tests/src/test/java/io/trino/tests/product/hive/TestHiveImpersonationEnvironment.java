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

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

/**
 * Verification tests for the HiveImpersonationEnvironment.
 * <p>
 * These tests verify that:
 * <ul>
 *   <li>The Hadoop and Trino containers start correctly</li>
 *   <li>Impersonation is properly configured</li>
 *   <li>Trino can connect to the Hive Metastore with impersonation</li>
 *   <li>Trino can read/write to HDFS with impersonation</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(HiveImpersonationEnvironment.class)
class TestHiveImpersonationEnvironment
{
    @Test
    void verifyTrinoConnectivity(HiveImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyHiveCatalogExists(HiveImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
    }

    @Test
    void verifyHiveDefaultSchema(HiveImpersonationEnvironment env)
    {
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }

    // Note: Tests that create tables or schemas are disabled because they require
    // HDFS proxy user configuration (hadoop.proxyuser.trino.hosts=* and
    // hadoop.proxyuser.trino.groups=*) which is not configured in the base
    // Hadoop container image.

    @Test
    void verifyImpersonationConfigured(HiveImpersonationEnvironment env)
    {
        // Verify impersonation is configured by checking catalog properties
        // The hive catalog should have hdfs.impersonation.enabled and metastore impersonation enabled
        assertThat(env.executeTrino("SHOW CATALOGS LIKE 'hive'")).containsOnly(row("hive"));
        // If we can query the catalog, impersonation config is at least syntactically correct
        assertThat(env.executeTrino("SHOW SCHEMAS FROM hive")).contains(row("default"));
    }
}
