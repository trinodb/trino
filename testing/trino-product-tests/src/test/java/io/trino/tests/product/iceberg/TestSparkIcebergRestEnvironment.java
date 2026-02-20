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
package io.trino.tests.product.iceberg;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verification tests for the SparkIcebergRestEnvironment.
 * <p>
 * These tests verify that the environment is correctly configured and that
 * Trino and Spark can both connect to the Iceberg REST Catalog and share data
 * through Iceberg tables.
 */
@ProductTest
@RequiresEnvironment(SparkIcebergRestEnvironment.class)
@TestGroup.IcebergRest
class TestSparkIcebergRestEnvironment
{
    @Test
    void verifyTrinoConnectivity(SparkIcebergRestEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifySparkConnectivity(SparkIcebergRestEnvironment env)
    {
        assertThat(env.executeSpark("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyRestCatalogEndpoint(SparkIcebergRestEnvironment env)
            throws Exception
    {
        // Verify the REST catalog server is accessible and responding
        URI uri = URI.create(env.getExternalRestCatalogUri() + "v1/config");
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        try {
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            int responseCode = connection.getResponseCode();
            // REST catalog should return 200 for the config endpoint
            assertThat(responseCode).isEqualTo(200);
        }
        finally {
            connection.disconnect();
        }
    }

    @Test
    void verifyIcebergRestCatalog(SparkIcebergRestEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.rest_test");
        try {
            env.executeTrinoUpdate("CREATE TABLE iceberg.rest_test.trino_table (x int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.rest_test.trino_table VALUES (42)");
            assertThat(env.executeTrino("SELECT * FROM iceberg.rest_test.trino_table"))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.rest_test.trino_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.rest_test");
        }
    }

    @Test
    void verifySparkCreatesTableTrinoReads(SparkIcebergRestEnvironment env)
    {
        // Spark creates namespace and table via REST catalog
        env.executeSparkUpdate("CREATE NAMESPACE IF NOT EXISTS iceberg_test.spark_rest_db");
        try {
            env.executeSparkUpdate("CREATE TABLE iceberg_test.spark_rest_db.spark_table (y INT) USING iceberg");
            env.executeSparkUpdate("INSERT INTO iceberg_test.spark_rest_db.spark_table VALUES (99)");

            // Trino reads from the same REST catalog
            assertThat(env.executeTrino("SELECT * FROM iceberg.spark_rest_db.spark_table"))
                    .containsOnly(row(99));
        }
        finally {
            // Cleanup via Spark
            env.executeSparkUpdate("DROP TABLE IF EXISTS iceberg_test.spark_rest_db.spark_table");
            env.executeSparkUpdate("DROP NAMESPACE IF EXISTS iceberg_test.spark_rest_db");
        }
    }

    @Test
    void verifyTrinoCreatesTableSparkReads(SparkIcebergRestEnvironment env)
    {
        // Trino creates schema and table via REST catalog
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.trino_rest_db");
        try {
            env.executeTrinoUpdate("CREATE TABLE iceberg.trino_rest_db.trino_table (z int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.trino_rest_db.trino_table VALUES (77)");

            // Spark reads from the same REST catalog
            assertThat(env.executeSpark("SELECT * FROM iceberg_test.trino_rest_db.trino_table"))
                    .containsOnly(row(77));
        }
        finally {
            // Cleanup via Trino
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.trino_rest_db.trino_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.trino_rest_db");
        }
    }

    @Test
    void verifyCustomTableProperty(SparkIcebergRestEnvironment env)
    {
        // Test that custom.table-property is allowed (configured in iceberg.allowed-extra-properties)
        // Custom properties must be passed via extra_properties MAP, not as direct table properties
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.props_test");
        try {
            env.executeTrinoUpdate(
                    "CREATE TABLE iceberg.props_test.props_table (id int) " +
                            "WITH (extra_properties = MAP(ARRAY['custom.table-property'], ARRAY['custom-value']))");

            // Verify table was created successfully
            assertThat(env.executeTrino("SHOW TABLES FROM iceberg.props_test"))
                    .containsOnly(row("props_table"));

            // Verify the extra property is visible in Iceberg's table properties
            assertThat(env.executeTrino(
                    "SELECT value FROM iceberg.props_test.\"props_table$properties\" " +
                            "WHERE key = 'custom.table-property'"))
                    .containsOnly(row("custom-value"));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.props_test.props_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.props_test");
        }
    }
}
