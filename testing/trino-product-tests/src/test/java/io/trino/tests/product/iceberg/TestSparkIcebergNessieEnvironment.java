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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verification tests for the SparkIcebergNessieEnvironment.
 * <p>
 * These tests verify that the environment is correctly configured and that
 * Trino, Spark, and Nessie work together for version-controlled Iceberg tables.
 */
@ProductTest
@RequiresEnvironment(SparkIcebergNessieEnvironment.class)
@TestGroup.IcebergNessie
class TestSparkIcebergNessieEnvironment
{
    @Test
    void verifyTrinoConnectivity(SparkIcebergNessieEnvironment env)
    {
        assertThat(env.executeTrino("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifySparkConnectivity(SparkIcebergNessieEnvironment env)
    {
        assertThat(env.executeSpark("SELECT 1")).containsOnly(row(1));
    }

    @Test
    void verifyNessieEndpoint(SparkIcebergNessieEnvironment env)
            throws Exception
    {
        // Verify Nessie server is accessible and responding
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(env.getExternalNessieUri() + "/config"))
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("defaultBranch");
    }

    @Test
    void verifyIcebergCatalog(SparkIcebergNessieEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.nessie_test");
        try {
            env.executeTrinoUpdate("CREATE TABLE iceberg.nessie_test.trino_table (x int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.nessie_test.trino_table VALUES (42)");
            assertThat(env.executeTrino("SELECT * FROM iceberg.nessie_test.trino_table"))
                    .containsOnly(row(42));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.nessie_test.trino_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.nessie_test");
        }
    }

    @Test
    void verifySparkCreatesTableTrinoReads(SparkIcebergNessieEnvironment env)
    {
        // Spark creates table in iceberg_test catalog (using Nessie)
        env.executeSparkUpdate("CREATE NAMESPACE IF NOT EXISTS iceberg_test.spark_db");
        try {
            env.executeSparkUpdate("CREATE TABLE iceberg_test.spark_db.spark_table (y INT) USING iceberg");
            env.executeSparkUpdate("INSERT INTO iceberg_test.spark_db.spark_table VALUES (99)");

            // Trino reads from iceberg catalog (same Nessie server)
            assertThat(env.executeTrino("SELECT * FROM iceberg.spark_db.spark_table"))
                    .containsOnly(row(99));
        }
        finally {
            // Cleanup via Spark
            env.executeSparkUpdate("DROP TABLE IF EXISTS iceberg_test.spark_db.spark_table");
            env.executeSparkUpdate("DROP NAMESPACE IF EXISTS iceberg_test.spark_db");
        }
    }

    @Test
    void verifyTrinoCreatesTableSparkReads(SparkIcebergNessieEnvironment env)
    {
        // Trino creates table in iceberg catalog (using Nessie)
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.trino_db");
        try {
            env.executeTrinoUpdate("CREATE TABLE iceberg.trino_db.trino_table (z int)");
            env.executeTrinoUpdate("INSERT INTO iceberg.trino_db.trino_table VALUES (77)");

            // Spark reads from iceberg_test catalog (same Nessie server)
            assertThat(env.executeSpark("SELECT * FROM iceberg_test.trino_db.trino_table"))
                    .containsOnly(row(77));
        }
        finally {
            // Cleanup via Trino
            env.executeTrinoUpdate("DROP TABLE IF EXISTS iceberg.trino_db.trino_table");
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg.trino_db");
        }
    }
}
