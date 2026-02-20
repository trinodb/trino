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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Delta Lake select compatibility between Trino and Spark.
 * <p>
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeSelectCompatibility
{
    @Test
    void testPartitionedSelectSpecialCharacters(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_partitioned_select_special" + randomNameSuffix();

        env.executeSparkUpdate("" +
                "CREATE TABLE default." + tableName +
                "         (a_number INT, a_string STRING)" +
                "         USING delta " +
                "         PARTITIONED BY (a_string)" +
                "         LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES " +
                    "(1, 'spark=equal'), " +
                    "(2, 'spark+plus'), " +
                    "(3, 'spark space')," +
                    "(4, 'spark:colon')," +
                    "(5, 'spark%percent')," +
                    "(6, 'spark/forwardslash')");
            env.executeTrinoUpdate("INSERT INTO delta.default." + tableName + " VALUES " +
                    "(10, 'trino=equal'), " +
                    "(20, 'trino+plus'), " +
                    "(30, 'trino space')," +
                    "(40, 'trino:colon')," +
                    "(50, 'trino%percent')," +
                    "(60, 'trino/forwardslash')");

            List<Row> expectedRows = List.of(
                    row(1, "spark=equal"),
                    row(2, "spark+plus"),
                    row(3, "spark space"),
                    row(4, "spark:colon"),
                    row(5, "spark%percent"),
                    row(6, "spark/forwardslash"),
                    row(10, "trino=equal"),
                    row(20, "trino+plus"),
                    row(30, "trino space"),
                    row(40, "trino:colon"),
                    row(50, "trino%percent"),
                    row(60, "trino/forwardslash"));

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            String deltaFilePath = (String) env.executeSpark("SELECT input_file_name() FROM default." + tableName + " WHERE a_number = 1").getOnlyValue();
            String trinoFilePath = (String) env.executeTrino("SELECT \"$path\" FROM delta.default." + tableName + " WHERE a_number = 1").getOnlyValue();
            // File paths returned by the input_file_name function are URI encoded https://github.com/delta-io/delta/issues/1517 while the $path of Trino is not
            assertThat(deltaFilePath).isNotEqualTo(trinoFilePath);
            assertThat(format("s3://%s%s", env.getBucketName(), URI.create(deltaFilePath).getPath())).isEqualTo(trinoFilePath);

            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName + " WHERE \"$path\" = '" + trinoFilePath + "'"))
                    .containsOnly(row(1, "spark=equal"));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }
}
