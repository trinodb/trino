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
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.azure.AzureEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;

@ProductTest
@RequiresEnvironment(AzureEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Azure
class TestAzureBlobFileSystem
{
    @BeforeEach
    void setUp(AzureEnvironment env)
    {
        String schemaLocation = env.getSchemaLocation();
        env.executeHiveCommand("dfs -rm -f -r " + schemaLocation);
        env.executeHiveCommand("dfs -mkdir -p " + schemaLocation);
    }

    @AfterEach
    void tearDown(AzureEnvironment env)
    {
        env.executeHiveCommand("dfs -mkdir -p " + env.getSchemaLocation());
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testPathContainsSpecialCharacter(AzureEnvironment env)
    {
        String tableName = "test_path_special_character_" + randomNameSuffix();
        String tableLocation = env.getSchemaLocation() + "/" + tableName;

        try {
            env.executeHiveUpdate("CREATE TABLE " + tableName + " (id bigint) PARTITIONED BY (part string) LOCATION '" + tableLocation + "'");
            env.executeHiveUpdate("INSERT INTO " + tableName + " VALUES " +
                    "(1, 'hive=equal')," +
                    "(2, 'hive+plus')," +
                    "(3, 'hive space')," +
                    "(4, 'hive:colon')," +
                    "(5, 'hive%percent')");
            env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " VALUES " +
                    "(11, 'trino=equal')," +
                    "(12, 'trino+plus')," +
                    "(13, 'trino space')," +
                    "(14, 'trino:colon')," +
                    "(15, 'trino%percent')");

            List<Row> expected = List.of(
                    row(1L, "hive=equal"),
                    row(2L, "hive+plus"),
                    row(3L, "hive space"),
                    row(4L, "hive:colon"),
                    row(5L, "hive%percent"),
                    row(11L, "trino=equal"),
                    row(12L, "trino+plus"),
                    row(13L, "trino space"),
                    row(14L, "trino:colon"),
                    row(15L, "trino%percent"));

            assertThat(env.executeHive("SELECT * FROM " + tableName)).containsOnly(expected.toArray(Row[]::new));
            assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName)).containsOnly(expected.toArray(Row[]::new));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
