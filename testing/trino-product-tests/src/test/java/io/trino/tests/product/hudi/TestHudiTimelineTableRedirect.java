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
package io.trino.tests.product.hudi;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.hive.HiveHudiRedirectionsEnvironment;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(HiveHudiRedirectionsEnvironment.class)
@TestGroup.HiveHudiRedirections
class TestHudiTimelineTableRedirect
{
    @Test
    void testTimelineTableRedirect(HiveHudiRedirectionsEnvironment env)
    {
        String tableName = "test_hudi_timeline_system_table_redirect_" + randomNameSuffix();
        String nonExistingTableName = tableName + "_non_existing";
        env.executeSparkUpdate(
                """
                CREATE TABLE default.%s (
                  id bigint,
                  name string,
                  price int,
                  ts bigint)
                USING hudi
                TBLPROPERTIES (
                  type = 'cow',
                  primaryKey = 'id',
                  preCombineField = 'ts')
                LOCATION 's3://%s/%s'
                """.formatted(tableName, env.getBucketName(), tableName));
        env.executeSparkUpdate("INSERT INTO default.%s VALUES (1, 'a1', 20, 1000), (2, 'a2', 40, 2000)".formatted(tableName));

        try {
            assertThat(env.executeTrino("SELECT action, state FROM hive.default.\"" + tableName + "$timeline\""))
                    .containsOnly(row("commit", "COMPLETED"));
            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.default.\"" + nonExistingTableName + "$timeline\""))
                    .rootCause()
                    .hasMessageContaining("Table 'hive.default.\"" + nonExistingTableName + "$timeline\"' does not exist");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE " + tableName);
        }
    }
}
