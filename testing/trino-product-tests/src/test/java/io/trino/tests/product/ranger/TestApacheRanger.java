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
package io.trino.tests.product.ranger;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(RangerEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Ranger
@TestGroup.ProfileSpecificTests
class TestApacheRanger
{
    @Test
    void testCreateTableAsSelect(RangerEnvironment env)
    {
        String tableName = "mariadb.test.nation_" + randomNameSuffix();

        try {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
            assertThat(env.executeTrinoUpdate(
                            "CREATE TABLE " + tableName + " AS SELECT n AS nationkey, format('nation-%s', CAST(n AS varchar)) AS comment FROM UNNEST(sequence(1, 25)) t(n)"))
                    .isEqualTo(25);
            assertThat(env.executeTrino("SELECT COUNT(*) FROM " + tableName)).containsOnly(row(25));
            assertThat(env.executeTrinoUpdate("TRUNCATE TABLE " + tableName)).isEqualTo(0);
            assertThat(env.executeTrinoUpdate(
                            "INSERT INTO " + tableName + " SELECT n AS nationkey, format('nation-%s', CAST(n AS varchar)) AS comment FROM UNNEST(sequence(1, 25)) t(n)"))
                    .isEqualTo(25);
            assertThat(env.executeTrinoUpdate("UPDATE " + tableName + " SET comment = 'updated comment'"))
                    .isEqualTo(25);
            assertThat(env.executeTrinoUpdate("DELETE FROM " + tableName)).isEqualTo(25);

            assertThatThrownBy(() -> env.executeTrino("SELECT COUNT(*) FROM " + tableName, "alice@trino"))
                    .isInstanceOf(RuntimeException.class);
            assertThatThrownBy(() -> env.executeTrino("TRUNCATE TABLE " + tableName, "alice@trino"))
                    .isInstanceOf(RuntimeException.class);
            assertThatThrownBy(() -> env.executeTrino("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation", "alice@trino"))
                    .isInstanceOf(RuntimeException.class);
            assertThatThrownBy(() -> env.executeTrino("UPDATE " + tableName + " SET comment = 'updated comment'", "alice@trino"))
                    .isInstanceOf(RuntimeException.class);
            assertThatThrownBy(() -> env.executeTrino("DELETE FROM " + tableName, "alice@trino"))
                    .isInstanceOf(RuntimeException.class);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
