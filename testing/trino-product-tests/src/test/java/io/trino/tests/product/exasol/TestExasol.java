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
package io.trino.tests.product.exasol;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

@ProductTest
@RequiresEnvironment(ExasolEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Exasol
@TestGroup.ProfileSpecificTests
class TestExasol
{
    @Test
    void testSelect(ExasolEnvironment env)
    {
        String schemaName = "test_" + randomNameSuffix();
        String tableName = schemaName + ".tab";
        env.executeExasolUpdate("CREATE SCHEMA " + schemaName);
        try {
            env.executeExasolUpdate("CREATE TABLE " + tableName + " (id integer, name varchar(5))");
            env.executeExasolUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')");
            assertThat(env.executeTrino("SELECT * FROM exasol." + tableName))
                    .containsOnly(row(BigDecimal.valueOf(1), "a"));
        }
        finally {
            env.executeExasolUpdate("DROP TABLE IF EXISTS " + tableName);
            env.executeExasolUpdate("DROP SCHEMA " + schemaName);
        }
    }
}
