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
package io.trino.tests.product.snowflake;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResultAssert;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(SnowflakeEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Snowflake
@TestGroup.ProfileSpecificTests
class TestSnowflake
{
    @Test
    void testCreateTableAsSelect(SnowflakeEnvironment env)
    {
        String tableName = "snowflake.tpch.nation_" + randomNameSuffix();

        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        assertThat(env.executeTrinoUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation"))
                .isEqualTo(25);
        try {
            QueryResultAssert.assertThat(env.executeTrino("SELECT COUNT(*) FROM " + tableName))
                    .containsOnly(row(BigDecimal.valueOf(25)));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE " + tableName);
        }
    }
}
