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
package io.trino.plugin.clickhouse;

import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseClickHouseConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
                return false;
            case SUPPORTS_TRUNCATE:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // Override to add table properties
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("" +
                        "CREATE TABLE clickhouse.tpch.region (\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'LOG'\n" +
                        ")");
    }
}
