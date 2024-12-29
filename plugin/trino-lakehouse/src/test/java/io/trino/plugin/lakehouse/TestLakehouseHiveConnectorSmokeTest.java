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
package io.trino.plugin.lakehouse;

import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.lakehouse.TableType.HIVE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLakehouseHiveConnectorSmokeTest
        extends BaseLakehouseConnectorSmokeTest
{
    protected TestLakehouseHiveConnectorSmokeTest()
    {
        super(HIVE);
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_TRUNCATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable newTrinoTable(String namePrefix, String tableDefinition)
    {
        if (tableDefinition.startsWith("(")) {
            tableDefinition += " WITH (transactional = true)";
        }
        else {
            tableDefinition = "WITH (transactional = true) " + tableDefinition;
        }
        return super.newTrinoTable(namePrefix, tableDefinition);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region")).isEqualTo(
                """
                CREATE TABLE lakehouse.tpch.region (
                   regionkey bigint,
                   name varchar(25),
                   comment varchar(152)
                )
                WITH (
                   format = 'ORC',
                   type = 'HIVE'
                )""");
    }
}
