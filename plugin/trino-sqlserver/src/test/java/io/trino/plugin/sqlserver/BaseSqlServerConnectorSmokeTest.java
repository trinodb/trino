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
package io.trino.plugin.sqlserver;

import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseSqlServerConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_MERGE,
                 SUPPORTS_ROW_LEVEL_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo(
                        """
                        CREATE TABLE sqlserver.dbo.region (
                           regionkey bigint,
                           name varchar(25),
                           comment varchar(152)
                        )
                        WITH (
                           data_compression = 'NONE'
                        )\
                        """);
    }
}
