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
package io.trino.plugin.druid;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.druid.DruidQueryRunner.createDruidQueryRunnerTpch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDruidLatestConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    private TestingDruidServer druidServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        druidServer = closeAfterClass(new TestingDruidServer("apache/druid:0.20.0"));
        return createDruidQueryRunnerTpch(
                druidServer,
                ImmutableMap.of(),
                ImmutableMap.of(),
                REQUIRED_TPCH_TABLES);
    }

    @AfterAll
    public void destroy()
    {
        druidServer = null; // closed by closeAfterClass
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA,
                    SUPPORTS_CREATE_TABLE,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_DELETE,
                    SUPPORTS_INSERT,
                    SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override // Override because an additional '__time' column exists
    public void testSelectInformationSchemaColumns()
    {
        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_schema = 'druid' AND table_name = 'region'"))
                .skippingTypesCheck()
                .matches("VALUES '__time', 'regionkey', 'name', 'comment'");
    }

    @Test
    @Override // Override because an additional '__time' column exists
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE region").getOnlyValue())
                .isEqualTo("""
                        CREATE TABLE druid.druid.region (
                           __time timestamp(3) NOT NULL,
                           comment varchar,
                           name varchar,
                           regionkey bigint NOT NULL
                        )""");
    }
}
