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
package io.trino.plugin.hive;

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.Test;

import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Redundant over TestHiveConnectorTest, but exists to exercise BaseConnectorSmokeTest
// Some features like views may be supported by Hive only.
public class TestHiveConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_MULTI_STATEMENT_WRITES -> true;
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_TOPN_PUSHDOWN,
                    SUPPORTS_TRUNCATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Override
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Override
    public void testMerge()
    {
        assertThatThrownBy(super::testMerge)
                .hasMessage(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("" +
                        "CREATE TABLE hive.tpch.region (\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar(25),\n" +
                        "   comment varchar(152)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC'\n" +
                        ")");
    }

    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        // Override because HivePrincipal's username is case-sensitive unlike TrinoPrincipal
        assertThatThrownBy(super::testCreateSchemaWithNonLowercaseOwnerName)
                .hasMessageContaining("Access Denied: Cannot create schema")
                .hasStackTraceContaining("CREATE SCHEMA");
    }
}
