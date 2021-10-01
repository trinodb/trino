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

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_VIEW:
                return true;

            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
                return true;

            case SUPPORTS_MULTI_STATEMENT_WRITES:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
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
                .matches("""
                                CREATE TABLE hive\\.tpch\\.region \\(
                                   regionkey bigint,
                                   name varchar\\(25\\),
                                   comment varchar\\(152\\)
                                \\)
                                WITH \\(
                                   extra_properties = map_from_entries\\(ARRAY.*\\),
                                   format = 'ORC'
                                \\)""");
    }
}
