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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.testing.AbstractTestDynamicRowFiltering;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public final class TestHiveDynamicRowFiltering
        extends AbstractTestDynamicRowFiltering
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .setHiveProperties(ImmutableMap.of(
                        "hive.dynamic-filtering.wait-timeout", "1h",
                        "hive.target-max-file-size", "10kB"))  // Needed to generate multiple splits to allow dynamic filter to be ready
                .build();
    }

    @BeforeAll
    public void createTestData()
    {
        assertUpdate(
                "CREATE TABLE orders AS SELECT " +
                        "CAST(clerk AS CHAR(15)) clerk, " +
                        "CAST(orderstatus AS CHAR(5)) orderstatus, " +
                        "custkey FROM tpch.tiny.orders",
                15000);
    }

    @Test
    @Timeout(30)
    public void testRowFilteringWithCharStrings()
    {
        for (JoinDistributionType joinDistributionType : JoinDistributionType.values()) {
            assertRowFiltering(
                    "SELECT o1.clerk, o1.custkey, CAST(o1.orderstatus AS VARCHAR(1)) FROM orders o1, orders o2 WHERE o1.clerk = o2.clerk AND o2.custkey < 10",
                    joinDistributionType,
                    "orders");

            assertNoRowFiltering(
                    "SELECT COUNT(*) FROM orders o1, orders o2 WHERE o1.orderstatus = o2.orderstatus AND o2.custkey < 20",
                    joinDistributionType,
                    "orders");
        }
    }

    @Override
    protected SchemaTableName getSchemaTableName(ConnectorTableHandle connectorHandle)
    {
        return ((HiveTableHandle) connectorHandle).getSchemaTableName();
    }
}
