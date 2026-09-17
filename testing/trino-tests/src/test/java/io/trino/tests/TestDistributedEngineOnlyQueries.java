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
package io.trino.tests;

import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributedEngineOnlyQueries
        extends AbstractDistributedEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = MemoryQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
        try {
            queryRunner.getCoordinator().getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withSessionProperties(TEST_CATALOG_PROPERTIES)
                    .build()));
            queryRunner.createCatalog(TESTING_CATALOG, "mock");
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Test
    public void testAggregationOverCrossJoinWithSingleNodeProbe()
    {
        // The probe side is a cross join of two single-node UNNEST sources, which the planner may spread
        // across nodes. The aggregation above it must still be merged into one group per key.
        assertThat(query(
                """
                SELECT s.k, COUNT(*) AS n
                FROM UNNEST(SEQUENCE(1, 1000)) AS a(x)
                CROSS JOIN UNNEST(SEQUENCE(1, 10)) AS b(y)
                CROSS JOIN (
                    SELECT z % 2 AS k
                    FROM UNNEST(SEQUENCE(1, 10)) AS c(z)
                    GROUP BY z % 2
                ) AS s
                GROUP BY s.k
                """))
                .matches("VALUES (BIGINT '0', BIGINT '10000'), (BIGINT '1', BIGINT '10000')");
    }
}
