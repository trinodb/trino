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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;

public class TestUnionWithReplicatedJoin
        extends BaseTestUnion
{
    public TestUnionWithReplicatedJoin()
    {
        super(ImmutableMap.of(JOIN_DISTRIBUTION_TYPE, BROADCAST.name()));
    }

    @Test
    public void testUnionWithNonComparableOutputAndJoin()
    {
        plan(
                """
                SELECT a.sketch
                FROM (
                    SELECT 1 AS id, empty_approx_set() AS sketch WHERE random() >= 0
                    UNION ALL
                    SELECT 2 AS id, empty_approx_set() AS sketch WHERE random() >= 0
                ) a
                INNER JOIN (
                    SELECT * FROM (VALUES 1, 2) AS v(id) WHERE random() >= 0
                ) b ON a.id = b.id
                """,
                OPTIMIZED_AND_VALIDATED,
                false);
    }
}
