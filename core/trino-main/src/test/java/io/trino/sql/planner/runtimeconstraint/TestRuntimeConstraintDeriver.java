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
package io.trino.sql.planner.runtimeconstraint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.UNION_ALL_PARTITIONS;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintKind.MEMBERSHIP;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static io.trino.util.StructuralTestUtil.sqlMapOf;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRuntimeConstraintDeriver
{
    @Test
    public void testEqualitySupportsComparableNonOrderableType()
    {
        MapType mapType = new MapType(BIGINT, BIGINT, new TypeOperators());
        Domain domain = Domain.singleValue(mapType, sqlMapOf(BIGINT, BIGINT, ImmutableMap.of(1L, 2L)));
        RuntimeConstraintId constraintId = new RuntimeConstraintId("constraint");
        RuntimeConstraintProducerGroup group = new RuntimeConstraintProducerGroup(
                new ProducerGroupId("group"),
                new PlanFragmentId("fragment"),
                new PlanNodeId("join"),
                UNION_ALL_PARTITIONS,
                ImmutableList.of(new RuntimeConstraintLane(0, mapType)),
                ImmutableList.of(new RuntimeConstraintDerivation(constraintId, MEMBERSHIP, ImmutableList.of(0), EQUAL, false)));

        assertThat(RuntimeConstraintDeriver.derive(
                group,
                ImmutableList.of(new RuntimeMembershipPayload(ImmutableList.of(domain), ORDINARY))))
                .containsEntry(constraintId, new RuntimeMembershipPayload(ImmutableList.of(domain), ORDINARY));
    }
}
