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
package io.trino.execution.scheduler.policy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static io.trino.execution.scheduler.policy.PlanUtils.createBroadcastJoinPlanFragment;
import static io.trino.execution.scheduler.policy.PlanUtils.createExchangePlanFragment;
import static io.trino.execution.scheduler.policy.PlanUtils.createJoinPlanFragment;
import static io.trino.execution.scheduler.policy.PlanUtils.createTableScanPlanFragment;
import static io.trino.execution.scheduler.policy.PlanUtils.createUnionPlanFragment;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static org.testng.Assert.assertEquals;

public class TestPhasedExecutionSchedule
{
    @Test
    public void testExchange()
    {
        PlanFragment aFragment = createTableScanPlanFragment("a");
        PlanFragment bFragment = createTableScanPlanFragment("b");
        PlanFragment cFragment = createTableScanPlanFragment("c");
        PlanFragment exchangeFragment = createExchangePlanFragment("exchange", aFragment, bFragment, cFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(aFragment, bFragment, cFragment, exchangeFragment));
        assertEquals(phases, ImmutableList.of(
                ImmutableSet.of(exchangeFragment.getId()),
                ImmutableSet.of(aFragment.getId()),
                ImmutableSet.of(bFragment.getId()),
                ImmutableSet.of(cFragment.getId())));
    }

    @Test
    public void testUnion()
    {
        PlanFragment aFragment = createTableScanPlanFragment("a");
        PlanFragment bFragment = createTableScanPlanFragment("b");
        PlanFragment cFragment = createTableScanPlanFragment("c");
        PlanFragment unionFragment = createUnionPlanFragment("union", aFragment, bFragment, cFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(aFragment, bFragment, cFragment, unionFragment));
        assertEquals(phases, ImmutableList.of(
                ImmutableSet.of(unionFragment.getId()),
                ImmutableSet.of(aFragment.getId()),
                ImmutableSet.of(bFragment.getId()),
                ImmutableSet.of(cFragment.getId())));
    }

    @Test
    public void testJoin()
    {
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment probeFragment = createTableScanPlanFragment("probe");
        PlanFragment joinFragment = createJoinPlanFragment(INNER, "join", buildFragment, probeFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(joinFragment, buildFragment, probeFragment));
        assertEquals(phases, ImmutableList.of(ImmutableSet.of(joinFragment.getId()), ImmutableSet.of(buildFragment.getId()), ImmutableSet.of(probeFragment.getId())));
    }

    @Test
    public void testRightJoin()
    {
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment probeFragment = createTableScanPlanFragment("probe");
        PlanFragment joinFragment = createJoinPlanFragment(RIGHT, "join", buildFragment, probeFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(joinFragment, buildFragment, probeFragment));
        assertEquals(phases, ImmutableList.of(ImmutableSet.of(joinFragment.getId()), ImmutableSet.of(buildFragment.getId()), ImmutableSet.of(probeFragment.getId())));
    }

    @Test
    public void testBroadcastJoin()
    {
        PlanFragment buildFragment = createTableScanPlanFragment("build");
        PlanFragment joinFragment = createBroadcastJoinPlanFragment("join", buildFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(joinFragment, buildFragment));
        assertEquals(phases, ImmutableList.of(ImmutableSet.of(joinFragment.getId(), buildFragment.getId())));
    }

    @Test
    public void testJoinWithDeepSources()
    {
        PlanFragment buildSourceFragment = createTableScanPlanFragment("buildSource");
        PlanFragment buildMiddleFragment = createExchangePlanFragment("buildMiddle", buildSourceFragment);
        PlanFragment buildTopFragment = createExchangePlanFragment("buildTop", buildMiddleFragment);
        PlanFragment probeSourceFragment = createTableScanPlanFragment("probeSource");
        PlanFragment probeMiddleFragment = createExchangePlanFragment("probeMiddle", probeSourceFragment);
        PlanFragment probeTopFragment = createExchangePlanFragment("probeTop", probeMiddleFragment);
        PlanFragment joinFragment = createJoinPlanFragment(INNER, "join", buildTopFragment, probeTopFragment);

        List<Set<PlanFragmentId>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(
                joinFragment,
                buildTopFragment,
                buildMiddleFragment,
                buildSourceFragment,
                probeTopFragment,
                probeMiddleFragment,
                probeSourceFragment));

        assertEquals(phases, ImmutableList.of(
                ImmutableSet.of(joinFragment.getId()),
                ImmutableSet.of(buildTopFragment.getId()),
                ImmutableSet.of(buildMiddleFragment.getId()),
                ImmutableSet.of(buildSourceFragment.getId()),
                ImmutableSet.of(probeTopFragment.getId()),
                ImmutableSet.of(probeMiddleFragment.getId()),
                ImmutableSet.of(probeSourceFragment.getId())));
    }
}
