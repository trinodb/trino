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
package io.trino.cache;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.getCacheMinWorkerSplitSeparation;
import static io.trino.cache.CacheCommonSubqueries.getLoadCachedDataPlanNode;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public class SplitAdmissionControllerProvider
{
    public static final SplitAdmissionController NOOP = new SplitAdmissionController()
    {
        @Override
        public boolean canScheduleSplit(CacheSplitId splitId, HostAddress address)
        {
            return true;
        }

        @Override
        public void splitsScheduled(List<Split> splits) {}
    };

    private final Map<PlanNodeId, PlanSignature> planSignatures;
    private final Map<PlanSignature, SplitAdmissionController> splitSchedulerManagers;

    public SplitAdmissionControllerProvider(List<PlanFragment> fragments, Session session)
    {
        requireNonNull(fragments, "fragments is null");
        requireNonNull(session, "session is null");

        int cacheMinWorkerSplitSeparation = getCacheMinWorkerSplitSeparation(session);
        if (cacheMinWorkerSplitSeparation > 0) {
            planSignatures = fragments.stream()
                    .flatMap(fragment -> PlanNodeSearcher.searchFrom(fragment.getRoot())
                            .where(CacheCommonSubqueries::isCacheChooseAlternativeNode)
                            .findAll()
                            .stream()
                            .map(ChooseAlternativeNode.class::cast))
                    .collect(toImmutableMap(PlanNode::getId, node -> getLoadCachedDataPlanNode(node).getPlanSignature().signature()));
            splitSchedulerManagers = planSignatures.values().stream()
                    .collect(groupingBy(identity(), counting()))
                    .entrySet().stream()
                    // only create admission controller for table scans with repeating signatures
                    .filter(entry -> entry.getValue() > 1)
                    .collect(toImmutableMap(Map.Entry::getKey, _ -> new MinSeparationSplitAdmissionController(cacheMinWorkerSplitSeparation)));
        }
        else {
            planSignatures = ImmutableMap.of();
            splitSchedulerManagers = ImmutableMap.of();
        }
    }

    public SplitAdmissionController get(PlanNodeId nodeId)
    {
        PlanSignature planSignature = planSignatures.get(nodeId);
        if (planSignature == null) {
            return NOOP;
        }
        return get(planSignature);
    }

    public SplitAdmissionController get(PlanSignature signature)
    {
        return splitSchedulerManagers.getOrDefault(signature, NOOP);
    }
}
