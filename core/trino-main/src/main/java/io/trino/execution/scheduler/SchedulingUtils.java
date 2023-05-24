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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SpatialJoinNode;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;

public final class SchedulingUtils
{
    private SchedulingUtils() {}

    public static boolean canStream(SubPlan plan, SubPlan source)
    {
        // can data from source be streamed through plan
        PlanFragmentId sourceFragmentId = source.getFragment().getId();

        PlanNode root = plan.getFragment().getRoot();
        RemoteSourceNode sourceNode = plan.getFragment().getRemoteSourceNodes().stream().filter(node -> node.getSourceFragmentIds().contains(sourceFragmentId)).collect(onlyElement());
        List<PlanNode> pathToSource = findPath(root, sourceNode).orElseThrow(() -> new RuntimeException("Could not find path from %s to %s in %s".formatted(root, sourceNode, plan.getFragment())));

        for (int pos = 0; pos < pathToSource.size() - 1; ++pos) {
            PlanNode node = pathToSource.get(pos);

            if (node instanceof JoinNode ||
                    node instanceof SemiJoinNode ||
                    node instanceof IndexJoinNode ||
                    node instanceof SpatialJoinNode) {
                PlanNode leftSource = node.getSources().get(0);
                PlanNode child = pathToSource.get(pos + 1);

                if (leftSource != child) {
                    return false;
                }
            }
        }
        return true;
    }

    private static Optional<List<PlanNode>> findPath(PlanNode start, PlanNode end)
    {
        PlanVisitor<Optional<List<PlanNode>>, Deque<PlanNode>> visitor = new PlanVisitor<>()
        {
            @Override
            protected Optional<List<PlanNode>> visitPlan(PlanNode node, Deque<PlanNode> queue)
            {
                queue.add(node);
                if (node == end) {
                    return Optional.of(ImmutableList.copyOf(queue));
                }
                for (PlanNode source : node.getSources()) {
                    Optional<List<PlanNode>> result = source.accept(this, queue);
                    if (result.isPresent()) {
                        return result;
                    }
                }
                queue.removeLast();
                return Optional.empty();
            }
        };

        return start.accept(visitor, new ArrayDeque<>());
    }
}
