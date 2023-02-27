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

import com.google.common.collect.ImmutableList;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static org.testng.Assert.assertEquals;

public class TestPlanNodeSearcher
{
    private static final PlanBuilder BUILDER = new PlanBuilder(new PlanNodeIdAllocator(), new AbstractMockMetadata() {}, TEST_SESSION);

    @Test
    public void testFindAll()
    {
        int size = 10;
        ProjectNode root = BUILDER.project(Assignments.of(), BUILDER.values());
        for (int i = 1; i < size; i++) {
            root = BUILDER.project(Assignments.of(), root);
        }

        List<PlanNodeId> rootToBottomIds = new ArrayList<>();
        PlanNode node = root;
        while (node instanceof ProjectNode) {
            rootToBottomIds.add(node.getId());
            node = ((ProjectNode) node).getSource();
        }

        List<PlanNodeId> findAllResult = PlanNodeSearcher.searchFrom(root)
                .where(ProjectNode.class::isInstance)
                .findAll()
                .stream()
                .map(PlanNode::getId)
                .collect(toImmutableList());

        assertEquals(rootToBottomIds, findAllResult);
    }

    @Test
    public void testFindAllMultipleSources()
    {
        List<JoinNode> joins = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            joins.add(BUILDER.join(INNER, BUILDER.values(), BUILDER.values()));
        }
        JoinNode leftSource = BUILDER.join(INNER, joins.get(0), joins.get(1));
        JoinNode rightSource = BUILDER.join(INNER, joins.get(2), joins.get(3));
        JoinNode root = BUILDER.join(INNER, leftSource, rightSource);

        ImmutableList.Builder<PlanNodeId> idsInPreOrder = ImmutableList.builder();
        joinNodePreorder(root, idsInPreOrder);

        List<PlanNodeId> findAllResult = PlanNodeSearcher.searchFrom(root)
                .where(JoinNode.class::isInstance)
                .findAll()
                .stream()
                .map(PlanNode::getId)
                .collect(toImmutableList());

        assertEquals(idsInPreOrder.build(), findAllResult);
    }

    /**
     * This method adds PlanNodeIds of JoinNodes to the builder in pre-order.
     * The plan tree must contain only JoinNodes and ValuesNodes.
     */
    private static void joinNodePreorder(PlanNode root, ImmutableList.Builder<PlanNodeId> builder)
    {
        if (root instanceof ValuesNode) {
            return;
        }

        if (root instanceof JoinNode join) {
            builder.add(root.getId());
            joinNodePreorder(join.getLeft(), builder);
            joinNodePreorder(join.getRight(), builder);
            return;
        }

        throw new IllegalArgumentException("unsupported node type: " + root.getClass().getSimpleName());
    }
}
