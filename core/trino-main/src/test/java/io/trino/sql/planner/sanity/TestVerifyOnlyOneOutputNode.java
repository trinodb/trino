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
package io.trino.sql.planner.sanity;

import com.google.common.collect.ImmutableList;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestVerifyOnlyOneOutputNode
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    @Test
    public void testValidateSuccessful()
    {
        // random seemingly valid plan
        PlanNode root =
                new OutputNode(idAllocator.getNextId(),
                        new ProjectNode(idAllocator.getNextId(),
                                new ValuesNode(
                                        idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of()),
                                Assignments.of()
                        ), ImmutableList.of(), ImmutableList.of());
        new VerifyOnlyOneOutputNode().validate(root, null, PLANNER_CONTEXT, null, null, WarningCollector.NOOP);
    }

    @Test
    public void testValidateFailed()
    {
        // random plan with 2 output nodes
        PlanNode root =
                new OutputNode(idAllocator.getNextId(),
                        new ExplainAnalyzeNode(idAllocator.getNextId(),
                                new OutputNode(idAllocator.getNextId(),
                                        new ProjectNode(idAllocator.getNextId(),
                                                new ValuesNode(
                                                        idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of()),
                                                Assignments.of()
                                        ), ImmutableList.of(), ImmutableList.of()
                                ), new Symbol("a"),
                                ImmutableList.of(),
                                false),
                        ImmutableList.of(), ImmutableList.of());
        assertThatThrownBy(() -> new VerifyOnlyOneOutputNode().validate(root, null, PLANNER_CONTEXT, null, null, WarningCollector.NOOP))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Expected plan to have single instance of OutputNode");
    }
}
