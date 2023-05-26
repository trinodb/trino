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

package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class SchedulingOrderVisitor
{
    public static List<PlanNodeId> scheduleOrder(PlanNode root)
    {
        ImmutableList.Builder<PlanNodeId> schedulingOrder = ImmutableList.builder();
        root.accept(new Visitor(schedulingOrder::add), null);
        return schedulingOrder.build();
    }

    private SchedulingOrderVisitor() {}

    private static class Visitor
            extends BuildSideJoinPlanVisitor<Void>
    {
        private final Consumer<PlanNodeId> schedulingOrder;

        public Visitor(Consumer<PlanNodeId> schedulingOrder)
        {
            this.schedulingOrder = requireNonNull(schedulingOrder, "schedulingOrder is null");
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            schedulingOrder.accept(node.getId());
            return null;
        }

        @Override
        public Void visitTableFunctionProcessor(TableFunctionProcessorNode node, Void context)
        {
            if (node.getSource().isEmpty()) {
                schedulingOrder.accept(node.getId());
            }
            else {
                node.getSource().orElseThrow().accept(this, context);
            }
            return null;
        }
    }
}
