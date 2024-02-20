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

import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SpatialJoinNode;

public class BuildSideJoinPlanVisitor<C>
        extends SimplePlanVisitor<C>
{
    @Override
    public Void visitJoin(JoinNode node, C context)
    {
        node.getRight().accept(this, context);
        node.getLeft().accept(this, context);
        return null;
    }

    @Override
    public Void visitSemiJoin(SemiJoinNode node, C context)
    {
        node.getFilteringSource().accept(this, context);
        node.getSource().accept(this, context);
        return null;
    }

    @Override
    public Void visitSpatialJoin(SpatialJoinNode node, C context)
    {
        node.getRight().accept(this, context);
        node.getLeft().accept(this, context);
        return null;
    }

    @Override
    public Void visitIndexJoin(IndexJoinNode node, C context)
    {
        node.getIndexSource().accept(this, context);
        node.getProbeSource().accept(this, context);
        return null;
    }
}
