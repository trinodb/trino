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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.Set;

import static io.trino.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.trino.sql.planner.plan.Patterns.limit;

public class PruneLimitColumns
        extends ProjectOffPushDownRule<LimitNode>
{
    public PruneLimitColumns()
    {
        super(limit());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, LimitNode limitNode, Set<Symbol> referencedOutputs)
    {
        Set<Symbol> prunedLimitInputs = ImmutableSet.<Symbol>builder()
                .addAll(referencedOutputs)
                .addAll(limitNode.getTiesResolvingScheme()
                        .map(OrderingScheme::getOrderBy)
                        .orElse(ImmutableList.of()))
                .addAll(limitNode.getPreSortedInputs())
                .build();

        return restrictChildOutputs(context.getIdAllocator(), limitNode, prunedLimitInputs);
    }
}
