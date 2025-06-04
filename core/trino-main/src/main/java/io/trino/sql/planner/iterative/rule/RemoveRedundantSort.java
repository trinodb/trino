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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.Cardinality;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.ValuesNode;

import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static io.trino.sql.planner.plan.Patterns.sort;

public class RemoveRedundantSort
        implements Rule<SortNode>
{
    private static final Pattern<SortNode> PATTERN = sort();

    @Override
    public Pattern<SortNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(SortNode node, Captures captures, Context context)
    {
        Cardinality cardinality = extractCardinality(node.getSource(), context.getLookup());
        if (cardinality.isEmpty()) {
            return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols()));
        }
        if (cardinality.isScalar()) {
            return Result.ofPlanNode(node.getSource());
        }
        return Result.empty();
    }
}
