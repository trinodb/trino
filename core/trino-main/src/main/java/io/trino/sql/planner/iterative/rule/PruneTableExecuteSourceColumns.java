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

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.TableExecuteNode;

import static io.trino.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.trino.sql.planner.plan.Patterns.tableExecute;

public class PruneTableExecuteSourceColumns
        implements Rule<TableExecuteNode>
{
    private static final Pattern<TableExecuteNode> PATTERN = tableExecute();

    @Override
    public Pattern<TableExecuteNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableExecuteNode tableExecuteNode, Captures captures, Context context)
    {
        ImmutableSet.Builder<Symbol> requiredInputs = ImmutableSet.<Symbol>builder()
                .addAll(tableExecuteNode.getColumns());

        if (tableExecuteNode.getPartitioningScheme().isPresent()) {
            PartitioningScheme partitioningScheme = tableExecuteNode.getPartitioningScheme().get();
            partitioningScheme.getPartitioning().getColumns().forEach(requiredInputs::add);
            partitioningScheme.getHashColumn().ifPresent(requiredInputs::add);
        }

        return restrictChildOutputs(context.getIdAllocator(), tableExecuteNode, requiredInputs.build())
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
