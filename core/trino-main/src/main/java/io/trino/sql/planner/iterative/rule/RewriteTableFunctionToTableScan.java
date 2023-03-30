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

import com.google.common.collect.ImmutableMap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.matching.Pattern.empty;
import static io.trino.sql.planner.plan.Patterns.sources;
import static io.trino.sql.planner.plan.Patterns.tableFunctionProcessor;
import static java.util.Objects.requireNonNull;

public class RewriteTableFunctionToTableScan
        implements Rule<TableFunctionProcessorNode>
{
    private static final Pattern<TableFunctionProcessorNode> PATTERN = tableFunctionProcessor()
            .with(empty(sources()));

    private final PlannerContext plannerContext;

    public RewriteTableFunctionToTableScan(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<TableFunctionProcessorNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFunctionProcessorNode node, Captures captures, Context context)
    {
        Optional<TableFunctionApplicationResult<TableHandle>> result = plannerContext.getMetadata().applyTableFunction(context.getSession(), node.getHandle());

        if (result.isEmpty()) {
            return Result.empty();
        }

        List<ColumnHandle> columnHandles = result.get().getColumnHandles();
        checkState(node.getOutputSymbols().size() == columnHandles.size(), "returned table does not match the node's output");
        ImmutableMap.Builder<Symbol, ColumnHandle> assignments = ImmutableMap.builder();
        for (int i = 0; i < columnHandles.size(); i++) {
            assignments.put(node.getOutputSymbols().get(i), columnHandles.get(i));
        }

        return Result.ofPlanNode(new TableScanNode(
                node.getId(),
                result.get().getTableHandle(),
                node.getOutputSymbols(),
                assignments.buildOrThrow(),
                TupleDomain.all(),
                Optional.empty(),
                false,
                Optional.empty()));
    }
}
