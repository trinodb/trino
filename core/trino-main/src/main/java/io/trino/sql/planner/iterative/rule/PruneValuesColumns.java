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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.plan.Patterns.values;
import static io.trino.util.MoreLists.filteredCopy;

public class PruneValuesColumns
        extends ProjectOffPushDownRule<ValuesNode>
{
    public PruneValuesColumns()
    {
        super(values());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, ValuesNode valuesNode, Set<Symbol> referencedOutputs)
    {
        // no symbols to prune
        if (valuesNode.getOutputSymbols().isEmpty()) {
            return Optional.empty();
        }

        List<Symbol> newOutputs = filteredCopy(valuesNode.getOutputSymbols(), referencedOutputs::contains);

        // no output symbols left
        if (newOutputs.isEmpty()) {
            return Optional.of(new ValuesNode(valuesNode.getId(), valuesNode.getRowCount()));
        }

        checkState(valuesNode.getRows().isPresent(), "rows is empty");
        // if any of ValuesNode's rows is specified by expression other than Row, the redundant piece cannot be extracted and pruned
        if (!valuesNode.getRows().get().stream().allMatch(Row.class::isInstance)) {
            return Optional.empty();
        }

        // for each output of project, the corresponding column in the values node
        int[] mapping = new int[newOutputs.size()];
        for (int i = 0; i < mapping.length; i++) {
            mapping[i] = valuesNode.getOutputSymbols().indexOf(newOutputs.get(i));
        }

        ImmutableList.Builder<Expression> rowsBuilder = ImmutableList.builder();
        for (Expression row : valuesNode.getRows().get()) {
            rowsBuilder.add(new Row(Arrays.stream(mapping)
                    .mapToObj(i -> ((Row) row).items().get(i))
                    .collect(Collectors.toList())));
        }

        return Optional.of(new ValuesNode(valuesNode.getId(), newOutputs, rowsBuilder.build()));
    }
}
