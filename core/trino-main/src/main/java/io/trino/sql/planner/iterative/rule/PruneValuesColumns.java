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
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Constant;
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
import static io.trino.spi.type.TypeUtils.readNativeValue;
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
        if (!valuesNode.getRows().get().stream().allMatch(row -> row instanceof Constant || row instanceof Row)) {
            return Optional.empty();
        }

        // for each output of project, the corresponding column in the values node
        int[] mapping = new int[newOutputs.size()];
        for (int i = 0; i < mapping.length; i++) {
            mapping[i] = valuesNode.getOutputSymbols().indexOf(newOutputs.get(i));
        }

        ImmutableList.Builder<Expression> rowsBuilder = ImmutableList.builder();
        for (Expression entry : valuesNode.getRows().get()) {
            switch (entry) {
                case Row row -> rowsBuilder.add(new Row(Arrays.stream(mapping)
                        .mapToObj(i -> row.getItems().get(i))
                        .collect(Collectors.toList())));
                case Constant row when row.getValue() instanceof SqlRow rowValue && row.getType() instanceof RowType type -> {
                    ImmutableList.Builder<Expression> newRow = ImmutableList.builder();
                    for (int i = 0; i < mapping.length; i++) {
                        Type fieldType = type.getFields().get(i).getType();
                        Object value = readNativeValue(fieldType, rowValue.getRawFieldBlock(i), rowValue.getRawIndex());
                        newRow.add(new Constant(fieldType, value));
                    }
                    rowsBuilder.add(new Row(newRow.build()));
                }
                default -> throw new IllegalStateException("Expected Row or Constant, but found: " + entry);
            }
        }

        return Optional.of(new ValuesNode(valuesNode.getId(), newOutputs, rowsBuilder.build()));
    }
}
