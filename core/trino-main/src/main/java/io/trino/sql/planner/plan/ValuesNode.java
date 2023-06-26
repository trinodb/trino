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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Row;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

@Immutable
public class ValuesNode
        extends PlanNode
{
    private final List<Symbol> outputSymbols;
    private final int rowCount;
    // If ValuesNode produces output symbols, each row in ValuesNode is represented by a single expression in `rows` list.
    // It can be an expression of type Row or any other expression that evaluates to RowType.
    // In case when output symbols are present but ValuesNode does not have any rows, `rows` is an Optional with empty list.
    // If ValuesNode does not produce any output symbols, `rows` is Optional.empty().
    private final Optional<List<Expression>> rows;

    /**
     * Constructor of ValuesNode with non-empty output symbols list
     */
    public ValuesNode(PlanNodeId id, List<Symbol> outputSymbols, List<Expression> rows)
    {
        this(id, outputSymbols, rows.size(), Optional.of(rows));
    }

    /**
     * Constructor of ValuesNode with empty output symbols list
     */
    public ValuesNode(PlanNodeId id, int rowCount)
    {
        this(id, ImmutableList.of(), rowCount, Optional.empty());
    }

    @JsonCreator
    public ValuesNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols,
            @JsonProperty("rowCount") int rowCount,
            @JsonProperty("rows") Optional<List<Expression>> rows)
    {
        super(id);
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputSymbols, "outputSymbols is null"));
        this.rowCount = rowCount;

        requireNonNull(rows, "rows is null");
        if (rows.isPresent()) {
            checkArgument(rowCount == rows.get().size(), "declared and actual row counts don't match: %s vs %s", rowCount, rows.get().size());

            // check row size consistency (only for rows specified as Row)
            List<Integer> rowSizes = rows.get().stream()
                    .map(row -> requireNonNull(row, "row is null"))
                    .filter(expression -> expression instanceof Row)
                    .map(expression -> ((Row) expression).getItems().size())
                    .distinct()
                    .collect(toImmutableList());
            checkState(rowSizes.size() <= 1, "mismatched rows. All rows must be the same size");

            // check if row size matches the number of output symbols (only for rows specified as Row)
            if (rowSizes.size() == 1) {
                checkState(getOnlyElement(rowSizes).equals(outputSymbols.size()), "row size doesn't match the number of output symbols: %s vs %s", getOnlyElement(rowSizes), outputSymbols.size());
            }
        }
        else {
            checkArgument(outputSymbols.size() == 0, "missing rows specification for Values with non-empty output symbols");
        }

        if (outputSymbols.size() == 0) {
            this.rows = Optional.empty();
        }
        else {
            this.rows = rows.map(ImmutableList::copyOf);
        }
    }

    @Override
    @JsonProperty
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty
    public int getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public Optional<List<Expression>> getRows()
    {
        return rows;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitValues(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }
}
