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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Pivot
        extends Relation
{
    private final Relation input;
    private final List<PivotAggregation> aggregations;
    private final List<Expression> pivotColumns;
    private final List<PivotValueGroup> valueGroups;
    private final Optional<GroupBy> groupBy;

    public Pivot(
            NodeLocation location,
            Relation input,
            List<PivotAggregation> aggregations,
            List<Expression> pivotColumns,
            List<PivotValueGroup> valueGroups,
            Optional<GroupBy> groupBy)
    {
        super(Optional.of(location));
        this.input = requireNonNull(input, "input is null");
        this.aggregations = ImmutableList.copyOf(aggregations);
        checkArgument(!this.aggregations.isEmpty(), "aggregations is empty");
        this.pivotColumns = ImmutableList.copyOf(pivotColumns);
        checkArgument(!this.pivotColumns.isEmpty(), "pivotColumns is empty");
        this.valueGroups = ImmutableList.copyOf(valueGroups);
        checkArgument(!this.valueGroups.isEmpty(), "valueGroups is empty");
        this.groupBy = requireNonNull(groupBy, "groupBy is null");
    }

    public Relation getInput()
    {
        return input;
    }

    public List<PivotAggregation> getAggregations()
    {
        return aggregations;
    }

    public List<Expression> getPivotColumns()
    {
        return pivotColumns;
    }

    public List<PivotValueGroup> getValueGroups()
    {
        return valueGroups;
    }

    public Optional<GroupBy> getGroupBy()
    {
        return groupBy;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPivot(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(input)
                .addAll(aggregations)
                .addAll(pivotColumns)
                .addAll(valueGroups)
                .addAll(groupBy.stream().toList())
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("input", input)
                .add("aggregations", aggregations)
                .add("pivotColumns", pivotColumns)
                .add("valueGroups", valueGroups)
                .add("groupBy", groupBy.orElse(null))
                .omitNullValues()
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pivot pivot = (Pivot) o;
        return Objects.equals(input, pivot.input) &&
                Objects.equals(aggregations, pivot.aggregations) &&
                Objects.equals(pivotColumns, pivot.pivotColumns) &&
                Objects.equals(valueGroups, pivot.valueGroups) &&
                Objects.equals(groupBy, pivot.groupBy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(input, aggregations, pivotColumns, valueGroups, groupBy);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
