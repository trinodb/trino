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

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract sealed class Node
        permits CallArgument,
                CaseStatementWhenClause,
                ControlStatement,
                Corresponding,
                DataTypeParameter,
                Descriptor,
                DescriptorField,
                ElseClause,
                ElseIfClause,
                EmptyTableTreatment,
                ExplainOption,
                Expression,
                FetchFirst,
                FrameBound,
                FunctionSpecification,
                GrantObject,
                GroupBy,
                GroupingElement,
                IntervalQualifier,
                JsonArrayElement,
                JsonObjectMember,
                JsonPathInvocation,
                JsonPathParameter,
                JsonTableColumnDefinition,
                JsonTablePlan,
                Limit,
                MeasureDefinition,
                MergeCase,
                Offset,
                OrderBy,
                ParameterDeclaration,
                PathElement,
                PathSpecification,
                PatternQuantifier,
                PatternSearchMode,
                PivotAggregation,
                PivotValueGroup,
                Predicate,
                ProcessingMode,
                Property,
                QueryPeriod,
                Relation,
                ReturnsClause,
                RoutineCharacteristic,
                Row.Field,
                RowDataType.Field,
                RowPattern,
                Select,
                SelectItem,
                SessionProperty,
                SkipTo,
                SortItem,
                Statement,
                SubsetDefinition,
                TableElement,
                TableFunctionArgument,
                TableFunctionDescriptorArgument,
                TableFunctionTableArgument,
                TransactionMode,
                UpdateAssignment,
                VariableDefinition,
                WindowDefinition,
                WindowFrame,
                WindowReference,
                WindowSpecification,
                With,
                WithQuery
{
    private final Optional<NodeLocation> location;

    @Deprecated
    protected Node(Optional<NodeLocation> location)
    {
        this.location = requireNonNull(location, "location is null");
    }

    protected Node(NodeLocation location)
    {
        this.location = Optional.ofNullable(location);
    }

    public Optional<NodeLocation> getLocation()
    {
        return location;
    }

    public abstract List<? extends Node> getChildren();

    // Force subclasses to have a proper equals and hashcode implementation
    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();

    /**
     * Compare with another node by considering internal state excluding any Node returned by getChildren()
     */
    public boolean shallowEquals(Node other)
    {
        throw new UnsupportedOperationException("not yet implemented: " + getClass().getName());
    }

    static boolean sameClass(Node left, Node right)
    {
        if (left == right) {
            return true;
        }

        return left.getClass() == right.getClass();
    }
}
