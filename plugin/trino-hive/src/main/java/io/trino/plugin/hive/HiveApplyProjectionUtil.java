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
package io.trino.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class HiveApplyProjectionUtil
{
    private HiveApplyProjectionUtil() {}

    public static List<ConnectorExpression> extractSupportedProjectedColumns(ConnectorExpression expression)
    {
        requireNonNull(expression, "expression is null");
        ImmutableList.Builder<ConnectorExpression> supportedSubExpressions = ImmutableList.builder();
        fillSupportedProjectedColumns(expression, supportedSubExpressions);
        return supportedSubExpressions.build();
    }

    private static void fillSupportedProjectedColumns(ConnectorExpression expression, ImmutableList.Builder<ConnectorExpression> supportedSubExpressions)
    {
        if (isPushDownSupported(expression)) {
            supportedSubExpressions.add(expression);
            return;
        }

        // If the whole expression is not supported, look for a partially supported projection
        for (ConnectorExpression child : expression.getChildren()) {
            fillSupportedProjectedColumns(child, supportedSubExpressions);
        }
    }

    @VisibleForTesting
    static boolean isPushDownSupported(ConnectorExpression expression)
    {
        return expression instanceof Variable ||
                (expression instanceof FieldDereference fieldDereference && isPushDownSupported(fieldDereference.getTarget()));
    }

    public static ProjectedColumnRepresentation createProjectedColumnRepresentation(ConnectorExpression expression)
    {
        ImmutableList.Builder<Integer> ordinals = ImmutableList.builder();

        Variable target;
        while (true) {
            if (expression instanceof Variable variable) {
                target = variable;
                break;
            }
            if (expression instanceof FieldDereference dereference) {
                ordinals.add(dereference.getField());
                expression = dereference.getTarget();
            }
            else {
                throw new IllegalArgumentException("expression is not a valid dereference chain");
            }
        }

        return new ProjectedColumnRepresentation(target, ordinals.build().reverse());
    }

    /**
     * Replace all connector expressions with variables as given by {@param expressionToVariableMappings} in a top down manner.
     * i.e. if the replacement occurs for the parent, the children will not be visited.
     */
    public static ConnectorExpression replaceWithNewVariables(ConnectorExpression expression, Map<ConnectorExpression, Variable> expressionToVariableMappings)
    {
        if (expressionToVariableMappings.containsKey(expression)) {
            return expressionToVariableMappings.get(expression);
        }

        if (expression instanceof Constant || expression instanceof Variable) {
            return expression;
        }

        if (expression instanceof FieldDereference fieldDereference) {
            ConnectorExpression newTarget = replaceWithNewVariables(fieldDereference.getTarget(), expressionToVariableMappings);
            return new FieldDereference(expression.getType(), newTarget, fieldDereference.getField());
        }

        if (expression instanceof Call call) {
            return new Call(
                    call.getType(),
                    call.getFunctionName(),
                    call.getArguments().stream()
                            .map(argument -> replaceWithNewVariables(argument, expressionToVariableMappings))
                            .collect(toImmutableList()));
        }

        // We cannot skip processing for unsupported expression shapes. This may lead to variables being left in ProjectionApplicationResult
        // which are no longer bound.
        throw new UnsupportedOperationException("Unsupported expression: " + expression);
    }

    /**
     * Returns the assignment key corresponding to the column represented by {@param projectedColumn} in the {@param assignments}, if one exists.
     * The variable in the {@param projectedColumn} can itself be a representation of another projected column. For example,
     * say a projected column representation has variable "x" and a dereferenceIndices=[0]. "x" can in-turn map to a projected
     * column handle with base="a" and [1, 2] as dereference indices. Then the method searches for a column handle in
     * {@param assignments} with base="a" and dereferenceIndices=[1, 2, 0].
     */
    public static Optional<String> find(Map<String, ColumnHandle> assignments, ProjectedColumnRepresentation projectedColumn)
    {
        HiveColumnHandle variableColumn = (HiveColumnHandle) assignments.get(projectedColumn.getVariable().getName());

        if (variableColumn == null) {
            return Optional.empty();
        }

        String baseColumnName = variableColumn.getBaseColumnName();

        List<Integer> variableColumnIndices = variableColumn.getHiveColumnProjectionInfo()
                .map(HiveColumnProjectionInfo::getDereferenceIndices)
                .orElse(ImmutableList.of());

        List<Integer> projectionIndices = ImmutableList.<Integer>builder()
                .addAll(variableColumnIndices)
                .addAll(projectedColumn.getDereferenceIndices())
                .build();

        for (Map.Entry<String, ColumnHandle> entry : assignments.entrySet()) {
            HiveColumnHandle column = (HiveColumnHandle) entry.getValue();
            if (column.getBaseColumnName().equals(baseColumnName) &&
                    column.getHiveColumnProjectionInfo()
                            .map(HiveColumnProjectionInfo::getDereferenceIndices)
                            .orElse(ImmutableList.of())
                            .equals(projectionIndices)) {
                return Optional.of(entry.getKey());
            }
        }

        return Optional.empty();
    }

    public static class ProjectedColumnRepresentation
    {
        private final Variable variable;
        private final List<Integer> dereferenceIndices;

        public ProjectedColumnRepresentation(Variable variable, List<Integer> dereferenceIndices)
        {
            this.variable = requireNonNull(variable, "variable is null");
            this.dereferenceIndices = ImmutableList.copyOf(requireNonNull(dereferenceIndices, "dereferenceIndices is null"));
        }

        public Variable getVariable()
        {
            return variable;
        }

        public List<Integer> getDereferenceIndices()
        {
            return dereferenceIndices;
        }

        public boolean isVariable()
        {
            return dereferenceIndices.isEmpty();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            ProjectedColumnRepresentation that = (ProjectedColumnRepresentation) obj;
            return Objects.equals(variable, that.variable) &&
                    Objects.equals(dereferenceIndices, that.dereferenceIndices);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(variable, dereferenceIndices);
        }
    }
}
