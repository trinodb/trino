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
package io.trino.plugin.base.projection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class ApplyProjectionUtil
{
    private ApplyProjectionUtil() {}

    public static List<ConnectorExpression> extractSupportedProjectedColumns(ConnectorExpression expression)
    {
        return extractSupportedProjectedColumns(expression, connectorExpression -> true);
    }

    public static List<ConnectorExpression> extractSupportedProjectedColumns(ConnectorExpression expression, Predicate<ConnectorExpression> expressionPredicate)
    {
        requireNonNull(expression, "expression is null");
        ImmutableList.Builder<ConnectorExpression> supportedSubExpressions = ImmutableList.builder();
        fillSupportedProjectedColumns(expression, supportedSubExpressions, expressionPredicate);
        return supportedSubExpressions.build();
    }

    private static void fillSupportedProjectedColumns(ConnectorExpression expression, ImmutableList.Builder<ConnectorExpression> supportedSubExpressions, Predicate<ConnectorExpression> expressionPredicate)
    {
        if (isPushdownSupported(expression, expressionPredicate)) {
            supportedSubExpressions.add(expression);
            return;
        }

        // If the whole expression is not supported, look for a partially supported projection
        for (ConnectorExpression child : expression.getChildren()) {
            fillSupportedProjectedColumns(child, supportedSubExpressions, expressionPredicate);
        }
    }

    @VisibleForTesting
    static boolean isPushdownSupported(ConnectorExpression expression, Predicate<ConnectorExpression> expressionPredicate)
    {
        return expressionPredicate.test(expression)
                && (expression instanceof Variable ||
                    (expression instanceof FieldDereference fieldDereference
                            && isPushdownSupported(fieldDereference.getTarget(), expressionPredicate)));
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
