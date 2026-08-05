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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.StringJoiner;

import static io.trino.spi.connector.Preconditions.checkArgument;
import static io.trino.spi.expression.Constant.TRUE;
import static java.util.Objects.requireNonNull;

public class Constraint
{
    private static final Constraint ALWAYS_TRUE = new Constraint(TupleDomain.all());
    private static final Constraint ALWAYS_FALSE = new Constraint(TupleDomain.none());

    private final TupleDomain<ColumnHandle> summary;
    private final ConnectorExpression expression;
    private final Map<String, ColumnHandle> assignments;

    @JsonCreator
    public Constraint(
            @JsonProperty("summary") TupleDomain<ColumnHandle> summary,
            @JsonProperty("expression") ConnectorExpression expression,
            @JsonProperty("assignments") Map<String, ColumnHandle> assignments)
    {
        this.summary = requireNonNull(summary, "summary is null");
        this.expression = requireNonNull(expression, "expression is null");
        this.assignments = Map.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(allVariablesCovered(expression, this.assignments), "assignments must cover all variables in expression");
    }

    public static Constraint alwaysTrue()
    {
        return ALWAYS_TRUE;
    }

    public static Constraint alwaysFalse()
    {
        return ALWAYS_FALSE;
    }

    public Constraint(TupleDomain<ColumnHandle> summary)
    {
        this(summary, TRUE, Map.of());
    }

    /**
     * @return a predicate which is equivalent to, or looser than {@link #getExpression}, and should be AND-ed with, {@link #getExpression}.
     */
    @JsonProperty("summary")
    public TupleDomain<ColumnHandle> getSummary()
    {
        return summary;
    }

    /**
     * May include an engine-internal {@code $engine_expression} conjunct that connectors should treat as opaque.
     *
     * @return an expression predicate which is different from, and should be AND-ed with, {@link #getSummary}.
     */
    @JsonProperty("expression")
    public ConnectorExpression getExpression()
    {
        return expression;
    }

    /**
     * @return mappings from variable names to column handles for all conjuncts of {@link #getExpression},
     *         including variables appearing in any {@code $engine_expression} conjunct.
     */
    @JsonProperty("assignments")
    public Map<String, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", Constraint.class.getSimpleName() + "[", "]");
        stringJoiner.add("summary=" + summary);
        stringJoiner.add("expression=" + expression);
        return stringJoiner.toString();
    }

    private static boolean allVariablesCovered(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        Deque<ConnectorExpression> stack = new ArrayDeque<>();
        stack.push(expression);
        while (!stack.isEmpty()) {
            ConnectorExpression current = stack.pop();
            if (current instanceof Variable variable && !assignments.containsKey(variable.getName())) {
                return false;
            }
            stack.addAll(current.getChildren());
        }
        return true;
    }
}
