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

public final class JsonTable
        extends Relation
{
    private final JsonPathInvocation jsonPathInvocation;
    private final List<JsonTableColumnDefinition> columns;
    private final Optional<JsonTablePlan> plan;
    private final Optional<ErrorBehavior> errorBehavior;

    public JsonTable(NodeLocation location, JsonPathInvocation jsonPathInvocation, List<JsonTableColumnDefinition> columns, Optional<JsonTablePlan> plan, Optional<ErrorBehavior> errorBehavior)
    {
        super(Optional.of(location));
        this.jsonPathInvocation = requireNonNull(jsonPathInvocation, "jsonPathInvocation is null");
        this.columns = ImmutableList.copyOf(columns);
        checkArgument(!columns.isEmpty(), "columns is empty");
        this.plan = requireNonNull(plan, "plan is null");
        this.errorBehavior = requireNonNull(errorBehavior, "errorBehavior is null");
    }

    public enum ErrorBehavior
    {
        ERROR,
        EMPTY
    }

    public JsonPathInvocation getJsonPathInvocation()
    {
        return jsonPathInvocation;
    }

    public List<JsonTableColumnDefinition> getColumns()
    {
        return columns;
    }

    public Optional<JsonTablePlan> getPlan()
    {
        return plan;
    }

    public Optional<ErrorBehavior> getErrorBehavior()
    {
        return errorBehavior;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonTable(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        children.add(jsonPathInvocation);
        children.addAll(columns);
        plan.ifPresent(children::add);
        return children.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("jsonPathInvocation", jsonPathInvocation)
                .add("columns", columns)
                .add("plan", plan.orElse(null))
                .add("errorBehavior", errorBehavior.orElse(null))
                .omitNullValues()
                .toString();
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
        JsonTable o = (JsonTable) obj;
        return Objects.equals(jsonPathInvocation, o.jsonPathInvocation) &&
                Objects.equals(columns, o.columns) &&
                Objects.equals(plan, o.plan) &&
                Objects.equals(errorBehavior, o.errorBehavior);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jsonPathInvocation, columns, plan, errorBehavior);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        JsonTable otherNode = (JsonTable) other;
        return Objects.equals(errorBehavior, otherNode.errorBehavior);
    }
}
