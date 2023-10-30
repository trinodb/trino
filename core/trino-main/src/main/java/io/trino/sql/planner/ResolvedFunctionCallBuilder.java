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
package io.trino.sql.planner;

import io.trino.metadata.ResolvedFunction;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Window;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ResolvedFunctionCallBuilder
{
    private final ResolvedFunction resolvedFunction;
    private List<Expression> argumentValues = new ArrayList<>();
    private Optional<NodeLocation> location = Optional.empty();
    private Optional<Window> window = Optional.empty();
    private Optional<Expression> filter = Optional.empty();
    private Optional<OrderBy> orderBy = Optional.empty();
    private boolean distinct;

    public static ResolvedFunctionCallBuilder builder(ResolvedFunction resolvedFunction)
    {
        return new ResolvedFunctionCallBuilder(resolvedFunction);
    }

    private ResolvedFunctionCallBuilder(ResolvedFunction resolvedFunction)
    {
        this.resolvedFunction = requireNonNull(resolvedFunction, "resolvedFunction is null");
    }

    public ResolvedFunctionCallBuilder addArgument(Expression value)
    {
        requireNonNull(value, "value is null");
        argumentValues.add(value);
        return this;
    }

    public ResolvedFunctionCallBuilder setArguments(List<Expression> values)
    {
        requireNonNull(values, "values is null");
        argumentValues = new ArrayList<>(values);
        return this;
    }

    public ResolvedFunctionCallBuilder setLocation(NodeLocation location)
    {
        this.location = Optional.of(requireNonNull(location, "location is null"));
        return this;
    }

    public ResolvedFunctionCallBuilder setWindow(Window window)
    {
        this.window = Optional.of(requireNonNull(window, "window is null"));
        return this;
    }

    public ResolvedFunctionCallBuilder setWindow(Optional<Window> window)
    {
        this.window = requireNonNull(window, "window is null");
        return this;
    }

    public ResolvedFunctionCallBuilder setFilter(Expression filter)
    {
        this.filter = Optional.of(requireNonNull(filter, "filter is null"));
        return this;
    }

    public ResolvedFunctionCallBuilder setFilter(Optional<Expression> filter)
    {
        this.filter = requireNonNull(filter, "filter is null");
        return this;
    }

    public ResolvedFunctionCallBuilder setOrderBy(OrderBy orderBy)
    {
        this.orderBy = Optional.of(requireNonNull(orderBy, "orderBy is null"));
        return this;
    }

    public ResolvedFunctionCallBuilder setOrderBy(Optional<OrderBy> orderBy)
    {
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        return this;
    }

    public ResolvedFunctionCallBuilder setDistinct(boolean distinct)
    {
        this.distinct = distinct;
        return this;
    }

    public FunctionCall build()
    {
        return new FunctionCall(
                location,
                resolvedFunction.toQualifiedName(),
                window,
                filter,
                orderBy,
                distinct,
                Optional.empty(),
                Optional.empty(),
                argumentValues);
    }
}
