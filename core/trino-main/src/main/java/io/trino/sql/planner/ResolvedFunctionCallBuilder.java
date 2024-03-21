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
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ResolvedFunctionCallBuilder
{
    private final ResolvedFunction resolvedFunction;
    private List<Expression> argumentValues = new ArrayList<>();

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

    public Call build()
    {
        return new Call(resolvedFunction, argumentValues);
    }
}
