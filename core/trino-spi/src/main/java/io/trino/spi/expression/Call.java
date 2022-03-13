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
package io.trino.spi.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public final class Call
        extends ConnectorExpression
{
    private final FunctionName functionName;
    private final List<ConnectorExpression> arguments;

    @JsonCreator
    public Call(
            @JsonProperty("type") Type type,
            @JsonProperty("functionName") FunctionName functionName,
            @JsonProperty("column") List<ConnectorExpression> arguments)
    {
        super(type);
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.arguments = List.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    @JsonProperty
    public FunctionName getFunctionName()
    {
        return functionName;
    }

    @JsonProperty
    public List<ConnectorExpression> getArguments()
    {
        return arguments;
    }

    @Override
    @JsonIgnore
    public List<? extends ConnectorExpression> getChildren()
    {
        return arguments;
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
        Call call = (Call) o;
        return Objects.equals(functionName, call.functionName) &&
                Objects.equals(arguments, call.arguments) &&
                Objects.equals(getType(), call.getType());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, arguments, getType());
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", Call.class.getSimpleName() + "[", "]");
        return stringJoiner
                .add("functionName=" + functionName)
                .add("arguments=" + arguments)
                .toString();
    }
}
