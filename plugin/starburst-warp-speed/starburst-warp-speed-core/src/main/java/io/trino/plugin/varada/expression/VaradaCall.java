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
package io.trino.plugin.varada.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

@JsonTypeName("call")
public class VaradaCall
        implements VaradaExpression
{
    private final String functionName;
    private final List<VaradaExpression> arguments;
    private final Type type;

    @JsonCreator
    public VaradaCall(@JsonProperty("functionName") String functionName,
            @JsonProperty("arguments") List<VaradaExpression> arguments,
            @JsonProperty("type") Type type)
    {
        this.functionName = requireNonNull(functionName);
        this.arguments = requireNonNull(arguments);
        this.type = requireNonNull(type);
    }

    @JsonProperty
    public String getFunctionName()
    {
        return functionName;
    }

    @JsonProperty
    public List<VaradaExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public List<? extends VaradaExpression> getChildren()
    {
        return arguments;
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return "VaradaCall{" +
                "functionName='" + functionName + '\'' +
                ", type=" + type +
                ", arguments=" + arguments +
                '}';
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
        VaradaCall varadaCall = (VaradaCall) o;
        return Objects.equals(functionName, varadaCall.getFunctionName()) &&
                Objects.equals(type, varadaCall.getType()) &&
                Objects.equals(arguments, varadaCall.getArguments());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, arguments, type);
    }
}
