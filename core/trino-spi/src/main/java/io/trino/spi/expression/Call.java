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

import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Call
        extends ConnectorExpression
{
    private final String name;
    private final List<ConnectorExpression> arguments;

    public Call(
            Type type,
            String name,
            List<ConnectorExpression> arguments)
    {
        super(type);
        this.name = name;
        this.arguments = new ArrayList<>(arguments);
    }

    public String getName()
    {
        return name;
    }

    public List<ConnectorExpression> getArguments()
    {
        return new ArrayList<>(arguments);
    }

    @Override
    protected <R, C> R accept(ConnectorExpressionVisitor<R, C> connectorExpressionVisitor, C context)
    {
        return connectorExpressionVisitor.visitCall(this, context);
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return new ArrayList<>(arguments);
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
        return Objects.equals(name, call.name) &&
                Objects.equals(arguments, call.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, arguments);
    }

    @Override
    public String toString()
    {
        return "Function{" +
                "name='" + name + '\'' +
                ", arguments=" + arguments +
                "}";
    }
}
