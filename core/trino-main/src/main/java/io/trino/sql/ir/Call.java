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
package io.trino.sql.ir;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.sql.ir.IrUtils.validateType;

@JsonSerialize
public record Call(ResolvedFunction function, List<Expression> arguments)
        implements Expression
{
    public Call
    {
        arguments = ImmutableList.copyOf(arguments);

        checkArgument(function.signature().getArgumentTypes().size() == arguments.size(), "Expected %s arguments, found: %s", function.signature().getArgumentTypes().size(), arguments.size());
        for (int i = 0; i < arguments.size(); i++) {
            validateType(function.signature().getArgumentType(i), arguments.get(i));
        }
    }

    @Override
    public Type type()
    {
        return function.signature().getReturnType();
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitCall(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return "%s(%s)".formatted(
                isBuiltinFunctionName(function.name()) ? function.name().getFunctionName() : function.name(),
                arguments.stream()
                        .map(Expression::toString)
                        .collect(Collectors.joining(", ")));
    }
}
