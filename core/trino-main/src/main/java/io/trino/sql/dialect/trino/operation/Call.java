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
package io.trino.sql.dialect.trino.operation;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.Attributes.RESOLVED_FUNCTION;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Call
        extends TrinoOperation
{
    private static final String NAME = "call";

    private final Result result;
    private final List<Value> arguments;
    private final Map<AttributeKey, Object> attributes;

    public Call(String resultName, List<Value> arguments, ResolvedFunction function, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(function, "function is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (function.signature().getArgumentTypes().size() != arguments.size()) {
            throw new TrinoException(IR_ERROR, format("expected %s arguments, found: %s", function.signature().getArgumentTypes().size(), arguments.size()));
        }
        for (int i = 0; i < arguments.size(); i++) {
            Type expectedType = function.signature().getArgumentType(i);
            Type actualType = trinoType(arguments.get(i).type());
            if (!expectedType.equals(actualType)) {
                throw new TrinoException(IR_ERROR, format("invalid argument type. expected: %s, actual: %s", expectedType, actualType));
            }
        }

        this.result = new Result(resultName, irType(function.signature().getReturnType()));

        this.arguments = ImmutableList.copyOf(arguments);

        // TODO: derive attributes from source attributes; derive attributes from ResolvedFunction
        this.attributes = RESOLVED_FUNCTION.asMap(function);
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return arguments;
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of();
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "call :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newArguments = new ArrayList<>(arguments);
        newArguments.set(index, newArgument);
        return new Call(
                result.name(),
                newArguments,
                RESOLVED_FUNCTION.getAttribute(attributes),
                ImmutableList.of());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (Call) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.arguments, that.arguments) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, arguments, attributes);
    }
}
