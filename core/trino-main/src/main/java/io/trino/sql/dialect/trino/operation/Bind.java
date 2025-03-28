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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.type.FunctionType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Bind
        extends TrinoOperation
{
    private static final String NAME = "bind";

    private final Result result;
    private final List<Value> values;
    private final Value lambda;
    private final Map<AttributeKey, Object> attributes;

    public Bind(String resultName, List<Value> values, Value lambda, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(values, "values is null");
        requireNonNull(lambda, "lambda is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        List<Type> lambdaArgumentTypes = ((FunctionType) trinoType(lambda.type())).getArgumentTypes();
        if (values.size() > lambdaArgumentTypes.size()) {
            throw new TrinoException(IR_ERROR, format("the number of bind arguments: %s exceeds the number of lambda arguments: %s", values.size(), lambdaArgumentTypes.size()));
        }
        for (int i = 0; i < values.size(); i++) {
            if (!trinoType(values.get(i).type()).equals(lambdaArgumentTypes.get(i))) {
                throw new TrinoException(IR_ERROR, format("bind argument %s has mismatching type: %s. expected: %s", i, trinoType(values.get(i).type()), lambdaArgumentTypes.get(i)));
            }
        }
        Type resultType = new FunctionType(lambdaArgumentTypes.subList(values.size(), lambdaArgumentTypes.size()), ((FunctionType) trinoType(lambda.type())).getReturnType());

        this.result = new Result(resultName, irType(resultType));

        this.values = ImmutableList.copyOf(values);

        this.lambda = lambda;

        // TODO
        this.attributes = ImmutableMap.of();
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.<Value>builder()
                .addAll(values)
                .add(lambda)
                .build();
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
        return "bind :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newValues = new ArrayList<>(values);
        if (index < values.size()) {
            newValues.set(index, newArgument);
        }
        return new Bind(
                result.name(),
                newValues,
                index == values.size() ? newArgument : lambda,
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
        var that = (Bind) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.values, that.values) &&
                Objects.equals(this.lambda, that.lambda) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, values, lambda, attributes);
    }
}
