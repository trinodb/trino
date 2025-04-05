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
import io.trino.sql.dialect.trino.TrinoDialect;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static java.util.Objects.requireNonNull;

public final class Case
        extends TrinoOperation
{
    private static final String NAME = "case";

    private final Result result;
    private final List<Value> when;
    private final List<Value> then;
    private final Value defaultValue;
    private final Map<AttributeKey, Object> attributes;

    public Case(String resultName, List<Value> when, List<Value> then, Value defaultValue, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(when, "when is null");
        requireNonNull(then, "then is null");
        requireNonNull(defaultValue, "defaultValue is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (when.isEmpty()) {
            throw new TrinoException(IR_ERROR, "case operation must have at least one operand");
        }

        if (when.size() != then.size()) {
            throw new TrinoException(IR_ERROR, "operand and result lists do not match is size");
        }

        if (!when.stream()
                .map(Value::type)
                .map(TrinoDialect::trinoType)
                .allMatch(type -> type.equals(BOOLEAN))) {
            throw new TrinoException(IR_ERROR, "all operands must be of boolean type");
        }

        Type thenType = trinoType(then.getFirst().type());
        if (!then.stream()
                .map(Value::type)
                .map(TrinoDialect::trinoType)
                .allMatch(type -> type.equals(thenType))) {
            throw new TrinoException(IR_ERROR, "all results must be of the same type");
        }

        if (!trinoType(defaultValue.type()).equals(thenType)) {
            throw new TrinoException(IR_ERROR, "default result must be of the same type as other results");
        }

        this.result = new Result(resultName, irType(thenType));

        this.when = ImmutableList.copyOf(when);

        this.then = ImmutableList.copyOf(then);

        this.defaultValue = defaultValue;

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
                .addAll(when)
                .addAll(then)
                .add(defaultValue)
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
        return "case :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newWhen = new ArrayList<>(when);
        if (index < when.size()) {
            newWhen.set(index, newArgument);
        }
        List<Value> newThen = new ArrayList<>(then);
        if (index >= when.size() && index < when.size() + then.size()) {
            newThen.set(index - when.size(), newArgument);
        }
        return new Case(
                result.name(),
                newWhen,
                newThen,
                index == when.size() + then.size() ? newArgument : defaultValue,
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
        var that = (Case) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.when, that.when) &&
                Objects.equals(this.then, that.then) &&
                Objects.equals(this.defaultValue, that.defaultValue) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, when, then, defaultValue, attributes);
    }
}
