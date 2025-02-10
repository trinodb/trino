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
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Between
        extends Operation
{
    private static final String NAME = "between";

    private final Result result;
    private final Value input;
    private final Value min;
    private final Value max;
    private final Map<AttributeKey, Object> attributes;

    public Between(String resultName, Value input, Value min, Value max, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(min, "min is null");
        requireNonNull(max, "max is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        this.result = new Result(resultName, irType(BOOLEAN));

        if (!trinoType(input.type()).equals(trinoType(min.type()))) {
            throw new TrinoException(IR_ERROR, format("lower range of between operation has mismatching type. expected: %s, actual: %s", trinoType(input.type()).getDisplayName(), trinoType(min.type()).getDisplayName()));
        }
        if (!trinoType(input.type()).equals(trinoType(max.type()))) {
            throw new TrinoException(IR_ERROR, format("upper range of between operation has mismatching type. expected: %s, actual: %s", trinoType(input.type()).getDisplayName(), trinoType(max.type()).getDisplayName()));
        }

        this.input = input;
        this.min = min;
        this.max = max;

        // TODO derive attributes
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
        return ImmutableList.of(input, min, max);
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
        return "pretty between";
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
        var that = (Between) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.input, that.input) &&
                Objects.equals(this.min, that.min) &&
                Objects.equals(this.max, that.max) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, input, min, max, attributes);
    }
}
