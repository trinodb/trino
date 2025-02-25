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
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static java.util.Objects.requireNonNull;

public final class Coalesce
        extends TrinoOperation
{
    private static final String NAME = "coalesce";

    private final Result result;
    private final List<Value> operands;
    private final Map<AttributeKey, Object> attributes;

    public Coalesce(String resultName, List<Value> operands, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(operands, "operands is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (operands.size() < 2) {
            throw new TrinoException(IR_ERROR, "coalesce operation must have at least two operands");
        }

        Type resultType = trinoType(operands.getFirst().type());
        if (!operands.stream()
                .map(Value::type)
                .map(TrinoDialect::trinoType)
                .allMatch(type -> type.equals(resultType))) {
            throw new TrinoException(IR_ERROR, "all operands must be of the same type");
        }

        this.result = new Result(resultName, irType(resultType));

        this.operands = ImmutableList.copyOf(operands);

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
        return operands;
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
        return "coalesce :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newOperands = new ArrayList<>(operands);
        newOperands.set(index, newArgument);
        return new Coalesce(
                result.name(),
                newOperands,
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
        var that = (Coalesce) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.operands, that.operands) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, operands, attributes);
    }
}
