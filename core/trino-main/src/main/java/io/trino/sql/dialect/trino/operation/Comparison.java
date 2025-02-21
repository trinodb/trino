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
import io.trino.spi.TrinoException;
import io.trino.sql.dialect.trino.Attributes.ComparisonOperator;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.Attributes.COMPARISON_OPERATOR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Comparison
        extends Operation
{
    private static final String NAME = "comparison";

    private final Result result;
    private final Value left;
    private final Value right;
    private final Map<AttributeKey, Object> attributes;

    public Comparison(String resultName, Value left, Value right, ComparisonOperator comparisonOperator, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(comparisonOperator, "comparisonOperator is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        this.result = new Result(resultName, irType(BOOLEAN));

        if (!trinoType(left.type()).equals(trinoType(right.type()))) {
            throw new TrinoException(IR_ERROR, format("comparison operands must be of the same type. found: %s, %s", trinoType(left.type()).getDisplayName(), trinoType(right.type()).getDisplayName()));
        }

        this.left = left;

        this.right = right;

        // TODO also derive attributes from source attributes
        this.attributes = COMPARISON_OPERATOR.asMap(comparisonOperator);
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of(left, right);
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
        return "comparison :)";
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
        var that = (Comparison) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.left, that.left) &&
                Objects.equals(this.right, that.right) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, left, right, attributes);
    }
}
