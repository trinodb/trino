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
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class Filter
        extends Operation
{
    private static final String NAME = "filter";

    private final Result result;
    private final Value input;
    private final Region predicate;
    private final Map<AttributeKey, Object> attributes;

    public Filter(String resultName, Value input, Block predicate, Map<AttributeKey, Object> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the Filter operation must be of relation type");
        }

        this.result = new Result(resultName, input.type()); // derives output type: same as input type

        this.input = input;

        if (predicate.parameters().size() != 1 ||
                !trinoType(predicate.parameters().getFirst().type()).equals(relationRowType(trinoType(input.type()))) ||
                !trinoType(predicate.getReturnedType()).equals(BOOLEAN)) {
            throw new TrinoException(IR_ERROR, "invalid predicate for Filter operation");
        }
        this.predicate = singleBlockRegion(predicate);

        // TODO derive attributes from source attributes
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
        return ImmutableList.of(input);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(predicate);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "pretty filter";
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
        var that = (Filter) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.input, that.input) &&
                Objects.equals(this.predicate, that.predicate) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, input, predicate, attributes);
    }
}
