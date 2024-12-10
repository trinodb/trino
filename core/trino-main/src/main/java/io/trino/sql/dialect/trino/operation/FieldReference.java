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
import io.trino.spi.type.RowType;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.Attributes.FIELD_INDEX;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class FieldReference
        extends Operation
{
    private static final String NAME = "field_reference";

    private final Result result;
    private final Value base;
    private final Map<AttributeKey, Object> attributes;

    public FieldReference(String resultName, Value base, int fieldIndex, Map<AttributeKey, Object> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(base, "base is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (!(trinoType(base.type()) instanceof RowType baseRowType)) {
            throw new TrinoException(IR_ERROR, "input to the FieldReference operation must be of row type. actual: " + trinoType(base.type()).getDisplayName());
        }

        if (fieldIndex < 0 || fieldIndex >= baseRowType.getFields().size()) {
            throw new TrinoException(IR_ERROR, format("invalid field index: %s. expected value in range [0, %s]", fieldIndex, baseRowType.getFields().size() - 1));
        }

        this.result = new Result(resultName, irType(baseRowType.getFields().get(fieldIndex).getType()));

        this.base = base;

        // TODO add source attributes for the selected field
        this.attributes = FIELD_INDEX.asMap(fieldIndex);
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of(base);
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
        return "pretty field reference";
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
        var that = (FieldReference) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.base, that.base) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, base, attributes);
    }
}
