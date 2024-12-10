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
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.TrinoDialect;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static java.util.Objects.requireNonNull;

public final class Row
        extends Operation
{
    private static final String NAME = "row";

    private final Result result;
    private final List<Value> fields;
    private final Map<AttributeKey, Object> attributes;

    public Row(String resultName, List<Value> fields, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(fields, "fields is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        // fails if there are no fields
        Type resultType = RowType.anonymous(
                fields.stream()
                        .map(Value::type)
                        .map(TrinoDialect::trinoType)
                        .collect(toImmutableList()));

        this.result = new Result(resultName, irType(resultType));

        this.fields = ImmutableList.copyOf(fields);

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
        return fields;
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
        return "row :)";
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
        var that = (Row) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.fields, that.fields) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, fields, attributes);
    }
}
