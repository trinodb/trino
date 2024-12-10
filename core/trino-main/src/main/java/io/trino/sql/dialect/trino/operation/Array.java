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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.VoidType.VOID;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Array
        implements Operation
{
    private static final String NAME = "array";

    private final Result result;
    private final List<Value> elements;
    private final Map<String, Object> attributes;

    public Array(String resultName, Type elementType, List<Value> elements, List<Map<String, Object>> sourceAttributes)
    {
        requireNonNull(resultName, "resultName is null");
        requireNonNull(elementType, "elementType is null");
        requireNonNull(elements, "elements is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (elementType.equals(VOID)) {
            throw new TrinoException(IR_ERROR, "cannot use void type for array elements");
        }

        this.result = new Result(resultName, new ArrayType(elementType));

        elements.stream()
                .forEach(element -> {
                    if (!element.type().equals(elementType)) {
                        throw new TrinoException(IR_ERROR, format("type of array element: %s does not match the declared type: %s", element.type().getDisplayName(), elementType.getDisplayName()));
                    }
                });
        this.elements = ImmutableList.copyOf(elements);

        // TODO derive attributes from source attributes
        this.attributes = ImmutableMap.of();
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return elements;
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of();
    }

    @Override
    public Map<String, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel)
    {
        return "array :)";
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {return true;}
        if (obj == null || obj.getClass() != this.getClass()) {return false;}
        var that = (Array) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.elements, that.elements) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, elements, attributes);
    }
}
