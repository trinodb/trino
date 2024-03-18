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
package io.trino.plugin.varada.dispatcher.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.varada.expression.TransformFunction;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class TransformedColumn
        extends RegularColumn
{
    private static final String TRANSFORM_FUNCTION = "transformFunction";
    private final TransformFunction transformFunction;

    @JsonCreator
    public TransformedColumn(@JsonProperty(COLUMN_NAME) String columnName,
            @JsonProperty(COLUMN_ID) String columnId,
            @JsonProperty(TRANSFORM_FUNCTION) TransformFunction transformFunction)
    {
        super(columnName, columnId);
        this.transformFunction = requireNonNull(transformFunction);
    }

    @JsonProperty(TRANSFORM_FUNCTION)
    public TransformFunction getTransformFunction()
    {
        return transformFunction;
    }

    @Override
    public int getOrder()
    {
        return 2;
    }

    @Override
    public boolean isTransformedColumn()
    {
        return true;
    }

    @JsonIgnore
    public String getTransformedName()
    {
        StringBuilder transformedName = new StringBuilder(getName()).append(".").append(transformFunction.transformType());
        transformFunction.transformParams().forEach(param -> transformedName.append(".").append(param.getValueAsString()));
        return transformedName.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TransformedColumn transformedColumn = (TransformedColumn) o;
        return Objects.equals(transformFunction, transformedColumn.transformFunction);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), transformFunction);
    }

    @Override
    public String toString()
    {
        return "TransformedColumn{" +
                "columnName='" + getName() + '\'' +
                ", columnId='" + getColumnId() + '\'' +
                ", transformFunction=" + transformFunction +
                '}';
    }
}
