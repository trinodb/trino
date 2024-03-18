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
package io.trino.plugin.varada.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

@JsonTypeName("variable")
public class VaradaVariable
        implements VaradaExpression
{
    private final ColumnHandle columnHandle;
    private final Type type;

    @JsonCreator
    public VaradaVariable(@JsonProperty("columnHandle") ColumnHandle columnHandle, @JsonProperty("type") Type type)
    {
        this.columnHandle = requireNonNull(columnHandle);
        this.type = requireNonNull(type);
    }

    @JsonProperty
    public ColumnHandle getColumnHandle()
    {
        return columnHandle;
    }

    @Override
    public List<? extends VaradaExpression> getChildren()
    {
        return Collections.emptyList();
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return "VaradaVariable{" +
                "columnHandle=" + columnHandle +
                ", type=" + type +
                '}';
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
        VaradaVariable varadaVariable = (VaradaVariable) o;
        return Objects.equals(getColumnHandle(), varadaVariable.getColumnHandle()) &&
                Objects.equals(getType(), varadaVariable.getType());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnHandle, type);
    }
}
