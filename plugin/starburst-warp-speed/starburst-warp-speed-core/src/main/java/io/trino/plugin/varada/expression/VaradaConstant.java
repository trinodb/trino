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
import io.trino.spi.type.Type;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public abstract class VaradaConstant
        implements VaradaExpression
{
    private final Type type;

    @JsonCreator
    public VaradaConstant(@JsonProperty("type") Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public abstract Object getValue();

    public abstract String getValueAsString();

    @Override
    public List<? extends VaradaExpression> getChildren()
    {
        return Collections.emptyList();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getValue(), type);
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
        VaradaConstant that = (VaradaConstant) o;
        return Objects.equals(getValueAsString(), that.getValueAsString()) &&
                Objects.equals(type, that.getType());
    }

    @Override
    public String toString()
    {
        return getValueAsString() + "::" + getType();
    }
}
