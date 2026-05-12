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
package io.trino.spi.expression;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Constant.class, name = "constant"),
        @JsonSubTypes.Type(value = Variable.class, name = "variable"),
        @JsonSubTypes.Type(value = Call.class, name = "call"),
        @JsonSubTypes.Type(value = FieldDereference.class, name = "field_dereference"),
})
public abstract class ConnectorExpression
{
    private final Type type;

    public ConnectorExpression(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty("type")
    public Type getType()
    {
        return type;
    }

    @JsonIgnore
    public abstract List<? extends ConnectorExpression> getChildren();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();
}
