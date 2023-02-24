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
package io.trino.spi.connector;

import io.trino.spi.type.Type;

import java.util.Objects;

import static io.trino.spi.connector.SchemaUtil.checkNotEmpty;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class ColumnSchema
{
    private final String name;
    private final Type type;
    private final boolean hidden;

    private ColumnSchema(String name, Type type, boolean hidden)
    {
        checkNotEmpty(name, "name");
        requireNonNull(type, "type is null");

        this.name = name.toLowerCase(ENGLISH);
        this.type = type;
        this.hidden = hidden;
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public boolean isHidden()
    {
        return hidden;
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
        ColumnSchema that = (ColumnSchema) o;
        return hidden == that.hidden
                && name.equals(that.name)
                && type.equals(that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, hidden);
    }

    @Override
    public String toString()
    {
        return new StringBuilder("ColumnSchema{")
                .append("name='").append(name).append('\'')
                .append(", type=").append(type)
                .append(", hidden=").append(hidden)
                .append('}')
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(ColumnMetadata columnMetadata)
    {
        return new Builder(columnMetadata);
    }

    public static class Builder
    {
        private String name;
        private Type type;
        private boolean hidden;

        private Builder() {}

        private Builder(ColumnMetadata columnMetadata)
        {
            this.name = columnMetadata.getName();
            this.type = columnMetadata.getType();
            this.hidden = columnMetadata.isHidden();
        }

        public Builder setName(String name)
        {
            this.name = requireNonNull(name, "name is null");
            return this;
        }

        public Builder setType(Type type)
        {
            this.type = requireNonNull(type, "type is null");
            return this;
        }

        public Builder setHidden(boolean hidden)
        {
            this.hidden = hidden;
            return this;
        }

        public ColumnSchema build()
        {
            return new ColumnSchema(name, type, hidden);
        }
    }
}
