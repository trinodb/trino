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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.spi.connector.SchemaUtil.checkNotEmpty;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ColumnMetadata
{
    private final String name;
    private final Type type;
    private final Optional<String> defaultValue;
    private final boolean autoIncrement;
    private final boolean nullable;
    private final boolean readOnly;
    private final Optional<String> comment;
    private final Optional<String> extraInfo;
    private final boolean hidden;
    private final Map<String, Object> properties;

    public ColumnMetadata(String name, Type type)
    {
        this(name, type, Optional.empty(), false, true, false, Optional.empty(), Optional.empty(), false, emptyMap());
    }

    // VisibleForTesting
    ColumnMetadata(
            String name,
            Type type,
            Optional<String> defaultValue,
            boolean autoIncrement,
            boolean nullable,
            boolean readOnly,
            Optional<String> comment,
            Optional<String> extraInfo,
            boolean hidden,
            Map<String, Object> properties)
    {
        checkNotEmpty(name, "name");
        requireNonNull(type, "type is null");
        requireNonNull(defaultValue, "defaultValue is null");
        requireNonNull(comment, "comment is null");
        requireNonNull(extraInfo, "extraInfo is null");
        requireNonNull(properties, "properties is null");

        this.name = name.toLowerCase(ENGLISH);
        this.type = type;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.extraInfo = extraInfo;
        this.hidden = hidden;
        this.properties = properties.isEmpty() ? emptyMap() : unmodifiableMap(new LinkedHashMap<>(properties));
        this.autoIncrement = autoIncrement;
        this.nullable = nullable;
        this.readOnly = readOnly;
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public Optional<String> getDefaultValue()
    {
        return defaultValue;
    }

    public boolean isAutoIncrement()
    {
        return autoIncrement;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public Optional<String> getExtraInfo()
    {
        return extraInfo;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    public Map<String, Object> getProperties()
    {
        return properties;
    }

    public ColumnSchema getColumnSchema()
    {
        return ColumnSchema.builder(this)
                .build();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ColumnMetadata{");
        sb.append("name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", ").append(nullable ? "nullable" : "nonnull");
        comment.ifPresent(text ->
                sb.append(", comment='").append(text).append('\''));
        extraInfo.ifPresent(text ->
                sb.append(", extraInfo='").append(text).append('\''));
        if (hidden) {
            sb.append(", hidden");
        }
        if (!properties.isEmpty()) {
            sb.append(", properties=").append(properties);
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, autoIncrement, nullable, readOnly, comment, extraInfo, hidden);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ColumnMetadata other = (ColumnMetadata) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                this.autoIncrement == other.autoIncrement &&
                this.nullable == other.nullable &&
                this.readOnly == other.readOnly &&
                Objects.equals(this.comment, other.comment) &&
                Objects.equals(this.extraInfo, other.extraInfo) &&
                this.hidden == other.hidden;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builderFrom(ColumnMetadata columnMetadata)
    {
        return new Builder(columnMetadata);
    }

    public static class Builder
    {
        private String name;
        private Type type;
        private Optional<String> defaultValue = Optional.empty();
        private boolean autoIncrement;
        private boolean nullable = true;
        private boolean readOnly;
        private Optional<String> comment = Optional.empty();
        private Optional<String> extraInfo = Optional.empty();
        private boolean hidden;
        private Map<String, Object> properties = emptyMap();

        private Builder() {}

        private Builder(ColumnMetadata columnMetadata)
        {
            this.name = columnMetadata.getName();
            this.type = columnMetadata.getType();
            this.defaultValue = columnMetadata.getDefaultValue();
            this.autoIncrement = columnMetadata.isAutoIncrement();
            this.nullable = columnMetadata.isNullable();
            this.readOnly = columnMetadata.isReadOnly();
            this.comment = columnMetadata.getComment();
            this.extraInfo = columnMetadata.getExtraInfo();
            this.hidden = columnMetadata.isHidden();
            this.properties = columnMetadata.getProperties();
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

        public Builder setDefaultValue(Optional<String> defaultValue)
        {
            this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
            return this;
        }

        public Builder setAutoIncrement(boolean autoIncrement)
        {
            this.autoIncrement = autoIncrement;
            return this;
        }

        public Builder setNullable(boolean nullable)
        {
            this.nullable = nullable;
            return this;
        }

        public Builder setReadOnly(boolean readOnly)
        {
            this.readOnly = readOnly;
            return this;
        }

        public Builder setComment(Optional<String> comment)
        {
            this.comment = requireNonNull(comment, "comment is null");
            return this;
        }

        public Builder setExtraInfo(Optional<String> extraInfo)
        {
            this.extraInfo = requireNonNull(extraInfo, "extraInfo is null");
            return this;
        }

        public Builder setHidden(boolean hidden)
        {
            this.hidden = hidden;
            return this;
        }

        public Builder setProperties(Map<String, Object> properties)
        {
            this.properties = requireNonNull(properties, "properties is null");
            return this;
        }

        public ColumnMetadata build()
        {
            return new ColumnMetadata(
                    name,
                    type,
                    defaultValue,
                    autoIncrement,
                    nullable,
                    readOnly,
                    comment,
                    extraInfo,
                    hidden,
                    properties);
        }
    }
}
