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
package io.trino.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public final class JdbcColumnHandle
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = instanceSize(JdbcColumnHandle.class);

    private final String columnName;
    private final JdbcTypeHandle jdbcTypeHandle;
    private final Type columnType;
    private final boolean nullable;
    private final Optional<String> comment;

    // All and only required fields
    public JdbcColumnHandle(String columnName, JdbcTypeHandle jdbcTypeHandle, Type columnType)
    {
        this(columnName, jdbcTypeHandle, columnType, true, Optional.empty());
    }

    /**
     * @deprecated This constructor is intended to be used by JSON deserialization only. Use {@link #builder()} instead.
     */
    @Deprecated
    @JsonCreator
    public JdbcColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("jdbcTypeHandle") JdbcTypeHandle jdbcTypeHandle,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("nullable") boolean nullable,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.jdbcTypeHandle = requireNonNull(jdbcTypeHandle, "jdbcTypeHandle is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.nullable = nullable;
        this.comment = requireNonNull(comment, "comment is null");
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public JdbcTypeHandle getJdbcTypeHandle()
    {
        return jdbcTypeHandle;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public boolean isNullable()
    {
        return nullable;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .setNullable(nullable)
                .setComment(comment)
                .build();
    }

    public ColumnSchema getColumnSchema()
    {
        return ColumnSchema.builder()
                .setName(columnName)
                .setType(columnType)
                .build();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        JdbcColumnHandle o = (JdbcColumnHandle) obj;
        return Objects.equals(this.columnName, o.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").skipNulls().join(
                columnName,
                columnType.getDisplayName(),
                jdbcTypeHandle.getJdbcTypeName().orElse(null));
    }

    public long getRetainedSizeInBytes()
    {
        // columnType is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE
                + sizeOf(nullable)
                + estimatedSizeOf(columnName)
                + sizeOf(comment, SizeOf::estimatedSizeOf)
                + jdbcTypeHandle.getRetainedSizeInBytes();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builderFrom(JdbcColumnHandle handle)
    {
        return new Builder(handle);
    }

    public static final class Builder
    {
        private String columnName;
        private JdbcTypeHandle jdbcTypeHandle;
        private Type columnType;
        private boolean nullable = true;
        private Optional<String> comment = Optional.empty();

        public Builder() {}

        private Builder(JdbcColumnHandle handle)
        {
            this.columnName = handle.getColumnName();
            this.jdbcTypeHandle = handle.getJdbcTypeHandle();
            this.columnType = handle.getColumnType();
            this.nullable = handle.isNullable();
            this.comment = handle.getComment();
        }

        public Builder setColumnName(String columnName)
        {
            this.columnName = columnName;
            return this;
        }

        public Builder setJdbcTypeHandle(JdbcTypeHandle jdbcTypeHandle)
        {
            this.jdbcTypeHandle = jdbcTypeHandle;
            return this;
        }

        public Builder setColumnType(Type columnType)
        {
            this.columnType = columnType;
            return this;
        }

        public Builder setNullable(boolean nullable)
        {
            this.nullable = nullable;
            return this;
        }

        public Builder setComment(Optional<String> comment)
        {
            this.comment = comment;
            return this;
        }

        public JdbcColumnHandle build()
        {
            return new JdbcColumnHandle(
                    columnName,
                    jdbcTypeHandle,
                    columnType,
                    nullable,
                    comment);
        }
    }
}
