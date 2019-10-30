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
package io.prestosql.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class JdbcColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final JdbcTypeHandle jdbcTypeHandle;
    private final Type columnType;
    private final boolean nullable;

    // All and only required fields
    public JdbcColumnHandle(
            String columnName,
            JdbcTypeHandle jdbcTypeHandle,
            Type columnType)
    {
        this(columnName, jdbcTypeHandle, columnType, true);
    }

    /**
     * @deprecated Use {@link #builder()} instead.
     */
    @Deprecated
    @JsonCreator
    public JdbcColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("jdbcTypeHandle") JdbcTypeHandle jdbcTypeHandle,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("nullable") boolean nullable)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.jdbcTypeHandle = requireNonNull(jdbcTypeHandle, "jdbcTypeHandle is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.nullable = nullable;
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

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .setNullable(nullable)
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
        return Joiner.on(":").join(
                columnName,
                columnType.getDisplayName(),
                jdbcTypeHandle.getJdbcTypeName());
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

        public Builder() {}

        private Builder(JdbcColumnHandle handle)
        {
            this.columnName = handle.getColumnName();
            this.jdbcTypeHandle = handle.getJdbcTypeHandle();
            this.columnType = handle.getColumnType();
            this.nullable = handle.isNullable();
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

        public JdbcColumnHandle build()
        {
            return new JdbcColumnHandle(
                    columnName,
                    jdbcTypeHandle,
                    columnType,
                    nullable);
        }
    }
}
