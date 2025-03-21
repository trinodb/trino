package io.trino.plugin.mysql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import io.airlift.slice.SizeOf;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.type.Type;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2025/02/14
 */
public class MySqlJdbcColumnHandle
        extends JdbcColumnHandle {
    private static final int INSTANCE_SIZE = instanceSize(MySqlJdbcColumnHandle.class);

    private final boolean autoIncrement;

    // All and only required fields
    public MySqlJdbcColumnHandle(String columnName, JdbcTypeHandle jdbcTypeHandle, Type columnType)
    {
        this(columnName, jdbcTypeHandle, columnType, true, Optional.empty(), false);
    }

    /**
     * @deprecated This constructor is intended to be used by JSON deserialization only. Use {@link #builder()} instead.
     */
    @Deprecated
    @JsonCreator
    public MySqlJdbcColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("jdbcTypeHandle") JdbcTypeHandle jdbcTypeHandle,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("nullable") boolean nullable,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("autoIncrement") boolean autoIncrement)
    {
        super(columnName, jdbcTypeHandle, columnType, nullable, comment);
        this.autoIncrement = autoIncrement;
    }

    @JsonProperty
    public boolean isAutoIncrement()
    {
        return autoIncrement;
    }

    @Override
    public ColumnMetadata getColumnMetadata()
    {
        ColumnMetadata.Builder columnMetadataBuilder = ColumnMetadata.builder()
                .setName(getColumnName())
                .setType(getColumnType())
                .setNullable(isNullable())
                .setComment(getComment());

        Map<String, Object> properties = new LinkedHashMap<>();
        if (autoIncrement) {
            properties.put(MySqlColumnProperties.AUTO_INCREMENT, true);
        }

        if (!properties.isEmpty()) {
            columnMetadataBuilder.setProperties(properties);
        }
        return columnMetadataBuilder.build();
    }


    public long getRetainedSizeInBytes()
    {
        // columnType is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE
                + sizeOf(isNullable())
                + estimatedSizeOf(getColumnName())
                + sizeOf(getComment(), SizeOf::estimatedSizeOf)
                + getJdbcTypeHandle().getRetainedSizeInBytes()
                + sizeOf(autoIncrement);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builderFrom(MySqlJdbcColumnHandle handle)
    {
        return new MySqlJdbcColumnHandle.Builder(handle);
    }

    public static class Builder
        extends JdbcColumnHandle.Builder
    {
        private String columnName;
        private JdbcTypeHandle jdbcTypeHandle;
        private Type columnType;
        private boolean nullable = true;
        private Optional<String> comment = Optional.empty();
        private boolean autoIncrement;

        public Builder() {}

        private Builder(MySqlJdbcColumnHandle handle)
        {
            this.columnName = handle.getColumnName();
            this.jdbcTypeHandle = handle.getJdbcTypeHandle();
            this.columnType = handle.getColumnType();
            this.nullable = handle.isNullable();
            this.comment = handle.getComment();
            this.autoIncrement = handle.isAutoIncrement();
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

        public Builder setAutoIncrement(boolean autoIncrement)
        {
            this.autoIncrement = autoIncrement;
            return this;
        }

        public MySqlJdbcColumnHandle build()
        {
            return new MySqlJdbcColumnHandle(
                    columnName,
                    jdbcTypeHandle,
                    columnType,
                    nullable,
                    comment,
                    autoIncrement);
        }
    }
}
