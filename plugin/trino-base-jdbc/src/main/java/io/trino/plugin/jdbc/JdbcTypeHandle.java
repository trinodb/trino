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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public final class JdbcTypeHandle
{
    private static final int INSTANCE_SIZE = instanceSize(JdbcTypeHandle.class);

    private final int jdbcType;
    private final Optional<String> jdbcTypeName;
    private final Optional<Integer> columnSize;
    private final Optional<Integer> decimalDigits;
    private final Optional<Integer> arrayDimensions;
    private final Optional<CaseSensitivity> caseSensitivity;

    @JsonCreator
    public JdbcTypeHandle(
            @JsonProperty("jdbcType") int jdbcType,
            @JsonProperty("jdbcTypeName") Optional<String> jdbcTypeName,
            @JsonProperty("columnSize") Optional<Integer> columnSize,
            @JsonProperty("decimalDigits") Optional<Integer> decimalDigits,
            @JsonProperty("arrayDimensions") Optional<Integer> arrayDimensions,
            @JsonProperty("caseSensitivity") Optional<CaseSensitivity> caseSensitivity)
    {
        this.jdbcType = jdbcType;
        this.jdbcTypeName = requireNonNull(jdbcTypeName, "jdbcTypeName is null");
        this.columnSize = requireNonNull(columnSize, "columnSize is null");
        this.decimalDigits = requireNonNull(decimalDigits, "decimalDigits is null");
        this.arrayDimensions = requireNonNull(arrayDimensions, "arrayDimensions is null");
        this.caseSensitivity = requireNonNull(caseSensitivity, "caseSensitivity is null");
    }

    @JsonProperty
    public int getJdbcType()
    {
        return jdbcType;
    }

    @JsonProperty
    public Optional<String> getJdbcTypeName()
    {
        return jdbcTypeName;
    }

    @JsonProperty
    public Optional<Integer> getColumnSize()
    {
        return columnSize;
    }

    @JsonIgnore
    public int getRequiredColumnSize()
    {
        return getColumnSize().orElseThrow(() -> new IllegalStateException("column size not present"));
    }

    @JsonProperty
    public Optional<Integer> getDecimalDigits()
    {
        return decimalDigits;
    }

    @JsonIgnore
    public int getRequiredDecimalDigits()
    {
        return getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
    }

    @JsonProperty
    public Optional<Integer> getArrayDimensions()
    {
        return arrayDimensions;
    }

    @JsonProperty
    public Optional<CaseSensitivity> getCaseSensitivity()
    {
        return caseSensitivity;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jdbcType, jdbcTypeName, columnSize, decimalDigits, arrayDimensions, caseSensitivity);
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
        JdbcTypeHandle that = (JdbcTypeHandle) o;
        return jdbcType == that.jdbcType &&
                Objects.equals(columnSize, that.columnSize) &&
                Objects.equals(decimalDigits, that.decimalDigits) &&
                Objects.equals(jdbcTypeName, that.jdbcTypeName) &&
                Objects.equals(arrayDimensions, that.arrayDimensions) &&
                Objects.equals(caseSensitivity, that.caseSensitivity);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("jdbcType", jdbcType)
                .add("jdbcTypeName", jdbcTypeName.orElse(null))
                .add("columnSize", columnSize.orElse(null))
                .add("decimalDigits", decimalDigits.orElse(null))
                .add("arrayDimensions", arrayDimensions.orElse(null))
                .add("caseSensitivity", caseSensitivity.orElse(null))
                .toString();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(jdbcType)
                + sizeOf(jdbcTypeName, SizeOf::estimatedSizeOf)
                + sizeOf(columnSize, SizeOf::sizeOf)
                + sizeOf(decimalDigits, SizeOf::sizeOf)
                + sizeOf(arrayDimensions, SizeOf::sizeOf)
                + sizeOf(caseSensitivity, ignored -> 0);
    }
}
