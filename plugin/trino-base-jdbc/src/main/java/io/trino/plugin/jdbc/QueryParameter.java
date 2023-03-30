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
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static java.util.Objects.requireNonNull;

public final class QueryParameter
{
    private final Optional<JdbcTypeHandle> jdbcType;
    private final Type type;
    private final Optional<Object> value;

    public QueryParameter(Type type, Optional<Object> value)
    {
        this(Optional.empty(), type, value);
    }

    public QueryParameter(JdbcTypeHandle jdbcType, Type type, Optional<Object> value)
    {
        this(Optional.of(jdbcType), type, value);
    }

    private QueryParameter(Optional<JdbcTypeHandle> jdbcType, Type type, Optional<Object> value)
    {
        this.jdbcType = requireNonNull(jdbcType, "jdbcType is null");
        this.type = requireNonNull(type, "type is null");
        this.value = requireNonNull(value, "value is null");
    }

    @JsonCreator
    public static QueryParameter fromValueAsBlock(Optional<JdbcTypeHandle> jdbcType, Type type, Block valueBlock)
    {
        requireNonNull(type, "type is null");
        requireNonNull(valueBlock, "valueBlock is null");
        Optional<Object> value = Optional.ofNullable(blockToNativeValue(type, valueBlock));
        return new QueryParameter(jdbcType, type, value);
    }

    @JsonProperty
    public Optional<JdbcTypeHandle> getJdbcType()
    {
        return jdbcType;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Block getValueBlock()
    {
        return nativeValueToBlock(type, value.orElse(null));
    }

    @JsonIgnore
    public Optional<Object> getValue()
    {
        return value;
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
        QueryParameter that = (QueryParameter) o;
        return jdbcType.equals(that.jdbcType)
                && type.equals(that.type)
                && value.equals(that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jdbcType, type, value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("jdbcType", jdbcType)
                .add("type", type)
                .add("value", value)
                .toString();
    }
}
