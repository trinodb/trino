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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import com.google.errorprone.annotations.DoNotCall;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.type.JsonType.JSON;
import static java.util.Objects.requireNonNull;

public final class GenericLiteral
        extends Literal
{
    private final Type type;
    private final String stringValue;
    private final Object rawValue;

    public static GenericLiteral constant(Type type, Object rawValue)
    {
        checkArgument(
                Primitives.wrap(type.getJavaType()).isAssignableFrom(rawValue.getClass()),
                "Improper Java type (%s) for type '%s'",
                rawValue.getClass().getName(),
                type.toString());

        return new GenericLiteral(type, null, rawValue);
    }

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static GenericLiteral fromJson(
            @JsonProperty Type type,
            @JsonProperty Block rawValueAsBlock,
            @JsonProperty String stringValue)
    {
        return new GenericLiteral(type, stringValue, readNativeValue(type, rawValueAsBlock, 0));
    }

    @Deprecated
    public GenericLiteral(Type type, String stringValue)
    {
        this(verifyLegacyType(type), stringValue, null);
    }

    private static Type verifyLegacyType(Type type)
    {
        if (type.equals(TINYINT) ||
                type.equals(SMALLINT) ||
                type.equals(INTEGER) ||
                type.equals(BIGINT) ||
                type.equals(BOOLEAN) ||
                type.equals(REAL) ||
                type.equals(DOUBLE) ||
                type.equals(DATE) ||
                type.equals(JSON) ||
                type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType ||
                type instanceof TimeWithTimeZoneType ||
                type instanceof TimeType ||
                type instanceof VarcharType ||
                type instanceof CharType ||
                type instanceof DecimalType ||
                type instanceof VarbinaryType) {
            throw new IllegalArgumentException("Call constant(%s, ...)".formatted(type));
        }

        return type;
    }

    public GenericLiteral(Type type, String stringValue, Object rawValue)
    {
        requireNonNull(type, "type is null");
        this.type = type;
        this.stringValue = stringValue;
        this.rawValue = rawValue;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty("value")
    public Block getRawValueAsBlock()
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, rawValue);
        return blockBuilder.build();
    }

    @Deprecated
    public String getValue()
    {
        return stringValue == null ? rawValue.toString() : stringValue;
    }

    // TODO: rename to getValue once the other implementation is gone
    public Object getRawValue()
    {
        return rawValue;
    }

    @JsonProperty
    public String getStringValue()
    {
        return stringValue;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitGenericLiteral(this, context);
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        return ImmutableList.of();
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
        GenericLiteral that = (GenericLiteral) o;
        return Objects.equals(type, that.type) && Objects.equals(stringValue, that.stringValue) && Objects.equals(rawValue, that.rawValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, stringValue, rawValue);
    }

    @Override
    public String toString()
    {
        return "Literal[%s, %s]".formatted(type, stringValue == null ? rawValue : stringValue);
    }
}
