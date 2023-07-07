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
package io.trino.plugin.mongodb.expression;

import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public class ExpressionInfo
{
    private final String name;
    private final Type type;
    private final Optional<String> mongoType;

    public ExpressionInfo(String name, Type type)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.mongoType = toMongoType(type);
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public Optional<String> getMongoType()
    {
        return mongoType;
    }

    private Optional<String> toMongoType(Type type)
    {
        if (type == TINYINT || type == SMALLINT || type == IntegerType.INTEGER) {
            return Optional.of("int");
        }

        if (type == BIGINT) {
            return Optional.of("long");
        }

        if (type instanceof DecimalType) {
            return Optional.of("decimal");
        }

        if (type instanceof VarcharType) {
            return Optional.of("string");
        }

        return Optional.empty();
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
        ExpressionInfo that = (ExpressionInfo) obj;
        return Objects.equals(name, that.name) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .toString();
    }
}
