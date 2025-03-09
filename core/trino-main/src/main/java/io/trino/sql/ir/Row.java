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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@JsonSerialize
public record Row(List<Field> fields)
        implements Expression
{
    public Row
    {
        requireNonNull(fields, "fields is null");
        fields = ImmutableList.copyOf(fields);
    }

    public static Row anonymousRow(List<Expression> values)
    {
        return new Row(values.stream()
                .map(Field::anonymousField)
                .collect(Collectors.toList()));
    }

    @Override
    public Type type()
    {
        return RowType.from(fields.stream().map(Field::asRowTypeField).collect(Collectors.toList()));
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitRow(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return fields.stream()
                .map(Field::value)
                .collect(Collectors.toList());
    }

    @Override
    public String toString()
    {
        return "(" +
                fields.stream()
                        .map(Field::toString)
                        .collect(Collectors.joining(", ")) +
                ")";
    }

    @JsonSerialize
    public record Field(Optional<String> name, Expression value)
    {
        public Field
        {
            requireNonNull(name, "name is null");
            requireNonNull(value, "value is null");
        }

        public static Field anonymousField(Expression value)
        {
            return new Field(Optional.empty(), value);
        }

        public RowType.Field asRowTypeField()
        {
            return new RowType.Field(name, value.type());
        }

        @Override
        public String toString()
        {
            return name.map(n -> n + " " + value).orElseGet(value::toString);
        }
    }
}
