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
package io.trino.sql.parser;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IntervalDayTimeDataType;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.TypeParameter;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;

class TreeNodes
{
    private TreeNodes() {}

    public static NodeLocation location(int line, int column)
    {
        return new NodeLocation(line, column);
    }

    public static Identifier identifier(NodeLocation location, String name)
    {
        return new Identifier(location, name, false);
    }

    public static DataType simpleType(NodeLocation location, String identifier)
    {
        return new GenericDataType(location, new Identifier(location, identifier, false), ImmutableList.of());
    }

    public static IntervalDayTimeDataType intervalType(NodeLocation location, IntervalDayTimeDataType.Field from, IntervalDayTimeDataType.Field to)
    {
        return new IntervalDayTimeDataType(location, from, to);
    }

    public static DateTimeDataType dateTimeType(NodeLocation location, DateTimeDataType.Type kind, boolean withTimeZone)
    {
        return new DateTimeDataType(location, kind, withTimeZone, Optional.empty());
    }

    public static DateTimeDataType dateTimeType(NodeLocation location, DateTimeDataType.Type kind, boolean withTimeZone, DataTypeParameter precision)
    {
        return new DateTimeDataType(location, kind, withTimeZone, Optional.of(precision));
    }

    public static RowDataType rowType(NodeLocation location, RowDataType.Field... fields)
    {
        return new RowDataType(location, Arrays.asList(fields));
    }

    public static RowDataType.Field field(NodeLocation location, String name, DataType type)
    {
        return field(location, name, false, type);
    }

    public static RowDataType.Field field(NodeLocation location, DataType type)
    {
        return new RowDataType.Field(location, Optional.empty(), type);
    }

    public static RowDataType.Field field(NodeLocation location, String name, boolean delimited, DataType type)
    {
        return new RowDataType.Field(
                location,
                Optional.of(new Identifier(location, name, delimited)),
                type);
    }

    public static GenericDataType parametricType(NodeLocation location, String name, DataTypeParameter... parameters)
    {
        return new GenericDataType(
                location,
                new Identifier(location, name, false),
                Arrays.asList(parameters));
    }

    public static GenericDataType parametricType(NodeLocation location, Identifier name, DataTypeParameter... parameters)
    {
        return new GenericDataType(location, name, Arrays.asList(parameters));
    }

    public static TypeParameter parameter(DataType type)
    {
        return new TypeParameter(type);
    }

    public static NumericParameter parameter(NodeLocation location, String value)
    {
        return new NumericParameter(location, value);
    }

    public static ColumnDefinition columnDefinition(NodeLocation location, String name, DataType type)
    {
        return new ColumnDefinition(location, qualifiedName(location, name), type, true, emptyList(), Optional.empty());
    }

    public static ColumnDefinition columnDefinition(NodeLocation location, String name, DataType type, boolean nullable)
    {
        return new ColumnDefinition(location, qualifiedName(location, name), type, nullable, emptyList(), Optional.empty());
    }

    public static ColumnDefinition columnDefinition(NodeLocation location, String name, DataType type, boolean nullable, String comment)
    {
        return new ColumnDefinition(location, qualifiedName(location, name), type, nullable, emptyList(), Optional.of(comment));
    }

    public static ColumnDefinition columnDefinition(NodeLocation location, String name, DataType type, boolean nullable, List<Property> properties)
    {
        return new ColumnDefinition(location, qualifiedName(location, name), type, nullable, properties, Optional.empty());
    }

    public static Property property(NodeLocation location, String name, Expression value)
    {
        return new Property(location, identifier(location, name), value);
    }

    public static QualifiedName qualifiedName(NodeLocation location, String part)
    {
        return QualifiedName.of(ImmutableList.of(identifier(location, part)));
    }
}
