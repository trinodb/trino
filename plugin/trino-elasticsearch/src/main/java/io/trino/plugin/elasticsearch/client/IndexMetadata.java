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
package io.trino.plugin.elasticsearch.client;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IndexMetadata
{
    private final ObjectType schema;

    public IndexMetadata(ObjectType schema)
    {
        this.schema = requireNonNull(schema, "schema is null");
    }

    public ObjectType getSchema()
    {
        return schema;
    }

    public static class Field
    {
        private final boolean asRawJson;
        private final boolean isArray;
        private final String name;
        private final Type type;

        public Field(boolean isArray, String name, Type type)
        {
            this(false, isArray, name, type);
        }

        public Field(boolean asRawJson, boolean isArray, String name, Type type)
        {
            checkArgument(!asRawJson || !isArray,
                    format("A column, (%s) cannot be declared as a Trino array and also be rendered as json.", name));
            this.asRawJson = asRawJson;
            this.isArray = isArray;
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        public boolean asRawJson()
        {
            return asRawJson;
        }

        public boolean isArray()
        {
            return isArray;
        }

        public String getName()
        {
            return name;
        }

        public Type getType()
        {
            return type;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(asRawJson, isArray, name, type);
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
            Field field = (Field) o;
            return asRawJson == field.asRawJson &&
                    isArray == field.isArray &&
                    name.equals(field.name) &&
                    type.equals(field.type);
        }
    }

    public interface Type {}

    public static class PrimitiveType
            implements Type
    {
        private final String name;

        public PrimitiveType(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        public String getName()
        {
            return name;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
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
            PrimitiveType that = (PrimitiveType) o;
            return name.equals(that.name);
        }
    }

    public static class DateTimeType
            implements Type
    {
        private final List<String> formats;

        public DateTimeType(List<String> formats)
        {
            requireNonNull(formats, "formats is null");

            this.formats = formats.stream().sorted().collect(toImmutableList());
        }

        public List<String> getFormats()
        {
            return formats;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(formats);
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
            DateTimeType that = (DateTimeType) o;
            return this.formats.size() == that.formats.size()
                    && this.formats.stream().collect(Collectors.toMap(Function.identity(), s -> 1L, Long::sum))
                    .equals(that.formats.stream().collect(Collectors.toMap(Function.identity(), s -> 1L, Long::sum)));
        }
    }

    public static class ObjectType
            implements Type
    {
        private final List<Field> fields;

        public ObjectType(List<Field> fields)
        {
            requireNonNull(fields, "fields is null");

            this.fields = ImmutableList.copyOf(fields);
        }

        public List<Field> getFields()
        {
            return fields;
        }
    }

    public static class ScaledFloatType
            implements Type
    {
        private final double scale;

        public ScaledFloatType(double scale)
        {
            this.scale = scale;
        }

        public double getScale()
        {
            return scale;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(scale);
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
            ScaledFloatType that = (ScaledFloatType) o;
            return this.scale == that.scale;
        }
    }
}
