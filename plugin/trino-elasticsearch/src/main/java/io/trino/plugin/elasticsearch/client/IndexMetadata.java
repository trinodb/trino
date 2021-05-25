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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

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
        private final boolean isArray;
        private final String name;
        private final Type type;

        public Field(boolean isArray, String name, Type type)
        {
            this.isArray = isArray;
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
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
    }
    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
    @JsonSubTypes(
        {
            @JsonSubTypes.Type(value = PrimitiveType.class, name = "primitiveType"),
            @JsonSubTypes.Type(value = DateTimeType.class, name = "dateTimeType"),
            @JsonSubTypes.Type(value = ObjectType.class, name = "objectType")
        })
    public interface Type {}

    public static class PrimitiveType
            implements Type
    {
        private final String name;
        @JsonCreator
        public PrimitiveType(@JsonProperty("name") String name)
        {
            this.name = requireNonNull(name, "name is null");
        }
        @JsonProperty
        public String getName()
        {
            return name;
        }
    }

    public static class DateTimeType
            implements Type
    {
        private final List<String> formats;
        @JsonCreator
        public DateTimeType(@JsonProperty("formats") List<String> formats)
        {
            requireNonNull(formats, "formats is null");

            this.formats = ImmutableList.copyOf(formats);
        }
        @JsonProperty
        public List<String> getFormats()
        {
            return formats;
        }
    }

    public static class ObjectType
            implements Type
    {
        private final List<Field> fields;
        @JsonCreator
        public ObjectType(@JsonProperty("fields") List<Field> fields)
        {
            requireNonNull(fields, "fields is null");

            this.fields = ImmutableList.copyOf(fields);
        }
        @JsonProperty
        public List<Field> getFields()
        {
            return fields;
        }
    }
}
