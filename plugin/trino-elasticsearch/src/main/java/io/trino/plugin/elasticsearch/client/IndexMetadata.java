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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class IndexMetadata
{
    private static Type ipAddressType;

    public static void init(TypeManager typeManager)
    {
        ipAddressType = typeManager.getType(new TypeSignature(StandardTypes.IPADDRESS));
    }

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
        private final FieldType type;

        public Field(boolean isArray, String name, FieldType fieldType)
        {
            this.isArray = isArray;
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(fieldType, "type is null");
        }

        public String getName()
        {
            return name;
        }

        public FieldType getType()
        {
            return type;
        }

        public Type toTrinoType()
        {
            Type trinoType = type.toTrinoType();
            return trinoType != null && isArray ? new ArrayType(trinoType) : trinoType;
        }

        public boolean supportsPredicates()
        {
            return type.supportsPredicates();
        }
    }

    public interface FieldType
    {
        default Type toTrinoType()
        {
            return null;
        }

        default boolean supportsPredicates()
        {
            return false;
        }
    }

    public static class PrimitiveType
            implements FieldType
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

        public Type toTrinoType()
        {
            switch (name) {
                case "float":
                    return REAL;
                case "double":
                    return DOUBLE;
                case "byte":
                    return TINYINT;
                case "short":
                    return SMALLINT;
                case "integer":
                    return INTEGER;
                case "long":
                    return BIGINT;
                case "text":
                case "keyword":
                    return VARCHAR;
                case "boolean":
                    return BOOLEAN;
                case "binary":
                    return VARBINARY;
                case "ip":
                    return ipAddressType;
            }

            return null;
        }

        @Override
        public boolean supportsPredicates()
        {
            switch (name.toLowerCase(ENGLISH)) {
                case "boolean":
                case "byte":
                case "short":
                case "integer":
                case "long":
                case "double":
                case "float":
                case "keyword":
                    return true;
            }

            return false;
        }
    }

    public static class DateTimeType
            implements FieldType
    {
        private final List<String> formats;

        public DateTimeType(List<String> formats)
        {
            requireNonNull(formats, "formats is null");

            this.formats = ImmutableList.copyOf(formats);
        }

        @Override
        public Type toTrinoType()
        {
            // otherwise, skip -- we don't support custom formats, yet
            if (formats.isEmpty()) {
                return TIMESTAMP_MILLIS;
            }
            return null;
        }

        @Override
        public boolean supportsPredicates()
        {
            return true;
        }
    }

    public static class ObjectType
            implements FieldType
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

        @Override
        public Type toTrinoType()
        {
            ImmutableList.Builder<RowType.Field> builder = ImmutableList.builder();
            for (IndexMetadata.Field field : fields) {
                Type trinoType = field.toTrinoType();
                if (trinoType != null) {
                    builder.add(RowType.field(field.getName(), trinoType));
                }
            }

            List<RowType.Field> fields = builder.build();

            if (!fields.isEmpty()) {
                return RowType.from(fields);
            }

            return null;
        }
    }
}
