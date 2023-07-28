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
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.client.QueryResultSetFormatResolver.formatNameForClass;
import static java.util.Objects.requireNonNull;

public class ResultSetJsonModule
        extends SimpleModule
{
    private static final String FORMAT_PROPERTY = "format";
    private static final Class<? extends QueryResults> DEFAULT_FORMAT = JsonQueryResults.class;
    private static final String DEFAULT_FORMAT_NAME = formatNameForClass(DEFAULT_FORMAT);

    public ResultSetJsonModule(QueryResultSetFormatResolver formatResolver)
    {
        super(QueryResults.class.getSimpleName() + "Module", Version.unknownVersion());

        TypeIdResolver typeResolver = new InternalTypeResolver(formatResolver::formatNameFromObject, formatResolver::getClassByFormatName);
        addSerializer(QueryResults.class, new InternalTypeSerializer(typeResolver));
        addDeserializer(QueryResults.class, new InternalTypeDeserializer(typeResolver));
    }

    private static class InternalTypeDeserializer
            extends StdDeserializer<QueryResults>
    {
        private final TypeDeserializer typeDeserializer;

        public InternalTypeDeserializer(TypeIdResolver typeIdResolver)
        {
            super(QueryResults.class);
            this.typeDeserializer = new AsPropertyTypeDeserializer(
                    TypeFactory.defaultInstance().constructType(QueryResults.class),
                    typeIdResolver,
                    FORMAT_PROPERTY,
                    false,
                    TypeFactory.defaultInstance().constructType(DEFAULT_FORMAT));
        }

        @SuppressWarnings("unchecked")
        @Override
        public QueryResults deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            return (QueryResults) typeDeserializer.deserializeTypedFromAny(jsonParser, deserializationContext);
        }
    }

    private static class InternalTypeSerializer
            extends StdSerializer<QueryResults>
    {
        private final TypeSerializer typeSerializer;
        private final TypeIdResolver typeIdResolver;

        public InternalTypeSerializer(TypeIdResolver typeIdResolver)
        {
            super(QueryResults.class);
            this.typeIdResolver = requireNonNull(typeIdResolver, "typeIdResolver is null");
            this.typeSerializer = new AsPropertyTypeSerializer(typeIdResolver, null, FORMAT_PROPERTY);
        }

        @Override
        public void serialize(QueryResults value, JsonGenerator generator, SerializerProvider provider)
                throws IOException
        {
            if (value == null) {
                provider.defaultSerializeNull(generator);
                return;
            }

            Class<?> type = value.getClass();
            JsonSerializer<QueryResults> serializer = createSerializer(provider, type);
            if (typeIdResolver.idFromValue(value).equals(DEFAULT_FORMAT_NAME)) {
                // do not emit default type information
                serializer.serialize(value, generator, provider);
            }
            else {
                serializer.serializeWithType(value, generator, provider, typeSerializer);
            }
        }

        @SuppressWarnings("unchecked")
        private static <T> JsonSerializer<T> createSerializer(SerializerProvider provider, Class<?> type)
                throws JsonMappingException
        {
            JavaType javaType = provider.constructType(type);
            return (JsonSerializer<T>) BeanSerializerFactory.instance.createSerializer(provider, javaType);
        }
    }

    private static class InternalTypeResolver
            extends TypeIdResolverBase
    {
        private final Function<Object, String> nameResolver;
        private final Function<String, Class<?>> classResolver;

        public InternalTypeResolver(Function<Object, String> nameResolver, Function<String, Class<?>> classResolver)
        {
            this.nameResolver = requireNonNull(nameResolver, "nameResolver is null");
            this.classResolver = requireNonNull(classResolver, "classResolver is null");
        }

        @Override
        public String idFromValue(Object value)
        {
            return idFromValueAndType(value, value.getClass());
        }

        @Override
        public String idFromValueAndType(Object value, Class<?> suggestedType)
        {
            requireNonNull(value, "value is null");
            return nameResolver.apply(value);
        }

        @Override
        public JavaType typeFromId(DatabindContext context, String id)
        {
            requireNonNull(id, "id is null");
            Class<?> typeClass = classResolver.apply(id);
            checkArgument(typeClass != null, "Unknown type ID: %s", id);
            return context.getTypeFactory().constructType(typeClass);
        }

        @Override
        public JsonTypeInfo.Id getMechanism()
        {
            return JsonTypeInfo.Id.NAME;
        }
    }
}
