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
package io.trino.metadata;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.cache.CacheBuilder;
import io.trino.cache.NonEvictableCache;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.Version;
import tools.jackson.databind.DatabindContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.jsontype.TypeDeserializer;
import tools.jackson.databind.jsontype.TypeIdResolver;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import tools.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import tools.jackson.databind.jsontype.impl.TypeIdResolverBase;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.BeanSerializerFactory;
import tools.jackson.databind.ser.std.StdSerializer;
import tools.jackson.databind.type.TypeFactory;

import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTypedJacksonModule<T>
        extends SimpleModule
{
    private static final TypeFactory TYPE_FACTORY = new ObjectMapper().getTypeFactory();

    private static final String TYPE_PROPERTY = "@type";

    protected AbstractTypedJacksonModule(
            Class<T> baseClass,
            Function<Object, String> nameResolver,
            Function<String, Class<?>> classResolver)
    {
        super(baseClass.getSimpleName() + "Module", Version.unknownVersion());

        TypeIdResolver typeResolver = new InternalTypeResolver(nameResolver, classResolver);

        addSerializer(baseClass, new InternalTypeSerializer<>(baseClass, typeResolver));
        addDeserializer(baseClass, new InternalTypeDeserializer<>(baseClass, typeResolver));
    }

    private static class InternalTypeDeserializer<T>
            extends StdDeserializer<T>
    {
        private final TypeDeserializer typeDeserializer;

        public InternalTypeDeserializer(Class<T> baseClass, TypeIdResolver typeIdResolver)
        {
            super(baseClass);
            this.typeDeserializer = new AsPropertyTypeDeserializer(
                    TYPE_FACTORY.constructType(baseClass),
                    typeIdResolver,
                    TYPE_PROPERTY,
                    false,
                    null,
                    JsonTypeInfo.As.PROPERTY,
                    true);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        {
            return (T) typeDeserializer.deserializeTypedFromAny(jsonParser, deserializationContext);
        }
    }

    private static class InternalTypeSerializer<T>
            extends StdSerializer<T>
    {
        private final TypeSerializer typeSerializer;
        private final NonEvictableCache<Class<?>, ValueSerializer<T>> serializerCache = buildNonEvictableCache(CacheBuilder.newBuilder());

        public InternalTypeSerializer(Class<T> baseClass, TypeIdResolver typeIdResolver)
        {
            super(baseClass);
            this.typeSerializer = new AsPropertyTypeSerializer(typeIdResolver, null, TYPE_PROPERTY);
        }

        @Override
        public void serialize(T value, JsonGenerator generator, SerializationContext context)
        {
            if (value == null) {
                context.defaultSerializeNullValue(generator);
                return;
            }

            try {
                Class<?> type = value.getClass();
                ValueSerializer<T> serializer = serializerCache.get(type, () -> createSerializer(context, type));
                serializer.serializeWithType(value, generator, context, typeSerializer);
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause != null) {
                    throwIfInstanceOf(cause, UncheckedIOException.class);
                }
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        private static <T> ValueSerializer<T> createSerializer(SerializationContext context, Class<?> type)
        {
            JavaType javaType = context.constructType(type);
            return (ValueSerializer<T>) BeanSerializerFactory.instance.createSerializer(context, javaType);
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
        public String idFromValue(DatabindContext context, Object value)
        {
            return idFromValueAndType(context, value, value.getClass());
        }

        @Override
        public String idFromValueAndType(DatabindContext context, Object value, Class<?> suggestedType)
        {
            requireNonNull(value, "value is null");
            String type = nameResolver.apply(value);
            checkArgument(type != null, "Unknown class: %s", value.getClass().getName());
            return type;
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
