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

import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.BasicSerializerFactory;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.trino.cache.NonEvictableCache;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTypedJacksonModule<T>
        extends SimpleModule
{
    private static final String TYPE_PROPERTY = "@type";

    private final Class<T> baseClass;
    private final TypeIdResolver typeResolver;

    protected AbstractTypedJacksonModule(
            Class<T> baseClass,
            Function<Object, String> nameResolver,
            Function<String, Class<?>> classResolver)
    {
        super(baseClass.getSimpleName() + "Module", Version.unknownVersion());

        this.baseClass = baseClass;
        typeResolver = new InternalTypeResolver(nameResolver, classResolver);

        addDeserializer(baseClass, new InternalTypeDeserializer<>(baseClass, typeResolver));
    }

    @Override
    public void setupModule(SetupContext context)
    {
        super.setupModule(context);
        // Each mapper gets its own serializer, which builds and caches bean serializers with that mapper's modifiers
        Optional<ObjectMapper> mapper = Optional.of(context.getOwner())
                .filter(ObjectMapper.class::isInstance)
                .map(ObjectMapper.class::cast);
        SimpleSerializers serializers = new SimpleSerializers();
        serializers.addSerializer(baseClass, new InternalTypeSerializer<>(baseClass, typeResolver, mapper));
        context.addSerializers(serializers);
    }

    private static class InternalTypeDeserializer<T>
            extends StdDeserializer<T>
    {
        private final TypeDeserializer typeDeserializer;

        public InternalTypeDeserializer(Class<T> baseClass, TypeIdResolver typeIdResolver)
        {
            super(baseClass);
            this.typeDeserializer = new AsPropertyTypeDeserializer(
                    TypeFactory.defaultInstance().constructType(baseClass),
                    typeIdResolver,
                    TYPE_PROPERTY,
                    false,
                    null,
                    As.PROPERTY,
                    true);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            return (T) typeDeserializer.deserializeTypedFromAny(jsonParser, deserializationContext);
        }
    }

    private static class InternalTypeSerializer<T>
            extends StdSerializer<T>
    {
        private final TypeSerializer typeSerializer;
        private final NonEvictableCache<Class<?>, JsonSerializer<T>> serializerCache = buildNonEvictableCache(CacheBuilder.newBuilder());
        private final Optional<ObjectMapper> mapper;

        public InternalTypeSerializer(Class<T> baseClass, TypeIdResolver typeIdResolver, Optional<ObjectMapper> mapper)
        {
            super(baseClass);
            this.typeSerializer = new AsPropertyTypeSerializer(typeIdResolver, null, TYPE_PROPERTY);
            this.mapper = requireNonNull(mapper, "mapper is null");
        }

        @Override
        public void serialize(T value, JsonGenerator generator, SerializerProvider provider)
                throws IOException
        {
            if (value == null) {
                provider.defaultSerializeNull(generator);
                return;
            }

            try {
                Class<?> type = value.getClass();
                JsonSerializer<T> serializer = serializerCache.get(type, () -> createSerializer(provider, type));
                serializer.serializeWithType(value, generator, provider, typeSerializer);
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause != null) {
                    throwIfInstanceOf(cause, IOException.class);
                }
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        private JsonSerializer<T> createSerializer(SerializerProvider provider, Class<?> type)
                throws JsonMappingException
        {
            JavaType javaType = provider.constructType(type);
            return (JsonSerializer<T>) serializerFactory().createSerializer(provider, javaType);
        }

        // The mapper's factory maps every subtype of the base class to this serializer, so only its
        // modifiers, such as Blackbird property accessors, are reused on a plain bean serializer factory
        private SerializerFactory serializerFactory()
        {
            SerializerFactory factory = BeanSerializerFactory.instance;
            if (mapper.isPresent() && mapper.get().getSerializerFactory() instanceof BasicSerializerFactory configured) {
                // Each added modifier goes first, so adding them in reverse keeps the mapper's order
                for (BeanSerializerModifier modifier : ImmutableList.copyOf(configured.getFactoryConfig().serializerModifiers()).reverse()) {
                    factory = factory.withSerializerModifier(modifier);
                }
            }
            return factory;
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
        public Id getMechanism()
        {
            return Id.NAME;
        }
    }
}
