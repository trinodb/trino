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
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.Version;
import tools.jackson.databind.BeanDescription;
import tools.jackson.databind.BeanProperty;
import tools.jackson.databind.DatabindContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.SerializationConfig;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import tools.jackson.databind.jsontype.TypeDeserializer;
import tools.jackson.databind.jsontype.TypeIdResolver;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import tools.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import tools.jackson.databind.jsontype.impl.TypeIdResolverBase;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.ValueSerializerModifier;
import tools.jackson.databind.type.TypeFactory;
import tools.jackson.databind.util.NameTransformer;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTypedJacksonModule<T>
        extends SimpleModule
{
    private static final TypeFactory TYPE_FACTORY = TypeFactory.createDefaultInstance();

    private static final String TYPE_PROPERTY = "@type";

    protected AbstractTypedJacksonModule(
            Class<T> baseClass,
            Function<Object, String> nameResolver,
            Function<String, Class<?>> classResolver)
    {
        super(baseClass.getSimpleName() + "Module", Version.unknownVersion());

        TypeIdResolver typeResolver = new InternalTypeResolver(nameResolver, classResolver);

        // Wrap the mapper-built bean serializers so the mapper's modifiers (e.g. Blackbird) still apply
        setSerializerModifier(new TypeIdSerializerModifier(baseClass, new AsPropertyTypeSerializer(typeResolver, null, TYPE_PROPERTY, null)));
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

    private static class TypeIdSerializerModifier
            extends ValueSerializerModifier
    {
        private final Class<?> baseClass;
        private final TypeSerializer typeSerializer;

        public TypeIdSerializerModifier(Class<?> baseClass, TypeSerializer typeSerializer)
        {
            this.baseClass = requireNonNull(baseClass, "baseClass is null");
            this.typeSerializer = requireNonNull(typeSerializer, "typeSerializer is null");
        }

        @Override
        public ValueSerializer<?> modifySerializer(SerializationConfig config, BeanDescription.Supplier beanDescription, ValueSerializer<?> serializer)
        {
            if (baseClass.isAssignableFrom(beanDescription.getBeanClass())) {
                return new TypeIdSerializer(serializer, typeSerializer);
            }
            return serializer;
        }
    }

    private static class TypeIdSerializer
            extends ValueSerializer<Object>
    {
        private final ValueSerializer<Object> delegate;
        private final TypeSerializer typeSerializer;

        @SuppressWarnings("unchecked")
        public TypeIdSerializer(ValueSerializer<?> delegate, TypeSerializer typeSerializer)
        {
            this.delegate = (ValueSerializer<Object>) requireNonNull(delegate, "delegate is null");
            this.typeSerializer = requireNonNull(typeSerializer, "typeSerializer is null");
        }

        @Override
        public void serialize(Object value, JsonGenerator generator, SerializationContext context)
        {
            delegate.serializeWithType(value, generator, context, typeSerializer);
        }

        @Override
        public void serializeWithType(Object value, JsonGenerator generator, SerializationContext context, TypeSerializer ignored)
        {
            serialize(value, generator, context);
        }

        @Override
        public void resolve(SerializationContext context)
        {
            delegate.resolve(context);
        }

        @Override
        public ValueSerializer<?> createContextual(SerializationContext context, BeanProperty property)
        {
            ValueSerializer<?> contextual = delegate.createContextual(context, property);
            if (contextual == delegate) {
                return this;
            }
            return new TypeIdSerializer(contextual, typeSerializer);
        }

        @Override
        public Class<?> handledType()
        {
            return delegate.handledType();
        }

        @Override
        public boolean usesObjectId()
        {
            return delegate.usesObjectId();
        }

        @Override
        public boolean isEmpty(SerializationContext context, Object value)
        {
            return delegate.isEmpty(context, value);
        }

        @Override
        public ValueSerializer<Object> unwrappingSerializer(NameTransformer unwrapper)
        {
            return delegate.unwrappingSerializer(unwrapper);
        }

        @Override
        public void acceptJsonFormatVisitor(JsonFormatVisitorWrapper visitor, JavaType type)
        {
            delegate.acceptJsonFormatVisitor(visitor, type);
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
