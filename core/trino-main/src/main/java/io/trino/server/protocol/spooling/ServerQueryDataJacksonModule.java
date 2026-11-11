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
package io.trino.server.protocol.spooling;

import io.trino.client.QueryData;
import io.trino.client.TypedQueryData;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.Segment;
import io.trino.server.protocol.JsonBytesQueryData;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.Version;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.BeanSerializerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serializes the QueryData for existing raw and encoded protocols.
 * The client module for QueryData is built on Jackson 2 ({@code QueryDataJacksonModule}), so the server provides
 * its own serializers. They have to produce the same JSON as the client ones.
 * <p>
 * If the passed QueryData is raw - serialize its' data as a materialized array of array of objects.
 * If the passed QueryData is bytes - just write them directly
 * Otherwise, this is a protocol extension and serialize it directly as an object.
 */
public class ServerQueryDataJacksonModule
        extends SimpleModule
{
    public ServerQueryDataJacksonModule()
    {
        super(ServerQueryDataJacksonModule.class.getSimpleName(), Version.unknownVersion());
        addSerializer(QueryData.class, new ServerQueryDataSerializer());
        addSerializer(Segment.class, new SegmentSerializer());
    }

    public static class ServerQueryDataSerializer
            extends ValueSerializer<QueryData>
    {
        private final Map<Class<?>, ValueSerializer<Object>> beanSerializers = new ConcurrentHashMap<>();

        @Override
        public void serialize(QueryData value, JsonGenerator generator, SerializationContext context)
        {
            switch (value) {
                case JsonBytesQueryData jsonBytesQueryData -> jsonBytesQueryData.writeTo(generator);
                case TypedQueryData typedQueryData -> {
                    // serialize as list of lists of objects
                    Iterable<?> iterable = typedQueryData.getIterable();
                    context.writeValue(generator, iterable);
                }
                case EncodedQueryData encodedQueryData -> beanSerializers
                        .computeIfAbsent(EncodedQueryData.class, type -> createBeanSerializer(context, context.constructType(type)))
                        .serialize(encodedQueryData, generator, context);
                default -> throw new IllegalArgumentException("Unsupported QueryData implementation: " + value.getClass().getSimpleName());
            }
        }

        @Override
        public boolean isEmpty(SerializationContext context, QueryData value)
        {
            // Omit the data field when there is no data, some clients assume it is absent then
            return value == null || value.isNull();
        }
    }

    public static class SegmentSerializer
            extends ValueSerializer<Segment>
    {
        private final Map<Class<?>, ValueSerializer<Object>> beanSerializers = new ConcurrentHashMap<>();

        @Override
        public void serialize(Segment value, JsonGenerator generator, SerializationContext context)
        {
            serializeWithType(value, generator, context, context.findTypeSerializer(context.constructType(Segment.class)));
        }

        @Override
        public void serializeWithType(Segment value, JsonGenerator generator, SerializationContext context, TypeSerializer typeSerializer)
        {
            beanSerializers
                    .computeIfAbsent(value.getClass(), type -> createBeanSerializer(context, context.constructSpecializedType(context.constructType(Segment.class), type)))
                    .serializeWithType(value, generator, context, typeSerializer);
        }
    }

    @SuppressWarnings("unchecked")
    private static ValueSerializer<Object> createBeanSerializer(SerializationContext context, JavaType javaType)
    {
        return (ValueSerializer<Object>) BeanSerializerFactory.instance.createSerializer(context, javaType, context.lazyIntrospectBeanDescription(javaType), null);
    }
}
