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
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.Version;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.BeanSerializerFactory;

/**
 * Encodes/decodes the QueryData for existing raw and encoded protocols.
 * <p></p>
 *
 * If the passed QueryData is raw - serialize its' data as a materialized array of array of objects.
 * If the passed QueryData is bytes - just write them directly
 * Otherwise, this is a protocol extension and serialize it directly as an object.
 */
public class ServerQueryDataJacksonModule
        extends SimpleModule
{
    private static final TypeReference<EncodedQueryData> ENCODED_FORMAT = new TypeReference<>() {};

    public ServerQueryDataJacksonModule()
    {
        super(ServerQueryDataJacksonModule.class.getSimpleName(), Version.unknownVersion());
        addSerializer(QueryData.class, new ServerQueryDataSerializer());
        addSerializer(Segment.class, new SegmentSerializer());
    }

    public static class ServerQueryDataSerializer
            extends ValueSerializer<QueryData>
    {
        @Override
        public void serialize(QueryData value, JsonGenerator generator, SerializationContext context)
        {
            switch (value) {
                case null -> context.defaultSerializeNullValue(generator);
                case JsonBytesQueryData jsonBytesQueryData -> jsonBytesQueryData.writeTo(generator);
                case TypedQueryData typedQueryData -> generator.writePOJO(typedQueryData.getIterable());
                case EncodedQueryData encodedQueryData -> createSerializer(context, context.constructType(EncodedQueryData.class))
                        .serialize(encodedQueryData, generator, context);
                default -> throw new IllegalArgumentException("Unsupported QueryData implementation: " + value.getClass().getSimpleName());
            }
        }

        @Override
        public boolean isEmpty(SerializationContext ctxt, QueryData value)
        {
            return value == null || value.isNull();
        }
    }

    public static class SegmentSerializer
            extends ValueSerializer<Segment>
    {
        @Override
        public void serialize(Segment value, JsonGenerator gen, SerializationContext context)
        {
            serializeWithType(value, gen, context, segmentSerializer(context));
        }

        @Override
        public void serializeWithType(Segment value, JsonGenerator gen, SerializationContext context, TypeSerializer typeSerializer)
        {
            createSerializer(context, context.constructSpecializedType(context.constructType(Segment.class), value.getClass()))
                    .serializeWithType(value, gen, context, typeSerializer);
        }

        private static TypeSerializer segmentSerializer(SerializationContext context)
        {
            try {
                return context.findTypeSerializer(context.constructType(Segment.class));
            }
            catch (JacksonException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> ValueSerializer<T> createSerializer(SerializationContext context, JavaType javaType)
    {
        return (ValueSerializer<T>) BeanSerializerFactory.instance.createSerializer(context, javaType);
    }
}
