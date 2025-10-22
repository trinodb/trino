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

import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.Segment;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.Version;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.BeanSerializerFactory;

/**
 * Encodes/decodes the direct and spooling protocols.
 * <p>
 * If the "data" fields starts with an array - this is the direct protocol which requires obtaining JsonParser
 * and then parsing rows lazily.
 * <p>
 * Otherwise, this is a spooling protocol.
 */
public class QueryDataJacksonModule
        extends SimpleModule
{
    public QueryDataJacksonModule()
    {
        super(QueryDataJacksonModule.class.getSimpleName(), Version.unknownVersion());
        addDeserializer(QueryData.class, new Deserializer());
        addSerializer(QueryData.class, new QueryDataSerializer());
        addSerializer(Segment.class, new SegmentSerializer());
    }

    public static class Deserializer
            extends ValueDeserializer<QueryData>
    {
        @Override
        public QueryData deserialize(JsonParser parser, DeserializationContext context)
        {
            // If this is not JSON_ARRAY we are dealing with direct data encoding
            if (parser.currentToken().equals(JsonToken.START_ARRAY)) {
                return new JsonQueryData(context.readTree(parser));
            }
            return context.readValue(parser, EncodedQueryData.class);
        }
    }

    public static class QueryDataSerializer
            extends ValueSerializer<QueryData>
    {
        @Override
        public void serialize(QueryData value, JsonGenerator generator, SerializationContext context)
        {
            if (value == null) {
                context.defaultSerializeNullValue(generator);
            }
            else if (value instanceof JsonQueryData jsonQueryData) {
                generator.writeTree(jsonQueryData.getNode());
            }
            else if (value instanceof TypedQueryData typedQueryData) {
                Iterable<?> iterable = typedQueryData.getIterable();
                context.findValueSerializer(iterable.getClass()).serialize(iterable, generator, context);
            }
            else if (value instanceof EncodedQueryData encodedQueryData) {
                JavaType javaType = context.constructType(EncodedQueryData.class);
                BeanSerializerFactory.instance
                        .createSerializer(context, javaType, context.lazyIntrospectBeanDescription(javaType), null)
                        .serialize(encodedQueryData, generator, context);
            }
            else {
                throw new IllegalArgumentException("Unsupported QueryData implementation: " + value.getClass().getSimpleName());
            }
        }

        @Override
        public boolean isEmpty(SerializationContext context, QueryData value)
        {
            // Important for compatibility with some clients that assume absent data field if data is null
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
        public void serializeWithType(Segment value, JsonGenerator generator, SerializationContext context, TypeSerializer typeSerializer)
        {
            createSerializer(context, context.constructSpecializedType(context.constructType(Segment.class), value.getClass()))
                    .serializeWithType(value, generator, context, typeSerializer);
        }

        private static TypeSerializer segmentSerializer(SerializationContext context)
        {
            return context.findTypeSerializer(context.constructType(Segment.class));
        }

        @SuppressWarnings("unchecked")
        private static <T> ValueSerializer<T> createSerializer(SerializationContext context, JavaType javaType)
        {
            return (ValueSerializer<T>) BeanSerializerFactory.instance.createSerializer(context, javaType, context.lazyIntrospectBeanDescription(javaType), null);
        }
    }
}
