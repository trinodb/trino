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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.Segment;

import java.io.IOException;

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
    private static final TypeReference<EncodedQueryData> ENCODED_FORMAT = new TypeReference<>() {};

    public QueryDataJacksonModule()
    {
        super(QueryDataJacksonModule.class.getSimpleName(), Version.unknownVersion());
        addDeserializer(QueryData.class, new Deserializer());
        addSerializer(QueryData.class, new QueryDataSerializer());
        addSerializer(Segment.class, new SegmentSerializer());
    }

    public static class Deserializer
            extends StdDeserializer<QueryData>
    {
        public Deserializer()
        {
            super(QueryData.class);
        }

        @Override
        public QueryData deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            // If this is not JSON_ARRAY we are dealing with direct data encoding
            if (jsonParser.currentToken().equals(JsonToken.START_ARRAY)) {
                return new JsonQueryData(jsonParser.readValueAsTree());
            }
            return jsonParser.readValueAs(ENCODED_FORMAT);
        }
    }

    public static class QueryDataSerializer
            extends StdSerializer<QueryData>
    {
        public QueryDataSerializer()
        {
            super(QueryData.class);
        }

        @Override
        public void serialize(QueryData value, JsonGenerator generator, SerializerProvider provider)
                throws IOException
        {
            if (value == null) {
                provider.defaultSerializeNull(generator);
            }
            else if (value instanceof JsonQueryData) {
                generator.writeTree(((JsonQueryData) value).getNode());
            }
            else if (value instanceof TypedQueryData) {
                provider.defaultSerializeValue(((TypedQueryData) value).getIterable(), generator); // serialize as list of lists of objects
            }
            else if (value instanceof EncodedQueryData) {
                createSerializer(provider, provider.constructType(EncodedQueryData.class)).serialize(value, generator, provider);
            }
            else {
                throw new IllegalArgumentException("Unsupported QueryData implementation: " + value.getClass().getSimpleName());
            }
        }

        @Override
        public boolean isEmpty(SerializerProvider provider, QueryData value)
        {
            // Important for compatibility with some clients that assume absent data field if data is null
            return value == null || value.isNull();
        }
    }

    public static class SegmentSerializer
            extends StdSerializer<Segment>
    {
        public SegmentSerializer()
        {
            super(Segment.class);
        }

        @Override
        public void serialize(Segment value, JsonGenerator gen, SerializerProvider provider)
                throws IOException
        {
            serializeWithType(value, gen, provider, segmentSerializer(provider));
        }

        @Override
        public void serializeWithType(Segment value, JsonGenerator gen, SerializerProvider provider, TypeSerializer typeSerializer)
                throws IOException
        {
            createSerializer(provider, provider.constructSpecializedType(provider.constructType(Segment.class), value.getClass()))
                    .serializeWithType(value, gen, provider, typeSerializer);
        }

        private static TypeSerializer segmentSerializer(SerializerProvider provider)
        {
            try {
                return provider.findTypeSerializer(provider.constructType(Segment.class));
            }
            catch (JsonMappingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> JsonSerializer<T> createSerializer(SerializerProvider provider, JavaType javaType)
            throws JsonMappingException
    {
        return (JsonSerializer<T>) BeanSerializerFactory.instance.createSerializer(provider, javaType);
    }
}
