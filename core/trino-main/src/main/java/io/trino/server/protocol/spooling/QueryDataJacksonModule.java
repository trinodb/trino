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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.trino.client.QueryData;
import io.trino.client.RawQueryData;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.InlineSegment;
import io.trino.client.spooling.Segment;
import io.trino.client.spooling.SpooledSegment;

import java.io.IOException;

/**
 * Encodes the QueryData for existing raw and encoded protocols.
 * <p></p>
 *
 * If the passed QueryData is raw - serialize its' data as a materialized array of array of objects.
 * Otherwise, this is a protocol extension and serialize it directly as an object.
 */
public class QueryDataJacksonModule
        extends SimpleModule
{
    public QueryDataJacksonModule()
    {
        super(QueryDataJacksonModule.class.getSimpleName(), Version.unknownVersion());
        addSerializer(QueryData.class, new Serializer());
        addSerializer(Segment.class, new SegmentSerializer());
        registerSubtypes(InlineSegment.class);
        registerSubtypes(SpooledSegment.class);
    }

    private static class Serializer
            extends StdSerializer<QueryData>
    {
        public Serializer()
        {
            super(QueryData.class);
        }

        @Override
        public void serialize(QueryData value, JsonGenerator generator, SerializerProvider provider)
                throws IOException
        {
            switch (value) {
                case null -> provider.defaultSerializeNull(generator);
                case RawQueryData ignored -> provider.defaultSerializeValue(value.getData(), generator);
                case EncodedQueryData encoded -> createSerializer(provider, provider.constructType(EncodedQueryData.class)).serialize(encoded, generator, provider);
                default -> throw new IllegalArgumentException("Unsupported QueryData implementation: " + value.getClass().getSimpleName());
            }
        }

        @Override
        public boolean isEmpty(SerializerProvider provider, QueryData value)
        {
            // Important for compatibility with some clients that assume absent data field if data is null
            return value == null || (value instanceof RawQueryData && value.getData() == null);
        }
    }

    private static class SegmentSerializer
            extends StdSerializer<Segment>
    {
        protected SegmentSerializer()
        {
            super(Segment.class);
        }

        @Override
        public void serialize(Segment value, JsonGenerator gen, SerializerProvider provider)
                throws IOException
        {
            createSerializer(provider, provider.constructSpecializedType(provider.constructType(Segment.class), value.getClass()))
                    .serializeWithType(value, gen, provider, segmentSerializer(provider));
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
