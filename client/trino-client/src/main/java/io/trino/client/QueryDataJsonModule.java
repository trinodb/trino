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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.List;

public class QueryDataJsonModule
        extends SimpleModule
{
    private static final TypeReference<Iterable<List<Object>>> LEGACY_FORMAT_REFERENCE = new TypeReference<Iterable<List<Object>>>(){};
    private static final TypeReference<EncodedQueryData> ENCODED_FORMAT_REFERENCE = new TypeReference<EncodedQueryData>(){};

    public QueryDataJsonModule()
    {
        super(QueryDataJsonModule.class.getSimpleName(), Version.unknownVersion());

        addSerializer(QueryData.class, new Serializer());
        addDeserializer(QueryData.class, new Deserializer());
    }

    private static class Deserializer
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
            // If this is not JSON_ARRAY we are dealing with chunked data
            if (!jsonParser.currentToken().equals(JsonToken.START_ARRAY)) {
                return jsonParser.readValueAs(ENCODED_FORMAT_REFERENCE);
            }
            return LegacyQueryData.create(jsonParser.readValueAs(LEGACY_FORMAT_REFERENCE));
        }
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
            if (value == null) {
                provider.defaultSerializeNull(generator);
                return;
            }

            if (value instanceof LegacyQueryData) {
                provider.defaultSerializeValue(value.getData(), generator);
                return;
            }

            if (value instanceof EncodedQueryData) {
                createEncodedSerializer(provider).serialize(value, generator, provider);
                return;
            }

            throw new IllegalArgumentException("Unknown QueryData implementation encountered: " + value.getClass().getSimpleName());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> JsonSerializer<T> createEncodedSerializer(SerializerProvider provider)
            throws JsonMappingException
    {
        return (JsonSerializer<T>) BeanSerializerFactory.instance.createSerializer(provider, provider.constructType(EncodedQueryData.class));
    }
}
