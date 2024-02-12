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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.trino.client.spooling.EncodedQueryData;

import java.io.IOException;
import java.util.List;

/**
 * Decodes the direct and encoded protocols.
 *
 * If the "data" fields starts with an array - this is the direct protocol which requires reading values and wrapping them with a class.
 *
 * Otherwise, this is an encoded protocol.
 */
public class QueryDataClientJacksonModule
        extends SimpleModule
{
    private static final TypeReference<Iterable<List<Object>>> DIRECT_FORMAT = new TypeReference<Iterable<List<Object>>>(){};
    private static final TypeReference<EncodedQueryData> ENCODED_FORMAT = new TypeReference<EncodedQueryData>(){};

    public QueryDataClientJacksonModule()
    {
        super(QueryDataClientJacksonModule.class.getSimpleName(), Version.unknownVersion());
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
            // If this is not JSON_ARRAY we are dealing with direct data encoding
            if (jsonParser.currentToken().equals(JsonToken.START_ARRAY)) {
                return RawQueryData.of(jsonParser.readValueAs(DIRECT_FORMAT));
            }
            return jsonParser.readValueAs(ENCODED_FORMAT);
        }
    }
}
