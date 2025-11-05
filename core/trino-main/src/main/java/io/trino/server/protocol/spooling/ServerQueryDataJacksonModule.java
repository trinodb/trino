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
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.trino.client.QueryData;
import io.trino.client.QueryDataJacksonModule;
import io.trino.client.QueryDataJacksonModule.Deserializer;
import io.trino.client.QueryDataJacksonModule.SegmentSerializer;
import io.trino.client.spooling.Segment;
import io.trino.server.protocol.JsonBytesQueryData;

import java.io.IOException;

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
    public ServerQueryDataJacksonModule()
    {
        super(ServerQueryDataJacksonModule.class.getSimpleName(), Version.unknownVersion());
        addDeserializer(QueryData.class, new Deserializer());
        addSerializer(QueryData.class, new ServerQueryDataSerializer());
        addSerializer(Segment.class, new SegmentSerializer());
    }

    public static class ServerQueryDataSerializer
            extends QueryDataJacksonModule.QueryDataSerializer
    {
        @Override
        public void serialize(QueryData value, JsonGenerator generator, SerializerProvider provider)
                throws IOException
        {
            if (value instanceof JsonBytesQueryData jsonBytesQueryData) {
                jsonBytesQueryData.writeTo(generator);
            }
            else {
                super.serialize(value, generator, provider);
            }
        }
    }
}
