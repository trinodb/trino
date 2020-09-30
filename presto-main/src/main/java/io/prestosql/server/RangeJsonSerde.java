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
package io.prestosql.server;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.type.Type;

import java.io.IOException;
import java.util.Optional;

import static io.prestosql.spi.predicate.Marker.Bound.EXACTLY;

public class RangeJsonSerde
{
    private RangeJsonSerde() {}

    public static class Serializer
            extends JsonSerializer<Range>
    {
        @Override
        public void serialize(Range range, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            jsonGenerator.writeNumber(range.isSingleValue() ? 1 : 0);
            if (range.isSingleValue()) {
                serializerProvider.findValueSerializer(Type.class).serialize(range.getType(), jsonGenerator, serializerProvider);
                serializerProvider.findValueSerializer(Block.class).serialize(range.getLow().getValueBlock().get(), jsonGenerator, serializerProvider);
                return;
            }

            JsonSerializer<Object> markerSerializer = serializerProvider.findValueSerializer(Marker.class);
            markerSerializer.serialize(range.getLow(), jsonGenerator, serializerProvider);
            markerSerializer.serialize(range.getHigh(), jsonGenerator, serializerProvider);
        }
    }

    public static class Deserializer
            extends JsonDeserializer<Range>
    {
        @Override
        public Range deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            boolean isSingleValue = jsonParser.getByteValue() == 1;
            jsonParser.nextToken();
            if (isSingleValue) {
                Type type = deserializationContext.readValue(jsonParser, Type.class);
                jsonParser.nextToken();
                Block block = deserializationContext.readValue(jsonParser, Block.class);
                Marker marker = new Marker(type, Optional.of(block), EXACTLY);
                return new Range(marker, marker);
            }

            Marker low = deserializationContext.readValue(jsonParser, Marker.class);
            jsonParser.nextToken();
            Marker high = deserializationContext.readValue(jsonParser, Marker.class);
            return new Range(low, high);
        }
    }
}
