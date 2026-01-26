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
package io.trino.server;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import tools.jackson.core.Base64Variants;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;

public final class SliceSerialization
{
    private SliceSerialization() {}

    public static class SliceSerializer
            extends ValueSerializer<Slice>
    {
        @Override
        public void serialize(Slice slice, JsonGenerator jsonGenerator, SerializationContext context)
        {
            jsonGenerator.writeBinary(Base64Variants.MIME_NO_LINEFEEDS, slice.byteArray(), slice.byteArrayOffset(), slice.length());
        }
    }

    public static class SliceDeserializer
            extends ValueDeserializer<Slice>
    {
        @Override
        public Slice deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        {
            return Slices.wrappedBuffer(jsonParser.getBinaryValue(Base64Variants.MIME_NO_LINEFEEDS));
        }
    }
}
