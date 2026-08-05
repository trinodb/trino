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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.airlift.units.DataSize;

import java.io.IOException;

public class DataSizeSerializer
        extends JsonSerializer<DataSize>
{
    public static final String SUCCINCT_DATA_SIZE_ENABLED = "dataSize.succinct.enabled";

    @Override
    public void serialize(DataSize dataSize, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException
    {
        if (dataSize == null) {
            jsonGenerator.writeNull();
            return;
        }

        if (Boolean.TRUE.equals(serializerProvider.getAttribute(SUCCINCT_DATA_SIZE_ENABLED))) {
            jsonGenerator.writeString(dataSize.succinct().toString());
            return;
        }
        jsonGenerator.writeString(dataSize.toBytesValueString());
    }
}
