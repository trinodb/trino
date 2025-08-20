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
package io.trino.decoder.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public interface JsonPayloadProvider
{
    ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    default JsonNode provide(InputStream inputStream)
            throws IOException
    {
        return OBJECT_MAPPER.readTree(inputStream);
    }

    default JsonNode provide(byte[] data)
            throws IOException
    {
        return provide(new ByteArrayInputStream(data));
    }
}
