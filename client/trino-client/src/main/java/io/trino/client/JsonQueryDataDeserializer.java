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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

import static io.trino.client.FixJsonDataUtils.fixData;
import static io.trino.client.QueryDataSerialization.JSON;

public class JsonQueryDataDeserializer
        implements QueryDataDeserializer
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<List<List<Object>>> TYPE = new TypeReference<List<List<Object>>>() {};

    @Nullable
    @Override
    public Iterable<List<Object>> deserialize(InputStream inputStream, List<Column> columns)
    {
        try {
            return fixData(columns, OBJECT_MAPPER.readValue(inputStream, TYPE));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public QueryDataSerialization getSerialization()
    {
        return JSON;
    }
}
