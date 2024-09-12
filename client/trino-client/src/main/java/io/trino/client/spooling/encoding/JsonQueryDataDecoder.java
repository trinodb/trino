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
package io.trino.client.spooling.encoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttributes;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

import static io.trino.client.FixJsonDataUtils.fixData;
import static java.util.Objects.requireNonNull;

public class JsonQueryDataDecoder
        implements QueryDataDecoder
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<List<List<Object>>> TYPE = new TypeReference<List<List<Object>>>() {};
    private final List<Column> columns;

    public JsonQueryDataDecoder(List<Column> columns)
    {
        this.columns = requireNonNull(columns, "columns is null");
    }

    @Override
    public Iterable<List<Object>> decode(InputStream stream, DataAttributes attributes)
    {
        try {
            return fixData(columns, OBJECT_MAPPER.readValue(stream, TYPE));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String encoding()
    {
        return "json";
    }

    public static class Factory
            implements QueryDataDecoder.Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            return new JsonQueryDataDecoder(columns);
        }

        @Override
        public String encoding()
        {
            return "json";
        }
    }

    public static class ZstdFactory
            extends Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            return new ZstdQueryDataDecoder(super.create(columns, queryAttributes));
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+zstd";
        }
    }

    public static class Lz4Factory
            extends Factory
    {
        @Override
        public QueryDataDecoder create(List<Column> columns, DataAttributes queryAttributes)
        {
            return new Lz4QueryDataDecoder(super.create(columns, queryAttributes));
        }

        @Override
        public String encoding()
        {
            return super.encoding() + "+lz4";
        }
    }

    public static class JsonSchema
    {
        private final int[] offsets;
        private final int step;

        @JsonCreator
        public JsonSchema(int[] offsets, int step)
        {
            this.offsets = offsets;
            this.step = step;
        }

        @JsonProperty("offsets")
        public int[] getOffsets()
        {
            return offsets;
        }

        @JsonProperty("step")
        public int getStep()
        {
            return step;
        }
    }
}
