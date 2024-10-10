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

import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.ResultRows;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.JsonDecodingUtils.TypeDecoder;

import java.io.InputStream;
import java.util.List;

import static io.trino.client.spooling.encoding.JsonDecodingUtils.createTypeDecoders;
import static java.util.Objects.requireNonNull;

public class JsonQueryDataDecoder
        implements QueryDataDecoder
{
    private final TypeDecoder[] decoders;

    JsonQueryDataDecoder(TypeDecoder[] decoders)
    {
        this.decoders = requireNonNull(decoders, "decoders is null");
    }

    @Override
    public ResultRows decode(InputStream stream, DataAttributes queryAttributes)
    {
        return new JsonResultRows(decoders, stream);
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
            return new JsonQueryDataDecoder(createTypeDecoders(columns));
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
}
