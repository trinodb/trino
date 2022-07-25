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
package io.trino.plugin.elasticsearch.decoders;

import io.airlift.slice.Slices;
import io.trino.plugin.elasticsearch.DecoderDescriptor;
import io.trino.spi.block.BlockBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.function.Supplier;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class SourceColumnDecoder
        implements Decoder
{
    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        VARCHAR.writeSlice(output, Slices.utf8Slice(hit.getSourceAsString()));
    }

    public static class Descriptor
            implements DecoderDescriptor
    {
        @Override
        public Decoder createDecoder()
        {
            return new SourceColumnDecoder();
        }
    }
}
