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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.elasticsearch.DecoderDescriptor;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.List;
import java.util.function.Supplier;

public class ArrayDecoder
        implements Decoder
{
    private final Decoder elementDecoder;

    public ArrayDecoder(Decoder elementDecoder)
    {
        this.elementDecoder = elementDecoder;
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        Object data = getter.get();

        if (data == null) {
            output.appendNull();
        }
        else if (data instanceof List<?> list) {
            ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> list.forEach(element -> elementDecoder.decode(hit, () -> element, elementBuilder)));
        }
        else {
            ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> elementDecoder.decode(hit, () -> data, elementBuilder));
        }
    }

    public static class Descriptor
            implements DecoderDescriptor
    {
        private final DecoderDescriptor elementDescriptor;

        @JsonCreator
        public Descriptor(DecoderDescriptor elementDescriptor)
        {
            this.elementDescriptor = elementDescriptor;
        }

        @JsonProperty
        public DecoderDescriptor getElementDescriptor()
        {
            return elementDescriptor;
        }

        @Override
        public Decoder createDecoder()
        {
            return new ArrayDecoder(elementDescriptor.createDecoder());
        }
    }
}
