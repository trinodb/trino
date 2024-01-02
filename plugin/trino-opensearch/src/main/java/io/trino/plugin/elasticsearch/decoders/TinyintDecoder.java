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
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TinyintDecoder
        implements Decoder
{
    private final String path;

    public TinyintDecoder(String path)
    {
        this.path = requireNonNull(path, "path is null");
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        Object value = getter.get();
        if (value == null) {
            output.appendNull();
            return;
        }

        long decoded;
        if (value instanceof Number number) {
            decoded = number.longValue();
        }
        else if (value instanceof String stringValue) {
            if (stringValue.isEmpty()) {
                output.appendNull();
                return;
            }
            try {
                decoded = Long.parseLong(stringValue);
            }
            catch (NumberFormatException e) {
                throw new TrinoException(TYPE_MISMATCH, format("Cannot parse value for field '%s' as TINYINT: %s", path, value));
            }
        }
        else {
            throw new TrinoException(TYPE_MISMATCH, format("Expected a numeric value for field '%s' of type TINYINT: %s [%s]", path, value, value.getClass().getSimpleName()));
        }

        if (decoded < Byte.MIN_VALUE || decoded > Byte.MAX_VALUE) {
            throw new TrinoException(TYPE_MISMATCH, format("Value out of range for field '%s' of type TINYINT: %s", path, decoded));
        }

        TINYINT.writeLong(output, decoded);
    }

    public static class Descriptor
            implements DecoderDescriptor
    {
        private final String path;

        @JsonCreator
        public Descriptor(String path)
        {
            this.path = path;
        }

        @JsonProperty
        public String getPath()
        {
            return path;
        }

        @Override
        public Decoder createDecoder()
        {
            return new TinyintDecoder(path);
        }
    }
}
