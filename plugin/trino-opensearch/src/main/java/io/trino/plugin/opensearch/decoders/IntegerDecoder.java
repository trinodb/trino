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
package io.trino.plugin.opensearch.decoders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.opensearch.DecoderDescriptor;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import org.opensearch.search.SearchHit;

import java.util.Objects;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IntegerDecoder
        implements Decoder
{
    private final String path;

    public IntegerDecoder(String path)
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
                throw new TrinoException(TYPE_MISMATCH, format("Cannot parse value for field '%s' as INTEGER: %s", path, value));
            }
        }
        else {
            throw new TrinoException(TYPE_MISMATCH, format("Expected a numeric value for field '%s' of type INTEGER: %s [%s]", path, value, value.getClass().getSimpleName()));
        }

        if (decoded < Integer.MIN_VALUE || decoded > Integer.MAX_VALUE) {
            throw new TrinoException(TYPE_MISMATCH, format("Value out of range for field '%s' of type INTEGER: %s", path, decoded));
        }

        INTEGER.writeLong(output, decoded);
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
            return new IntegerDecoder(path);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Descriptor that = (Descriptor) o;
            return Objects.equals(this.path, that.path);
        }

        @Override
        public int hashCode()
        {
            return path.hashCode();
        }
    }
}
