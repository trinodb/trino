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
package io.prestosql.elasticsearch.decoders;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.function.Supplier;

import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.prestosql.spi.type.IntegerType.INTEGER;
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
        if (value instanceof Number) {
            decoded = ((Number) value).longValue();
        }
        else if (value instanceof String) {
            try {
                decoded = Long.parseLong((String) value);
            }
            catch (NumberFormatException e) {
                throw new PrestoException(TYPE_MISMATCH, format("Cannot parse value for field '%s' as INTEGER: %s", path, value));
            }
        }
        else {
            throw new PrestoException(TYPE_MISMATCH, format("Expected a numeric value for field '%s' of type INTEGER: %s [%s]", path, value, value.getClass().getSimpleName()));
        }

        if (decoded < Integer.MIN_VALUE || decoded > Integer.MAX_VALUE) {
            throw new PrestoException(TYPE_MISMATCH, format("Value out of range for field '%s' of type INTEGER: %s", path, decoded));
        }

        INTEGER.writeLong(output, decoded);
    }
}
