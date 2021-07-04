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

import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;

import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.lang.String.format;

public class DoubleDecoder
        extends AbstractDecoder<Double>
{
    public DoubleDecoder()
    {
        super(DOUBLE);
    }

    @Override
    public Double convert(String path, Object value)
    {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        else if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            }
            catch (NumberFormatException e) {
                throw new TrinoException(TYPE_MISMATCH, format("Cannot parse value for field '%s' as DOUBLE: %s", path, value));
            }
        }
        throw new TrinoException(TYPE_MISMATCH, format("Expected a numeric value for field %s of type DOUBLE: %s [%s]", path, value, value.getClass().getSimpleName()));
    }

    @Override
    public void write(BlockBuilder output, Double value)
    {
        DOUBLE.writeDouble(output, value);
    }
}
