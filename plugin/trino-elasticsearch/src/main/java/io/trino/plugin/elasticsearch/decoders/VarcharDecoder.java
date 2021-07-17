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
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;

import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

public class VarcharDecoder
        extends AbstractDecoder<String>
{
    public VarcharDecoder()
    {
        super(VARCHAR);
    }

    @Override
    public String convert(String path, Object value)
    {
        if (value instanceof String || value instanceof Number) {
            return value.toString();
        }
        throw new TrinoException(TYPE_MISMATCH, format("Expected a string or numeric value for field '%s' of type VARCHAR: %s [%s]", path, value, value.getClass().getSimpleName()));
    }

    @Override
    public void write(BlockBuilder output, String value)
    {
        VARCHAR.writeSlice(output, Slices.utf8Slice(value));
    }
}
