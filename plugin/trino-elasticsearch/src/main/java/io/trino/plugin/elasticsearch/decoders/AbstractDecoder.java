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

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public abstract class AbstractDecoder<OutputType>
        implements Decoder
{
    Type type;

    public AbstractDecoder(Type type)
    {
        this.type = type;
    }

    public Type getType()
    {
        return type;
    }

    /**
     * Converts object to {@link OutputType}
     */
    protected abstract OutputType convert(String path, Object value);

    /**
     * Writes the value to respective BlockBuilder
     */
    protected abstract void write(BlockBuilder output, OutputType value);

    @Override
    public void decode(String path, Supplier<Object> valueSupplier, BlockBuilder output)
    {
        requireNonNull(path, "path is null");
        final Object value = valueSupplier.get();
        if (value == null) {
            output.appendNull();
        }
        else {
            write(output, convert(path, value));
        }
    }
}
