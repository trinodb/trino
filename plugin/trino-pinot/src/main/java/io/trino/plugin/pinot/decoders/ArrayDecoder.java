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
package io.trino.plugin.pinot.decoders;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.pinot.decoders.DecoderFactory.createDecoder;
import static java.util.Objects.requireNonNull;

public class ArrayDecoder
        implements Decoder
{
    private final ArrayType type;
    private final Decoder elementDecoder;

    public ArrayDecoder(Type type)
    {
        requireNonNull(type, "type is null");
        checkState(type instanceof ArrayType, "Unexpected type %s", type);
        this.type = (ArrayType) type;
        this.elementDecoder = createDecoder(this.type.getElementType());
    }

    @Override
    public void decode(Supplier<Object> getter, BlockBuilder output)
    {
        List<?> value = (List<?>) getter.get();
        if (value == null) {
            output.appendNull();
        }
        else {
            BlockBuilder elementBlockBuilder = type.getElementType().createBlockBuilder(null, 1);
            for (int i = 0; i < value.size(); i++) {
                int index = i;
                elementDecoder.decode(() -> value.get(index), elementBlockBuilder);
            }
            type.writeObject(output, elementBlockBuilder.build());
        }
    }
}
