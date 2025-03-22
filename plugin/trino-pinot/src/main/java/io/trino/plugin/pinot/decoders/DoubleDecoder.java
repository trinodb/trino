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

import java.util.function.Supplier;

import static io.trino.spi.type.DoubleType.DOUBLE;

public class DoubleDecoder
        implements Decoder
{
    @Override
    public void decode(Supplier<Object> getter, BlockBuilder output)
    {
        Object value = getter.get();
        if (value == null) {
            output.appendNull();
        }
        else if (value instanceof String string) {
            // Pinot returns NEGATIVE_INFINITY, POSITIVE_INFINITY as a String
            DOUBLE.writeDouble(output, Double.valueOf(string));
        }
        else {
            DOUBLE.writeDouble(output, ((Number) value).doubleValue());
        }
    }
}
