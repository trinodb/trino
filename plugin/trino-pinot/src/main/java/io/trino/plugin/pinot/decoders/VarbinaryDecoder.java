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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static java.lang.String.format;

public class VarbinaryDecoder
        implements Decoder
{
    @Override
    public void decode(Supplier<Object> getter, BlockBuilder output)
    {
        Object value = getter.get();
        if (value == null) {
            output.appendNull();
        }
        else if (value instanceof String) {
            Slice slice = Slices.wrappedBuffer(toBytes((String) value));
            output.writeBytes(slice, 0, slice.length()).closeEntry();
        }
        else {
            throw new TrinoException(TYPE_MISMATCH, format("Expected a string value of type VARBINARY: %s [%s]", value, value.getClass().getSimpleName()));
        }
    }

    public static byte[] toBytes(String stringValue)
    {
        try {
            return Hex.decodeHex(stringValue.toCharArray());
        }
        catch (DecoderException e) {
            throw new IllegalArgumentException("Value: " + stringValue + " is not Hex encoded", e);
        }
    }
}
