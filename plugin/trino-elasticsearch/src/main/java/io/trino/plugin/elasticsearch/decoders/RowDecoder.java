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

import java.util.Map;
import java.util.function.Supplier;

import static io.trino.plugin.elasticsearch.ScanQueryPageSource.getField;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowDecoder
        implements Decoder
{
    private final Map<String, Decoder> elementsDecoder;

    public RowDecoder(Map<String, Decoder> elementsDecoder)
    {
        this.elementsDecoder = elementsDecoder;
    }

    @Override
    public void decode(String path, Supplier<Object> getter, BlockBuilder output)
    {
        requireNonNull(path, "path is null");
        Object data = getter.get();
        if (data == null) {
            output.appendNull();
        }
        else if (data instanceof Map) {
            BlockBuilder row = output.beginBlockEntry();
            elementsDecoder.entrySet().stream()
                    .forEach(elementDecoder -> elementDecoder.getValue().decode(elementDecoder.getKey(), () -> getField((Map<String, Object>) data, elementDecoder.getKey()), row));
            output.closeEntry();
        }
        else {
            throw new TrinoException(TYPE_MISMATCH, format("Expected object for field '%s' of type ROW: %s [%s]", path, data, data.getClass().getSimpleName()));
        }
    }
}
