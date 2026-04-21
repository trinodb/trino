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
package io.trino.util.variant;

import io.airlift.slice.Slice;

import java.util.function.IntUnaryOperator;

import static io.trino.spi.variant.VariantEncoder.encodeArrayHeading;
import static io.trino.spi.variant.VariantEncoder.encodedArraySize;

final class PlannedArrayValue
        implements VariantWriter.PlannedValue
{
    private final VariantWriter.PlannedValue[] elements;
    private int size = -1;

    PlannedArrayValue(VariantWriter.PlannedValue[] elements)
    {
        this.elements = elements;
    }

    @Override
    public void finalize(IntUnaryOperator sortedFieldIdMapping)
    {
        if (size >= 0) {
            throw new IllegalStateException("finalize() already called");
        }

        int totalElementLength = 0;
        for (VariantWriter.PlannedValue element : elements) {
            element.finalize(sortedFieldIdMapping);
            totalElementLength += element.size();
        }
        size = encodedArraySize(elements.length, totalElementLength);
    }

    @Override
    public int size()
    {
        if (size < 0) {
            throw new IllegalStateException("finalize() must be called before size()");
        }
        return size;
    }

    @Override
    public int write(Slice out, int offset)
    {
        int count = elements.length;
        int headerSize = encodeArrayHeading(count, i -> elements[i].size(), out, offset);

        int written = headerSize;
        for (int i = 0; i < count; i++) {
            written += elements[i].write(out, offset + written);
        }
        return written;
    }
}
