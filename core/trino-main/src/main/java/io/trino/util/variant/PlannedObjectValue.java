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

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.variant.VariantEncoder.encodeObjectHeading;
import static io.trino.spi.variant.VariantEncoder.encodedObjectSize;
import static io.trino.util.variant.PrimitiveMapVariantWriter.determineWriteOrder;
import static java.util.Objects.requireNonNull;

final class PlannedObjectValue
        implements VariantWriter.PlannedValue
{
    // Initially, field IDs are provisional and must be remapped in finalize()
    // This array is mutated in finalize()
    private final int[] fieldIds;
    private final VariantWriter.PlannedValue[] values;

    private int size = -1;

    PlannedObjectValue(int[] fieldIds, VariantWriter.PlannedValue[] values)
    {
        this.fieldIds = requireNonNull(fieldIds, "fieldIds is null");
        this.values = requireNonNull(values, "values is null");
        checkArgument(fieldIds.length == values.length, "fieldIds length %s does not match values length %s", fieldIds.length, values.length);
    }

    @Override
    public void finalize(IntUnaryOperator sortedFieldIdMapping)
    {
        if (size >= 0) {
            throw new IllegalStateException("finalize() already called");
        }

        int maxFieldId = -1;
        for (int i = 0; i < fieldIds.length; i++) {
            int finalFieldId = sortedFieldIdMapping.applyAsInt(fieldIds[i]);
            fieldIds[i] = finalFieldId;
            maxFieldId = Math.max(maxFieldId, finalFieldId);
        }

        int totalElementLength = 0;
        for (VariantWriter.PlannedValue plannedValue : values) {
            plannedValue.finalize(sortedFieldIdMapping);
            totalElementLength += plannedValue.size();
        }

        size = encodedObjectSize(maxFieldId, fieldIds.length, totalElementLength);
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
        if (size < 0) {
            throw new IllegalStateException("finalize() must be called before write()");
        }

        int[] writeOrder = determineWriteOrder(fieldIds);

        int written = encodeObjectHeading(
                fieldIds.length,
                i -> fieldIds[writeOrder[i]],
                i -> values[writeOrder[i]].size(),
                out,
                offset);

        for (int entry : writeOrder) {
            written += values[entry].write(out, offset + written);
        }

        return written;
    }
}
