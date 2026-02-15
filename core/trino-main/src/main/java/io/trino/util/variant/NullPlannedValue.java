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
import io.trino.util.variant.VariantWriter.PlannedValue;

import java.util.function.IntUnaryOperator;

import static io.trino.spi.variant.VariantEncoder.ENCODED_NULL_SIZE;
import static io.trino.spi.variant.VariantEncoder.encodeNull;

final class NullPlannedValue
        implements PlannedValue
{
    static final PlannedValue NULL_PLANNED_VALUE = new NullPlannedValue();

    private NullPlannedValue() {}

    @Override
    public void finalize(IntUnaryOperator sortedFieldIdMapping) {}

    @Override
    public int size()
    {
        return ENCODED_NULL_SIZE;
    }

    @Override
    public int write(Slice out, int offset)
    {
        return encodeNull(out, offset);
    }
}
