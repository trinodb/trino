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
package io.trino.hive.formats;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.RowType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A SparseRowType is a RowType that conveys which fields are active in a row.
 * <p>
 * It manages the positional mapping between the "sparse" fields and the
 * underlying "dense" RowType. It allows position-based deserializers to know
 * the complete schema for a Row while also knowing which fields need to be
 * deserialized. Name-based deserializers can simply use the dense fields
 * from the underlying RowType.
 */
public class SparseRowType
        extends RowType
{
    private final List<Field> sparseFields;
    private final int[] offsets;

    private SparseRowType(List<Field> sparseFields, List<Field> denseFields, int[] offsets)
    {
        super(makeSignature(denseFields), denseFields);
        this.sparseFields = sparseFields;
        this.offsets = offsets;
    }

    /**
     * Create a SparseRowType from a list of fields and a mask indicating which fields are active.
     */
    public static SparseRowType from(List<Field> fields, boolean[] mask)
    {
        checkArgument(fields.size() == mask.length);

        int[] offsets = new int[fields.size()];
        ImmutableList.Builder<Field> denseFields = ImmutableList.builder();

        int offset = 0;
        for (int i = 0; i < mask.length; i++) {
            if (mask[i]) {
                denseFields.add(fields.get(i));
                offsets[i] = offset++;
            }
            else {
                offsets[i] = -1;
            }
        }

        return new SparseRowType(ImmutableList.copyOf(fields), denseFields.build(), offsets);
    }

    public static SparseRowType initial(List<Field> fields, Integer activeField)
    {
        boolean[] mask = new boolean[fields.size()];
        mask[activeField] = true;
        return SparseRowType.from(fields, mask);
    }

    public List<Field> getSparseFields()
    {
        return sparseFields;
    }

    /**
     * Get the offset to the dense field for the sparseField at sparsePosition.
     */
    public Integer getOffset(int sparsePosition)
    {
        return offsets[sparsePosition] >= 0
                ? offsets[sparsePosition]
                : null;
    }
}
