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
package io.trino.plugin.thrift.api.datatypes;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.thrift.api.TrinoThriftBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.thrift.api.datatypes.TrinoThriftTypeUtils.calculateOffsets;
import static io.trino.plugin.thrift.api.datatypes.TrinoThriftTypeUtils.sameSizeIfPresent;
import static io.trino.plugin.thrift.api.datatypes.TrinoThriftTypeUtils.totalSize;

final class SliceData
        implements TrinoThriftColumnData
{
    private final boolean[] nulls;
    private final int[] sizes;
    private final byte[] bytes;

    public SliceData(@Nullable boolean[] nulls, @Nullable int[] sizes, @Nullable byte[] bytes)
    {
        checkArgument(sameSizeIfPresent(nulls, sizes), "nulls and values must be of the same size");
        checkArgument(totalSize(nulls, sizes) == (bytes != null ? bytes.length : 0), "total bytes size doesn't match expected size");
        this.nulls = nulls;
        this.sizes = sizes;
        this.bytes = bytes;
    }

    @Nullable
    public boolean[] getNulls()
    {
        return nulls;
    }

    @Nullable
    public int[] getSizes()
    {
        return sizes;
    }

    @Nullable
    public byte[] getBytes()
    {
        return bytes;
    }

    @Override
    public Block toBlock(Type desiredType)
    {
        checkArgument(desiredType.getJavaType() == Slice.class, "type doesn't match: %s", desiredType);
        Slice values = bytes == null ? Slices.EMPTY_SLICE : Slices.wrappedBuffer(bytes);
        int numberOfRecords = numberOfRecords();
        return new VariableWidthBlock(
                numberOfRecords,
                values,
                calculateOffsets(sizes, nulls, numberOfRecords),
                Optional.ofNullable(nulls));
    }

    @Override
    public int numberOfRecords()
    {
        if (nulls != null) {
            return nulls.length;
        }
        if (sizes != null) {
            return sizes.length;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SliceData other = (SliceData) obj;
        return Arrays.equals(this.nulls, other.nulls) &&
                Arrays.equals(this.sizes, other.sizes) &&
                Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(nulls), Arrays.hashCode(sizes), Arrays.hashCode(bytes));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRecords", numberOfRecords())
                .toString();
    }

    public static TrinoThriftBlock fromSliceBasedBlock(Block block, Type type, CreateSliceThriftBlockFunction create)
    {
        int positions = block.getPositionCount();
        if (positions == 0) {
            return create.apply(null, null, null);
        }
        boolean[] nulls = null;
        int[] sizes = null;
        byte[] bytes = null;
        int bytesIndex = 0;
        for (int position = 0; position < positions; position++) {
            if (block.isNull(position)) {
                if (nulls == null) {
                    nulls = new boolean[positions];
                }
                nulls[position] = true;
            }
            else {
                Slice value = type.getSlice(block, position);
                if (sizes == null) {
                    sizes = new int[positions];
                    int totalBytes = totalSliceBytes(block);
                    if (totalBytes > 0) {
                        bytes = new byte[totalBytes];
                    }
                }
                int length = value.length();
                sizes[position] = length;
                if (length > 0) {
                    checkState(bytes != null);
                    value.getBytes(0, bytes, bytesIndex, length);
                    bytesIndex += length;
                }
            }
        }
        checkState(bytes == null || bytesIndex == bytes.length);
        return create.apply(nulls, sizes, bytes);
    }

    private static int totalSliceBytes(Block block)
    {
        int totalBytes = 0;
        int positions = block.getPositionCount();
        for (int position = 0; position < positions; position++) {
            totalBytes += block.getSliceLength(position);
        }
        return totalBytes;
    }

    public interface CreateSliceThriftBlockFunction
    {
        TrinoThriftBlock apply(boolean[] nulls, int[] sizes, byte[] bytes);
    }
}
