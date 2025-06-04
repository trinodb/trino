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

package io.trino.operator.aggregation.histogram;

import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class SingleHistogramState
        implements HistogramState
{
    private static final int INSTANCE_SIZE = instanceSize(SingleHistogramState.class);

    private final Type keyType;
    private final MethodHandle readFlat;
    private final MethodHandle writeFlat;
    private final MethodHandle hashFlat;
    private final MethodHandle identicalFlatBlock;
    private final MethodHandle hashBlock;
    private TypedHistogram typedHistogram;
    private SqlMap tempSerializedState;

    public SingleHistogramState(
            Type keyType,
            MethodHandle readFlat,
            MethodHandle writeFlat,
            MethodHandle hashFlat,
            MethodHandle identicalFlatBlock,
            MethodHandle hashBlock)
    {
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.readFlat = requireNonNull(readFlat, "readFlat is null");
        this.writeFlat = requireNonNull(writeFlat, "writeFlat is null");
        this.hashFlat = requireNonNull(hashFlat, "hashFlat is null");
        this.identicalFlatBlock = requireNonNull(identicalFlatBlock, "identicalFlatBlock is null");
        this.hashBlock = requireNonNull(hashBlock, "hashBlock is null");
    }

    @Override
    public void add(ValueBlock block, int position, long count)
    {
        if (typedHistogram == null) {
            typedHistogram = new TypedHistogram(keyType, readFlat, writeFlat, hashFlat, identicalFlatBlock, hashBlock, false);
        }
        typedHistogram.add(0, block, position, count);
    }

    @Override
    public void writeAll(MapBlockBuilder out)
    {
        if (typedHistogram == null) {
            out.appendNull();
            return;
        }
        typedHistogram.serialize(0, out);
    }

    @Override
    public long getEstimatedSize()
    {
        long estimatedSize = INSTANCE_SIZE;

        if (typedHistogram != null) {
            estimatedSize += typedHistogram.getEstimatedSize();
        }
        return estimatedSize;
    }

    void setTempSerializedState(SqlMap tempSerializedState)
    {
        this.tempSerializedState = tempSerializedState;
    }

    SqlMap removeTempSerializedState()
    {
        SqlMap sqlMap = tempSerializedState;
        checkState(sqlMap != null, "tempDeserializeBlock is null");
        tempSerializedState = null;
        return sqlMap;
    }
}
