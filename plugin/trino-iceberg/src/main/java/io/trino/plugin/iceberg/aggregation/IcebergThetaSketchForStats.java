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
package io.trino.plugin.iceberg.aggregation;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import org.apache.datasketches.Family;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.types.Conversions;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.io.ByteBuffers.getBytes;
import static io.trino.plugin.iceberg.IcebergTypes.convertTrinoValueToIceberg;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergTypeForNewColumn;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.util.Objects.requireNonNull;

@AggregationFunction(value = IcebergThetaSketchForStats.NAME, hidden = true)
public final class IcebergThetaSketchForStats
{
    private IcebergThetaSketchForStats() {}

    public static final String NAME = "$iceberg_theta_stat";

    @InputFunction
    @TypeParameter("T")
    public static void input(@TypeParameter("T") Type type, @AggregationState DataSketchState state, @BlockPosition @SqlType("T") Block block, @BlockIndex int index)
    {
        verify(!block.isNull(index), "Input function is not expected to be called on a NULL input");

        Object trinoValue = readNativeValue(type, block, index);
        org.apache.iceberg.types.Type icebergType = toIcebergTypeForNewColumn(type, new AtomicInteger(1));
        Object icebergValue = convertTrinoValueToIceberg(type, trinoValue);
        ByteBuffer byteBuffer = Conversions.toByteBuffer(icebergType, icebergValue);
        requireNonNull(byteBuffer, "byteBuffer is null"); // trino value isn't null
        byte[] bytes = getBytes(byteBuffer);
        getOrCreateUpdateSketch(state).update(bytes);
    }

    @CombineFunction
    public static void combine(@AggregationState DataSketchState state, @AggregationState DataSketchState otherState)
    {
        Union union = SetOperation.builder().buildUnion();
        addIfPresent(union, state.getUpdateSketch());
        addIfPresent(union, state.getCompactSketch());
        addIfPresent(union, otherState.getUpdateSketch());
        addIfPresent(union, otherState.getCompactSketch());

        state.setUpdateSketch(null);
        state.setCompactSketch(union.getResult());
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(@AggregationState DataSketchState state, BlockBuilder out)
    {
        if (state.getUpdateSketch() == null && state.getCompactSketch() == null) {
            getOrCreateUpdateSketch(state);
        }
        DataSketchStateSerializer.serializeToVarbinary(state, out);
    }

    private static UpdateSketch getOrCreateUpdateSketch(@AggregationState DataSketchState state)
    {
        UpdateSketch sketch = state.getUpdateSketch();
        if (sketch == null) {
            // Must match Iceberg table statistics specification
            // https://iceberg.apache.org/puffin-spec/#apache-datasketches-theta-v1-blob-type
            sketch = UpdateSketch.builder()
                    .setFamily(Family.ALPHA)
                    .build();
            state.setUpdateSketch(sketch);
        }
        return sketch;
    }

    private static void addIfPresent(Union union, @Nullable Sketch input)
    {
        if (input != null) {
            union.union(input);
        }
    }
}
