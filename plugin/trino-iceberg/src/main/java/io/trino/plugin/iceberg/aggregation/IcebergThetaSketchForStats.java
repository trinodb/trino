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

import com.google.common.math.LongMath;
import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.Fixed12Block;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import jakarta.annotation.Nullable;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.theta.ThetaSetOperation;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.ThetaUnion;
import org.apache.datasketches.theta.UpdatableThetaSketch;

import static io.trino.plugin.iceberg.util.Timestamps.timestampToNanos;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Math.toIntExact;
import static java.math.RoundingMode.UNNECESSARY;

@AggregationFunction(value = IcebergThetaSketchForStats.NAME, hidden = true)
public final class IcebergThetaSketchForStats
{
    private IcebergThetaSketchForStats() {}

    public static final String NAME = "$iceberg_theta_stat";

    private static final byte[] TRUE_BYTES = {1};
    private static final byte[] FALSE_BYTES = {0};

    // The sketch must be fed with the Iceberg single-value serialization of the value, which depends on the type.
    // The sketch hashes update(long) like 8 little-endian bytes and update(int[]) like 4 little-endian bytes
    // per element, so most of the types do not need to be serialized.

    @InputFunction
    public static void inputUnknown(@AggregationState DataSketchState state, @BlockPosition @SqlType("unknown") ValueBlock block, @BlockIndex int position)
    {
        // unknown type is always NULL
    }

    @InputFunction
    public static void inputBigint(@AggregationState DataSketchState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        getOrCreateUpdateSketch(state).update(value);
    }

    @InputFunction
    public static void inputInteger(@AggregationState DataSketchState state, @SqlType(StandardTypes.INTEGER) long value)
    {
        getOrCreateUpdateSketch(state).update(new int[] {toIntExact(value)});
    }

    @InputFunction
    public static void inputDate(@AggregationState DataSketchState state, @SqlType(StandardTypes.DATE) long value)
    {
        getOrCreateUpdateSketch(state).update(new int[] {toIntExact(value)});
    }

    @InputFunction
    public static void inputReal(@AggregationState DataSketchState state, @SqlType(StandardTypes.REAL) long value)
    {
        getOrCreateUpdateSketch(state).update(new int[] {toIntExact(value)});
    }

    @InputFunction
    public static void inputDouble(@AggregationState DataSketchState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        getOrCreateUpdateSketch(state).update(doubleToRawLongBits(value));
    }

    @InputFunction
    public static void inputBoolean(@AggregationState DataSketchState state, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        if (value) {
            getOrCreateUpdateSketch(state).update(TRUE_BYTES);
        }
        else {
            getOrCreateUpdateSketch(state).update(FALSE_BYTES);
        }
    }

    @InputFunction
    @LiteralParameters("x")
    public static void inputVarchar(
            @AggregationState DataSketchState state,
            @BlockPosition @SqlType(value = "varchar(x)", nativeContainerType = Slice.class) VariableWidthBlock block,
            @BlockIndex int position)
    {
        getOrCreateUpdateSketch(state).update(block.getRawSlice().toByteBuffer(block.getRawSliceOffset(position), block.getSliceLength(position)));
    }

    @InputFunction
    public static void inputVarbinary(
            @AggregationState DataSketchState state,
            @BlockPosition @SqlType(value = StandardTypes.VARBINARY, nativeContainerType = Slice.class) VariableWidthBlock block,
            @BlockIndex int position)
    {
        getOrCreateUpdateSketch(state).update(block.getRawSlice().toByteBuffer(block.getRawSliceOffset(position), block.getSliceLength(position)));
    }

    @InputFunction
    public static void inputUuid(@AggregationState DataSketchState state, @SqlType(StandardTypes.UUID) Slice value)
    {
        getOrCreateUpdateSketch(state).update(value.toByteBuffer());
    }

    @InputFunction
    public static void inputTime(@AggregationState DataSketchState state, @SqlType("time(6)") long value)
    {
        getOrCreateUpdateSketch(state).update(LongMath.divide(value, PICOSECONDS_PER_MICROSECOND, UNNECESSARY));
    }

    @InputFunction
    public static void inputTimestamp(@AggregationState DataSketchState state, @SqlType("timestamp(6)") long value)
    {
        getOrCreateUpdateSketch(state).update(value);
    }

    @InputFunction
    public static void inputTimestampWithTimeZone(
            @AggregationState DataSketchState state,
            @BlockPosition @SqlType(value = "timestamp(6) with time zone", nativeContainerType = LongTimestampWithTimeZone.class) Fixed12Block block,
            @BlockIndex int position)
    {
        getOrCreateUpdateSketch(state).update(timestampTzToMicros(unpackMillisUtc(block.getFixed12First(position)), block.getFixed12Second(position)));
    }

    @InputFunction
    public static void inputTimestampNanos(
            @AggregationState DataSketchState state,
            @BlockPosition @SqlType(value = "timestamp(9)", nativeContainerType = LongTimestamp.class) Fixed12Block block,
            @BlockIndex int position)
    {
        getOrCreateUpdateSketch(state).update(timestampToNanos(block.getFixed12First(position), block.getFixed12Second(position)));
    }

    @InputFunction
    public static void inputTimestampWithTimeZoneNanos(
            @AggregationState DataSketchState state,
            @BlockPosition @SqlType(value = "timestamp(9) with time zone", nativeContainerType = LongTimestampWithTimeZone.class) Fixed12Block block,
            @BlockIndex int position)
    {
        getOrCreateUpdateSketch(state).update(timestampTzToNanos(unpackMillisUtc(block.getFixed12First(position)), block.getFixed12Second(position)));
    }

    @CombineFunction
    public static void combine(@AggregationState DataSketchState state, @AggregationState DataSketchState otherState)
    {
        ThetaUnion union = ThetaSetOperation.builder().buildUnion();
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

    static UpdatableThetaSketch getOrCreateUpdateSketch(DataSketchState state)
    {
        UpdatableThetaSketch sketch = state.getUpdateSketch();
        if (sketch == null) {
            // Must match Iceberg table statistics specification
            // https://iceberg.apache.org/puffin-spec/#apache-datasketches-theta-v1-blob-type
            sketch = UpdatableThetaSketch.builder()
                    .setFamily(Family.ALPHA)
                    .build();
            state.setUpdateSketch(sketch);
        }
        return sketch;
    }

    private static void addIfPresent(ThetaUnion union, @Nullable ThetaSketch input)
    {
        if (input != null) {
            union.union(input);
        }
    }
}
