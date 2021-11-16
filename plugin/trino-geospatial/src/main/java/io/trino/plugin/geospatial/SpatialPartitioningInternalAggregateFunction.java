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
package io.trino.plugin.geospatial;

import com.esri.core.geometry.Envelope;
import io.airlift.slice.Slice;
import io.trino.geospatial.KdbTreeUtils;
import io.trino.geospatial.Rectangle;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.trino.geospatial.KdbTree.buildKdbTree;
import static io.trino.geospatial.serde.GeometrySerde.deserializeEnvelope;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static io.trino.plugin.geospatial.SpatialPartitioningAggregateFunction.NAME;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;

@AggregationFunction(value = NAME, decomposable = false, hidden = true)
public final class SpatialPartitioningInternalAggregateFunction
{
    private static final int MAX_SAMPLE_COUNT = 1_000_000;

    private SpatialPartitioningInternalAggregateFunction() {}

    @InputFunction
    public static void input(SpatialPartitioningState state, @SqlType(GEOMETRY_TYPE_NAME) Slice slice, @SqlType(INTEGER) long partitionCount)
    {
        Envelope envelope = deserializeEnvelope(slice);
        if (envelope.isEmpty()) {
            return;
        }

        if (state.getCount() == 0) {
            state.setPartitionCount(toIntExact(partitionCount));
            state.setSamples(new ArrayList<>());
        }

        // use reservoir sampling
        Rectangle extent = new Rectangle(envelope.getXMin(), envelope.getYMin(), envelope.getXMax(), envelope.getYMax());
        List<Rectangle> samples = state.getSamples();
        if (samples.size() <= MAX_SAMPLE_COUNT) {
            samples.add(extent);
        }
        else {
            long sampleIndex = ThreadLocalRandom.current().nextLong(state.getCount());
            if (sampleIndex < MAX_SAMPLE_COUNT) {
                samples.set(toIntExact(sampleIndex), extent);
            }
        }

        state.setCount(state.getCount() + 1);
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(SpatialPartitioningState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
            return;
        }

        List<Rectangle> samples = state.getSamples();

        int partitionCount = state.getPartitionCount();
        int maxItemsPerNode = (samples.size() + partitionCount - 1) / partitionCount;
        VARCHAR.writeString(out, KdbTreeUtils.toJson(buildKdbTree(maxItemsPerNode, samples)));
    }
}
