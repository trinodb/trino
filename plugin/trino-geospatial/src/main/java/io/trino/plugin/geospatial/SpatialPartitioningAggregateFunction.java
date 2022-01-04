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

import io.airlift.slice.Slice;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static io.trino.plugin.geospatial.SpatialPartitioningAggregateFunction.NAME;

@AggregationFunction(value = NAME, decomposable = false)
public final class SpatialPartitioningAggregateFunction
{
    public static final String NAME = "spatial_partitioning";

    private SpatialPartitioningAggregateFunction() {}

    @InputFunction
    public static void input(SpatialPartitioningState state, @SqlType(GEOMETRY_TYPE_NAME) Slice slice)
    {
        throw new UnsupportedOperationException("spatial_partitioning(geometry) aggregate function should be re-written into spatial_partitioning(geometry, partitionCount)");
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(SpatialPartitioningState state, BlockBuilder out)
    {
        throw new UnsupportedOperationException("spatial_partitioning(geometry) aggregate function should be re-written into spatial_partitioning(geometry, partitionCount)");
    }
}
