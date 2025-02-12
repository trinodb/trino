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
package io.trino.plugin.geospatial.aggregation;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.trino.geospatial.GeometryType;
import io.trino.geospatial.serde.GeometrySerde;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.Set;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

/**
 * Aggregate form of ST_ConvexHull, which takes a set of geometries and computes the convex hull
 * of all the geometries in the set. The output is a single geometry.
 */
@Description("Returns a geometry that is the convex hull of all the geometries in the set.")
@AggregationFunction("convex_hull_agg")
public final class ConvexHullAggregation
{
    private static final Joiner OR_JOINER = Joiner.on(" or ");

    private ConvexHullAggregation() {}

    @InputFunction
    public static void input(@AggregationState GeometryState state,
            @SqlType(StandardTypes.GEOMETRY) Slice input)
    {
        OGCGeometry geometry = GeometrySerde.deserialize(input);
        if (state.getGeometry() == null) {
            state.setGeometry(geometry.convexHull());
        }
        else if (!geometry.isEmpty()) {
            state.setGeometry(state.getGeometry().union(geometry).convexHull());
        }
    }

    @CombineFunction
    public static void combine(@AggregationState GeometryState state,
            @AggregationState GeometryState otherState)
    {
        if (state.getGeometry() == null) {
            state.setGeometry(otherState.getGeometry());
        }
        else if (otherState.getGeometry() != null && !otherState.getGeometry().isEmpty()) {
            state.setGeometry(state.getGeometry().union(otherState.getGeometry()).convexHull());
        }
    }

    @OutputFunction(StandardTypes.GEOMETRY)
    public static void output(@AggregationState GeometryState state, BlockBuilder out)
    {
        if (state.getGeometry() == null) {
            out.appendNull();
        }
        else {
            GEOMETRY.writeSlice(out, GeometrySerde.serialize(state.getGeometry()));
        }
    }

    private static void validateType(String function, OGCGeometry geometry, Set<GeometryType> validTypes)
    {
        GeometryType type = GeometryType.getForEsriGeometryType(geometry.geometryType());
        if (!validTypes.contains(type)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("%s only applies to %s. Input type is: %s", function, OR_JOINER.join(validTypes), type));
        }
    }
}
