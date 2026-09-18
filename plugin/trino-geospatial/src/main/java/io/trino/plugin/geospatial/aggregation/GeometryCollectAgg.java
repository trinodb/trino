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

import io.trino.plugin.geospatial.GeoFunctions;
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
import org.locationtech.jts.geom.Geometry;

import java.util.List;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

@Description("Returns a geometry collection containing the input geometries.")
@AggregationFunction("geometry_collect_agg")
public final class GeometryCollectAgg
{
    private GeometryCollectAgg() {}

    @InputFunction
    public static void input(@AggregationState GeometryListState state, @SqlType(StandardTypes.GEOMETRY) Geometry geometry)
    {
        List<Geometry> geometries = state.getGeometries();
        if (geometries == null) {
            state.setSrid(geometry.getSRID());
            state.addGeometry(geometry);
            return;
        }

        state.setSrid(accumulateSrid(state.getSrid(), geometry.getSRID()));
        state.addGeometry(geometry);
    }

    @CombineFunction
    public static void combine(@AggregationState GeometryListState state, @AggregationState GeometryListState otherState)
    {
        List<Geometry> otherGeometries = otherState.getGeometries();
        if (otherGeometries == null) {
            return;
        }
        List<Geometry> geometries = state.getGeometries();
        if (geometries == null) {
            state.setSrid(otherState.getSrid());
            for (Geometry geometry : otherGeometries) {
                state.addGeometry(geometry);
            }
            return;
        }

        state.setSrid(accumulateSrid(state.getSrid(), otherState.getSrid()));
        for (Geometry geometry : otherGeometries) {
            state.addGeometry(geometry);
        }
    }

    @OutputFunction(StandardTypes.GEOMETRY)
    public static void output(@AggregationState GeometryListState state, BlockBuilder out)
    {
        List<Geometry> geometries = state.getGeometries();
        if (geometries == null) {
            out.appendNull();
        }
        else {
            GEOMETRY.writeObject(out, GeoFunctions.stCollectGeometries(geometries));
        }
    }

    private static int accumulateSrid(int currentSrid, int geometrySrid)
    {
        if (currentSrid == 0) {
            return geometrySrid;
        }
        if (geometrySrid == 0) {
            return currentSrid;
        }
        if (currentSrid != geometrySrid) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    format("SRID mismatch: %d vs %d", currentSrid, geometrySrid));
        }
        return currentSrid;
    }
}
