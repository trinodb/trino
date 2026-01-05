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

import static io.trino.geospatial.GeometryUtils.safeUnion;
import static io.trino.geospatial.serde.JtsGeometrySerde.validateAndGetSrid;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;

/**
 * Aggregate form of ST_Union which takes a set of geometries and unions them into a single geometry using an iterative approach,
 * resulting in no intersecting regions.  The output may be a multi-geometry, a single geometry or a geometry collection.
 */
@Description("Returns a geometry that represents the point set union of the input geometries.")
@AggregationFunction("geometry_union_agg")
public final class GeometryUnionAgg
{
    private GeometryUnionAgg() {}

    @InputFunction
    public static void input(@AggregationState GeometryState state, @SqlType(StandardTypes.GEOMETRY) Geometry geometry)
    {
        if (state.getGeometry() == null) {
            state.setGeometry(geometry);
        }
        else if (!geometry.isEmpty()) {
            int srid = validateAndGetSrid(state.getGeometry(), geometry);
            Geometry result = safeUnion(state.getGeometry(), geometry);
            result.setSRID(srid);
            state.setGeometry(result);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState GeometryState state, @AggregationState GeometryState otherState)
    {
        if (state.getGeometry() == null) {
            state.setGeometry(otherState.getGeometry());
        }
        else if (otherState.getGeometry() != null && !otherState.getGeometry().isEmpty()) {
            int srid = validateAndGetSrid(state.getGeometry(), otherState.getGeometry());
            Geometry result = safeUnion(state.getGeometry(), otherState.getGeometry());
            result.setSRID(srid);
            state.setGeometry(result);
        }
    }

    @OutputFunction(StandardTypes.GEOMETRY)
    public static void output(@AggregationState GeometryState state, BlockBuilder out)
    {
        if (state.getGeometry() == null) {
            out.appendNull();
        }
        else {
            GEOMETRY.writeObject(out, state.getGeometry());
        }
    }
}
