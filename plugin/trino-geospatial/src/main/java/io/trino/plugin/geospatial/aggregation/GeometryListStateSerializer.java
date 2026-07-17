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

import io.trino.geospatial.serde.JtsGeometrySerde;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;

public class GeometryListStateSerializer
        implements AccumulatorStateSerializer<GeometryListState>
{
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @Override
    public Type getSerializedType()
    {
        return GEOMETRY;
    }

    @Override
    public void serialize(GeometryListState state, BlockBuilder out)
    {
        List<Geometry> geometries = state.getGeometries();
        if (geometries == null) {
            out.appendNull();
            return;
        }
        GEOMETRY.writeSlice(out, JtsGeometrySerde.serialize(stateGeometry(geometries, state.getSrid())));
    }

    @Override
    public void deserialize(Block block, int index, GeometryListState state)
    {
        GeometryCollection collection = (GeometryCollection) JtsGeometrySerde.deserialize(GEOMETRY.getSlice(block, index));
        state.setGeometries(stateGeometries(collection));
        state.setSrid(collection.getSRID());
    }

    private static Geometry stateGeometry(List<Geometry> geometries, int srid)
    {
        GeometryCollection collection = GEOMETRY_FACTORY.createGeometryCollection(geometries.toArray(new Geometry[0]));
        collection.setSRID(srid);
        return collection;
    }

    private static List<Geometry> stateGeometries(GeometryCollection collection)
    {
        List<Geometry> geometries = new ArrayList<>(collection.getNumGeometries());
        for (int i = 0; i < collection.getNumGeometries(); i++) {
            geometries.add(collection.getGeometryN(i));
        }
        return geometries;
    }
}
