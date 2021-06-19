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

import io.trino.geospatial.serde.GeometrySerde;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;

public class GeometryStateSerializer
        implements AccumulatorStateSerializer<GeometryState>
{
    @Override
    public Type getSerializedType()
    {
        return GEOMETRY;
    }

    @Override
    public void serialize(GeometryState state, BlockBuilder out)
    {
        if (state.getGeometry() == null) {
            out.appendNull();
        }
        else {
            GEOMETRY.writeSlice(out, GeometrySerde.serialize(state.getGeometry()));
        }
    }

    @Override
    public void deserialize(Block block, int index, GeometryState state)
    {
        state.setGeometry(GeometrySerde.deserialize(GEOMETRY.getSlice(block, index)));
    }
}
