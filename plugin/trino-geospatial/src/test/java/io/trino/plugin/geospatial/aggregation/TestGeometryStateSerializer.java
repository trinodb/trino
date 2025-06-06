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
import io.trino.operator.aggregation.state.StateCompiler;
import io.trino.plugin.geospatial.GeometryType;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateSerializer;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.geospatial.aggregation.GeometryStateFactory.GroupedGeometryState;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGeometryStateSerializer
{
    @Test
    public void testSerializeDeserialize()
    {
        AccumulatorStateFactory<GeometryState> factory = StateCompiler.generateStateFactory(GeometryState.class);
        AccumulatorStateSerializer<GeometryState> serializer = StateCompiler.generateStateSerializer(GeometryState.class);
        GeometryState state = factory.createSingleState();

        state.setGeometry(OGCGeometry.fromText("POINT (1 2)"));

        BlockBuilder builder = GeometryType.GEOMETRY.createBlockBuilder(null, 1);
        serializer.serialize(state, builder);
        Block block = builder.build();

        assertThat(GeometryType.GEOMETRY.getObjectValue(block, 0)).isEqualTo("POINT (1 2)");

        state.setGeometry(null);
        serializer.deserialize(block, 0, state);

        assertThat(state.getGeometry().asText()).isEqualTo("POINT (1 2)");
    }

    @Test
    public void testSerializeDeserializeGrouped()
    {
        AccumulatorStateFactory<GeometryState> factory = StateCompiler.generateStateFactory(GeometryState.class);
        AccumulatorStateSerializer<GeometryState> serializer = StateCompiler.generateStateSerializer(GeometryState.class);
        GroupedGeometryState state = (GroupedGeometryState) factory.createGroupedState();

        // Add state to group 1
        state.setGroupId(1);
        state.setGeometry(OGCGeometry.fromText("POINT (1 2)"));
        // Add another state to group 2, to show that this doesn't affect the group under test (group 1)
        state.setGroupId(2);
        state.setGeometry(OGCGeometry.fromText("POINT (2 3)"));
        // Return to group 1
        state.setGroupId(1);

        BlockBuilder builder = GeometryType.GEOMETRY.createBlockBuilder(null, 1);
        serializer.serialize(state, builder);
        Block block = builder.build();

        assertThat(GeometryType.GEOMETRY.getObjectValue(block, 0)).isEqualTo("POINT (1 2)");

        state.setGeometry(null);
        serializer.deserialize(block, 0, state);

        // Assert the state of group 1
        assertThat(state.getGeometry().asText()).isEqualTo("POINT (1 2)");
        // Verify nothing changed in group 2
        state.setGroupId(2);
        assertThat(state.getGeometry().asText()).isEqualTo("POINT (2 3)");
        // Groups we did not touch are null
        state.setGroupId(3);
        assertThat(state.getGeometry()).isNull();
    }
}
