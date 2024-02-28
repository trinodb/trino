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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGeometryStateFactory
{
    private final GeometryStateFactory factory = new GeometryStateFactory();

    @Test
    public void testCreateSingleStateEmpty()
    {
        GeometryState state = factory.createSingleState();
        assertThat(state.getGeometry()).isNull();
        assertThat(0).isEqualTo(state.getEstimatedSize());
    }

    @Test
    public void testCreateSingleStatePresent()
    {
        GeometryState state = factory.createSingleState();
        state.setGeometry(OGCGeometry.fromText("POINT (1 2)"));
        assertThat(OGCGeometry.fromText("POINT (1 2)")).isEqualTo(state.getGeometry());
        assertThat(state.getEstimatedSize() > 0)
                .describedAs("Estimated memory size was " + state.getEstimatedSize())
                .isTrue();
    }

    @Test
    public void testCreateGroupedStateEmpty()
    {
        GeometryState state = factory.createGroupedState();
        assertThat(state.getGeometry()).isNull();
        assertThat(state.getEstimatedSize() > 0)
                .describedAs("Estimated memory size was " + state.getEstimatedSize())
                .isTrue();
    }

    @Test
    public void testCreateGroupedStatePresent()
    {
        GeometryState state = factory.createGroupedState();
        assertThat(state.getGeometry()).isNull();
        assertThat(state instanceof GeometryStateFactory.GroupedGeometryState).isTrue();
        GeometryStateFactory.GroupedGeometryState groupedState = (GeometryStateFactory.GroupedGeometryState) state;

        groupedState.setGroupId(1);
        assertThat(state.getGeometry()).isNull();
        groupedState.setGeometry(OGCGeometry.fromText("POINT (1 2)"));
        assertThat(state.getGeometry()).isEqualTo(OGCGeometry.fromText("POINT (1 2)"));

        groupedState.setGroupId(2);
        assertThat(state.getGeometry()).isNull();
        groupedState.setGeometry(OGCGeometry.fromText("POINT (3 4)"));
        assertThat(state.getGeometry()).isEqualTo(OGCGeometry.fromText("POINT (3 4)"));

        groupedState.setGroupId(1);
        assertThat(state.getGeometry()).isNotNull();
    }
}
