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

import org.junit.jupiter.api.Test;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGeometryStateFactory
{
    private static final WKTReader WKT_READER = new WKTReader();

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
            throws ParseException
    {
        GeometryState state = factory.createSingleState();
        state.setGeometry(WKT_READER.read("POINT (1 2)"));
        assertThat(state.getGeometry().toText()).isEqualTo("POINT (1 2)");
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
            throws ParseException
    {
        GeometryState state = factory.createGroupedState();
        assertThat(state.getGeometry()).isNull();
        assertThat(state).isInstanceOf(GeometryStateFactory.GroupedGeometryState.class);
        GeometryStateFactory.GroupedGeometryState groupedState = (GeometryStateFactory.GroupedGeometryState) state;

        groupedState.setGroupId(1);
        assertThat(state.getGeometry()).isNull();
        groupedState.setGeometry(WKT_READER.read("POINT (1 2)"));
        assertThat(state.getGeometry().toText()).isEqualTo("POINT (1 2)");

        groupedState.setGroupId(2);
        assertThat(state.getGeometry()).isNull();
        groupedState.setGeometry(WKT_READER.read("POINT (3 4)"));
        assertThat(state.getGeometry().toText()).isEqualTo("POINT (3 4)");

        groupedState.setGroupId(1);
        assertThat(state.getGeometry()).isNotNull();
    }
}
