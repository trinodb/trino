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

import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestGeometryCollectGeoAggregation
        extends AbstractTestGeoAggregationFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new GeoPlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testBasicAggregation()
    {
        assertAggregatedGeometries("empty", null);
        assertAggregatedGeometries("points", "MULTIPOINT ((0 0), (1 1))", "POINT (0 0)", "POINT (1 1)");
        assertAggregatedGeometries("mixed geometries", "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (1 1, 2 2))", "POINT (0 0)", "LINESTRING (1 1, 2 2)");
    }

    @Test
    public void testSridAndZMetadata()
    {
        assertThat(assertions.query(
                """
                SELECT ST_AsEWKT(geometry_collect_agg(geometry))
                FROM (VALUES
                    ST_SetSRID(ST_GeometryFromText('POINT Z (0 0 1)'), 4326),
                    ST_SetSRID(ST_GeometryFromText('POINT Z (1 1 2)'), 4326)
                ) t(geometry)
                """))
                .matches("VALUES VARCHAR 'SRID=4326;MULTIPOINT Z ((0 0 1), (1 1 2))'");
    }

    @Test
    public void testSridMismatch()
    {
        assertThat(assertions.query(
                """
                SELECT geometry_collect_agg(geometry)
                FROM (VALUES
                    ST_SetSRID(ST_Point(0, 0), 4326),
                    ST_SetSRID(ST_Point(1, 1), 3857)
                ) t(geometry)
                """))
                .failure()
                .hasMessage("SRID mismatch: 4326 vs 3857");
    }

    @Test
    public void testCombineDoesNotNestPartialCollections()
            throws ParseException
    {
        GeometryListState state = new GeometryListStateFactory.SingleGeometryListState();
        GeometryCollectAgg.input(state, geometry("POINT (0 0)", 4326));

        GeometryListState otherState = new GeometryListStateFactory.SingleGeometryListState();
        GeometryCollectAgg.input(otherState, geometry("POINT (1 1)", 4326));
        GeometryCollectAgg.input(otherState, geometry("LINESTRING (2 2, 3 3)", 4326));

        GeometryCollectAgg.combine(state, otherState);

        assertThat(state.getGeometries())
                .extracting(Geometry::toText)
                .containsExactly("POINT (0 0)", "POINT (1 1)", "LINESTRING (2 2, 3 3)");
        assertThat(state.getSrid()).isEqualTo(4326);
    }

    @Override
    protected String getFunctionName()
    {
        return "geometry_collect_agg";
    }

    private static Geometry geometry(String wkt, int srid)
            throws ParseException
    {
        Geometry geometry = new WKTReader().read(wkt);
        geometry.setSRID(srid);
        return geometry;
    }
}
