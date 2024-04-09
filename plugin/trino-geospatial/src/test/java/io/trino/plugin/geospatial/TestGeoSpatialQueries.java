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

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGeoSpatialQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().build();
        // Distributed runner exercises the client protocol as well.
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .build();
        try {
            queryRunner.installPlugin(new GeoPlugin());
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testGeometryResult()
    {
        assertThat(query("SELECT ST_Point(52.233, 21.016)"))
                .result().matches(MaterializedResult.resultBuilder(getSession(), GEOMETRY)
                        .row("POINT (52.233 21.016)")
                        .build());

        assertThat(query("SELECT ST_GeometryFromText('POLYGON((0 0, 0 1, 1 1, 1 1, 1 0, 0 0))')"))
                .result().matches(MaterializedResult.resultBuilder(getSession(), GEOMETRY)
                        .row("POLYGON ((0 0, 1 0, 1 1, 1 1, 0 1, 0 0))")
                        .build());
    }

    @Test
    public void testSphericalGeographyResult()
    {
        assertThat(query("SELECT to_spherical_geography(ST_GeometryFromText('POLYGON((0 0, 0 1, 1 1, 1 1, 1 0, 0 0))'))"))
                .result().matches(MaterializedResult.resultBuilder(getSession(), SPHERICAL_GEOGRAPHY)
                        .row("POLYGON ((0 0, 1 0, 1 1, 1 1, 0 1, 0 0))")
                        .build());
    }
}
