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
package io.trino.plugin.postgresql;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPostgreSqlGeometryType
        extends AbstractTestQueryFramework
{
    private TestingPostgreSqlServer postgreSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer(
                DockerImageName.parse("postgis/postgis:17-3.4-alpine").asCompatibleSubstituteFor("postgres"),
                false));
        return PostgreSqlQueryRunner.builder(postgreSqlServer).build();
    }

    @Test
    void testGeometryRead()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geometry_read", "(geom geometry)")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (ST_Point(1, 1))");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ST_Point(1, 1)");
        }
    }

    @Test
    void testGeometryReadWithSrid()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geometry_read", "(geom geometry(point, 4326))")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (ST_SetSRID(ST_Point(1, 1), 4326))");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ST_Point(1, 1)");
        }
    }

    @Test
    void testGeometryReadWithSridAndZ()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geometry_read", "(geom geometry(pointz, 4326))")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (ST_SetSRID(ST_MakePoint(1, 2, 3), 4326))");

            assertThat(query("SELECT ST_AsEWKT(geom), ST_SRID(geom), ST_CoordDim(geom) FROM " + table.getName()))
                    .matches("VALUES (VARCHAR 'SRID=4326;POINT Z (1 2 3)', 4326, TINYINT '3')");
        }
    }

    @Test
    void testGeometryWrite()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geometry_write", "(geom geometry)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (ST_Point(1, 1))", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ST_Point(1, 1)");
        }
        try (TestTable testTable = newTrinoTable(
                "test_geometry_ctas",
                "AS SELECT ST_Point(1, 1) geom")) {
            assertThat(query(format("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s'", testTable.getName())))
                    .matches("VALUES (varchar'geom', varchar'Geometry')");
            assertThat(query("SELECT * FROM " + testTable.getName()))
                    .matches("VALUES ST_Point(1, 1)");
        }
    }

    @Test
    void testGeometryWriteWithSrid()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geometry_write", "(geom geometry)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (ST_SetSRID(ST_Point(1, 1), 4326))", 1);
            assertThat(query("SELECT ST_SRID(geom) FROM " + table.getName()))
                    .matches("VALUES 4326");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ST_Point(1, 1)");
        }
        try (TestTable table = newTrinoTable(
                "test_geometry_ctas_with_srid",
                "AS SELECT ST_SetSRID(ST_Point(1, 1), 4326) geom")) {
            assertThat(query(format("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s'", table.getName())))
                    .matches("VALUES (varchar'geom', varchar'Geometry')");
            assertThat(query("SELECT ST_SRID(geom) FROM " + table.getName()))
                    .matches("VALUES 4326");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ST_Point(1, 1)");
        }
    }

    @Test
    void testGeometryWriteWithSridAndZ()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geometry_write", "(geom geometry(pointz, 4326))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (ST_SetSRID(ST_GeometryFromText('POINT Z (1 2 3)'), 4326))", 1);

            assertThat(query("SELECT ST_AsEWKT(geom), ST_SRID(geom), ST_CoordDim(geom) FROM " + table.getName()))
                    .matches("VALUES (VARCHAR 'SRID=4326;POINT Z (1 2 3)', 4326, TINYINT '3')");
            assertThat(query("SELECT * FROM TABLE(system.query(query => 'SELECT ST_AsEWKT(geom), ST_SRID(geom), ST_Z(geom) FROM tpch." + table.getName() + "'))"))
                    .matches("VALUES (VARCHAR 'SRID=4326;POINT(1 2 3)', 4326, DOUBLE '3.0')");
        }
    }

    @Test
    void testGeometryWriteWithSridIntoConstrainedColumn()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geometry_write", "(geom geometry(point, 4326))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (ST_SetSRID(ST_Point(1, 1), 4326))", 1);
            assertThat(query("SELECT ST_SRID(geom) FROM " + table.getName()))
                    .matches("VALUES 4326");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ST_Point(1, 1)");
        }
    }

    @Test
    void testPointRead()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_point_read", "(geom point)")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (NULL), (point(1.23, -1))");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES NULL, ST_Point(1.23, -1)");
        }
    }

    @Test
    void testPointWrite()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_point_write", "(geom point)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES NULL, St_Point(12.345, -1.2)", 2);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES NULL, ST_Point(12.345, -1.2)");
        }
    }

    @Test
    void testPointWriteFailure()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_point_write", "(geom point)")) {
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES (ST_LineString(ARRAY[ST_Point(0,0), ST_Point(1,1)]))",
                    "Expected Point geometry when writing to PostgreSQL point column, but got LineString.*");
        }
    }

    @Test
    void testGeometryWriteRejectsMismatchedSridIntoConstrainedColumn()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geometry_write", "(geom geometry(point, 4326))")) {
            assertQueryFails(
                    "INSERT INTO " + table.getName() + " VALUES (ST_SetSRID(ST_Point(1, 1), 3857))",
                    ".*Geometry SRID \\(3857\\) does not match column SRID \\(4326\\).*");
        }
    }

    @Test
    void testGeographyUnsupported()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geography_read", "(geog geography)")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (ST_GeogFromText('SRID=4326;POINT(1 2)'))");

            assertQueryFails("SELECT * FROM " + table.getName(), "\\QTable 'tpch." + table.getName() + "' has no supported columns (all 1 columns are not supported)");
            assertQueryFails("SHOW COLUMNS FROM " + table.getName(), "\\QTable 'tpch." + table.getName() + "' has no supported columns (all 1 columns are not supported)");
        }
    }

    @Test
    void testGeometryNullRead()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geometry_null_read", "(id int, geom geometry)")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, NULL), (2, ST_Point(1, 1))");
            assertThat(query("SELECT geom FROM " + table.getName()))
                    .matches("VALUES CAST(NULL AS Geometry), ST_Point(1,1)");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE geom IS NULL"))
                    .matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE geom IS NOT NULL"))
                    .matches("VALUES 2");
        }
    }

    @Test
    void testGeometryNullWrite()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geometry_null_write", "(id int, geom geometry)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, NULL), (2, ST_Point(1, 1))", 2);
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE geom IS NULL"))
                    .matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE geom IS NOT NULL"))
                    .matches("VALUES 2");
        }
    }
}
