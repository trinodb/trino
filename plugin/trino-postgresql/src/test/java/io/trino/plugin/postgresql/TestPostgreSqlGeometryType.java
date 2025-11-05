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
    void testGeometryWrite()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_geometry_write", "(geom geometry)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (ST_Point(1, 1))", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ST_Point(1, 1)");
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
