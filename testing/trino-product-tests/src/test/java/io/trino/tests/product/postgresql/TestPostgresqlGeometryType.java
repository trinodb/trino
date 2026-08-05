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
package io.trino.tests.product.postgresql;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

@ProductTest
@RequiresEnvironment(PostgresqlPostgisEnvironment.class)
@TestGroup.PostgresqlPostgis
@TestGroup.ProfileSpecificTests
class TestPostgresqlGeometryType
{
    private static final String TABLE_NAME = "public.areas";

    @BeforeEach
    @AfterEach
    void dropTestTable(PostgresqlPostgisEnvironment env)
    {
        env.executePostgresqlUpdate("DROP TABLE IF EXISTS " + TABLE_NAME);
    }

    @Test
    void testReadGeometryFromPostgres(PostgresqlPostgisEnvironment env)
    {
        env.executePostgresqlUpdate("CREATE TABLE " + TABLE_NAME + " (id SERIAL PRIMARY KEY, geom geometry(Geometry, 4326))");
        env.executePostgresqlUpdate("INSERT INTO " + TABLE_NAME + " (geom) VALUES (ST_SetSRID(ST_Point(1, 2), 4326))");

        assertThat(env.executeTrino("SELECT ST_AsText(geom) FROM postgresql." + TABLE_NAME))
                .containsOnly(row("POINT (1 2)"));
    }

    @Test
    void testWriteGeometryFromTrino(PostgresqlPostgisEnvironment env)
    {
        env.executePostgresqlUpdate("CREATE TABLE " + TABLE_NAME + " (id SERIAL PRIMARY KEY, geom geometry(Geometry, 4326))");

        env.executeTrinoUpdate("INSERT INTO postgresql." + TABLE_NAME + " (geom) VALUES (ST_GeometryFromText('POINT (3 4)'))");

        assertThat(env.executePostgresql("SELECT ST_AsText(geom) FROM " + TABLE_NAME))
                .containsOnly(row("POINT(3 4)"));
        assertThat(env.executeTrino("SELECT ST_AsText(geom) FROM postgresql." + TABLE_NAME))
                .containsOnly(row("POINT (3 4)"));
    }

    @Test
    void testNullGeometry(PostgresqlPostgisEnvironment env)
    {
        env.executePostgresqlUpdate("CREATE TABLE " + TABLE_NAME + " (id SERIAL PRIMARY KEY, geom geometry(Geometry, 4326))");
        env.executePostgresqlUpdate("INSERT INTO " + TABLE_NAME + " (geom) VALUES (NULL), (ST_SetSRID(ST_Point(5, 6), 4326))");

        assertThat(env.executeTrino("SELECT id FROM postgresql." + TABLE_NAME + " WHERE geom IS NULL"))
                .containsOnly(row(1));
        assertThat(env.executeTrino("SELECT ST_AsText(geom) FROM postgresql." + TABLE_NAME + " WHERE geom IS NOT NULL"))
                .containsOnly(row("POINT (5 6)"));
    }

    @Test
    void testCTASGeometryFromTrino(PostgresqlPostgisEnvironment env)
    {
        env.executeTrinoUpdate("CREATE TABLE postgresql." + TABLE_NAME + " AS SELECT ST_GeometryFromText('POINT (3 4)') AS geom");

        assertThat(env.executePostgresql("SELECT ST_AsText(geom) FROM " + TABLE_NAME))
                .containsOnly(row("POINT(3 4)"));
        assertThat(env.executeTrino("SELECT ST_AsText(geom) FROM postgresql." + TABLE_NAME))
                .containsOnly(row("POINT (3 4)"));
    }
}
