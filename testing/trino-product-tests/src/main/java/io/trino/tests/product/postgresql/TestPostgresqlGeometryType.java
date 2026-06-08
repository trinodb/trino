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

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.POSTGRESQL;
import static io.trino.tests.product.TestGroups.POSTGRESQL_POSTGIS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onPostgres;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPostgresqlGeometryType
        extends ProductTest
{
    private static final String TABLE_NAME = "public.areas";

    @BeforeMethodWithContext
    @AfterMethodWithContext
    public void dropTestTable()
    {
        onPostgres().executeQuery("DROP TABLE IF EXISTS " + TABLE_NAME);
    }

    @Test(groups = {POSTGRESQL_POSTGIS, PROFILE_SPECIFIC_TESTS})
    public void testReadGeometryFromPostgres()
    {
        onPostgres().executeQuery("CREATE TABLE " + TABLE_NAME + " (id SERIAL PRIMARY KEY, geom geometry(Geometry, 4326))");
        onPostgres().executeQuery("INSERT INTO " + TABLE_NAME + " (geom) VALUES (ST_SetSRID(ST_Point(1, 2), 4326))");

        assertThat(onTrino().executeQuery("SELECT ST_AsText(geom) FROM postgresql." + TABLE_NAME))
                .containsOnly(row("POINT (1 2)"));
    }

    @Test(groups = {POSTGRESQL_POSTGIS, PROFILE_SPECIFIC_TESTS})
    public void testWriteGeometryFromTrino()
    {
        onPostgres().executeQuery("CREATE TABLE " + TABLE_NAME + " (id SERIAL PRIMARY KEY, geom geometry(Geometry, 4326))");

        onTrino().executeQuery("INSERT INTO postgresql." + TABLE_NAME + " (geom) VALUES (ST_GeometryFromText('POINT (3 4)'))");

        assertThat(onPostgres().executeQuery("SELECT ST_AsText(geom) FROM " + TABLE_NAME))
                .containsOnly(row("POINT(3 4)"));
        assertThat(onTrino().executeQuery("SELECT ST_AsText(geom) FROM postgresql." + TABLE_NAME))
                .containsOnly(row("POINT (3 4)"));
    }

    @Test(groups = {POSTGRESQL_POSTGIS, PROFILE_SPECIFIC_TESTS})
    public void testNullGeometry()
    {
        onPostgres().executeQuery("CREATE TABLE " + TABLE_NAME + " (id SERIAL PRIMARY KEY, geom geometry(Geometry, 4326))");
        onPostgres().executeQuery("INSERT INTO " + TABLE_NAME + " (geom) VALUES (NULL), (ST_SetSRID(ST_Point(5, 6), 4326))");

        assertThat(onTrino().executeQuery("SELECT id FROM postgresql." + TABLE_NAME + " WHERE geom IS NULL"))
                .containsOnly(row(1));
        assertThat(onTrino().executeQuery("SELECT ST_AsText(geom) FROM postgresql." + TABLE_NAME + " WHERE geom IS NOT NULL"))
                .containsOnly(row("POINT (5 6)"));
    }

    @Test(groups = {POSTGRESQL_POSTGIS, PROFILE_SPECIFIC_TESTS})
    public void testCTASGeometryFromTrino()
    {
        onTrino().executeQuery("CREATE TABLE postgresql." + TABLE_NAME + " AS SELECT ST_GeometryFromText('POINT (3 4)') AS geom");

        assertThat(onPostgres().executeQuery("SELECT ST_AsText(geom) FROM " + TABLE_NAME))
                .containsOnly(row("POINT(3 4)"));
        assertThat(onTrino().executeQuery("SELECT ST_AsText(geom) FROM postgresql." + TABLE_NAME))
                .containsOnly(row("POINT (3 4)"));
    }

    @Test(groups = {POSTGRESQL, PROFILE_SPECIFIC_TESTS})
    public void testReadPointFromPostgres()
    {
        onPostgres().executeQuery("CREATE TABLE " + TABLE_NAME + " (id SERIAL PRIMARY KEY, loc point)");
        onPostgres().executeQuery("INSERT INTO " + TABLE_NAME + " (loc) VALUES ('(1.123456789, -0.123456789)')");

        assertThat(onTrino().executeQuery("SELECT ST_AsText(loc) FROM postgresql." + TABLE_NAME))
                .containsOnly(row("POINT (1.123456789 -0.123456789)"));
    }

    @Test(groups = {POSTGRESQL, PROFILE_SPECIFIC_TESTS})
    public void testWritePointFromTrino()
    {
        onPostgres().executeQuery("CREATE TABLE " + TABLE_NAME + " (id SERIAL PRIMARY KEY, loc point)");

        onTrino().executeQuery("INSERT INTO postgresql." + TABLE_NAME + " (loc) VALUES (st_point(1, 2))");

        assertThat(onPostgres().executeQuery("SELECT loc::varchar FROM " + TABLE_NAME))
                .containsOnly(row("(1,2)"));
        assertThat(onTrino().executeQuery("SELECT ST_AsText(loc) FROM postgresql." + TABLE_NAME))
                .containsOnly(row("POINT (1 2)"));
    }
}
