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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;

abstract class BasePostgresqlPointType
{
    private static final String TABLE_NAME = "public.areas";

    @BeforeEach
    @AfterEach
    void dropTestTable(PostgresqlEnvironment env)
    {
        env.executePostgresqlUpdate("DROP TABLE IF EXISTS " + TABLE_NAME);
    }

    @Test
    void testReadPointFromPostgres(PostgresqlEnvironment env)
    {
        env.executePostgresqlUpdate("CREATE TABLE " + TABLE_NAME + " (id SERIAL PRIMARY KEY, loc point)");
        env.executePostgresqlUpdate("INSERT INTO " + TABLE_NAME + " (loc) VALUES ('(1.123456789, -0.123456789)')");

        assertThat(env.executeTrino("SELECT ST_AsText(loc) FROM postgresql." + TABLE_NAME))
                .containsOnly(row("POINT (1.123456789 -0.123456789)"));
    }

    @Test
    void testWritePointFromTrino(PostgresqlEnvironment env)
    {
        env.executePostgresqlUpdate("CREATE TABLE " + TABLE_NAME + " (id SERIAL PRIMARY KEY, loc point)");

        env.executeTrinoUpdate("INSERT INTO postgresql." + TABLE_NAME + " (loc) VALUES (st_point(1, 2))");

        assertThat(env.executePostgresql("SELECT loc::varchar FROM " + TABLE_NAME))
                .containsOnly(row("(1,2)"));
        assertThat(env.executeTrino("SELECT ST_AsText(loc) FROM postgresql." + TABLE_NAME))
                .containsOnly(row("POINT (1 2)"));
    }
}
