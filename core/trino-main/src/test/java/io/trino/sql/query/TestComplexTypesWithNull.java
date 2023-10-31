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
package io.trino.sql.query;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Regression test for https://github.com/trinodb/trino/issues/9528
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestComplexTypesWithNull
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testRowTypeWithNull()
    {
        assertThat(assertions.query("""
                SELECT r.a, r.b, c
                FROM (VALUES ROW(CAST(ROW(1, NULL) AS ROW(a INTEGER, b INTEGER)))) t(r)
                JOIN (VALUES 1) u(c) ON c = r.a
                """))
                .matches("VALUES (1, CAST(NULL AS INTEGER), 1)");
    }

    @Test
    public void testArrayTypeWithNull()
    {
        assertThat(assertions.query("""
                SELECT t.a, t.b, c
                FROM UNNEST(ARRAY[CAST(ROW(1, NULL) as ROW(a INTEGER, b INTEGER)) ]) t
                JOIN (VALUES 1) u(c) ON c = t.a
                """))
                .matches("VALUES (1, CAST(NULL AS INTEGER), 1)");
    }

    @Test
    public void testNestedRowTypeWithNull()
    {
        assertThat(assertions.query("""
                SELECT r.a, r[2].b, r[2].c, c FROM
                (VALUES ROW(CAST(ROW(1, ROW(1, NULL)) AS ROW(a INTEGER, ROW(b INTEGER, c INTEGER))))) t(r)
                JOIN (VALUES 1) u(c) ON c = r.a
                """))
                .matches("VALUES (1, 1, CAST(NULL AS INTEGER), 1)");
    }

    @Test
    public void testNestedArrayTypeWithNull()
    {
        assertThat(assertions.query("""
                SELECT r.a, r.b, c FROM
                (VALUES CAST(ROW(ROW(1, ARRAY[NULL])) AS ROW(ROW(a INTEGER, b ARRAY(INTEGER))))) t(r)
                JOIN (VALUES 1) u(c) ON c = r.a
                """))
                .matches("VALUES (1, ARRAY[CAST(NULL AS INTEGER)], 1)");
    }
}
