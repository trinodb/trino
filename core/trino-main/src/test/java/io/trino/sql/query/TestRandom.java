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

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestRandom
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void test()
    {
        assertThat(assertions.query(
                """
                WITH t(a, b) AS (SELECT random(), random())
                SELECT a = b
                FROM t
                """))
                .matches("VALUES false");

        assertThat(assertions.query(
                """
                WITH t(a, b) AS (VALUES (random(), random()))
                SELECT a = b
                FROM t
                """))
                .matches("VALUES false");

        assertThat(assertions.query(
                """
                WITH t(a, b) AS (SELECT transform(array[1], x -> random())[1], transform(array[1], x -> random())[1])
                SELECT a = b
                FROM t
                """))
                .matches("VALUES false");
    }
}
