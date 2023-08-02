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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestWindow
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    @Timeout(2)
    public void testManyFunctionsWithSameWindow()
    {
        assertThat(assertions.query("""
                SELECT
                          SUM(a) OVER w,
                          COUNT(a) OVER w,
                          MIN(a) OVER w,
                          MAX(a) OVER w,
                          SUM(b) OVER w,
                          COUNT(b) OVER w,
                          MIN(b) OVER w,
                          MAX(b) OVER w,
                          SUM(c) OVER w,
                          COUNT(c) OVER w,
                          MIN(c) OVER w,
                          MAX(c) OVER w,
                          SUM(d) OVER w,
                          COUNT(d) OVER w,
                          MIN(d) OVER w,
                          MAX(d) OVER w,
                          SUM(e) OVER w,
                          COUNT(e) OVER w,
                          MIN(e) OVER w,
                          MAX(e) OVER w,
                          SUM(f) OVER w,
                          COUNT(f) OVER w,
                          MIN(f) OVER w,
                          MAX(f) OVER w
                        FROM (
                            VALUES (1, 1, 1, 1, 1, 1, 1)
                        ) AS t(k, a, b, c, d, e, f)
                        WINDOW w AS (ORDER BY k ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
                """))
                .matches("VALUES (BIGINT '1', BIGINT '1', 1, 1, BIGINT '1', BIGINT '1', 1, 1, BIGINT '1', BIGINT '1', 1, 1, BIGINT '1', BIGINT '1', 1, 1, BIGINT '1', BIGINT '1', 1, 1, BIGINT '1', BIGINT '1', 1, 1)");
    }
}
