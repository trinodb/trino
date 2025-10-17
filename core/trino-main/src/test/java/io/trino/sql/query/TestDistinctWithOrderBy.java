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
public class TestDistinctWithOrderBy
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testOrderByReferenceWithMixedStyles()
    {
        assertThat(assertions.query("SELECT DISTINCT t.A FROM (VALUES 2, 1, 2) t(a) ORDER BY t.a"))
                .ordered()
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a FROM (VALUES 2, 1, 2) t(a) ORDER BY A"))
                .ordered()
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a FROM (VALUES 2, 1, 2) t(a) ORDER BY t.A"))
                .ordered()
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT t.a FROM (VALUES 2, 1, 2) t(a) ORDER BY A"))
                .ordered()
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a + B FROM (VALUES (2, 2), (1, 1), (2, 2)) t(a, b) ORDER BY a + b"))
                .ordered()
                .matches("VALUES 2, 4");

        assertThat(assertions.query("SELECT DISTINCT a + B FROM (VALUES (2, 2), (1, 1), (2, 2)) t(a, b) ORDER BY a + t.b"))
                .ordered()
                .matches("VALUES 2, 4");

        assertThat(assertions.query("SELECT DISTINCT a + t.B FROM (VALUES (2, 2), (1, 1), (2, 2)) t(a, b) ORDER BY a + b"))
                .ordered()
                .matches("VALUES 2, 4");

        assertThat(assertions.query("SELECT DISTINCT a + t.B FROM (VALUES (2, 2), (1, 1), (2, 2)) t(a, b) ORDER BY a + t.b"))
                .ordered()
                .matches("VALUES 2, 4");

        assertThat(assertions.query("SELECT DISTINCT a, b a FROM (VALUES (2, 10), (1, 20), (2, 10)) T(a, b) ORDER BY T.a"))
                .ordered()
                .matches("VALUES (1, 20), (2, 10)");

        // tests with delimited and lower case identifiers
        assertThat(assertions.query("SELECT DISTINCT a as x FROM (VALUES 2, 1, 2) t(a) ORDER BY \"x\""))
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a as \"x\" FROM (VALUES 2, 1, 2) t(a) ORDER BY x"))
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a as \"x\" FROM (VALUES 2, 1, 2) t(a) ORDER BY \"x\""))
                .matches("VALUES 1, 2");

        // tests with delimited and upper case identifiers
        assertThat(assertions.query("SELECT DISTINCT a as X FROM (VALUES 2, 1, 2) t(a) ORDER BY \"X\""))
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a as \"X\" FROM (VALUES 2, 1, 2) t(a) ORDER BY X"))
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a as \"X\" FROM (VALUES 2, 1, 2) t(a) ORDER BY \"X\""))
                .matches("VALUES 1, 2");

        // tests with mixed lower (in SELECT DISTINCT) and upper (in ORDER BY) case identifiers
        assertThat(assertions.query("SELECT DISTINCT a as x FROM (VALUES 2, 1, 2) t(a) ORDER BY X"))
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a as x FROM (VALUES 2, 1, 2) t(a) ORDER BY \"X\""))
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a as \"x\" FROM (VALUES 2, 1, 2) t(a) ORDER BY X"))
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a as \"x\" FROM (VALUES 2, 1, 2) t(a) ORDER BY \"X\""))
                .matches("VALUES 1, 2");

        // tests with mixed upper (in SELECT DISTINCT) and lower (in ORDER BY) case identifiers
        assertThat(assertions.query("SELECT DISTINCT a as X FROM (VALUES 2, 1, 2) t(a) ORDER BY x"))
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a as X FROM (VALUES 2, 1, 2) t(a) ORDER BY \"x\""))
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a as \"X\" FROM (VALUES 2, 1, 2) t(a) ORDER BY x"))
                .matches("VALUES 1, 2");

        assertThat(assertions.query("SELECT DISTINCT a as \"X\" FROM (VALUES 2, 1, 2) t(a) ORDER BY \"x\""))
                .matches("VALUES 1, 2");
    }

    @Test
    public void testSelectAllAliases()
    {
        assertThat(assertions.query("SELECT DISTINCT t.r.* AS (a, b) FROM (VALUES ROW(CAST(ROW(1,1) AS ROW(a BIGINT, b BIGINT)))) t(r) ORDER BY a"))
                .matches("VALUES (BIGINT '1', BIGINT '1')");
    }

    @Test
    public void testColumnAliasing()
    {
        assertThat(assertions.query("SELECT DISTINCT 1 AS a, a + b FROM (VALUES (1, 2)) t(a, b) ORDER BY a + b"))
                .failure().hasMessage("line 1:1: For SELECT DISTINCT, ORDER BY expressions must appear in select list");

        assertThat(assertions.query("SELECT DISTINCT -a AS a, a + b FROM (VALUES (1, 2)) t(a, b) ORDER BY a + b"))
                .failure().hasMessage("line 1:1: For SELECT DISTINCT, ORDER BY expressions must appear in select list");

        assertThat(assertions.query("SELECT DISTINCT a, a + b FROM (VALUES (1, 2)) t(a, b) ORDER BY a + b"))
                .failure().hasMessage("line 1:1: For SELECT DISTINCT, ORDER BY expressions must appear in select list");
    }
}
