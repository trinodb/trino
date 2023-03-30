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

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestSelectAll
{
    private QueryAssertions assertions;

    private static final String UNSUPPORTED_DECORRELATION_MESSAGE = ".*: Given correlated subquery is not supported";

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
    public void testSelectAllFromRow()
    {
        // named row fields and aliases
        assertThat(assertions.query(
                "SELECT alias1, alias2 FROM (SELECT CAST((ROW (1, 'a')) AS ROW (f1 bigint, f2 varchar(1))).* AS (alias1, alias2)) t"))
                .matches("SELECT BIGINT '1' alias1, 'a' alias2");

        // unnamed row fields and aliases
        assertThat(assertions.query(
                "SELECT alias1, alias2 FROM (SELECT (ROW (1, 'a')).* AS (alias1, alias2)) t"))
                .matches("SELECT 1 alias1, 'a' alias2");

        // named row fields, no aliases
        assertThat(assertions.query(
                "SELECT f1, f2 FROM (SELECT CAST((ROW (1, 'a')) AS ROW (f1 bigint, f2 varchar(1))).*) t"))
                .matches("SELECT BIGINT '1' f1, 'a' f2");
        assertThat(assertions.query(
                "SELECT f1 FROM (SELECT t.r.* FROM (VALUES ROW(CAST(ROW(1) AS ROW(f1 integer))), ROW(CAST(ROW(2) AS ROW(f1 integer)))) t(r))"))
                .matches("VALUES 1, 2");

        // unnamed row fields, no aliases
        assertThat(assertions.query("SELECT (ROW (1, 'a')).*")).matches("SELECT 1, 'a'");

        // wildcard from nested row
        assertThat(assertions.query("SELECT a.b.* FROM (VALUES (ROW (ROW (1, 2, 3)))) A (b)")).matches("SELECT 1, 2, 3");
        assertThat(assertions.query("SELECT b[1][1].* FROM (VALUES (ROW (ROW (ROW ( ROW (1, 2, 3)))))) A (b)")).matches("SELECT 1, 2, 3");

        // mixed select items
        assertThat(assertions.query("SELECT (1, 2).*, 3")).matches("SELECT 1, 2, 3");
        assertThat(assertions.query("SELECT *, (x, 2*x).* AS (a, b), 3*x AS c FROM (VALUES 1) t(x)")).matches("SELECT 1, 1 a, 2 b, 3 c");

        // multiple wildcards
        assertThat(assertions.query("SELECT ROW(a, b).*, ROW(b, c).*, ROW(a, c).* FROM (VALUES (1, 2, 3)) t(a, b, c)")).matches("VALUES (1, 2, 2, 3, 1, 3)");

        // non-deterministic expression precomputed
        MaterializedResult materializedResult = assertions.execute("SELECT (x, x, x, x, x, x, x, x).* FROM (SELECT rand()) T(x)");
        long distinctValuesCount = materializedResult.getMaterializedRows().get(0).getFields().stream().distinct().count();
        assertEquals(1, distinctValuesCount, "rand() must be computed once only");

        // non-deterministic subquery
        MaterializedResult materializedResult1 = assertions.execute("SELECT (SELECT (rand(), rand(), rand(), rand())).*");
        long distinctValuesCount1 = materializedResult1.getMaterializedRows().get(0).getFields().stream().distinct().count();
        assertTrue(distinctValuesCount1 >= 3, "rand() must be computed multiple times");

        assertThat(assertions.query("SELECT 1, (2, 3).*")).matches("SELECT 1, 2, 3");
    }

    @Test
    public void testSelectAllFromTable()
    {
        // qualified name from alias, column aliases
        assertThat(assertions.query("SELECT a, b, c FROM (SELECT T.* FROM (VALUES (1, 2, 3)) T (a, b, c))")).matches("SELECT 1 a, 2 b, 3 c");

        // qualified name from alias, column aliases
        assertThat(assertions.query("SELECT a, b, c FROM (SELECT T.* AS (a, b, c) FROM (VALUES (1, 2, 3)) T (x, y, z))")).matches("SELECT 1 a, 2 b, 3 c");
    }

    @Test
    public void testSelectAllWithOrderBy()
    {
        // order by row field
        assertThat(assertions.query(
                "SELECT t.r.* FROM (VALUES ROW(ROW(1, 'a', true)), ROW(ROW(2, 'b', false))) t(r) ORDER BY 2 DESC"))
                .ordered()
                .matches("VALUES (2, 'b', false), (1, 'a', true)");
        assertThat(assertions.query(
                "SELECT t.r.* AS (x, y, z) FROM (VALUES ROW(ROW(1, 'a', true)), ROW(ROW(2, 'b', false))) t(r) ORDER BY y DESC"))
                .ordered()
                .matches("VALUES (2, 'b', false), (1, 'a', true)");

        // order by row field, named row fields
        assertThat(assertions.query(
                "SELECT t.r.* FROM (VALUES ROW(CAST(ROW(1) AS ROW(f1 integer))), ROW(CAST(ROW(2) AS ROW(f1 integer)))) t(r) ORDER BY 1 DESC"))
                .ordered()
                .matches("VALUES 2, 1");
        assertThat(assertions.query(
                "SELECT t.r.* FROM (VALUES ROW(CAST(ROW(1) AS ROW(f1 integer))), ROW(CAST(ROW(2) AS ROW(f1 integer)))) t(r) ORDER BY f1 DESC"))
                .ordered()
                .matches("VALUES 2, 1");
        assertThatThrownBy(() -> assertions.query(
                "SELECT t.r.* FROM (VALUES ROW(CAST(ROW(1) AS ROW(f1 integer))), ROW(CAST(ROW(2) AS ROW(f2 integer)))) t(r) ORDER BY f1 DESC"))
                .hasMessageMatching(".*Column 'f1' cannot be resolved");
        assertThat(assertions.query(
                "SELECT t.r.* AS (f1) FROM (VALUES ROW(CAST(ROW(1) AS ROW(f1 integer))), ROW(CAST(ROW(2) AS ROW(f2 integer)))) t(r) ORDER BY f1 DESC"))
                .ordered()
                .matches("VALUES 2, 1");
        assertThatThrownBy(() -> assertions.query(
                "SELECT t.r.* AS (x) FROM (VALUES ROW(CAST(ROW(1) AS ROW(f1 bigint))), ROW(CAST(ROW(2) AS ROW(f1 bigint)))) t(r) ORDER BY f1 DESC"))
                .hasMessageMatching(".*Column 'f1' cannot be resolved");

        // order by row
        assertThat(assertions.query(
                "SELECT t.r.* AS (x, y, z) FROM (VALUES ROW(ROW(1, 'a', true)), ROW(ROW(2, 'b', false))) t(r) ORDER BY r DESC"))
                .ordered()
                .matches("VALUES (2, 'b', false), (1, 'a', true)");

        // mixed wildcards
        assertThat(assertions.query(
                "SELECT *, t.r.* AS (x, y) FROM (VALUES ROW(ROW('a', true)), ROW(ROW('b', false))) t(r) ORDER BY y"))
                .ordered()
                .matches("VALUES (ROW('b', false), 'b', false), (ROW('a', true), 'a', true)");
    }

    @Test
    public void testSelectAllWithGroupBy()
    {
        assertThat(assertions.query(
                "SELECT t.r.* FROM (VALUES ROW(ROW(1, 'a', true)), ROW(ROW(1, 'a', true))) t(r) GROUP BY r"))
                .matches("VALUES (1, 'a', true)");
        assertThat(assertions.query(
                "SELECT t.r.* FROM (VALUES ROW(ROW('a', true)), ROW(ROW('a', true)), ROW(ROW('a', false)), ROW(ROW('b', true))) t(r) GROUP BY r"))
                .matches("VALUES ('a', true), ('a', false), ('b', true)");
    }

    @Test
    public void testSelectAllWithAggregationAndOrderBy()
    {
        assertThat(assertions.query(
                "SELECT t.r.* FROM (VALUES ROW(ROW(1, 'a', true)), ROW(ROW(2, 'b', false))) t(r) GROUP BY r ORDER BY r DESC"))
                .ordered()
                .matches("VALUES (2, 'b', false), (1, 'a', true)");
        assertThat(assertions.query(
                "SELECT r, x, count(x), t.r.* FROM (VALUES (ROW(1), 'a'), (ROW(2), 'b'), (ROW(1), 'a'), (ROW(1), 'b')) t(r, x) GROUP BY r, x ORDER BY r, x DESC"))
                .ordered()
                .matches("VALUES (ROW(1), 'b', BIGINT '1', 1), (ROW(1), 'a', 2, 1), (ROW(2), 'b', 1, 2)");
        assertThat(assertions.query(
                "SELECT array_agg(x), t.r.* FROM (VALUES (ROW(1), 'a'), (ROW(2), 'b'), (ROW(1), 'a'), (ROW(1), 'b')) t(r, x) GROUP BY r, x ORDER BY r, x DESC"))
                .ordered()
                .matches("VALUES (ARRAY['b'], 1), (ARRAY['a', 'a'], 1), (ARRAY['b'], 2)");
        assertThat(assertions.query(
                "SELECT array_agg(x), t.r.* FROM (VALUES (ROW(1), 'a'), (ROW(2), 'b'), (ROW(1), 'a'), (ROW(1), 'b')) t(r, x) GROUP BY r ORDER BY r DESC"))
                .ordered()
                .matches("VALUES (ARRAY['b'], 2), (ARRAY['a', 'a', 'b'], 1)");
        assertThat(assertions.query(
                "SELECT array_agg(x), t.r.* FROM (VALUES (ROW(1, true), 'a'), (ROW(2, false), 'b'), (ROW(1, true), 'c')) t(r, x) GROUP BY r ORDER BY r[2]"))
                .ordered()
                .matches("VALUES (ARRAY['b'], 2, false), (ARRAY['a', 'c'], 1, true)");
        assertThat(assertions.query(
                "SELECT array_agg(r[2]), t.r.* FROM (VALUES ROW(ROW(1, true)), ROW(ROW(2, false)), ROW(ROW(1, true))) t(r) GROUP BY r ORDER BY r[2]"))
                .ordered()
                .matches("VALUES (ARRAY[false], 2, false), (ARRAY[true, true], 1, true)");
    }

    @Test
    public void testSelectAllFromOuterScopeTable()
    {
        // scalar subquery
        assertThat(assertions.query("SELECT (SELECT t.*) FROM (VALUES 1, 2) t(a)")).matches("VALUES 1, 2");
        assertThat(assertions.query("SELECT (SELECT t.* FROM (SELECT 'a', 'b')) FROM (VALUES 1, 2) t(a)")).matches("VALUES 1, 2");
        assertThat(assertions.query("SELECT (SELECT t.* FROM (VALUES 0)) FROM (VALUES 1, 2) t(a)")).matches("VALUES 1, 2");
        assertThat(assertions.query("SELECT (SELECT t.* FROM (VALUES 0)) FROM (SELECT * FROM (VALUES 1, 1, 1) LIMIT 2) t(a)")).matches("VALUES 1, 1");
        assertThat(assertions.query(
                "SELECT (SELECT t.* FROM (VALUES 0)) FROM (VALUES (1, 1), (2, 2)) t(a, b)"))
                .matches("VALUES " +
                        "CAST(ROW(ROW(1, 1)) AS row(row(a integer, b integer))), " +
                        "CAST(ROW(ROW(2, 2)) AS row(row(a integer, b integer)))");
        // the following query should fail due to multiple rows returned from subquery, but instead fails to decorrelate
        assertThatThrownBy(() -> assertions.query("SELECT (SELECT t.* FROM (VALUES 0, 1)) FROM (VALUES 2) t(a)")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);
        // filter in subquery
        assertThat(assertions.query("SELECT (SELECT t.* FROM (VALUES 0) WHERE true) FROM (VALUES 1) t(a)")).matches("VALUES 1");
        assertThat(assertions.query("SELECT (SELECT t.* FROM (VALUES 0) WHERE 0 = 0) FROM (VALUES 1) t(a)")).matches("VALUES 1");
        assertThatThrownBy(() -> assertions.query("SELECT (SELECT t.* FROM (VALUES 0) t2(b) WHERE b > 1) FROM (VALUES 1) t(a)")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);
        assertThatThrownBy(() -> assertions.query("SELECT (SELECT t.* FROM (VALUES 0) WHERE false) FROM (VALUES 1) t(a)")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);
        // limit in subquery
        assertThat(assertions.query("SELECT (SELECT t.* FROM (VALUES 0) LIMIT 1) FROM (VALUES 1, 2) t(a)")).matches("VALUES 1, 2");
        assertThat(assertions.query("SELECT (SELECT t.* FROM (VALUES 0) LIMIT 5) FROM (VALUES 1, 2) t(a)")).matches("VALUES 1, 2");
        assertThat(assertions.query("SELECT (SELECT t.* FROM (VALUES 0) LIMIT 0) FROM (VALUES 1, 2) t(a)")).matches("VALUES CAST(NULL AS INTEGER), CAST(NULL AS INTEGER)");
        assertThatThrownBy(() -> assertions.query("SELECT (SELECT t.* FROM (VALUES 0, 1) LIMIT 1) FROM (VALUES 2, 3) t(a)")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);
        assertThatThrownBy(() -> assertions.query("SELECT (SELECT t.* FROM (SELECT * FROM (VALUES 0, 1) LIMIT 1)) FROM (VALUES 2, 3) t(a)")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);
        // alias shadowing
        assertThat(assertions.query("SELECT (SELECT t.* FROM (VALUES 0) t) FROM (VALUES 1) t(a)")).matches("VALUES 0");
        assertThat(assertions.query("SELECT(SELECT(SELECT t.* FROM (VALUES 0)) FROM (VALUES 1) t(a)) FROM (VALUES 2) t(a)")).matches("VALUES 1");

        // EXISTS subquery
        assertThat(assertions.query("SELECT EXISTS(SELECT t.* FROM (VALUES 0)) FROM (VALUES 1, 2) t(a)")).matches("VALUES true, true");
        assertThat(assertions.query("SELECT EXISTS(SELECT t.* FROM (VALUES 1) t2(b) WHERE t2.b > t.a) FROM (VALUES 0, 2) t(a)")).matches("VALUES true, false");

        // IN subquery
        assertThatThrownBy(() -> assertions.query("SELECT 1 IN (SELECT t.*) FROM (VALUES 1, 2) t(a)")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);

        // lateral relation
        assertThat(assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.*)")).matches("VALUES (0, 0), (1, 1)");
        assertThat(assertions.query("SELECT t.a, t2.d FROM (VALUES (0, 1), (2, 3)) t(a, b), LATERAL (SELECT t.*) t2(c, d)")).matches("VALUES (0, 1), (2, 3)");
        // limit in lateral relation
        assertThat(assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.* LIMIT 5)")).matches("VALUES (0, 0), (1, 1)");
        assertThatThrownBy(() -> assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.* LIMIT 0)")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);
        // filter in lateral relation
        assertThat(assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.* WHERE true)")).matches("VALUES (0, 0), (1, 1)");
        assertThat(assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.* WHERE 0 = 0)")).matches("VALUES (0, 0), (1, 1)");
        assertThatThrownBy(() -> assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.* WHERE false)")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);
        assertThatThrownBy(() -> assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.* WHERE t.a = 0)")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);
        // FROM in lateral relation
        assertThat(assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.* FROM (VALUES 1))")).matches("VALUES (0, 0), (1, 1)");
        assertThat(assertions.query("SELECT t.* FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.*) t")).matches("VALUES (0, 0), (1, 1)");
        assertThatThrownBy(() -> assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.* FROM (VALUES 1, 2))")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);
        assertThatThrownBy(() -> assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.* FROM (VALUES 1, 2) LIMIT 1)")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);
        assertThatThrownBy(() -> assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t.* FROM (SELECT * FROM (VALUES 1, 2) LIMIT 1))")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);

        // reference to further outer scope relation
        assertThat(assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t2.* from (VALUES 3, 4) t2(b), LATERAL (SELECT t.*))")).matches("VALUES (0, 3), (1, 3), (0, 4), (1, 4)");
        assertThat(assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t2.* from (VALUES 2), LATERAL (SELECT t.*) t2(b))")).matches("VALUES (0, 0), (1, 1)");
        assertThat(assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT t2.b from (VALUES 2), LATERAL (SELECT t.*) t2(b))")).matches("VALUES (0, 0), (1, 1)");
        assertThatThrownBy(() -> assertions.query("SELECT * FROM (VALUES 0) t(a), LATERAL (SELECT t2.* from (VALUES 1, 2), LATERAL (SELECT t.*) t2(b))")).hasMessageMatching(UNSUPPORTED_DECORRELATION_MESSAGE);
        assertThat(assertions.query("SELECT * FROM (VALUES 0, 1) t(a), LATERAL (SELECT * from (VALUES 2), LATERAL (SELECT t.*))")).matches("VALUES (0, 2, 0), (1, 2, 1)");
    }
}
