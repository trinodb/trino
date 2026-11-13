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

import static io.trino.spi.StandardErrorCode.COLLATION_MISMATCH;
import static io.trino.spi.StandardErrorCode.COLLATION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.collation.CollationRegistry.collationRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestCollateExpression
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testShowCollations()
    {
        assertThat(assertions.query("SHOW COLLATIONS"))
                .ordered()
                .skippingTypesCheck()
                .matches("VALUES ('ucs_basic', 'NO PAD'), ('unicode', 'NO PAD'), ('unicode_ci', 'NO PAD')");
        assertThat(assertions.query("SHOW COLLATIONS LIKE 'unicode\\_%' ESCAPE '\\'"))
                .skippingTypesCheck()
                .matches("VALUES ('unicode_ci', 'NO PAD')");
        assertThat(assertions.query("SHOW COLLATIONS LIKE 'ucs%'"))
                .skippingTypesCheck()
                .matches("VALUES ('ucs_basic', 'NO PAD')");
    }

    @Test
    public void testCollate()
    {
        assertThat(assertions.expression("a COLLATE unicode_ci")
                .binding("a", "'Alice'"))
                .hasType(createVarcharType(5, collationRegistry().getCollation("unicode_ci")))
                .isEqualTo("Alice");
        assertThat(assertions.query("SELECT 'Alice' COLLATE unicode_ci = 'ALICE'"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT 'Alice' = 'ALICE'"))
                .matches("VALUES false");
        // COLLATE binds tighter than concatenation, which keeps the operand's collation
        assertThat(assertions.query("SELECT COLLATION FOR ('a' || 'b' COLLATE unicode)"))
                .matches("VALUES varchar 'unicode'");
        assertThat(assertions.query("SELECT COLLATION FOR ('a' || 'b')"))
                .matches("VALUES varchar 'ucs_basic'");
        assertThat(assertions.query("SELECT NULL COLLATE unicode"))
                .matches("VALUES CAST(NULL AS varchar COLLATE unicode)");

        assertThat(assertions.query("SELECT 'a' COLLATE no_such_collation"))
                .failure().hasErrorCode(COLLATION_NOT_FOUND);
        // a collation name is an identifier: unquoted it folds to lower case, quoted it is taken as written
        assertThat(assertions.query("SELECT COLLATION FOR ('a' COLLATE UNICODE_CI)"))
                .matches("VALUES varchar 'unicode_ci'");
        assertThat(assertions.query("SELECT COLLATION FOR ('a' COLLATE \"unicode_ci\")"))
                .matches("VALUES varchar 'unicode_ci'");
        assertThat(assertions.query("SELECT 'a' COLLATE \"UNICODE_CI\""))
                .failure().hasErrorCode(COLLATION_NOT_FOUND);
        assertThat(assertions.query("SELECT 1 COLLATE unicode"))
                .failure().hasErrorCode(TYPE_MISMATCH);
    }

    @Test
    public void testLiteralsYield()
    {
        // a literal takes the collation of the column it is compared with
        assertThat(assertions.query(
                """
                SELECT count(*) FROM (VALUES CAST('Alice' AS varchar COLLATE unicode_ci), CAST('ALICE' AS varchar COLLATE unicode_ci), CAST('Bob' AS varchar COLLATE unicode_ci)) t(name)
                WHERE name = 'alice'
                """))
                .matches("VALUES BIGINT '2'");
        assertThat(assertions.query(
                """
                SELECT count(*) FROM (VALUES CAST('Alice' AS varchar COLLATE unicode_ci), CAST('Bob' AS varchar COLLATE unicode_ci)) t(name)
                WHERE name IN ('ALICE', 'carol')
                """))
                .matches("VALUES BIGINT '1'");
        assertThat(assertions.query(
                """
                SELECT count(*) FROM (VALUES CAST('Müller' AS varchar COLLATE unicode), CAST('Mutter' AS varchar COLLATE unicode)) t(name)
                WHERE name BETWEEN 'm' AND 'mv'
                """))
                .matches("VALUES BIGINT '2'");
    }

    @Test
    public void testDetermination()
    {
        // an explicit collation decides between implicit ones
        assertThat(assertions.query(
                "SELECT CAST('a' AS varchar COLLATE unicode) = CAST('A' AS varchar COLLATE unicode_ci) COLLATE unicode_ci"))
                .matches("VALUES true");
        assertThat(assertions.query(
                "SELECT 'a' COLLATE unicode = 'a' COLLATE unicode_ci"))
                .failure().hasErrorCode(COLLATION_MISMATCH).hasMessageContaining("Explicit collations [unicode, unicode_ci] conflict");
        // every comparison form applies collation determination
        for (String comparison : new String[] {"<", "<=", ">", ">=", "<>", "IS DISTINCT FROM", "IS NOT DISTINCT FROM"}) {
            assertThat(assertions.query(
                    "SELECT CAST('a' AS varchar COLLATE unicode) %s CAST('a' AS varchar COLLATE unicode_ci)".formatted(comparison)))
                    .failure().hasErrorCode(COLLATION_MISMATCH);
        }
        assertThat(assertions.query(
                "SELECT CAST('a' AS varchar COLLATE unicode) IN (CAST('a' AS varchar COLLATE unicode_ci))"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        assertThat(assertions.query(
                "SELECT CAST('a' AS varchar COLLATE unicode) BETWEEN CAST('a' AS varchar COLLATE unicode_ci) AND 'z'"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        assertThat(assertions.query(
                "SELECT nullif(CAST('a' AS varchar COLLATE unicode), CAST('a' AS varchar COLLATE unicode_ci))"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        assertThat(assertions.query("SELECT 'A' COLLATE unicode_ci IS DISTINCT FROM 'a'"))
                .matches("VALUES false");
        assertThat(assertions.query("SELECT nullif('A' COLLATE unicode_ci, 'a')"))
                .matches("VALUES CAST(NULL AS varchar(1) COLLATE unicode_ci)");
        assertThat(assertions.query("SELECT nullif(CAST('A' AS varchar), 'a' COLLATE unicode_ci)"))
                .matches("VALUES CAST(NULL AS varchar)");
        // a column of the default collation is implicit too, so it conflicts with a collated one
        assertThat(assertions.query(
                "SELECT CAST('a' AS varchar COLLATE unicode) = CAST('a' AS varchar)"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
    }

    @Test
    public void testCombination()
    {
        // combining different implicit collations is valid until the result is compared
        assertThat(assertions.query(
                "SELECT CASE WHEN true THEN CAST('a' AS varchar COLLATE unicode) ELSE CAST('b' AS varchar COLLATE unicode_ci) END"))
                .matches("VALUES varchar 'a'");
        assertThat(assertions.query(
                "SELECT CASE WHEN true THEN CAST('a' AS varchar COLLATE unicode) ELSE CAST('b' AS varchar COLLATE unicode_ci) END = 'a'"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        assertThat(assertions.query(
                "SELECT COLLATION FOR (CASE WHEN true THEN CAST('a' AS varchar COLLATE unicode) ELSE CAST('b' AS varchar COLLATE unicode_ci) END)"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        // a literal branch takes the column's collation
        assertThat(assertions.query(
                "SELECT COLLATION FOR (coalesce(CAST(NULL AS varchar COLLATE unicode), 'x'))"))
                .matches("VALUES varchar 'unicode'");
        assertThat(assertions.query(
                "SELECT COLLATION FOR (CASE WHEN true THEN CAST('a' AS varchar COLLATE unicode) ELSE 'b' END)"))
                .matches("VALUES varchar 'unicode'");
        // explicit collations in different branches conflict
        assertThat(assertions.query(
                "SELECT CASE WHEN true THEN 'a' COLLATE unicode ELSE 'b' COLLATE unicode_ci END"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
    }

    @Test
    public void testFunctionResults()
    {
        // string functions keep the collation of their arguments
        assertThat(assertions.query("SELECT COLLATION FOR (upper('a' COLLATE unicode))"))
                .matches("VALUES varchar 'unicode'");
        assertThat(assertions.query("SELECT COLLATION FOR (substr(CAST('abc' AS varchar COLLATE unicode_ci), 2))"))
                .matches("VALUES varchar 'unicode_ci'");
        assertThat(assertions.query("SELECT upper('ä' COLLATE unicode) = 'Ä'"))
                .matches("VALUES true");
        // generic functions carry the collation through their type variable
        assertThat(assertions.query("SELECT COLLATION FOR (max(x)) FROM (VALUES CAST('a' AS varchar COLLATE unicode)) t(x)"))
                .matches("VALUES varchar 'unicode'");
        assertThat(assertions.query("SELECT COLLATION FOR (element_at(ARRAY['a' COLLATE unicode], 1))"))
                .matches("VALUES varchar 'unicode'");
        // a function of uncollated arguments keeps the default collation
        assertThat(assertions.query("SELECT COLLATION FOR (upper('a'))"))
                .matches("VALUES varchar 'ucs_basic'");
        // concatenating different implicit collations has no collation
        assertThat(assertions.query(
                "SELECT CAST('a' AS varchar COLLATE unicode) || CAST('b' AS varchar COLLATE unicode_ci) = 'ab'"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        // a function declared over default-collation strings nested in an array accepts collated ones
        assertThat(assertions.query("SELECT concat_ws(',', ARRAY['a' COLLATE unicode_ci, 'b'])"))
                .matches("VALUES varchar 'a,b'");
        assertThat(assertions.query("SELECT array_join(ARRAY['a' COLLATE unicode_ci, 'b'], '-')"))
                .matches("VALUES varchar 'a-b'");
    }

    @Test
    public void testCollatedStringsAndChar()
    {
        // a char has no collation, so comparing it with a collated string would compare code
        // points; every comparison of the two is an error instead
        for (String query : new String[] {
                "SELECT 'a' COLLATE unicode_ci = CAST('A' AS char(1))",
                "SELECT 'a' COLLATE unicode_ci < CAST('B' AS char(1))",
                "SELECT 'a' COLLATE unicode_ci BETWEEN CAST('A' AS char(1)) AND CAST('B' AS char(1))",
                "SELECT 'a' COLLATE unicode_ci IN (CAST('A' AS char(1)))",
                "SELECT greatest('a' COLLATE unicode_ci, CAST('B' AS char(1)))",
                "SELECT contains(ARRAY['a' COLLATE unicode_ci], CAST('A' AS char(1)))",
                "SELECT ROW('a' COLLATE unicode_ci) = ROW(CAST('A' AS char(1)))",
                "SELECT x = CAST('A' AS char(1)) FROM (VALUES CAST('a' AS varchar COLLATE unicode_ci)) t(x)",
        }) {
            assertThat(assertions.query(query)).as(query)
                    .failure().hasErrorCode(COLLATION_MISMATCH);
        }
        // a char cast to varchar compares under the collation
        assertThat(assertions.query("SELECT 'a' COLLATE unicode_ci = CAST(CAST('A' AS char(1)) AS varchar)"))
                .matches("VALUES true");
        // a char and an uncollated string compare as before
        assertThat(assertions.query("SELECT CAST('a' AS varchar) = CAST('a' AS char(1))"))
                .matches("VALUES true");
    }

    @Test
    public void testSetOperations()
    {
        // a literal column takes the collation of the corresponding column
        assertThat(assertions.query("SELECT COLLATION FOR (x) FROM (SELECT 'a' COLLATE unicode UNION ALL SELECT 'b') t(x)"))
                .matches("VALUES varchar 'unicode', varchar 'unicode'");
        assertThat(assertions.query("SELECT COLLATION FOR (x) FROM (SELECT 'b' UNION ALL SELECT CAST('a' AS varchar COLLATE unicode_ci)) t(x)"))
                .matches("VALUES varchar 'unicode_ci', varchar 'unicode_ci'");
        // and the column is compared under it
        assertThat(assertions.query("SELECT count(*) FROM (SELECT CAST('Alice' AS varchar COLLATE unicode_ci) UNION SELECT 'ALICE') t(x)"))
                .matches("VALUES BIGINT '1'");
        assertThat(assertions.query("SELECT count(*), COLLATION FOR (arbitrary(x)) FROM (SELECT 'Alice' COLLATE unicode_ci INTERSECT SELECT 'ALICE') t(x)"))
                .matches("VALUES (BIGINT '1', varchar 'unicode_ci')");
        // a literal stays coercible through a subquery
        assertThat(assertions.query("SELECT COLLATION FOR (x) FROM (SELECT * FROM (SELECT 'b') s(y) UNION ALL SELECT 'a' COLLATE unicode) t(x)"))
                .matches("VALUES varchar 'unicode', varchar 'unicode'");

        // different implicit collations combine without a collation, valid until compared
        String mixed = "(SELECT CAST('a' AS varchar COLLATE unicode) UNION ALL SELECT CAST('b' AS varchar COLLATE unicode_ci)) t(x)";
        assertThat(assertions.query("SELECT CAST(x AS varchar) FROM " + mixed))
                .matches("VALUES varchar 'a', varchar 'b'");
        assertThat(assertions.query("SELECT x FROM %s WHERE x = 'a'".formatted(mixed)))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        assertThat(assertions.query("SELECT x FROM %s ORDER BY x".formatted(mixed)))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        assertThat(assertions.query("SELECT x COLLATE unicode FROM %s ORDER BY 1".formatted(mixed)))
                .ordered()
                .matches("VALUES CAST('a' AS varchar COLLATE unicode), CAST('b' AS varchar COLLATE unicode)");
        // operations that compare rows need a collation
        for (String operation : new String[] {"UNION", "UNION DISTINCT", "INTERSECT", "EXCEPT"}) {
            assertThat(assertions.query("SELECT CAST('a' AS varchar COLLATE unicode) %s SELECT CAST('b' AS varchar COLLATE unicode_ci)".formatted(operation)))
                    .failure().hasErrorCode(COLLATION_MISMATCH);
        }
        // as relation columns, explicitly collated values are implicit, so they combine without a collation
        assertThat(assertions.query("SELECT CAST(x AS varchar) FROM (SELECT 'a' COLLATE unicode UNION ALL SELECT 'b' COLLATE unicode_ci) t(x)"))
                .matches("VALUES varchar 'a', varchar 'b'");
        assertThat(assertions.query("SELECT x FROM (SELECT 'a' COLLATE unicode UNION ALL SELECT 'b' COLLATE unicode_ci) t(x) WHERE x = 'a'"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
    }

    @Test
    public void testRelationsKeepDerivations()
    {
        // a literal column stays coercible through a CTE, a join, and a lateral relation
        assertThat(assertions.query("WITH c(y) AS (SELECT 'b') SELECT COLLATION FOR (x) FROM (SELECT y FROM c UNION ALL SELECT 'a' COLLATE unicode) t(x)"))
                .matches("VALUES varchar 'unicode', varchar 'unicode'");
        assertThat(assertions.query("SELECT COLLATION FOR (x) FROM (SELECT l.y FROM (SELECT 'b') l(y) CROSS JOIN (SELECT 1) r(z) UNION ALL SELECT 'a' COLLATE unicode) t(x)"))
                .matches("VALUES varchar 'unicode', varchar 'unicode'");
        // a column alias list of a WITH query keeps the derivation of each column
        assertThat(assertions.query("WITH c(y) AS (SELECT 'b'), d AS (SELECT 'e' z) SELECT COLLATION FOR (x) FROM (SELECT y FROM c UNION ALL SELECT z FROM d UNION ALL SELECT 'a' COLLATE unicode) t(x)"))
                .matches("VALUES varchar 'unicode', varchar 'unicode', varchar 'unicode'");
        // a collated column keeps its collation through them
        assertThat(assertions.query("WITH c(y) AS (SELECT 'b' COLLATE unicode) SELECT COLLATION FOR (y), y = 'B' FROM c"))
                .matches("VALUES (varchar 'unicode', false)");
        assertThat(assertions.query("WITH c(y) AS (SELECT 'b' COLLATE unicode_ci) SELECT count(*) FROM c JOIN (VALUES 'B') v(z) ON c.y = v.z"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testJoinsAndSubqueries()
    {
        // join criteria compare columns of the joined relations under the determined collation
        assertThat(assertions.query("SELECT count(*) FROM (SELECT CAST('a' AS varchar COLLATE unicode_ci) x) l JOIN (SELECT 'A' y) r ON l.x = r.y"))
                .matches("VALUES BIGINT '1'");
        assertThat(assertions.query("SELECT count(*) FROM (SELECT CAST('a' AS varchar COLLATE unicode) x) l JOIN (SELECT CAST('a' AS varchar COLLATE unicode_ci) y) r ON l.x = r.y"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        assertThat(assertions.query("SELECT 'ALICE' COLLATE unicode_ci IN (SELECT 'alice' COLLATE unicode_ci)"))
                .matches("VALUES true");
        // USING compares the key columns under the determined collation
        assertThat(assertions.query("SELECT k FROM (SELECT CAST('A' AS varchar COLLATE unicode_ci) k) l JOIN (SELECT 'a' k) r USING (k)"))
                .matches("VALUES CAST('A' AS varchar COLLATE unicode_ci)");
        assertThat(assertions.query("SELECT COLLATION FOR (k) FROM (SELECT CAST('A' AS varchar COLLATE unicode_ci) k) l JOIN (SELECT 'a' k) r USING (k)"))
                .matches("VALUES varchar 'unicode_ci'");
        assertThat(assertions.query("SELECT * FROM (SELECT CAST('a' AS varchar COLLATE unicode) k) l JOIN (SELECT CAST('a' AS varchar COLLATE unicode_ci) k) r USING (k)"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        // a scalar subquery has the derivation of its column, so a literal yields
        assertThat(assertions.query("SELECT CAST('X' AS varchar COLLATE unicode_ci) = (SELECT 'x')"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT (SELECT 'A') = CAST('a' AS varchar COLLATE unicode_ci)"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT (SELECT CASE WHEN true THEN CAST('a' AS varchar COLLATE unicode) ELSE CAST('b' AS varchar COLLATE unicode_ci) END) = 'A'"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
    }

    @Test
    public void testPivot()
    {
        // a pivot value is compared with the pivot column under the determined collation
        String input = "(SELECT CAST(k AS varchar COLLATE unicode_ci) k, v FROM (VALUES ('A', 1), ('a', 2), ('b', 3)) t(k, v))";
        assertThat(assertions.query("SELECT * FROM %s PIVOT (sum(v) FOR k IN ('a' AS a, 'B' AS b))".formatted(input)))
                .matches("VALUES (BIGINT '3', BIGINT '3')");
        assertThat(assertions.query("SELECT * FROM (SELECT CASE WHEN true THEN CAST('a' AS varchar COLLATE unicode) ELSE CAST('b' AS varchar COLLATE unicode_ci) END k, 1 v) PIVOT (sum(v) FOR k IN ('a' AS a))"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        // a string aggregate in a pivot keeps its argument's collation
        assertThat(assertions.query("SELECT x = 'A' FROM (SELECT CAST(k AS varchar COLLATE unicode_ci) k, v FROM (VALUES ('a', 'x')) t(k, v)) PIVOT (listagg(k, ',') WITHIN GROUP (ORDER BY k) FOR v IN ('x' AS x))"))
                .matches("VALUES true");
    }

    @Test
    public void testUnnest()
    {
        // an unnested element, field, key, or value has its collation
        String none = "(SELECT ARRAY[CASE WHEN rand() >= 0 THEN CAST('a' AS varchar COLLATE unicode) ELSE CAST('b' AS varchar COLLATE unicode_ci) END] a)";
        for (String query : new String[] {
                "SELECT x FROM %s CROSS JOIN UNNEST(a) t(x) ORDER BY x".formatted(none),
                "SELECT x FROM %s CROSS JOIN UNNEST(a) t(x) GROUP BY x".formatted(none),
                "SELECT x = 'A' FROM %s CROSS JOIN UNNEST(a) t(x)".formatted(none),
        }) {
            assertThat(assertions.query(query)).as(query)
                    .failure().hasErrorCode(COLLATION_MISMATCH);
        }
        assertThat(assertions.query("SELECT x = 'A' FROM UNNEST(ARRAY[CAST('a' AS varchar COLLATE unicode_ci)]) t(x)"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT COLLATION FOR (k), COLLATION FOR (v) FROM UNNEST(map(ARRAY[CAST('k' AS varchar COLLATE unicode_ci)], ARRAY[CAST('v' AS varchar COLLATE unicode)])) t(k, v)"))
                .matches("VALUES (varchar 'unicode_ci', varchar 'unicode')");
        assertThat(assertions.query("SELECT f = 'A' FROM UNNEST(ARRAY[ROW(CAST('a' AS varchar COLLATE unicode_ci))]) t(f)"))
                .matches("VALUES true");
    }

    @Test
    public void testCompositeColumns()
    {
        // a composite relation column keeps the derivations of its strings
        assertThat(assertions.query("SELECT r = ROW(CAST('A' AS varchar COLLATE unicode_ci)) FROM (SELECT ROW('a')) t(r)"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT r[1] = 'A' FROM (SELECT ROW(CASE WHEN true THEN CAST('a' AS varchar COLLATE unicode) ELSE CAST('b' AS varchar COLLATE unicode_ci) END)) t(r)"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        assertThat(assertions.query("SELECT ARRAY[CAST('a' AS varchar COLLATE unicode)] UNION SELECT ARRAY[CAST('b' AS varchar COLLATE unicode_ci)]"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        assertThat(assertions.query("SELECT ARRAY[CAST('a' AS varchar COLLATE unicode)] UNION ALL SELECT ARRAY[CAST('b' AS varchar COLLATE unicode_ci)]"))
                .matches("VALUES ARRAY[varchar 'a'], ARRAY[varchar 'b']");
        // SELECT DISTINCT * compares every column
        assertThat(assertions.query("SELECT DISTINCT * FROM (SELECT CASE WHEN true THEN CAST('a' AS varchar COLLATE unicode) ELSE CAST('b' AS varchar COLLATE unicode_ci) END x)"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        assertThat(assertions.query("SELECT count(*) FROM (SELECT DISTINCT * FROM (SELECT CAST('a' AS varchar COLLATE unicode_ci) x UNION ALL SELECT 'A'))"))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testValues()
    {
        assertThat(assertions.query("SELECT COLLATION FOR (x) FROM (VALUES 'a' COLLATE unicode, 'b') t(x)"))
                .matches("VALUES varchar 'unicode', varchar 'unicode'");
        assertThat(assertions.query("SELECT COLLATION FOR (y) FROM (VALUES (1, 'a'), (2, 'b' COLLATE unicode)) t(x, y)"))
                .matches("VALUES varchar 'unicode', varchar 'unicode'");
        assertThat(assertions.query("SELECT count(*) FROM (VALUES CAST('Alice' AS varchar COLLATE unicode_ci), 'ALICE') t(x) WHERE x = 'alice'"))
                .matches("VALUES BIGINT '2'");
        // an uncollated VALUES column of literals stays coercible
        assertThat(assertions.query("SELECT COLLATION FOR (x) FROM (SELECT x FROM (VALUES 'a', 'b') v(x) UNION ALL SELECT 'c' COLLATE unicode) t(x)"))
                .matches("VALUES varchar 'unicode', varchar 'unicode', varchar 'unicode'");
        assertThat(assertions.query("SELECT x FROM (VALUES 'a' COLLATE unicode, 'b' COLLATE unicode_ci) t(x)"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        assertThat(assertions.query("SELECT x FROM (VALUES CAST('a' AS varchar COLLATE unicode), CAST('b' AS varchar COLLATE unicode_ci)) t(x) ORDER BY x"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
    }

    @Test
    public void testSpecialFormResults()
    {
        // forms with their own syntax keep their argument's collation like ordinary functions
        assertThat(assertions.query("SELECT COLLATION FOR (trim(' a ' COLLATE unicode))"))
                .matches("VALUES varchar 'unicode'");
        assertThat(assertions.query("SELECT COLLATION FOR (overlay('abc' COLLATE unicode PLACING 'x' FROM 1))"))
                .matches("VALUES varchar 'unicode'");
        assertThat(assertions.query("SELECT COLLATION FOR (('abc' COLLATE unicode).upper())"))
                .matches("VALUES varchar 'unicode'");
        // the result is compared under that collation, not by code point
        assertThat(assertions.query("SELECT trim(' A ' COLLATE unicode_ci) = 'a'"))
                .matches("VALUES true");
        // and keeps it as a relation column, at the top of a select list
        assertThat(assertions.query("SELECT COLLATION FOR (x) FROM (SELECT upper('a' COLLATE unicode) x)"))
                .matches("VALUES varchar 'unicode'");
        assertThat(assertions.query("SELECT x = 'a' FROM (SELECT upper('a' COLLATE unicode_ci) x)"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT x = 'a' FROM (SELECT ('A' COLLATE unicode_ci).lower() x)"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT count(*) FROM (SELECT trim(x COLLATE unicode_ci) k FROM (VALUES ' a', 'A ') t(x)) GROUP BY k"))
                .matches("VALUES BIGINT '2'");
    }

    private static final String NONE_ARRAY = "ARRAY[CAST('a' AS varchar COLLATE unicode), CAST('b' AS varchar COLLATE unicode_ci)]";
    private static final String NONE_VALUE = "CASE WHEN true THEN CAST('a' AS varchar COLLATE unicode) ELSE CAST('b' AS varchar COLLATE unicode_ci) END";

    @Test
    public void testCompositeValuesWithoutCollation()
    {
        // an array of strings of different implicit collations may be stored and returned, but
        // not compared: each element has derivation none
        assertThat(assertions.query("SELECT cardinality(%s)".formatted(NONE_ARRAY)))
                .matches("VALUES BIGINT '2'");
        assertThat(assertions.query("SELECT CAST(%s AS array(varchar))".formatted(NONE_ARRAY)))
                .matches("VALUES ARRAY[varchar 'a', varchar 'b']");
        for (String query : new String[] {
                "SELECT %s = ARRAY['A', 'B']".formatted(NONE_ARRAY),
                "SELECT contains(%s, 'A')".formatted(NONE_ARRAY),
                "SELECT %s[1] = 'A'".formatted(NONE_ARRAY),
                "SELECT element_at(%s, 1) = 'A'".formatted(NONE_ARRAY),
                "SELECT array_max(%s)".formatted(NONE_ARRAY),
                "SELECT array_sort(%s)".formatted(NONE_ARRAY),
                "SELECT array_distinct(%s)".formatted(NONE_ARRAY),
                "SELECT ROW(%s) = ROW('A')".formatted(NONE_VALUE),
                "SELECT ROW(1, %s) IN (ROW(1, 'A'))".formatted(NONE_VALUE),
                "SELECT ROW(%s)[1] = 'A'".formatted(NONE_VALUE),
                "SELECT x FROM (VALUES 1) t(x) ORDER BY ROW(%s)".formatted(NONE_VALUE),
                "SELECT count(*) FROM (VALUES 1) t(x) GROUP BY %s".formatted(NONE_ARRAY),
                "SELECT array_agg(ROW(%s))".formatted(NONE_VALUE),
                "SELECT greatest(CAST('B' AS varchar COLLATE unicode), CAST('a' AS varchar COLLATE unicode_ci))",
        }) {
            assertThat(assertions.query(query)).as(query)
                    .failure().hasErrorCode(COLLATION_MISMATCH);
        }
        // a cast gives every element the target type's collation
        assertThat(assertions.query("SELECT CAST(%s AS array(varchar COLLATE unicode_ci)) = ARRAY['A', 'B']".formatted(NONE_ARRAY)))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT CAST(ROW(%s) AS row(f varchar)).f = 'A'".formatted(NONE_VALUE)))
                .matches("VALUES false");
        // explicit collations conflict inside composites as at the top level
        assertThat(assertions.query("SELECT ROW('a' COLLATE unicode) = ROW('a' COLLATE unicode_ci)"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
    }

    @Test
    public void testLiteralsYieldInsideComposites()
    {
        // a literal string inside a row, array, or map takes the collation it is compared with
        String ci = "CAST('a' AS varchar COLLATE unicode_ci)";
        for (String query : new String[] {
                "SELECT ROW(1, %s) = ROW(1, 'A')".formatted(ci),
                "SELECT ROW(1, %s) IN (ROW(1, 'A'), ROW(2, 'B'))".formatted(ci),
                "SELECT ARRAY['A'] = ARRAY[%s]".formatted(ci),
                "SELECT contains(ARRAY[%s], 'A')".formatted(ci),
                "SELECT array_position(ARRAY[%s], 'A') = 1".formatted(ci),
                "SELECT map(ARRAY[1], ARRAY[%s]) = map(ARRAY[1], ARRAY['A'])".formatted(ci),
                "SELECT map(ARRAY[%s], ARRAY[1])['A'] = 1".formatted(ci),
                "SELECT greatest('A', %s) = 'a'".formatted(ci),
                "SELECT CASE %s WHEN 'A' THEN true ELSE false END".formatted(ci),
        }) {
            assertThat(assertions.query(query)).as(query)
                    .matches("VALUES true");
        }
        // a literal yields to an element's collation, so ordering uses the collation
        assertThat(assertions.query("SELECT greatest('B', 'a' COLLATE unicode_ci)"))
                .matches("VALUES CAST('B' AS varchar(1) COLLATE unicode_ci)");
    }

    @Test
    public void testFunctionTypeVariables()
    {
        // the strings bound to each type variable combine separately, so a map's keys and values
        // keep their own collations
        String map = "map(ARRAY[CAST('k' AS varchar COLLATE unicode_ci)], ARRAY[CAST('v' AS varchar COLLATE unicode)])";
        assertThat(assertions.query("SELECT element_at(%s, 'K') = 'v'".formatted(map)))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT COLLATION FOR (element_at(%s, 'K'))".formatted(map)))
                .matches("VALUES varchar 'unicode'");
        // an explicit key collation does not change the values' collation
        assertThat(assertions.query("SELECT COLLATION FOR (element_at(map(ARRAY['k'], ARRAY[CAST('v' AS varchar COLLATE unicode)]), 'K' COLLATE unicode_ci))"))
                .matches("VALUES varchar 'unicode'");
        // element_at compares its key with the map's keys, so keys without a collation fail
        assertThat(assertions.query("SELECT element_at(map(ARRAY[%s], ARRAY[1]), 'a')".formatted(NONE_VALUE)))
                .failure().hasErrorCode(COLLATION_MISMATCH);
        // NULLIF compares each string position
        assertThat(assertions.query("SELECT NULLIF(ROW(CAST('a' AS varchar COLLATE unicode_ci)), ROW('A')) IS NULL"))
                .matches("VALUES true");
    }

    @Test
    public void testLambdaArgumentsKeepCollations()
    {
        // a lambda receives the elements with their collations
        for (String query : new String[] {
                "SELECT any_match(%s, x -> x = 'A')".formatted(NONE_ARRAY),
                "SELECT transform(%s, x -> x = 'A')".formatted(NONE_ARRAY),
                "SELECT zip_with(%s, ARRAY['A', 'B'], (x, y) -> x = y)".formatted(NONE_ARRAY),
                "SELECT array_sort(%s, (x, y) -> IF(x < y, -1, 1))".formatted(NONE_ARRAY),
        }) {
            assertThat(assertions.query(query)).as(query)
                    .failure().hasErrorCode(COLLATION_MISMATCH);
        }
        assertThat(assertions.query("SELECT any_match(ARRAY[CAST('a' AS varchar COLLATE unicode_ci)], x -> x = 'A')"))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT filter(ARRAY[CAST('a' AS varchar COLLATE unicode_ci), CAST('b' AS varchar COLLATE unicode_ci)], x -> x = 'A')"))
                .matches("VALUES ARRAY[CAST('a' AS varchar COLLATE unicode_ci)]");
        assertThat(assertions.query("SELECT COLLATION FOR (transform(ARRAY['a' COLLATE unicode], x -> upper(x))[1])"))
                .matches("VALUES varchar 'unicode'");
    }

    @Test
    public void testElementsKeepCollations()
    {
        // an element or field selected from a composite has that element's collation
        assertThat(assertions.query("SELECT COLLATION FOR (ARRAY['a' COLLATE unicode][1])"))
                .matches("VALUES varchar 'unicode'");
        assertThat(assertions.query("SELECT COLLATION FOR (CAST(ROW('a') AS row(f varchar COLLATE unicode)).f)"))
                .matches("VALUES varchar 'unicode'");
        assertThat(assertions.query("SELECT COLLATION FOR (ROW(1, 'a' COLLATE unicode)[2])"))
                .matches("VALUES varchar 'unicode'");
        assertThat(assertions.query("SELECT COLLATION FOR (map(ARRAY[1], ARRAY['a' COLLATE unicode])[1])"))
                .matches("VALUES varchar 'unicode'");
        // the derivation of an element is kept: an explicit element conflicts with another explicit collation
        assertThat(assertions.query("SELECT ARRAY['a' COLLATE unicode][1] = 'a' COLLATE unicode_ci"))
                .failure().hasErrorCode(COLLATION_MISMATCH);
    }

    @Test
    public void testCollationBlindStringFunctions()
    {
        // functions that search strings compare code points, which a collation would not
        for (String query : new String[] {
                "SELECT strpos('ABC' COLLATE unicode_ci, 'b')",
                "SELECT starts_with('ABC' COLLATE unicode_ci, 'a')",
                "SELECT replace('ABC' COLLATE unicode_ci, 'b', 'x')",
                "SELECT regexp_like('ABC' COLLATE unicode_ci, 'b')",
                "SELECT split('A,B' COLLATE unicode_ci, ',')",
                "SELECT approx_set('a' COLLATE unicode_ci)",
                "SELECT url_extract_parameter('http://x/?NAME=1' COLLATE unicode_ci, 'name')",
                "SELECT json_array_contains('[\"A\"]', 'a' COLLATE unicode_ci)",
                "SELECT position('b' IN 'ABC' COLLATE unicode_ci)",
        }) {
            assertThat(assertions.query(query)).as(query)
                    .failure().hasErrorCode(NOT_SUPPORTED);
        }
        // a cast to the default collation compares code points
        assertThat(assertions.query("SELECT strpos(CAST('ABC' COLLATE unicode_ci AS varchar), 'B')"))
                .matches("VALUES BIGINT '2'");
        // functions that do not compare contents keep binding
        assertThat(assertions.query("SELECT length('ABC' COLLATE unicode_ci), upper('a' COLLATE unicode_ci) = 'A'"))
                .matches("VALUES (BIGINT '3', true)");
        assertThat(assertions.query("SELECT word_stem('running' COLLATE unicode_ci) = 'RUN'"))
                .matches("VALUES true");
    }

    @Test
    public void testAggregateResultsKeepCollations()
    {
        // a string aggregate or window function returns its argument's collation
        String values = "(SELECT CAST(v AS varchar COLLATE unicode_ci) x FROM (VALUES 'b', 'A', 'a') t(v))";
        assertThat(assertions.query("SELECT listagg(x, ',') WITHIN GROUP (ORDER BY x) = 'a,a,b' FROM %s".formatted(values)))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT DISTINCT listagg(x, ',') WITHIN GROUP (ORDER BY x) OVER () = 'A,A,B' FROM %s".formatted(values)))
                .matches("VALUES true");
        assertThat(assertions.query("SELECT max(x) = 'B', COLLATION FOR (max(x)) FROM %s".formatted(values)))
                .matches("VALUES (true, varchar 'unicode_ci')");
        assertThat(assertions.query("SELECT DISTINCT max(x) OVER () = 'B' FROM %s".formatted(values)))
                .matches("VALUES true");
    }

    @Test
    public void testUncollatedKeys()
    {
        String uncollated = "CASE WHEN x = 1 THEN CAST('a' AS varchar COLLATE unicode) ELSE CAST('b' AS varchar COLLATE unicode_ci) END";
        for (String query : new String[] {
                "SELECT x FROM (VALUES 1) t(x) ORDER BY %s",
                "SELECT count(*) FROM (VALUES 1) t(x) GROUP BY %s",
                "SELECT DISTINCT %s FROM (VALUES 1) t(x)",
                "SELECT row_number() OVER (PARTITION BY %s) FROM (VALUES 1) t(x)",
                "SELECT row_number() OVER (ORDER BY %s) FROM (VALUES 1) t(x)",
                "SELECT count(DISTINCT %s) FROM (VALUES 1) t(x)",
                "SELECT max(%s) FROM (VALUES 1) t(x)",
                "SELECT array_agg(x ORDER BY %s) FROM (VALUES 1) t(x)",
        }) {
            assertThat(assertions.query(query.formatted(uncollated)))
                    .failure().hasErrorCode(COLLATION_MISMATCH);
        }
        // an explicit collation makes the key orderable
        assertThat(assertions.query("SELECT x FROM (VALUES 1) t(x) ORDER BY (%s) COLLATE unicode".formatted(uncollated)))
                .matches("VALUES 1");
    }

    @Test
    public void testOrderAndGroup()
    {
        assertThat(assertions.query(
                "SELECT x FROM (VALUES 'z', 'ä', 'a') t(x) ORDER BY x"))
                .ordered()
                .matches("VALUES 'a', 'z', 'ä'");
        assertThat(assertions.query(
                "SELECT x FROM (VALUES 'z', 'ä', 'a') t(x) ORDER BY x COLLATE unicode"))
                .ordered()
                .matches("VALUES 'a', 'ä', 'z'");
        assertThat(assertions.query(
                "SELECT count(*) FROM (SELECT DISTINCT x COLLATE unicode_ci FROM (VALUES 'Alice', 'ALICE', 'Bob') t(x))"))
                .matches("VALUES BIGINT '2'");
        assertThat(assertions.query(
                "SELECT count(*) FROM (SELECT x COLLATE unicode_ci, count(*) FROM (VALUES 'Alice', 'ALICE', 'Bob') t(x) GROUP BY x COLLATE unicode_ci)"))
                .matches("VALUES BIGINT '2'");
    }
}
